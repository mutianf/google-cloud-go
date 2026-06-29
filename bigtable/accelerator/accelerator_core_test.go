// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package accelerator

import (
	"context"
	"errors"
	"testing"

	v2pb "cloud.google.com/go/bigtable/apiv2/bigtablepb"
	"cloud.google.com/go/bigtable/accelerator/resourcemanager"
	"cloud.google.com/go/bigtable/internal/session"
	btransport "cloud.google.com/go/bigtable/internal/transport"
	"go.opentelemetry.io/otel/metric"
	"google.golang.org/api/option"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// mockSessionTableApi is a session.SessionTableApi stub the tests hand back
// from mockSessionClient.OpenSessionTable.
type mockSessionTableApi struct {
	mutateRowFn func(ctx context.Context, req *v2pb.SessionMutateRowRequest) (*v2pb.SessionMutateRowResponse, error)
}

func (m *mockSessionTableApi) ReadRow(ctx context.Context, req *v2pb.SessionReadRowRequest) (*v2pb.SessionReadRowResponse, error) {
	return nil, status.Error(codes.Unimplemented, "mock ReadRow")
}

func (m *mockSessionTableApi) MutateRow(ctx context.Context, req *v2pb.SessionMutateRowRequest) (*v2pb.SessionMutateRowResponse, error) {
	if m.mutateRowFn != nil {
		return m.mutateRowFn(ctx, req)
	}
	return &v2pb.SessionMutateRowResponse{}, nil
}

func (m *mockSessionTableApi) Close() error { return nil }

// mockSessionClient hands back a fixed SessionTableApi for any table name.
type mockSessionClient struct {
	table          session.SessionTableApi
	lastTableName  string
	newTableCalled int
}

func (m *mockSessionClient) OpenSessionTable(name string) session.SessionTableApi {
	m.lastTableName = name
	m.newTableCalled++
	return m.table
}

func (m *mockSessionClient) OpenAuthorizedView(_, _ string) session.SessionTableApi {
	return m.table
}

func (m *mockSessionClient) OpenMaterializedView(_ string) session.SessionTableApi {
	return m.table
}

func (m *mockSessionClient) MeterProvider() metric.MeterProvider { return nil }

func (m *mockSessionClient) SessionDebug() btransport.SessionDebugProvider { return nil }
func (m *mockSessionClient) ChannelDebug() btransport.ChannelDebugProvider { return nil }
func (m *mockSessionClient) ConfigDebug() btransport.ConfigDebugProvider   { return nil }

func (m *mockSessionClient) AddSessionLoadListener(func(float64)) func() { return func() {} }

func (m *mockSessionClient) Close() error { return nil }

// stubSessionClient swaps the resourcemanager's SessionClient factory so
// resourcemanager.New returns a ResourceManager backed by the given mock.
// Restores via t.Cleanup. Must NOT be used with t.Parallel — the seam is
// package-level state.
func stubSessionClient(t *testing.T, sc *mockSessionClient) {
	t.Helper()
	restore := resourcemanager.TestHookSessionClient(func(
		_ context.Context,
		_, _, _ string,
		_ ...option.ClientOption,
	) (session.SessionClient, error) {
		return sc, nil
	})
	t.Cleanup(restore)
}

// newTestChannel is a convenience for tests: stubs the SessionClient factory
// and builds an AcceleratorChannel against the given mock.
func newTestChannel(t *testing.T, sc *mockSessionClient) *AcceleratorChannel {
	t.Helper()
	stubSessionClient(t, sc)
	channel, err := NewAcceleratorChannel(context.Background(), "p", "i", "ap")
	if err != nil {
		t.Fatalf("NewAcceleratorChannel error: %v", err)
	}
	return channel
}

func TestNewAcceleratorChannel_Constructs(t *testing.T) {
	channel := newTestChannel(t, &mockSessionClient{table: &mockSessionTableApi{}})
	if channel.rm == nil {
		t.Error("AcceleratorChannel.rm is nil after construction")
	}
}

func TestInvoke_MutateRow_DispatchesThroughSession(t *testing.T) {
	called := false
	tbl := &mockSessionTableApi{
		mutateRowFn: func(ctx context.Context, req *v2pb.SessionMutateRowRequest) (*v2pb.SessionMutateRowResponse, error) {
			called = true
			if got := string(req.Key); got != "k" {
				t.Errorf("MutateRow req.Key = %q; want %q", got, "k")
			}
			return &v2pb.SessionMutateRowResponse{}, nil
		},
	}
	sc := &mockSessionClient{table: tbl}
	channel := newTestChannel(t, sc)

	reqV2 := &v2pb.MutateRowRequest{
		TableName: "projects/p/instances/i/tables/t",
		RowKey:    []byte("k"),
	}
	if err := channel.Invoke(context.Background(),
		v2pb.Bigtable_MutateRow_FullMethodName,
		reqV2, &v2pb.MutateRowResponse{}); err != nil {
		t.Fatalf("Invoke(MutateRow) error: %v", err)
	}
	if !called {
		t.Error("Expected SessionTableApi.MutateRow to be called")
	}
	if sc.lastTableName != "t" {
		t.Errorf("Expected NewSessionTable called with leaf %q; got %q", "t", sc.lastTableName)
	}
}

func TestInvoke_MutateRow_CachesPerTableAndMethod(t *testing.T) {
	sc := &mockSessionClient{table: &mockSessionTableApi{}}
	channel := newTestChannel(t, sc)

	reqV2 := &v2pb.MutateRowRequest{
		TableName: "projects/p/instances/i/tables/t",
		RowKey:    []byte("k"),
	}
	for i := 0; i < 3; i++ {
		if err := channel.Invoke(context.Background(),
			v2pb.Bigtable_MutateRow_FullMethodName,
			reqV2, &v2pb.MutateRowResponse{}); err != nil {
			t.Fatalf("Invoke iter %d: %v", i, err)
		}
	}
	if sc.newTableCalled != 1 {
		t.Errorf("Expected NewSessionTable called once across 3 same-table MutateRow invokes; got %d", sc.newTableCalled)
	}
}

func TestInvoke_MutateRow_PropagatesSessionError(t *testing.T) {
	sentinel := errors.New("session boom")
	tbl := &mockSessionTableApi{
		mutateRowFn: func(ctx context.Context, req *v2pb.SessionMutateRowRequest) (*v2pb.SessionMutateRowResponse, error) {
			return nil, sentinel
		},
	}
	channel := newTestChannel(t, &mockSessionClient{table: tbl})

	reqV2 := &v2pb.MutateRowRequest{
		TableName: "projects/p/instances/i/tables/t",
		RowKey:    []byte("k"),
	}
	err := channel.Invoke(context.Background(),
		v2pb.Bigtable_MutateRow_FullMethodName,
		reqV2, &v2pb.MutateRowResponse{})
	if !errors.Is(err, sentinel) {
		t.Errorf("Invoke(MutateRow) err = %v; want %v", err, sentinel)
	}
}

func TestInvoke_UnknownMethod_ReturnsUnimplemented(t *testing.T) {
	channel := newTestChannel(t, &mockSessionClient{table: &mockSessionTableApi{}})

	err := channel.Invoke(context.Background(), "/google.bigtable.v2.Bigtable/SampleRowKeys", nil, nil)
	if status.Code(err) != codes.Unimplemented {
		t.Errorf("Invoke(unknown) code = %v; want Unimplemented", status.Code(err))
	}
}

func TestNewStream_ReadRows_ReturnsStream(t *testing.T) {
	channel := newTestChannel(t, &mockSessionClient{table: &mockSessionTableApi{}})

	stream, err := channel.NewStream(context.Background(), nil, v2pb.Bigtable_ReadRows_FullMethodName)
	if err != nil {
		t.Fatalf("NewStream(ReadRows) returned error: %v", err)
	}
	if stream == nil {
		t.Fatal("NewStream(ReadRows) returned nil stream")
	}
	if got := status.Code(stream.RecvMsg(nil)); got != codes.Unimplemented {
		t.Errorf("RecvMsg code = %v; want Unimplemented", got)
	}
}

func TestNewStream_UnknownMethod_ReturnsUnimplemented(t *testing.T) {
	channel := newTestChannel(t, &mockSessionClient{table: &mockSessionTableApi{}})

	_, err := channel.NewStream(context.Background(), nil, "/google.bigtable.v2.Bigtable/SampleRowKeys")
	if status.Code(err) != codes.Unimplemented {
		t.Errorf("NewStream(unknown) code = %v; want Unimplemented", status.Code(err))
	}
}
