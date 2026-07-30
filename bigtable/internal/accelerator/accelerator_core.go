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

// Package accelerator exposes an in-process grpc.ClientConnInterface that
// translates Bigtable V2 RPCs into proto-native session vRPCs handled by
// internal/session.
//
// AcceleratorChannel is scoped to one (project, instance, appProfile). It
// owns a ResourceManager (which in turn owns the underlying SessionClient
// and a per-(table, method) session-table cache). AcceleratorChannel itself
// holds no direct reference to the SessionClient — all dispatch flows
// through the ResourceManager. Close releases pools and the underlying
// connection via the ResourceManager.
package accelerator

import (
	"bytes"
	"context"
	"io"

	v2pb "cloud.google.com/go/bigtable/apiv2/bigtablepb"
	"cloud.google.com/go/bigtable/internal"
	"cloud.google.com/go/bigtable/internal/accelerator/adapters"
	"cloud.google.com/go/bigtable/internal/accelerator/metrics"
	"cloud.google.com/go/bigtable/internal/accelerator/resourcemanager"
	btopt "cloud.google.com/go/bigtable/internal/option"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	gmetadata "google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// Default data-plane dial parameters. These mirror the unexported
// bigtable.prodAddr / mtlsProdAddr / Scope / clientUserAgent constants; they
// are duplicated here (rather than imported) to keep the daemon from pulling
// in the full bigtable client package, matching how internal/session already
// duplicates its feature-flag metadata to avoid the same import cycle.
const (
	prodAddr     = "bigtable.UNIVERSE_DOMAIN:443"
	mtlsProdAddr = "bigtable.mtls.googleapis.com:443"
	dataScope    = "https://www.googleapis.com/auth/bigtable.data"
)

var userAgent = "cbt-go-accelerator/v" + internal.Version

// Ensure AcceleratorChannel implements grpc.ClientConnInterface.
var _ grpc.ClientConnInterface = (*AcceleratorChannel)(nil)

// AcceleratorChannel is an in-process grpc.ClientConnInterface backed by
// internal/session. It owns a ResourceManager that holds the SessionClient
// and caches per-(table, method) SessionTableApi instances. One channel per
// (project, instance, appProfile).
type AcceleratorChannel struct {
	rm       *resourcemanager.ResourceManager
	recorder *metrics.MetricsRecorder
}

// NewAcceleratorChannel constructs an AcceleratorChannel scoped to
// (project, instance, appProfile). The dial happens inside
// resourcemanager.New; the resulting SessionClient is owned by the
// ResourceManager (and closed via Close, below).
func NewAcceleratorChannel(
	ctx context.Context,
	project, instance, appProfile string,
	opts ...option.ClientOption,
) (*AcceleratorChannel, error) {
	// session.NewClient forwards opts straight to gtransport.Dial
	// without supplying a default endpoint, so without these the dial target
	// is empty ("received empty target in Build()"). Establish the standard
	// Bigtable data-plane endpoint, scope, and user agent first, then let the
	// caller's opts override. Mirrors bigtable.NewClient's use of
	// btopt.DefaultClientOptions.
	defaultOpts, err := btopt.DefaultClientOptions(prodAddr, mtlsProdAddr, dataScope, userAgent)
	if err != nil {
		return nil, err
	}
	opts = append(defaultOpts, opts...)

	rm, err := resourcemanager.New(ctx, project, instance, appProfile, opts...)
	if err != nil {
		return nil, err
	}
	return &AcceleratorChannel{
		rm:       rm,
		recorder: &metrics.MetricsRecorder{},
	}, nil
}

// Invoke implements grpc.ClientConnInterface for unary V2 RPCs.
func (c *AcceleratorChannel) Invoke(ctx context.Context, method string, args, reply interface{}, _ ...grpc.CallOption) error {
	switch method {
	case v2pb.Bigtable_MutateRow_FullMethodName:
		return c.mutateRowImpl(ctx, args, reply)
	default:
		return status.Errorf(codes.Unimplemented, "accelerator: method %s not implemented", method)
	}
}

func (c *AcceleratorChannel) mutateRowImpl(ctx context.Context, args, reply interface{}) error {
	reqV2, ok := args.(*v2pb.MutateRowRequest)
	if !ok {
		return status.Errorf(codes.Internal, "accelerator: unexpected request type %T for MutateRow", args)
	}
	respV2, ok := reply.(*v2pb.MutateRowResponse)
	if !ok {
		return status.Errorf(codes.Internal, "accelerator: unexpected reply type %T for MutateRow", reply)
	}

	reqAdapter := adapters.DefaultMutateRowRequestAdapter
	resource, err := reqAdapter.ExtractResource(reqV2)
	if err != nil {
		return err
	}
	sessionReq, err := reqAdapter.Adapt(reqV2)
	if err != nil {
		return err
	}

	// ResourceManager opens a fresh handle per call; the underlying read/
	// write session pools are deduped inside session.Client, so this is
	// cheap. release is a no-op (handles are not pooled at this layer).
	tbl, release, err := c.rm.GetSessionTable(resource, "MutateRow")
	if err != nil {
		return err
	}
	defer release()

	sessionResp, err := tbl.MutateRow(ctx, sessionReq)
	if err != nil {
		return err
	}

	respAdapter := adapters.DefaultMutateRowResponseAdapter
	adapted, err := respAdapter.Adapt(sessionResp)
	if err != nil {
		return err
	}
	proto.Reset(respV2)
	if adapted != nil {
		proto.Merge(respV2, adapted)
	}
	return nil
}

// NewStream implements grpc.ClientConnInterface for streaming V2 RPCs.
// ReadRows is dispatched through session.TableAPI.ReadRow lazily: the
// session call is deferred until the first RecvMsg so the consumer's pull
// rate controls when work happens.
func (c *AcceleratorChannel) NewStream(ctx context.Context, _ *grpc.StreamDesc, method string, _ ...grpc.CallOption) (grpc.ClientStream, error) {
	switch method {
	case v2pb.Bigtable_ReadRows_FullMethodName:
		return &readRowsClientStream{ctx: ctx, c: c}, nil
	default:
		return nil, status.Errorf(codes.Unimplemented, "accelerator: streaming method %s not implemented", method)
	}
}

// Close releases resources held by the channel by closing the ResourceManager
// (which closes the underlying session Client and all its pools).
func (c *AcceleratorChannel) Close() error {
	if c.rm == nil {
		return nil
	}
	return c.rm.Close()
}

// readRowsClientStream implements grpc.ClientStream for the V2 ReadRows RPC,
// dispatching to a single SessionTableApi.ReadRow call lazily on the first
// RecvMsg. Backpressure flows naturally: no session work happens until the
// consumer pulls.
//
// Concurrency contract (matches google.golang.org/grpc.ClientStream): it is
// safe to have one goroutine calling SendMsg and another calling RecvMsg on
// the same stream at the same time, but it is NOT safe to call SendMsg from
// multiple goroutines, nor to call RecvMsg from multiple goroutines, nor to
// call CloseSend concurrently with SendMsg. Callers that violate this
// contract race on the internal state below. The daemon's stream
// interceptor (interceptors.go) is the only production caller and always
// drives one stream sequentially from a single per-RPC goroutine, so the
// contract is satisfied trivially there.
//
// State machine:
//   - awaitingSend: SendMsg has not been called.
//   - awaitingRecv: request captured; session call has not run.
//   - done: session call has completed (or errored) and the single response
//     has been delivered (or never will be). Subsequent RecvMsg returns EOF.
type readRowsClientStream struct {
	ctx context.Context
	c   *AcceleratorChannel

	req  *v2pb.ReadRowsRequest
	sent bool
	done bool
}

func (s *readRowsClientStream) Header() (gmetadata.MD, error) { return nil, nil }
func (s *readRowsClientStream) Trailer() gmetadata.MD         { return nil }
func (s *readRowsClientStream) CloseSend() error              { return nil }
func (s *readRowsClientStream) Context() context.Context      { return s.ctx }

func (s *readRowsClientStream) SendMsg(m any) error {
	if s.sent {
		return status.Error(codes.Internal, "accelerator: ReadRows SendMsg called more than once")
	}
	req, ok := m.(*v2pb.ReadRowsRequest)
	if !ok {
		return status.Errorf(codes.Internal, "accelerator: unexpected request type %T for ReadRows", m)
	}
	if err := validateSingleRowReadRequest(req); err != nil {
		return err
	}
	s.req = req
	s.sent = true
	return nil
}

func (s *readRowsClientStream) RecvMsg(m any) error {
	if s.done {
		return io.EOF
	}
	if !s.sent {
		return status.Error(codes.Internal, "accelerator: ReadRows RecvMsg called before SendMsg")
	}
	resp, ok := m.(*v2pb.ReadRowsResponse)
	if !ok {
		return status.Errorf(codes.Internal, "accelerator: unexpected reply type %T for ReadRows", m)
	}

	reqAdapter := adapters.DefaultReadRowRequestAdapter
	resource, err := reqAdapter.ExtractResource(s.req)
	if err != nil {
		s.done = true
		return err
	}
	sessionReq, err := reqAdapter.Adapt(s.req)
	if err != nil {
		s.done = true
		return err
	}

	tbl, release, err := s.c.rm.GetSessionTable(resource, "ReadRow")
	if err != nil {
		s.done = true
		return err
	}
	defer release()

	sessionResp, err := tbl.ReadRow(s.ctx, sessionReq)
	if err != nil {
		s.done = true
		return err
	}

	adapted, err := adapters.DefaultReadRowResponseAdapter.Adapt(sessionResp)
	if err != nil {
		s.done = true
		return err
	}
	proto.Reset(resp)
	if adapted != nil {
		proto.Merge(resp, adapted)
	}
	s.done = true
	return nil
}

// validateSingleRowReadRequest rejects request shapes the session transport
// cannot express. SessionReadRow targets exactly one row by key; the only
// accepted shapes are (a) exactly one RowKey and no RowRanges, or (b) no
// RowKeys and one closed-closed RowRange whose start and end are equal
// (which pins the range to a single row). Anything broader must fail fast
// rather than silently drop rows.
func validateSingleRowReadRequest(req *v2pb.ReadRowsRequest) error {
	if req == nil {
		return status.Error(codes.InvalidArgument, "accelerator: nil ReadRowsRequest")
	}
	if req.Reversed {
		return status.Error(codes.Unimplemented, "accelerator: reversed ReadRows not supported")
	}
	if req.Rows == nil {
		return status.Error(codes.Unimplemented, "accelerator: ReadRows without a single row key not supported")
	}
	nKeys, nRanges := len(req.Rows.RowKeys), len(req.Rows.RowRanges)
	switch {
	case nKeys == 1 && nRanges == 0:
		return nil
	case nKeys == 0 && nRanges == 1:
		return validateSingleRowRange(req.Rows.RowRanges[0])
	default:
		return status.Errorf(codes.Unimplemented, "accelerator: ReadRows must specify exactly one row (got %d row keys, %d row ranges)", nKeys, nRanges)
	}
}

// validateSingleRowRange accepts only a closed-closed range whose start and
// end bounds are byte-equal — the only range shape that pins to exactly one
// row.
func validateSingleRowRange(r *v2pb.RowRange) error {
	if r == nil {
		return status.Error(codes.Unimplemented, "accelerator: nil RowRange")
	}
	start, ok := r.StartKey.(*v2pb.RowRange_StartKeyClosed)
	if !ok {
		return status.Error(codes.Unimplemented, "accelerator: ReadRows RowRange must use start_key_closed")
	}
	end, ok := r.EndKey.(*v2pb.RowRange_EndKeyClosed)
	if !ok {
		return status.Error(codes.Unimplemented, "accelerator: ReadRows RowRange must use end_key_closed")
	}
	if !bytes.Equal(start.StartKeyClosed, end.EndKeyClosed) {
		return status.Error(codes.Unimplemented, "accelerator: ReadRows RowRange must have equal start and end bounds")
	}
	return nil
}
