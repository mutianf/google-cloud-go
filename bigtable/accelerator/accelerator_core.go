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
	"context"

	v2pb "cloud.google.com/go/bigtable/apiv2/bigtablepb"
	"cloud.google.com/go/bigtable/accelerator/adapters"
	"cloud.google.com/go/bigtable/accelerator/metrics"
	"cloud.google.com/go/bigtable/accelerator/resourcemanager"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	gmetadata "google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

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

	// ResourceManager caches by (resource, method); MutateRow keys with
	// "MutateRow" so read and write entries stay independent. On cache hit
	// the SessionClient is not consulted.
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
//
// TODO: wire ReadRows through session.SessionTableApi.ReadRow once a streaming
// shim is in place (today's session transport handles single-row reads only).
func (c *AcceleratorChannel) NewStream(ctx context.Context, _ *grpc.StreamDesc, method string, _ ...grpc.CallOption) (grpc.ClientStream, error) {
	switch method {
	case v2pb.Bigtable_ReadRows_FullMethodName:
		return &readRowsClientStream{ctx: ctx}, nil
	default:
		return nil, status.Errorf(codes.Unimplemented, "accelerator: streaming method %s not implemented", method)
	}
}

// Close releases resources held by the channel by closing the ResourceManager
// (which closes every cached SessionTableApi, then the SessionClient
// connection).
func (c *AcceleratorChannel) Close() error {
	if c.rm == nil {
		return nil
	}
	return c.rm.Close()
}

// readRowsClientStream is a placeholder for future ReadRows streaming support.
type readRowsClientStream struct {
	ctx context.Context
}

func (s *readRowsClientStream) Header() (gmetadata.MD, error) { return nil, nil }
func (s *readRowsClientStream) Trailer() gmetadata.MD         { return nil }
func (s *readRowsClientStream) CloseSend() error              { return nil }
func (s *readRowsClientStream) Context() context.Context      { return s.ctx }
func (s *readRowsClientStream) SendMsg(_ any) error {
	return status.Error(codes.Unimplemented, "accelerator: ReadRows not yet implemented")
}
func (s *readRowsClientStream) RecvMsg(_ any) error {
	return status.Error(codes.Unimplemented, "accelerator: ReadRows not yet implemented")
}
