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
	"io"
	"strings"

	btpb "cloud.google.com/go/bigtable/apiv2/bigtablepb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
)

// bigtableServerStub satisfies grpc's requirement that a typed server
// implementation be registered for the Bigtable service before it will
// accept RPCs. The proxy interceptors short-circuit every method before it
// reaches this stub — the embedded UnimplementedBigtableServer is only
// here to make the type satisfy btpb.BigtableServer.
type bigtableServerStub struct {
	btpb.UnimplementedBigtableServer
}

// newBigtableServerStub creates a new stub instance.
func newBigtableServerStub() *bigtableServerStub {
	return &bigtableServerStub{}
}

// proxyUnaryInterceptor intercepts every incoming unary RPC, allocates the
// correct response message via the global proto registry, and delegates to the
// channel's Invoke. The single switch on method name lives in
// AcceleratorChannel.Invoke; this interceptor is switch-free.
//
// The response type is registered in protoregistry.GlobalTypes as a side
// effect of importing btpb (the generated init() in the btpb package does the
// registration), so any RPC defined in the v2 Bigtable service is resolvable
// without per-method wiring here.
func proxyUnaryInterceptor(channel *AcceleratorChannel) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, _ grpc.UnaryHandler) (interface{}, error) {
		_, output, err := resolveMethodIO(info.FullMethod)
		if err != nil {
			return nil, err
		}
		reply := output.New().Interface()
		if err := channel.Invoke(ctx, info.FullMethod, req, reply); err != nil {
			return nil, err
		}
		return reply, nil
	}
}

// proxyStreamInterceptor mirrors proxyUnaryInterceptor for server-streaming
// RPCs. For each incoming stream it resolves the method's input/output types
// via protoreflect, opens a client stream against the AcceleratorChannel,
// forwards the single request from the server side, then pumps responses
// back until io.EOF. Backpressure flows naturally: each ss.SendMsg blocks on
// HTTP/2 flow control when the client is slow, which holds back the next
// clientStream.RecvMsg.
//
// Only server-streaming RPCs are handled. Client-streaming and bidi RPCs are
// rejected with Unimplemented because no AcceleratorChannel method shape
// supports them today.
func proxyStreamInterceptor(channel *AcceleratorChannel) grpc.StreamServerInterceptor {
	return func(_ interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, _ grpc.StreamHandler) error {
		if info.IsClientStream {
			return status.Errorf(codes.Unimplemented, "accelerator: client-streaming method %s not supported", info.FullMethod)
		}
		input, output, err := resolveMethodIO(info.FullMethod)
		if err != nil {
			return err
		}

		req := input.New().Interface()
		if err := ss.RecvMsg(req); err != nil {
			return err
		}

		desc := &grpc.StreamDesc{
			StreamName:    info.FullMethod,
			ServerStreams: true,
		}
		clientStream, err := channel.NewStream(ss.Context(), desc, info.FullMethod)
		if err != nil {
			return err
		}
		if err := clientStream.SendMsg(req); err != nil {
			return err
		}
		if err := clientStream.CloseSend(); err != nil {
			return err
		}

		for {
			resp := output.New().Interface().(proto.Message)
			err := clientStream.RecvMsg(resp)
			if err == io.EOF {
				return nil
			}
			if err != nil {
				return err
			}
			if err := ss.SendMsg(resp); err != nil {
				return err
			}
		}
	}
}

// resolveMethodIO looks up a gRPC method by its full name (e.g.
// "/google.bigtable.v2.Bigtable/MutateRow") in the global proto registry and
// returns the message types for its input and output.
func resolveMethodIO(fullMethod string) (protoreflect.MessageType, protoreflect.MessageType, error) {
	serviceName, methodName, ok := strings.Cut(strings.TrimPrefix(fullMethod, "/"), "/")
	if !ok {
		return nil, nil, status.Errorf(codes.Internal, "invalid method name: %s", fullMethod)
	}
	desc, err := protoregistry.GlobalFiles.FindDescriptorByName(protoreflect.FullName(serviceName))
	if err != nil {
		return nil, nil, status.Errorf(codes.Unimplemented, "service %s not found: %v", serviceName, err)
	}
	svc, ok := desc.(protoreflect.ServiceDescriptor)
	if !ok {
		return nil, nil, status.Errorf(codes.Internal, "descriptor %s is not a service", serviceName)
	}
	method := svc.Methods().ByName(protoreflect.Name(methodName))
	if method == nil {
		return nil, nil, status.Errorf(codes.Unimplemented, "method %s not found on service %s", methodName, serviceName)
	}
	inputType, err := protoregistry.GlobalTypes.FindMessageByName(method.Input().FullName())
	if err != nil {
		return nil, nil, status.Errorf(codes.Internal, "input type %s not registered: %v", method.Input().FullName(), err)
	}
	outputType, err := protoregistry.GlobalTypes.FindMessageByName(method.Output().FullName())
	if err != nil {
		return nil, nil, status.Errorf(codes.Internal, "output type %s not registered: %v", method.Output().FullName(), err)
	}
	return inputType, outputType, nil
}
