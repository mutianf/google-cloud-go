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

// AcceleratorServer hosts the gRPC server-side scaffolding for the
// accelerator daemon: a UDS listener, lifecycle watchdogs (stdin EOF and
// parent-PID reparent), and proxy interceptors that forward every RPC
// through an AcceleratorChannel. The interceptor implementations live in
// interceptors.go alongside bigtableServerStub.
package accelerator

import (
	"io"
	"log"
	"net"
	"os"
	"sync"
	"time"

	btpb "cloud.google.com/go/bigtable/apiv2/bigtablepb"
	"google.golang.org/grpc"
)

// AcceleratorServer manages the lifecycle of the local gRPC UDS server.
type AcceleratorServer struct {
	udsPath      string
	grpcServer   *grpc.Server
	service      *bigtableServerStub
	listener     net.Listener
	shutdownChan chan struct{}
	stopOnce     sync.Once
	StdinReader  io.Reader // Configurable stdin reader for testing
	channel      *AcceleratorChannel
}

// NewAcceleratorServer creates a new AcceleratorServer instance.
func NewAcceleratorServer(udsPath string, channel *AcceleratorChannel) *AcceleratorServer {
	s := &AcceleratorServer{
		udsPath:      udsPath,
		shutdownChan: make(chan struct{}),
		StdinReader:  os.Stdin,
		channel:      channel,
		service:      newBigtableServerStub(),
	}
	return s
}

// Start boots the gRPC server on the Unix Domain Socket asynchronously.
func (s *AcceleratorServer) Start() error {
	_ = os.Remove(s.udsPath)

	l, err := net.Listen("unix", s.udsPath)
	if err != nil {
		log.Printf("failed to start accelerator: %v", err)
		return err
	}
	s.listener = l

	// Restrict the socket to the owning user — the daemon is paired 1:1 with
	// its parent process, so no other local user should be able to issue RPCs
	// against it. The OS umask alone is not sufficient here (defaults to 022
	// in most environments, which leaves the socket world-readable).
	if err := os.Chmod(s.udsPath, 0600); err != nil {
		_ = l.Close()
		_ = os.Remove(s.udsPath)
		log.Printf("failed to chmod accelerator socket: %v", err)
		return err
	}

	// The unary interceptor short-circuits every unary RPC and routes it
	// through the AcceleratorChannel. The stream interceptor does the same
	// for server-streaming RPCs (ReadRows today).
	s.grpcServer = grpc.NewServer(
		grpc.UnaryInterceptor(proxyUnaryInterceptor(s.channel)),
		grpc.StreamInterceptor(proxyStreamInterceptor(s.channel)),
	)

	// Register the empty bigtableServerStub. gRPC requires a typed
	// server implementation to be registered before it will accept RPCs for
	// the service, but the interceptors above short-circuit every method on
	// the stub before it is reached — the stub exists solely to satisfy
	// gRPC's registration contract.
	btpb.RegisterBigtableServer(s.grpcServer, s.service)

	go func() {
		_ = s.grpcServer.Serve(s.listener)
	}()

	go s.monitorStdin()
	go s.monitorParentPid()

	return nil
}

// Stop gracefully stops the server, cleans up the UDS socket, and notifies
// monitors.
func (s *AcceleratorServer) Stop() {
	s.stopOnce.Do(func() {
		close(s.shutdownChan)

		if s.grpcServer != nil {
			s.grpcServer.GracefulStop()
		}

		if s.listener != nil {
			_ = s.listener.Close()
		}

		// Close the channel after gRPC has drained — backends owned by the
		// channel's resource manager are torn down here, not while in-flight
		// RPCs may still be using them.
		if s.channel != nil {
			_ = s.channel.Close()
		}

		_ = os.Remove(s.udsPath)
	})
}

// monitorStdin monitors standard input for an EOF signal. Tests that don't
// want this watchdog set StdinReader to nil.
func (s *AcceleratorServer) monitorStdin() {
	if s.StdinReader == nil {
		return
	}
	buf := make([]byte, 1)
	for {
		select {
		case <-s.shutdownChan:
			return
		default:
			_, err := s.StdinReader.Read(buf)
			if err != nil {
				s.Stop()
				return
			}
		}
	}
}

// monitorParentPid stops the daemon if the parent PID becomes 1 (orphaned)
// or otherwise changes.
func (s *AcceleratorServer) monitorParentPid() {
	initialPpid := os.Getppid()
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-s.shutdownChan:
			return
		case <-ticker.C:
			currentPpid := os.Getppid()
			if currentPpid == 1 || currentPpid != initialPpid {
				s.Stop()
				return
			}
		}
	}
}

// ShutdownChan returns a read-only channel that is closed when the server
// shuts down. Main entrypoints block on this to know when to exit.
func (s *AcceleratorServer) ShutdownChan() <-chan struct{} {
	return s.shutdownChan
}
