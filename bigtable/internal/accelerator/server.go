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
	"bufio"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strings"
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
	StdinReader  io.Reader    // Configurable stdin reader for testing; nil disables stdin watchdog
	stdinBuf     *bufio.Reader // wraps StdinReader once in Start(); shared by readSecret + monitorStdin
	authSecret   string
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
	// Read the auth secret from stdin BEFORE binding — Python mints the secret
	// before Popen returns, so it is always present on the pipe.
	if s.StdinReader != nil {
		if err := s.readSecret(); err != nil {
			return err
		}
	}

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

	// Auth interceptors run first; when authSecret is empty (test mode with
	// StdinReader=nil) they are no-ops. Proxy interceptors follow and route
	// each RPC through the AcceleratorChannel.
	s.grpcServer = grpc.NewServer(
		grpc.ChainUnaryInterceptor(
			authUnaryInterceptor(s.authSecret),
			proxyUnaryInterceptor(s.channel),
		),
		grpc.ChainStreamInterceptor(
			authStreamInterceptor(s.authSecret),
			proxyStreamInterceptor(s.channel),
		),
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

// readSecret reads the auth secret written by the Python parent process to
// stdin before the daemon binds. It wraps StdinReader once in a bufio.Reader
// (stored as s.stdinBuf) so monitorStdin can continue consuming the same
// buffered stream without losing bytes.
//
// An empty or whitespace-only secret is rejected so the daemon fails closed:
// serving with an empty authSecret would disable the auth interceptor (see
// checkAuthToken), so we refuse to start rather than accept unauthenticated
// callers. This guarantees the shipped binary — which always feeds os.Stdin —
// either has a real secret or never binds the socket.
func (s *AcceleratorServer) readSecret() error {
	s.stdinBuf = bufio.NewReader(s.StdinReader)
	line, err := s.stdinBuf.ReadString('\n')
	if err != nil {
		return fmt.Errorf("accelerator: failed to read auth secret from stdin: %w", err)
	}
	secret := strings.TrimSpace(line)
	if secret == "" {
		return fmt.Errorf("accelerator: received empty auth secret from stdin; refusing to serve unauthenticated")
	}
	s.authSecret = secret
	return nil
}

// monitorStdin monitors standard input for an EOF signal. Tests that don't
// want this watchdog set StdinReader to nil.
func (s *AcceleratorServer) monitorStdin() {
	if s.StdinReader == nil {
		return
	}
	// Use the buffered reader created by readSecret so bytes already consumed
	// into the buffer are not lost (in practice stdin only carries the secret
	// line followed by EOF, but using the same reader is correct).
	var r io.Reader
	if s.stdinBuf != nil {
		r = s.stdinBuf
	} else {
		r = s.StdinReader
	}
	buf := make([]byte, 1)
	for {
		select {
		case <-s.shutdownChan:
			return
		default:
			_, err := r.Read(buf)
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
