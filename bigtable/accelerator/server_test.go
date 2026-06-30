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
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestAcceleratorServer_Permissions(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "accelerator-perm-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)
	udsPath := filepath.Join(tmpDir, "bt_proxy.sock")

	// Zero-value AcceleratorChannel is sufficient here: this test only
	// exercises Start/Stop; no RPCs are issued, so the channel's
	// ResourceManager is never dereferenced. Close handles nil rm.
	channel := &AcceleratorChannel{}
	server := NewAcceleratorServer(udsPath, channel)
	server.StdinReader = nil
	if err := server.Start(); err != nil {
		t.Fatalf("failed to start server: %v", err)
	}
	defer server.Stop()

	info, err := os.Stat(udsPath)
	if err != nil {
		t.Fatalf("failed to stat socket file: %v", err)
	}
	if mode := info.Mode().Perm(); mode != 0600 {
		t.Errorf("expected socket permissions 0600, got %o", mode)
	}
}

func TestAcceleratorServer_LifecycleStdinEOF(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "accelerator-lifecycle-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)
	udsPath := filepath.Join(tmpDir, "bt_proxy.sock")

	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("failed to create pipe: %v", err)
	}
	defer r.Close()

	channel := &AcceleratorChannel{}
	server := NewAcceleratorServer(udsPath, channel)
	server.StdinReader = r // inject the pipe instead of os.Stdin
	if err := server.Start(); err != nil {
		t.Fatalf("failed to start server: %v", err)
	}
	defer server.Stop()

	if err := w.Close(); err != nil {
		t.Fatalf("failed to close write pipe: %v", err)
	}

	select {
	case <-server.shutdownChan:
		// success
	case <-time.After(3 * time.Second):
		t.Error("server failed to shut down within timeout after stdin EOF")
	}
}

func TestAcceleratorServer_StartFailure(t *testing.T) {
	udsPath := "/nonexistent-dir-123456/bt_proxy.sock"
	channel := &AcceleratorChannel{}
	server := NewAcceleratorServer(udsPath, channel)
	server.StdinReader = nil
	if err := server.Start(); err == nil {
		t.Error("expected error when starting server with nonexistent UDS path, but got nil")
	}
}
