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

package resourcemanager

import (
	"context"
	"strings"

	"cloud.google.com/go/bigtable/internal/session"
	"google.golang.org/api/option"
)

// SessionClientFactory constructs the underlying session Client for a
// ResourceManager. The default factory wraps session.NewClient.
type SessionClientFactory = func(
	ctx context.Context,
	project, instance, appProfile string,
	opts ...option.ClientOption,
) (session.Client, error)

// newSessionClient is the factory ResourceManager.New uses to construct the
// underlying session Client. Tests override this via TestHookSessionClient.
var newSessionClient SessionClientFactory = func(
	ctx context.Context,
	project, instance, appProfile string,
	opts ...option.ClientOption,
) (session.Client, error) {
	return session.NewClient(ctx, project, instance, appProfile, nil, opts...)
}

// TestHookSessionClient swaps the session Client factory used by New for the
// duration of a test. Returns a restore function the test must call (typically
// via t.Cleanup) to revert. Package-level state mutation is not parallel-safe.
func TestHookSessionClient(f SessionClientFactory) func() {
	orig := newSessionClient
	newSessionClient = f
	return func() { newSessionClient = orig }
}

// ResourceManager owns a session Client and vends per-resource
// session.TableAPI handles to the accelerator's dispatch path.
//
// It deliberately does NOT cache the handles. session.Client already
// dedupes the expensive resource — the per-(resource, permission) read and
// write session pools — inside sessionClient.getOrCreateSessionPool, so
// repeated OpenTable calls for the same table share the same underlying
// pools. The handles OpenTable returns are cheap wrappers whose Close is a
// no-op today (see internal/session/table.go), so there is nothing worth
// pooling at this layer.
type ResourceManager struct {
	sc session.Client
}

// New dials Bigtable via internal/session and constructs a ResourceManager
// scoped to (project, instance, appProfile). The ResourceManager takes
// ownership of the session Client — Close releases it.
func New(
	ctx context.Context,
	project, instance, appProfile string,
	opts ...option.ClientOption,
) (*ResourceManager, error) {
	sc, err := newSessionClient(ctx, project, instance, appProfile, opts...)
	if err != nil {
		return nil, err
	}
	return &ResourceManager{sc: sc}, nil
}

// noopRelease is returned by GetSessionTable. Handles are not pooled here, so
// there is nothing to release; the thunk keeps call sites uniform and leaves
// room to reintroduce pooling later without touching callers.
func noopRelease() {}

// GetSessionTable returns a session.TableAPI for the table named by resource.
//
// Wire format note: V2 RPCs carry a full table resource name
// ("projects/P/instances/I/tables/T"). session.Client.OpenTable prepends the
// project/instance/tables/ prefix itself, so ResourceManager hands it just
// the leaf segment.
//
// method is accepted for call-site clarity ("ReadRow" / "MutateRow") but does
// not affect which handle is returned: OpenTable vends a single handle that
// routes reads and writes to their respective pools internally. The returned
// release thunk is a no-op.
func (rm *ResourceManager) GetSessionTable(resource, method string) (session.TableAPI, func(), error) {
	return rm.sc.OpenTable(tableLeaf(resource)), noopRelease, nil
}

// Close closes the underlying session Client, tearing down its pools. Handles
// previously returned by GetSessionTable become unusable afterwards.
func (rm *ResourceManager) Close() error {
	if rm.sc != nil {
		return rm.sc.Close()
	}
	return nil
}

// tableLeaf extracts the leaf "T" from "projects/P/instances/I/tables/T".
// Returns the input unchanged if it does not have a "/" — best-effort.
func tableLeaf(fullName string) string {
	if i := strings.LastIndex(fullName, "/"); i >= 0 {
		return fullName[i+1:]
	}
	return fullName
}
