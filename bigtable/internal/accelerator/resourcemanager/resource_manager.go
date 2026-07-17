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

// SessionClientFactory constructs the underlying SessionClient for a
// ResourceManager. The default factory wraps session.NewSessionClient.
type SessionClientFactory = func(
	ctx context.Context,
	project, instance, appProfile string,
	opts ...option.ClientOption,
) (session.SessionClient, error)

// newSessionClient is the factory ResourceManager.New uses to construct the
// underlying SessionClient. Tests override this via TestHookSessionClient.
var newSessionClient SessionClientFactory = func(
	ctx context.Context,
	project, instance, appProfile string,
	opts ...option.ClientOption,
) (session.SessionClient, error) {
	return session.NewSessionClient(ctx, project, instance, appProfile, nil, opts...)
}

// TestHookSessionClient swaps the SessionClient factory used by New for the
// duration of a test. Returns a restore function the test must call (typically
// via t.Cleanup) to revert. Package-level state mutation is not parallel-safe.
func TestHookSessionClient(f SessionClientFactory) func() {
	orig := newSessionClient
	newSessionClient = f
	return func() { newSessionClient = orig }
}

// ResourceManager owns a SessionClient and a PoolCache of per-(resource,
// method) SessionTableApi instances. On cache hit, GetSessionTable returns
// the cached entry without consulting SessionClient. On miss, it opens a
// fresh SessionTableApi via the SessionClient and caches it.
//
// Wire format note: V2 RPCs carry a full table resource name
// ("projects/P/instances/I/tables/T"). session.SessionClient.OpenSessionTable
// prepends the project/instance/tables/ prefix itself, so ResourceManager
// hands it just the leaf segment.
type ResourceManager struct {
	sc    session.SessionClient
	cache *PoolCache[session.SessionTableApi]
}

// New dials Bigtable via internal/session and constructs a ResourceManager
// scoped to (project, instance, appProfile). The ResourceManager takes
// ownership of the SessionClient — Close releases the cache and then the
// SessionClient connection.
func New(
	ctx context.Context,
	project, instance, appProfile string,
	opts ...option.ClientOption,
) (*ResourceManager, error) {
	sc, err := newSessionClient(ctx, project, instance, appProfile, opts...)
	if err != nil {
		return nil, err
	}
	rm := &ResourceManager{sc: sc}
	rm.cache = NewPoolCache[session.SessionTableApi](DefaultPoolCacheSize, rm.openSessionTable)
	return rm, nil
}

// openSessionTable is the PoolCache factory invoked on cache miss. It is a
// method (not a closure) so ResourceManager.sc remains the only reference to
// the SessionClient — no captured-state lifetime issues.
func (rm *ResourceManager) openSessionTable(resource, _ string) (session.SessionTableApi, error) {
	return rm.sc.OpenSessionTable(tableLeaf(resource)), nil
}

// GetSessionTable returns the cached SessionTableApi for (resource, method),
// constructing one via SessionClient on cache miss. The returned release
// thunk MUST be called once the caller is done with the handle, even on
// error from the dispatched RPC.
func (rm *ResourceManager) GetSessionTable(resource, method string) (session.SessionTableApi, func(), error) {
	return rm.cache.GetOrOpen(resource, method)
}

// Close closes every cached SessionTableApi, then closes the underlying
// SessionClient connection.
func (rm *ResourceManager) Close() error {
	var firstErr error
	if rm.cache != nil {
		if err := rm.cache.Close(); err != nil {
			firstErr = err
		}
	}
	if rm.sc != nil {
		if err := rm.sc.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// tableLeaf extracts the leaf "T" from "projects/P/instances/I/tables/T".
// Returns the input unchanged if it does not have a "/" — best-effort.
func tableLeaf(fullName string) string {
	if i := strings.LastIndex(fullName, "/"); i >= 0 {
		return fullName[i+1:]
	}
	return fullName
}
