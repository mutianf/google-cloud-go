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

// Tests for GOAWAY-triggered channel recycling (maybeRecycleChannel /
// SetChannelRecycler). Equivalent to Java's ChannelPoolDpImpl
// shouldRecycleChannel / recycleChannel on GOAWAY.

package internal

import (
	"sync/atomic"
	"testing"
	"time"
)

// TestChannelRecycler_FiredOnReadyGoAway verifies that when a Ready session
// closes with close reason "GoAway", the channelRecycler callback is invoked
// with the session's channel index.
func TestChannelRecycler_FiredOnReadyGoAway(t *testing.T) {
	var recycledIdx atomic.Int32
	recycledIdx.Store(-1)

	p := newTestPool(t, 0, 10)
	p.SetChannelRecycler(func(idx int32) { recycledIdx.Store(idx) })

	sh := injectActiveSession(t, p, "s1", time.Now())
	sh.session.setChannelIndex(3)
	sh.session.setCloseReason("GoAway")

	p.onClose(sh, nil)

	if got := recycledIdx.Load(); got != 3 {
		t.Errorf("channelRecycler called with idx %d, want 3", got)
	}
}

// TestChannelRecycler_FiredOnStartingGoAway verifies that a session that gets
// GOAWAY before ever reaching Ready (still in startingSessions) also triggers
// the channelRecycler. A draining server can reject during the handshake.
func TestChannelRecycler_FiredOnStartingGoAway(t *testing.T) {
	var recycledIdx atomic.Int32
	recycledIdx.Store(-1)

	p := newTestPool(t, 0, 10)
	p.SetChannelRecycler(func(idx int32) { recycledIdx.Store(idx) })

	sh := injectStartingSession(t, p, "s-starting")
	sh.session.setChannelIndex(1)
	sh.session.setCloseReason("GoAway")

	p.onClose(sh, nil) // starting branch fires

	if got := recycledIdx.Load(); got != 1 {
		t.Errorf("channelRecycler called with idx %d, want 1", got)
	}
}

// TestChannelRecycler_NotFiredOnNonGoAway verifies that normal session closes
// (stream error, heartbeat miss, etc.) do NOT trigger channel recycling —
// only server-sent GOAWAY warrants a connection replacement.
func TestChannelRecycler_NotFiredOnNonGoAway(t *testing.T) {
	called := false
	p := newTestPool(t, 0, 10)
	p.SetChannelRecycler(func(idx int32) { called = true })

	sh := injectActiveSession(t, p, "s1", time.Now())
	sh.session.setChannelIndex(2)
	// close reason left as default (empty) — not GoAway

	p.onClose(sh, nil)

	if called {
		t.Error("channelRecycler must not be called for non-GoAway close")
	}
}

// TestChannelRecycler_NotFiredWhenChannelIndexNegative verifies that sessions
// with no channel index (-1, e.g. non-BigtableChannelPool paths) do not
// trigger recycling even if the close reason is GoAway.
func TestChannelRecycler_NotFiredWhenChannelIndexNegative(t *testing.T) {
	called := false
	p := newTestPool(t, 0, 10)
	p.SetChannelRecycler(func(idx int32) { called = true })

	sh := injectActiveSession(t, p, "s1", time.Now())
	// channelIndex stays -1 (default from NewSession / initSessionDebug)
	sh.session.setCloseReason("GoAway")

	p.onClose(sh, nil)

	if called {
		t.Error("channelRecycler must not be called when channelIndex is -1")
	}
}
