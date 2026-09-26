// SPDX-FileCopyrightText: Copyright The Miniflux Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package storage

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestWebSessionGenerationMonitor_CurrentLoadsAndCaches(t *testing.T) {
	var calls atomic.Int32

	monitor := &webSessionGenerationMonitor{}
	monitor.lookup = func(context.Context) (int64, error) {
		calls.Add(1)
		return 7, nil
	}

	ctx := context.Background()

	gen, err := monitor.Current(ctx)
	if err != nil {
		t.Fatalf("Current() error: %v", err)
	}
	if gen != 7 {
		t.Fatalf("Current() = %d, want 7", gen)
	}

	for i := 0; i < 5; i++ {
		gen, err = monitor.Current(ctx)
		if err != nil {
			t.Fatalf("cached Current() error: %v", err)
		}
		if gen != 7 {
			t.Fatalf("cached Current() = %d, want 7", gen)
		}
	}

	if count := calls.Load(); count != 1 {
		t.Fatalf("lookup called %d times, want 1 (cached value must be reused)", count)
	}
}

func TestWebSessionGenerationMonitor_ColdCacheLookupErrorFails(t *testing.T) {
	lookupErr := errors.New("database unavailable")
	monitor := &webSessionGenerationMonitor{}
	monitor.lookup = func(context.Context) (int64, error) {
		return 0, lookupErr
	}

	if _, err := monitor.Current(context.Background()); !errors.Is(err, lookupErr) {
		t.Fatalf("Current() error = %v, want %v", err, lookupErr)
	}
}

func TestWebSessionGenerationMonitor_RefreshErrorKeepsLastKnownValue(t *testing.T) {
	var generation int64 = 3
	monitor := &webSessionGenerationMonitor{}
	monitor.lookup = func(context.Context) (int64, error) {
		return atomic.LoadInt64(&generation), nil
	}
	ctx := context.Background()

	if gen, err := monitor.Refresh(ctx); err != nil || gen != 3 {
		t.Fatalf("initial Refresh() = (%d, %v), want (3, nil)", gen, err)
	}

	monitor.lookup = func(context.Context) (int64, error) {
		return 0, errors.New("database unavailable")
	}

	gen, err := monitor.Current(ctx)
	if err != nil {
		t.Fatalf("Current() after lookup failure must keep serving the last known value, got error %v", err)
	}
	if gen != 3 {
		t.Fatalf("Current() = %d, want the last known generation 3", gen)
	}
}

func TestWebSessionGenerationMonitor_AdoptIsMonotonic(t *testing.T) {
	t.Run("adopts higher generation", func(t *testing.T) {
		monitor := &webSessionGenerationMonitor{}
		monitor.Adopt(5)
		monitor.Adopt(6)

		monitor.mu.RLock()
		got := monitor.generation
		loaded := monitor.loaded
		monitor.mu.RUnlock()

		if !loaded {
			t.Error("Adopt must mark the monitor as loaded")
		}
		if got != 6 {
			t.Errorf("generation = %d, want 6", got)
		}
	})

	t.Run("ignores lower generation", func(t *testing.T) {
		monitor := &webSessionGenerationMonitor{}
		monitor.Adopt(5)
		monitor.Adopt(4)

		monitor.mu.RLock()
		got := monitor.generation
		monitor.mu.RUnlock()

		if got != 5 {
			t.Errorf("generation = %d, want 5 (older notifications must be ignored)", got)
		}
	})
}

func TestWebSessionGenerationMonitor_ExpiredCacheRefreshes(t *testing.T) {
	var generation int64 = 1
	monitor := &webSessionGenerationMonitor{}
	monitor.lookup = func(context.Context) (int64, error) {
		return atomic.LoadInt64(&generation), nil
	}
	ctx := context.Background()

	if _, err := monitor.Current(ctx); err != nil {
		t.Fatalf("initial Current() error: %v", err)
	}

	atomic.StoreInt64(&generation, 2)

	monitor.mu.Lock()
	monitor.fetchedAt = time.Now().Add(-webSessionGenerationCacheTTL - time.Second)
	monitor.mu.Unlock()

	gen, err := monitor.Current(ctx)
	if err != nil {
		t.Fatalf("Current() after expiry error: %v", err)
	}
	if gen != 2 {
		t.Fatalf("Current() = %d, want 2 (stale cache must be refreshed)", gen)
	}
}
