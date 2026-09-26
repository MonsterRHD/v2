// SPDX-FileCopyrightText: Copyright The Miniflux Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package webhook

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"miniflux.app/v2/internal/model"
)

type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

func newFakeClock() *fakeClock {
	return &fakeClock{now: time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)}
}

// fakeDeliveryStore is an in-memory implementation of DeliveryStore that
// mimics the SQL state transitions closely enough to exercise the dispatcher
// state machine without PostgreSQL.
type fakeDeliveryStore struct {
	mu             sync.Mutex
	clock          *fakeClock
	deliveries     map[int64]*model.WebhookDelivery
	entries        map[int64]*model.Entry
	settings       map[int64]*model.Integration
	integrationErr error
}

func newFakeDeliveryStore(clock *fakeClock) *fakeDeliveryStore {
	return &fakeDeliveryStore{
		clock:      clock,
		deliveries: make(map[int64]*model.WebhookDelivery),
		entries:    make(map[int64]*model.Entry),
		settings:   make(map[int64]*model.Integration),
	}
}

func (s *fakeDeliveryStore) addDelivery(d *model.WebhookDelivery) {
	s.mu.Lock()
	defer s.mu.Unlock()

	copy := *d
	if copy.ID == 0 {
		copy.ID = int64(len(s.deliveries) + 1)
	}

	s.deliveries[copy.ID] = &copy
}

func (s *fakeDeliveryStore) get(id int64) *model.WebhookDelivery {
	s.mu.Lock()
	defer s.mu.Unlock()

	d := s.deliveries[id]
	copy := *d
	return &copy
}

func (s *fakeDeliveryStore) ReclaimExpiredInFlightWebhookDeliveries(lease time.Duration) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	cutoff := s.clock.Now().Add(-lease)
	var count int64

	for _, d := range s.deliveries {
		if d.Status == model.WebhookDeliveryStatusInFlight && d.ClaimedAt != nil && d.ClaimedAt.Before(cutoff) {
			d.Status = model.WebhookDeliveryStatusRetryWaiting
			d.NextAttemptAt = s.clock.Now()
			count++
		}
	}

	return count, nil
}

func (s *fakeDeliveryStore) ClaimDueWebhookDeliveries(batchSize int) (model.WebhookDeliveryList, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var claimed model.WebhookDeliveryList

	now := s.clock.Now()
	for _, d := range s.deliveries {
		if len(claimed) >= batchSize {
			break
		}

		if (d.Status == model.WebhookDeliveryStatusPending || d.Status == model.WebhookDeliveryStatusRetryWaiting) && !d.NextAttemptAt.After(now) {
			d.Status = model.WebhookDeliveryStatusInFlight
			d.Attempts++
			claimTime := now
			d.ClaimedAt = &claimTime
			d.LastAttemptAt = &claimTime
			copy := *d
			claimed = append(claimed, &copy)
		}
	}

	return claimed, nil
}

func (s *fakeDeliveryStore) MarkWebhookDeliverySucceeded(id int64, httpStatus int) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	d, ok := s.deliveries[id]
	if !ok || d.Status != model.WebhookDeliveryStatusInFlight {
		return 0, nil
	}

	d.Status = model.WebhookDeliveryStatusSucceeded
	status := httpStatus
	d.LastHTTPStatus = &status
	d.UpdatedAt = s.clock.Now()

	return 1, nil
}

func (s *fakeDeliveryStore) MarkWebhookDeliveryWaitingRetry(id int64, httpStatus *int, errText string, nextAttemptAt time.Time) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	d, ok := s.deliveries[id]
	if !ok || d.Status != model.WebhookDeliveryStatusInFlight {
		return 0, nil
	}

	d.Status = model.WebhookDeliveryStatusRetryWaiting
	d.LastHTTPStatus = httpStatus
	d.LastError = errText
	d.NextAttemptAt = nextAttemptAt
	d.UpdatedAt = s.clock.Now()

	return 1, nil
}

func (s *fakeDeliveryStore) MarkWebhookDeliveryFailed(id int64, httpStatus *int, errText string) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	d, ok := s.deliveries[id]
	if !ok || d.Status != model.WebhookDeliveryStatusInFlight {
		return 0, nil
	}

	d.Status = model.WebhookDeliveryStatusFailed
	d.LastHTTPStatus = httpStatus
	d.LastError = errText
	d.UpdatedAt = s.clock.Now()

	return 1, nil
}

func (s *fakeDeliveryStore) EntryForWebhookDelivery(userID, entryID int64) (*model.Entry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.entries[entryID], nil
}

func (s *fakeDeliveryStore) Integration(userID int64) (*model.Integration, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.integrationErr != nil {
		return nil, s.integrationErr
	}

	return s.settings[userID], nil
}

type remoteServer struct {
	*httptest.Server
	mu       sync.Mutex
	requests int64
	eventIDs []string
	statuses []int
}

func newRemoteServer(t *testing.T, statuses ...int) *remoteServer {
	t.Helper()

	remote := &remoteServer{statuses: statuses}
	remote.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		remote.mu.Lock()
		defer remote.mu.Unlock()

		idx := int(atomic.AddInt64(&remote.requests, 1)) - 1
		remote.eventIDs = append(remote.eventIDs, r.Header.Get(EventIDHeader))

		status := http.StatusOK
		if idx < len(remote.statuses) {
			status = remote.statuses[idx]
		}

		if status == http.StatusTooManyRequests || status == http.StatusServiceUnavailable {
			w.Header().Set("Retry-After", "30")
		}

		w.WriteHeader(status)

		if status >= 400 {
			_, _ = w.Write([]byte("remote failure body"))
		}
	}))
	t.Cleanup(remote.Close)

	return remote
}

func (r *remoteServer) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return int(atomic.LoadInt64(&r.requests))
}

func (r *remoteServer) eventIDList() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.eventIDs...)
}

func newTestDispatcher(store DeliveryStore, clock *fakeClock, initial, max time.Duration, multiplier, maxAttempts int) *Dispatcher {
	return &Dispatcher{
		store:             store,
		pollInterval:      time.Hour,
		lease:             2 * time.Minute,
		initialBackoff:    initial,
		maxBackoff:        max,
		backoffMultiplier: multiplier,
		batchSize:         4,
		clock:             clock,
		kick:              make(chan struct{}, 1),
	}
}

func pendingDelivery(id int64, url string, maxAttempts int) *model.WebhookDelivery {
	return &model.WebhookDelivery{
		ID:            id,
		EventID:       fmt.Sprintf("event-%d", id),
		UserID:        1,
		EntryID:       id,
		WebhookURL:    url,
		Status:        model.WebhookDeliveryStatusPending,
		Attempts:      0,
		MaxAttempts:   maxAttempts,
		NextAttemptAt: time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC),
	}
}

func dispatcherEntry(id int64) *model.Entry {
	entry := model.NewEntry()
	entry.ID = id
	entry.UserID = 1
	entry.FeedID = 10
	entry.Title = "Dispatch entry"
	entry.URL = "https://example.com/a"
	entry.Feed.ID = 10
	entry.Feed.UserID = 1
	entry.Feed.Category = &model.Category{ID: 3, Title: "Cat"}
	return entry
}

func TestBackoffDelay(t *testing.T) {
	initial := 10 * time.Second
	max := 80 * time.Second

	got := []time.Duration{
		backoffDelay(1, initial, max, 2),
		backoffDelay(2, initial, max, 2),
		backoffDelay(3, initial, max, 2),
		backoffDelay(4, initial, max, 2),
		backoffDelay(5, initial, max, 2),
	}

	want := []time.Duration{10 * time.Second, 20 * time.Second, 40 * time.Second, 80 * time.Second, 80 * time.Second}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("attempt %d: got %s want %s", i+1, got[i], want[i])
		}
	}
}

func TestDispatcherSucceedsOnFirstAttempt(t *testing.T) {
	configureIntegrationAllowPrivateNetworksOption(t)

	clock := newFakeClock()
	store := newFakeDeliveryStore(clock)
	remote := newRemoteServer(t, http.StatusOK)

	store.settings[1] = &model.Integration{WebhookEnabled: true}
	store.entries[1] = dispatcherEntry(1)
	store.addDelivery(pendingDelivery(1, remote.URL, 3))

	d := newTestDispatcher(store, clock, 10*time.Second, 80*time.Second, 2, 0)
	d.processOnce(context.Background())

	final := store.get(1)
	if final.Status != model.WebhookDeliveryStatusSucceeded {
		t.Fatalf("Expected succeeded, got %q (error: %s)", final.Status, final.LastError)
	}

	if remote.count() != 1 {
		t.Fatalf("Expected exactly 1 remote request, got %d", remote.count())
	}

	ids := remote.eventIDList()
	if len(ids) != 1 || ids[0] != "event-1" {
		t.Fatalf("Unexpected event ids: %v", ids)
	}

	// A terminal record is never claimed or sent again.
	d.processOnce(context.Background())
	d.processOnce(context.Background())

	if remote.count() != 1 {
		t.Fatalf("Expected still 1 remote request after terminal status, got %d", remote.count())
	}
}

func TestDispatcherRetriesWithBackoffThenSucceeds(t *testing.T) {
	configureIntegrationAllowPrivateNetworksOption(t)

	clock := newFakeClock()
	store := newFakeDeliveryStore(clock)
	remote := newRemoteServer(t, http.StatusInternalServerError, http.StatusOK)

	store.settings[1] = &model.Integration{WebhookEnabled: true}
	store.entries[1] = dispatcherEntry(1)
	store.addDelivery(pendingDelivery(1, remote.URL, 5))

	d := newTestDispatcher(store, clock, 10*time.Second, 80*time.Second, 2, 0)

	d.processOnce(context.Background())
	afterFirst := store.get(1)
	if afterFirst.Status != model.WebhookDeliveryStatusRetryWaiting || afterFirst.Attempts != 1 {
		t.Fatalf("Expected retry_waiting after 500, got %+v", afterFirst)
	}

	if afterFirst.NextAttemptAt != clock.Now().Add(10*time.Second) {
		t.Fatalf("Expected next attempt in 10s, got %s (now %s)", afterFirst.NextAttemptAt, clock.Now())
	}

	// Not due yet.
	clock.Advance(5 * time.Second)
	d.processOnce(context.Background())
	if remote.count() != 1 {
		t.Fatalf("Delivery must not be sent before the backoff deadline")
	}

	// Due again.
	clock.Advance(5 * time.Second)
	d.processOnce(context.Background())

	final := store.get(1)
	if final.Status != model.WebhookDeliveryStatusSucceeded || final.Attempts != 2 {
		t.Fatalf("Expected success on second attempt, got %+v", final)
	}

	if remote.count() != 2 {
		t.Fatalf("Expected 2 requests, got %d", remote.count())
	}

	ids := remote.eventIDList()
	if ids[0] != ids[1] || ids[0] != "event-1" {
		t.Fatalf("Event identifier must remain stable across retries: %v", ids)
	}
}

func TestDispatcherRetrySequenceExhaustionAndRetryAfter(t *testing.T) {
	configureIntegrationAllowPrivateNetworksOption(t)

	clock := newFakeClock()
	store := newFakeDeliveryStore(clock)
	remote := newRemoteServer(t,
		http.StatusServiceUnavailable, // attempt 2 (Retry-After: 30)
		http.StatusTooManyRequests,    // attempt 3 (Retry-After: 30)
		http.StatusInternalServerError, // attempt 4, budget exhausted
	)

	store.settings[1] = &model.Integration{WebhookEnabled: true}
	store.entries[1] = dispatcherEntry(1)

	delivery := pendingDelivery(1, "http://127.0.0.1:1/unreachable", 4)
	store.addDelivery(delivery)

	d := newTestDispatcher(store, clock, 10*time.Second, 80*time.Second, 2, 0)

	// Attempt 1: transport error (no response) -> exponential backoff 10s.
	d.processOnce(context.Background())
	d1 := store.get(1)
	if d1.Status != model.WebhookDeliveryStatusRetryWaiting || d1.LastHTTPStatus != nil {
		t.Fatalf("Expected retry_waiting without HTTP status after transport error, got %+v", d1)
	}

	if d1.NextAttemptAt != clock.Now().Add(10*time.Second) {
		t.Fatalf("Attempt 1 should schedule +10s, got %s", d1.NextAttemptAt)
	}

	// Attempt 2: 503 with Retry-After: 30.
	store.deliveries[1].WebhookURL = remote.URL
	clock.Advance(10 * time.Second)
	d.processOnce(context.Background())
	d2 := store.get(1)
	if d2.Status != model.WebhookDeliveryStatusRetryWaiting || d2.Attempts != 2 {
		t.Fatalf("Expected retry_waiting at attempt 2, got %+v", d2)
	}

	if d2.NextAttemptAt != clock.Now().Add(30*time.Second) {
		t.Fatalf("Retry-After should win (30s), got %s", d2.NextAttemptAt)
	}

	// Attempt 3: 429 with Retry-After: 30 again.
	clock.Advance(30 * time.Second)
	d.processOnce(context.Background())
	d3 := store.get(1)
	if d3.Status != model.WebhookDeliveryStatusRetryWaiting || d3.Attempts != 3 {
		t.Fatalf("Expected retry_waiting at attempt 3, got %+v", d3)
	}

	if d3.NextAttemptAt != clock.Now().Add(30*time.Second) {
		t.Fatalf("Retry-After should win again (30s), got %s", d3.NextAttemptAt)
	}

	// Attempt 4: 500 with attempts == max_attempts -> terminal failure.
	clock.Advance(30 * time.Second)
	d.processOnce(context.Background())
	d4 := store.get(1)
	if d4.Status != model.WebhookDeliveryStatusFailed || d4.Attempts != 4 {
		t.Fatalf("Expected failed after exhausting 4 attempts, got %+v", d4)
	}

	if d4.LastHTTPStatus == nil || *d4.LastHTTPStatus != http.StatusInternalServerError {
		t.Fatalf("Expected last HTTP status 500, got %+v", d4.LastHTTPStatus)
	}

	requests := remote.count()
	d.processOnce(context.Background())
	if remote.count() != requests {
		t.Fatal("Failed delivery must never be retried automatically")
	}
}

func TestDispatcherPermanentRejectionFailsImmediately(t *testing.T) {
	configureIntegrationAllowPrivateNetworksOption(t)

	clock := newFakeClock()
	store := newFakeDeliveryStore(clock)
	remote := newRemoteServer(t, http.StatusNotFound)

	store.settings[1] = &model.Integration{WebhookEnabled: true}
	store.entries[1] = dispatcherEntry(1)
	store.addDelivery(pendingDelivery(1, remote.URL, 10))

	d := newTestDispatcher(store, clock, 10*time.Second, 80*time.Second, 2, 0)
	d.processOnce(context.Background())

	final := store.get(1)
	if final.Status != model.WebhookDeliveryStatusFailed || final.Attempts != 1 {
		t.Fatalf("Expected immediate failure, got %+v", final)
	}

	if final.LastHTTPStatus == nil || *final.LastHTTPStatus != http.StatusNotFound {
		t.Fatalf("Expected 404 recorded, got %+v", final.LastHTTPStatus)
	}

	if !strings.Contains(final.LastError, "404") || !strings.Contains(final.LastError, "remote failure body") {
		t.Fatalf("Failure reason should mention the status and response body, got %q", final.LastError)
	}

	if remote.count() != 1 {
		t.Fatalf("Permanent rejection must be sent exactly once, got %d", remote.count())
	}
}

func TestDispatcherPausesWhileWebhookDisabled(t *testing.T) {
	configureIntegrationAllowPrivateNetworksOption(t)

	clock := newFakeClock()
	store := newFakeDeliveryStore(clock)
	remote := newRemoteServer(t, http.StatusOK)

	store.settings[1] = &model.Integration{WebhookEnabled: false}
	store.entries[1] = dispatcherEntry(1)
	store.addDelivery(pendingDelivery(1, remote.URL, 3))

	d := newTestDispatcher(store, clock, 10*time.Second, 80*time.Second, 2, 0)
	d.processOnce(context.Background())

	final := store.get(1)
	if final.Status != model.WebhookDeliveryStatusInFlight {
		t.Fatalf("Delivery should pause in_flight while disabled, got %q", final.Status)
	}

	if remote.count() != 0 {
		t.Fatalf("No request should be sent while webhook is disabled, got %d", remote.count())
	}

	// Re-enabling and reclaiming after the lease resumes delivery with the
	// same event identifier.
	store.settings[1].WebhookEnabled = true
	past := clock.Now().Add(-10 * time.Minute)
	store.deliveries[1].ClaimedAt = &past

	d.processOnce(context.Background())

	resumed := store.get(1)
	if resumed.Status != model.WebhookDeliveryStatusSucceeded {
		t.Fatalf("Expected resumed success, got %q", resumed.Status)
	}

	ids := remote.eventIDList()
	if len(ids) != 1 || ids[0] != "event-1" {
		t.Fatalf("Takeover must reuse the original event id, got %v", ids)
	}
}

func TestDispatcherReclaimsStaleInFlightWithSameEventID(t *testing.T) {
	configureIntegrationAllowPrivateNetworksOption(t)

	clock := newFakeClock()
	store := newFakeDeliveryStore(clock)
	remote := newRemoteServer(t, http.StatusOK)

	store.settings[1] = &model.Integration{WebhookEnabled: true}
	store.entries[1] = dispatcherEntry(1)

	// Simulate a process that died while the HTTP request was in flight.
	stale := pendingDelivery(1, remote.URL, 5)
	stale.Status = model.WebhookDeliveryStatusInFlight
	stale.Attempts = 1
	claimedAt := clock.Now().Add(-10 * time.Minute)
	stale.ClaimedAt = &claimedAt
	stale.LastAttemptAt = &claimedAt
	store.addDelivery(stale)

	d := newTestDispatcher(store, clock, 10*time.Second, 80*time.Second, 2, 0)
	d.processOnce(context.Background())

	final := store.get(1)
	if final.Status != model.WebhookDeliveryStatusSucceeded || final.Attempts != 2 {
		t.Fatalf("Expected takeover success with incremented attempts, got %+v", final)
	}

	ids := remote.eventIDList()
	if len(ids) != 1 || ids[0] != "event-1" {
		t.Fatalf("Takeover must reuse the same event id, got %v", ids)
	}
}

func TestDispatcherFailsWhenEntryDisappeared(t *testing.T) {
	configureIntegrationAllowPrivateNetworksOption(t)

	clock := newFakeClock()
	store := newFakeDeliveryStore(clock)

	store.settings[1] = &model.Integration{WebhookEnabled: true}
	store.addDelivery(pendingDelivery(1, "https://hook.example/x", 3))

	d := newTestDispatcher(store, clock, 10*time.Second, 80*time.Second, 2, 0)
	d.processOnce(context.Background())

	final := store.get(1)
	if final.Status != model.WebhookDeliveryStatusFailed {
		t.Fatalf("Expected failed when the entry disappeared, got %q", final.Status)
	}
}

func TestDispatcherKeepsInFlightWhenSettingsUnavailable(t *testing.T) {
	configureIntegrationAllowPrivateNetworksOption(t)

	clock := newFakeClock()
	store := newFakeDeliveryStore(clock)
	store.integrationErr = fmt.Errorf("database is down")
	store.entries[1] = dispatcherEntry(1)
	store.addDelivery(pendingDelivery(1, "https://hook.example/x", 3))

	d := newTestDispatcher(store, clock, 10*time.Second, 80*time.Second, 2, 0)
	d.processOnce(context.Background())

	// The row is claimed (in_flight) but the outcome write must be skipped so
	// lease-based reclaim can retry after the database recovers.
	final := store.get(1)
	if final.Status != model.WebhookDeliveryStatusInFlight {
		t.Fatalf("Expected the record to stay in_flight, got %q", final.Status)
	}
}

func TestDispatcherWakesOnNotify(t *testing.T) {
	configureIntegrationAllowPrivateNetworksOption(t)

	clock := newFakeClock()
	store := newFakeDeliveryStore(clock)
	remote := newRemoteServer(t, http.StatusOK)

	store.settings[1] = &model.Integration{WebhookEnabled: true}
	store.entries[1] = dispatcherEntry(1)

	d := &Dispatcher{
		store:             store,
		pollInterval:      time.Hour,
		lease:             2 * time.Minute,
		initialBackoff:    10 * time.Second,
		maxBackoff:        80 * time.Second,
		backoffMultiplier: 2,
		batchSize:         4,
		clock:             clock,
		kick:              make(chan struct{}, 1),
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go d.Run(ctx)

	// Wait for the startup sweep to settle, then enqueue and notify.
	time.Sleep(50 * time.Millisecond)
	store.addDelivery(pendingDelivery(1, remote.URL, 3))
	NotifyPending()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if store.get(1).Status == model.WebhookDeliveryStatusSucceeded {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	if store.get(1).Status != model.WebhookDeliveryStatusSucceeded {
		t.Fatalf("Delivery was not dispatched after NotifyPending: %+v", store.get(1))
	}
}
