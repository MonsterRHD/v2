// SPDX-FileCopyrightText: Copyright The Miniflux Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package integration

import (
	"sync"
	"testing"
	"time"

	"miniflux.app/v2/internal/config"
	"miniflux.app/v2/internal/model"
)

func configureTestOptions(t *testing.T) {
	t.Helper()

	opts, err := config.NewConfigParser().ParseEnvironmentVariables()
	if err != nil {
		t.Fatalf("Unable to parse test options: %v", err)
	}

	previous := config.Opts
	config.Opts = opts
	t.Cleanup(func() {
		config.Opts = previous
	})
}

type fakeSaveEntryStore struct {
	mu                sync.Mutex
	created           []fakeCreatedDelivery
	syncCalls         []fakeSyncCall
	nextDelivery      *model.WebhookDelivery
	createErr         error
	syncErr           error
}

type fakeCreatedDelivery struct {
	userID      int64
	entryID     int64
	webhookURL  string
	maxAttempts int
}

type fakeSyncCall struct {
	userID      int64
	entryIDs    []int64
	starred     bool
	webhookURLs map[int64]string
	maxAttempts int
}

func (s *fakeSaveEntryStore) CreateWebhookSaveDelivery(userID, entryID int64, webhookURL string, maxAttempts int) (*model.WebhookDelivery, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.created = append(s.created, fakeCreatedDelivery{userID, entryID, webhookURL, maxAttempts})
	if s.createErr != nil {
		return nil, s.createErr
	}

	if s.nextDelivery != nil {
		return s.nextDelivery, nil
	}

	return &model.WebhookDelivery{ID: 1, UserID: userID, EntryID: entryID, WebhookURL: webhookURL, Status: model.WebhookDeliveryStatusPending, EventID: "evt-fake"}, nil
}

func (s *fakeSaveEntryStore) SetEntriesStarredStateAndWebhookDeliveries(userID int64, entryIDs []int64, starred bool, webhookURLs map[int64]string, maxAttempts int) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.syncCalls = append(s.syncCalls, fakeSyncCall{userID, append([]int64(nil), entryIDs...), starred, webhookURLs, maxAttempts})
	return s.syncErr
}

func (s *fakeSaveEntryStore) snapshot() ([]fakeCreatedDelivery, []fakeSyncCall) {
	s.mu.Lock()
	defer s.mu.Unlock()

	created := append([]fakeCreatedDelivery(nil), s.created...)
	syncCalls := append([]fakeSyncCall(nil), s.syncCalls...)
	return created, syncCalls
}

func TestEnqueueSaveEntryWithWebhookCreatesDurableRecord(t *testing.T) {
	store := &fakeSaveEntryStore{}

	entry := &model.Entry{ID: 55, URL: "https://example.com/a", Title: "A"}
	entry.Feed = &model.Feed{WebhookURL: "https://feed.example/hook"}
	settings := &model.Integration{UserID: 9, WebhookEnabled: true, WebhookURL: "https://user.example/hook"}

	delivery, err := EnqueueSaveEntry(store, entry, settings)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if delivery == nil || delivery.EventID != "evt-fake" || delivery.Status != model.WebhookDeliveryStatusPending {
		t.Fatalf("Unexpected delivery: %+v", delivery)
	}

	created, _ := store.snapshot()
	if len(created) != 1 {
		t.Fatalf("Expected exactly one delivery creation, got %d", len(created))
	}

	// Feed-level webhook URL takes precedence over the user-level URL.
	if created[0].webhookURL != "https://feed.example/hook" {
		t.Fatalf("Expected feed webhook URL snapshot, got %q", created[0].webhookURL)
	}

	if created[0].userID != 9 || created[0].entryID != 55 {
		t.Fatalf("Unexpected delivery keys: %+v", created[0])
	}

	// Give the fire-and-forget goroutine a moment to run; with no other
	// integration enabled it must be a harmless no-op.
	time.Sleep(20 * time.Millisecond)
}

func TestEnqueueSaveEntryFallsBackToUserWebhookURL(t *testing.T) {
	store := &fakeSaveEntryStore{}

	entry := &model.Entry{ID: 56, URL: "https://example.com/b", Title: "B"}
	entry.Feed = &model.Feed{}
	settings := &model.Integration{UserID: 9, WebhookEnabled: true, WebhookURL: "https://user.example/hook"}

	if _, err := EnqueueSaveEntry(store, entry, settings); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	created, _ := store.snapshot()
	if len(created) != 1 || created[0].webhookURL != "https://user.example/hook" {
		t.Fatalf("Expected user webhook URL, got %+v", created)
	}
}

func TestEnqueueSaveEntryWithoutWebhookDoesNotPersist(t *testing.T) {
	store := &fakeSaveEntryStore{}

	entry := &model.Entry{ID: 57, URL: "https://example.com/c", Title: "C"}
	settings := &model.Integration{UserID: 9, WebhookEnabled: false}

	delivery, err := EnqueueSaveEntry(store, entry, settings)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if delivery != nil {
		t.Fatalf("No delivery should be returned when webhook is disabled")
	}

	created, _ := store.snapshot()
	if len(created) != 0 {
		t.Fatalf("No delivery row should be created when webhook is disabled, got %+v", created)
	}
}

func TestSyncStarredSaveEntriesEnqueuesOnStar(t *testing.T) {
	store := &fakeSaveEntryStore{}

	entry1 := &model.Entry{ID: 1, Title: "1"}
	entry1.Feed = &model.Feed{}
	entry2 := &model.Entry{ID: 2, Title: "2"}
	entry2.Feed = &model.Feed{WebhookURL: "https://feed.example/2"}

	settings := &model.Integration{UserID: 3, WebhookEnabled: true, WebhookURL: "https://user.example/hook"}

	if err := SyncStarredSaveEntries(store, model.Entries{entry1, entry2}, true, settings); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	_, syncCalls := store.snapshot()
	if len(syncCalls) != 1 {
		t.Fatalf("Expected one transactional sync call, got %d", len(syncCalls))
	}

	call := syncCalls[0]
	if !call.starred || call.userID != 3 || len(call.entryIDs) != 2 {
		t.Fatalf("Unexpected sync call: %+v", call)
	}

	if call.webhookURLs[1] != "https://user.example/hook" || call.webhookURLs[2] != "https://feed.example/2" {
		t.Fatalf("Unexpected per-entry webhook URLs: %+v", call.webhookURLs)
	}

	// Other integrations are triggered asynchronously; wait briefly to ensure
	// no panic from the fan-out goroutines.
	time.Sleep(20 * time.Millisecond)
}

func TestSyncStarredSaveEntriesCancelsOnUnstar(t *testing.T) {
	store := &fakeSaveEntryStore{}

	entry := &model.Entry{ID: 1, Title: "1"}
	entry.Feed = &model.Feed{}
	settings := &model.Integration{UserID: 3, WebhookEnabled: true, WebhookURL: "https://user.example/hook"}

	if err := SyncStarredSaveEntries(store, model.Entries{entry}, false, settings); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	_, syncCalls := store.snapshot()
	if len(syncCalls) != 1 || syncCalls[0].starred || len(syncCalls[0].webhookURLs) != 0 {
		t.Fatalf("Expected an unstar sync without enqueued URLs, got %+v", syncCalls)
	}
}
