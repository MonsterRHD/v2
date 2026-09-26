// SPDX-FileCopyrightText: Copyright The Miniflux Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package storage

import (
	"database/sql"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/lib/pq"

	"miniflux.app/v2/internal/database"
	"miniflux.app/v2/internal/model"
)

func newWebhookDeliveryTestStorage(t *testing.T) (*Storage, *sql.DB) {
	t.Helper()

	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		t.Skip("DATABASE_URL not set; skipping PostgreSQL-backed webhook delivery tests")
	}

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatalf("Unable to open database connection: %v", err)
	}

	if err := db.Ping(); err != nil {
		db.Close()
		t.Skipf("PostgreSQL is not available (%v); skipping webhook delivery tests", err)
	}

	if err := database.Migrate(db); err != nil {
		db.Close()
		t.Fatalf("Unable to run database migrations: %v", err)
	}

	// Registered first so that fixture cleanups run before the connection
	// is closed (t.Cleanup callbacks are executed in LIFO order).
	t.Cleanup(func() {
		db.Close()
	})

	return NewStorage(db), db
}

// webhookDeliveryFixture holds the IDs required to create delivery records.
type webhookDeliveryFixture struct {
	userID     int64
	categoryID int64
	feedID     int64
	entryID    int64
}

func createWebhookDeliveryFixture(t *testing.T, db *sql.DB, suffix string) webhookDeliveryFixture {
	t.Helper()
	return createWebhookDeliveryFixtureWithWebhook(t, db, suffix, true)
}

func createWebhookDeliveryFixtureWithWebhook(t *testing.T, db *sql.DB, suffix string, webhookEnabled bool) webhookDeliveryFixture {
	t.Helper()

	var fixture webhookDeliveryFixture

	username := fmt.Sprintf("wh_delivery_test_%s_%d", suffix, time.Now().UnixNano())
	if err := db.QueryRow(`INSERT INTO users (username) VALUES ($1) RETURNING id`, username).Scan(&fixture.userID); err != nil {
		t.Fatalf("Unable to create user: %v", err)
	}

	t.Cleanup(func() {
		if _, err := db.Exec(`DELETE FROM users WHERE id = $1`, fixture.userID); err != nil {
			t.Errorf("Unable to delete fixture user: %v", err)
		}
	})

	if _, err := db.Exec(`INSERT INTO integrations (user_id, webhook_enabled) VALUES ($1, $2)`, fixture.userID, webhookEnabled); err != nil {
		t.Fatalf("Unable to create integration row: %v", err)
	}

	if err := db.QueryRow(`INSERT INTO categories (user_id, title) VALUES ($1, $2) RETURNING id`, fixture.userID, "Test category").Scan(&fixture.categoryID); err != nil {
		t.Fatalf("Unable to create category: %v", err)
	}

	if err := db.QueryRow(`
		INSERT INTO feeds
			(user_id, category_id, title, feed_url, site_url)
		VALUES
			($1, $2, $3, $4, $5)
		RETURNING id`,
		fixture.userID, fixture.categoryID, "Test feed", "https://example.com/feed.xml", "https://example.com/",
	).Scan(&fixture.feedID); err != nil {
		t.Fatalf("Unable to create feed: %v", err)
	}

	if err := db.QueryRow(`
		INSERT INTO entries
			(user_id, feed_id, hash, published_at, changed_at, created_at, title, url, status, content)
		VALUES
			($1, $2, $3, now(), now(), now(), $4, $5, 'unread', '')
		RETURNING id`,
		fixture.userID, fixture.feedID, fmt.Sprintf("hash-%d", time.Now().UnixNano()), "Test entry", "https://example.com/article.html",
	).Scan(&fixture.entryID); err != nil {
		t.Fatalf("Unable to create entry: %v", err)
	}

	return fixture
}

func TestWebhookDeliveryCreateIdempotentAndQueries(t *testing.T) {
	store, db := newWebhookDeliveryTestStorage(t)

	fixture := createWebhookDeliveryFixture(t, db, "create")

	delivery, err := store.CreateWebhookSaveDelivery(fixture.userID, fixture.entryID, "https://hook.example/test", 5)
	if err != nil {
		t.Fatalf("Unable to create webhook delivery: %v", err)
	}

	if delivery.Status != model.WebhookDeliveryStatusPending || delivery.Attempts != 0 || delivery.MaxAttempts != 5 {
		t.Fatalf("Unexpected initial delivery: %+v", delivery)
	}

	if delivery.EventID == "" || delivery.NextAttemptAt.IsZero() {
		t.Fatalf("Event id and next attempt time must be set: %+v", delivery)
	}

	// Duplicate save returns the same active record.
	duplicate, err := store.CreateWebhookSaveDelivery(fixture.userID, fixture.entryID, "https://hook.example/other", 9)
	if err != nil {
		t.Fatalf("Unable to create duplicate webhook delivery: %v", err)
	}

	if duplicate.ID != delivery.ID || duplicate.EventID != delivery.EventID {
		t.Fatalf("Duplicate save must reuse the active delivery, got %+v vs %+v", duplicate, delivery)
	}

	if duplicate.WebhookURL != "https://hook.example/test" {
		t.Fatalf("Webhook URL snapshot must not be overwritten, got %q", duplicate.WebhookURL)
	}

	single, err := store.WebhookDeliveryByEntry(fixture.userID, fixture.entryID)
	if err != nil {
		t.Fatalf("Unable to fetch delivery by entry: %v", err)
	}

	if single == nil || single.ID != delivery.ID {
		t.Fatalf("WebhookDeliveryByEntry returned %+v", single)
	}

	batch, err := store.WebhookDeliveriesByEntries(fixture.userID, []int64{fixture.entryID, fixture.entryID + 999999})
	if err != nil {
		t.Fatalf("Unable to fetch deliveries by entries: %v", err)
	}

	if len(batch) != 1 || batch[fixture.entryID].ID != delivery.ID {
		t.Fatalf("Batch query returned unexpected map: %+v", batch)
	}

	emptyBatch, err := store.WebhookDeliveriesByEntries(fixture.userID, nil)
	if err != nil || len(emptyBatch) != 0 {
		t.Fatalf("Empty entry list must return an empty map, got %+v err=%v", emptyBatch, err)
	}
}

func TestWebhookDeliveryCancelOnlyUnsent(t *testing.T) {
	store, db := newWebhookDeliveryTestStorage(t)

	fixture := createWebhookDeliveryFixture(t, db, "cancel")

	// Never-sent pending delivery is canceled on un-save.
	pending, err := store.CreateWebhookSaveDelivery(fixture.userID, fixture.entryID, "https://hook.example/cancel", 3)
	if err != nil {
		t.Fatalf("Unable to create pending delivery: %v", err)
	}

	if err := store.SetEntriesStarredStateAndWebhookDeliveries(fixture.userID, []int64{fixture.entryID}, false, nil, 3); err != nil {
		t.Fatalf("Unable to cancel unsent deliveries: %v", err)
	}

	canceled, err := store.WebhookDeliveryByEntry(fixture.userID, fixture.entryID)
	if err != nil {
		t.Fatalf("Unable to fetch canceled delivery: %v", err)
	}

	if canceled.Status != model.WebhookDeliveryStatusCanceled {
		t.Fatalf("Pending delivery should be canceled, got %q", canceled.Status)
	}

	// A new save after cancellation creates a brand new record/event id.
	requeued, err := store.CreateWebhookSaveDelivery(fixture.userID, fixture.entryID, "https://hook.example/cancel", 3)
	if err != nil {
		t.Fatalf("Unable to re-create delivery after cancellation: %v", err)
	}

	if requeued.ID == pending.ID || requeued.EventID == pending.EventID {
		t.Fatalf("A new save after cancellation must create a new event id")
	}

	// Records already on the wire must not be canceled.
	for _, status := range []string{model.WebhookDeliveryStatusInFlight, model.WebhookDeliveryStatusRetryWaiting, model.WebhookDeliveryStatusSucceeded} {
		if _, err := db.Exec(`UPDATE webhook_save_deliveries SET status = $2, attempts = 1 WHERE id = $1`, requeued.ID, status); err != nil {
			t.Fatalf("Unable to force status %s: %v", status, err)
		}

		if err := store.SetEntriesStarredStateAndWebhookDeliveries(fixture.userID, []int64{fixture.entryID}, false, nil, 3); err != nil {
			t.Fatalf("Cancel call failed for status %s: %v", status, err)
		}

		after, err := store.WebhookDeliveryByEntry(fixture.userID, fixture.entryID)
		if err != nil {
			t.Fatalf("Unable to refetch delivery: %v", err)
		}

		if after.Status != status {
			t.Fatalf("Delivery in status %s must not be canceled, got %q", status, after.Status)
		}
	}
}

func TestWebhookDeliveryStarredTransactionRollback(t *testing.T) {
	store, db := newWebhookDeliveryTestStorage(t)

	fixture := createWebhookDeliveryFixture(t, db, "rollback")

	// A foreign-key violation on the delivery insert must roll back the
	// starred state update as well.
	unknownEntryID := fixture.entryID + 9_999_999
	err := store.SetEntriesStarredStateAndWebhookDeliveries(
		fixture.userID,
		[]int64{fixture.entryID, unknownEntryID},
		true,
		map[int64]string{fixture.entryID: "https://hook.example/tx", unknownEntryID: "https://hook.example/tx"},
		3,
	)
	if err == nil {
		t.Fatal("Expected a transaction error, got nil")
	}

	var starred bool
	if err := db.QueryRow(`SELECT starred FROM entries WHERE id = $1`, fixture.entryID).Scan(&starred); err != nil {
		t.Fatalf("Unable to check starred state: %v", err)
	}

	if starred {
		t.Fatal("Starred state update must have been rolled back")
	}

	batch, err := store.WebhookDeliveriesByEntries(fixture.userID, []int64{fixture.entryID})
	if err != nil {
		t.Fatalf("Unable to check deliveries: %v", err)
	}

	if len(batch) != 0 {
		t.Fatalf("No delivery row should survive the rollback, got %d", len(batch))
	}
}

func TestWebhookDeliveryStarredTransactionCommit(t *testing.T) {
	store, db := newWebhookDeliveryTestStorage(t)

	fixture := createWebhookDeliveryFixture(t, db, "commit")

	webhookURLs := map[int64]string{fixture.entryID: "https://hook.example/star"}
	if err := store.SetEntriesStarredStateAndWebhookDeliveries(fixture.userID, []int64{fixture.entryID}, true, webhookURLs, 4); err != nil {
		t.Fatalf("Unable to star and enqueue: %v", err)
	}

	var starred bool
	if err := db.QueryRow(`SELECT starred FROM entries WHERE id = $1`, fixture.entryID).Scan(&starred); err != nil {
		t.Fatalf("Unable to check starred state: %v", err)
	}

	if !starred {
		t.Fatal("Entry should be starred")
	}

	delivery, err := store.WebhookDeliveryByEntry(fixture.userID, fixture.entryID)
	if err != nil {
		t.Fatalf("Unable to fetch delivery: %v", err)
	}

	if delivery == nil || delivery.Status != model.WebhookDeliveryStatusPending || delivery.MaxAttempts != 4 {
		t.Fatalf("Unexpected delivery after starring: %+v", delivery)
	}
}

func TestWebhookDeliveryResetFailedKeepsEventID(t *testing.T) {
	store, db := newWebhookDeliveryTestStorage(t)

	fixture := createWebhookDeliveryFixture(t, db, "reset")

	delivery, err := store.CreateWebhookSaveDelivery(fixture.userID, fixture.entryID, "https://hook.example/reset", 2)
	if err != nil {
		t.Fatalf("Unable to create delivery: %v", err)
	}

	// Reset is rejected while the record is not failed.
	if _, err := store.ResetFailedWebhookDelivery(fixture.userID, fixture.entryID); err != ErrWebhookDeliveryNotFailed {
		t.Fatalf("Expected ErrWebhookDeliveryNotFailed, got %v", err)
	}

	if _, err := db.Exec(`UPDATE webhook_save_deliveries SET status = 'failed', attempts = 2, last_http_status = 404, last_error = 'nope' WHERE id = $1`, delivery.ID); err != nil {
		t.Fatalf("Unable to force failed status: %v", err)
	}

	reset, err := store.ResetFailedWebhookDelivery(fixture.userID, fixture.entryID)
	if err != nil {
		t.Fatalf("Unable to reset failed delivery: %v", err)
	}

	if reset.EventID != delivery.EventID {
		t.Fatalf("Event identifier must be preserved on manual retry")
	}

	if reset.Status != model.WebhookDeliveryStatusPending || reset.Attempts != 0 || reset.LastError != "" || reset.LastHTTPStatus != nil {
		t.Fatalf("Unexpected reset delivery: %+v", reset)
	}

	// Missing record yields the not-found sentinel error.
	if _, err := store.ResetFailedWebhookDelivery(fixture.userID, fixture.entryID+9_999_999); err != ErrWebhookDeliveryNotFound {
		t.Fatalf("Expected ErrWebhookDeliveryNotFound, got %v", err)
	}
}

func TestWebhookDeliveryClaimConcurrencyAndReclaim(t *testing.T) {
	store, db := newWebhookDeliveryTestStorage(t)

	const nbDeliveries = 25
	ownedIDs := make(map[int64]bool)
	eventIDsBeforeReclaim := make(map[int64]string)

	for range nbDeliveries {
		fixture := createWebhookDeliveryFixture(t, db, "claim")
		delivery, err := store.CreateWebhookSaveDelivery(fixture.userID, fixture.entryID, "https://hook.example/claim", 3)
		if err != nil {
			t.Fatalf("Unable to create delivery: %v", err)
		}

		ownedIDs[delivery.ID] = true
	}

	// Multiple concurrent claimers must never grab the same record.
	var mu sync.Mutex
	claimedIDs := make(map[int64]int)

	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for {
				deliveries, err := store.ClaimDueWebhookDeliveries(3)
				if err != nil {
					t.Errorf("Claim failed: %v", err)
					return
				}

				if len(deliveries) == 0 {
					return
				}

				mu.Lock()
				for _, delivery := range deliveries {
					claimedIDs[delivery.ID]++
				}
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	// Assertions are scoped to the records created by this test so the suite
	// stays reliable against a shared, possibly non-empty scratch database.
	for id := range ownedIDs {
		if claimedIDs[id] != 1 {
			t.Fatalf("Delivery #%d was claimed %d times, want exactly 1", id, claimedIDs[id])
		}
	}

	// All claimed records are in_flight with attempts=1.
	var inFlight, attempts int
	if err := db.QueryRow(`SELECT count(*), coalesce(max(attempts), 0) FROM webhook_save_deliveries WHERE status = 'in_flight' AND id = ANY($1)`, pq.Array(ownedIDList(ownedIDs))).Scan(&inFlight, &attempts); err != nil {
		t.Fatalf("Unable to count in-flight deliveries: %v", err)
	}

	if inFlight != nbDeliveries || attempts != 1 {
		t.Fatalf("Expected %d in-flight deliveries with 1 attempt, got %d with max attempts %d", nbDeliveries, inFlight, attempts)
	}

	// Future rows are not claimable.
	futureFixture := createWebhookDeliveryFixture(t, db, "future")
	future, err := store.CreateWebhookSaveDelivery(futureFixture.userID, futureFixture.entryID, "https://hook.example/future", 3)
	if err != nil {
		t.Fatalf("Unable to create future delivery: %v", err)
	}

	if _, err := db.Exec(`UPDATE webhook_save_deliveries SET next_attempt_at = now() + interval '1 hour' WHERE id = $1`, future.ID); err != nil {
		t.Fatalf("Unable to postpone delivery: %v", err)
	}

	due, err := store.ClaimDueWebhookDeliveries(10)
	if err != nil {
		t.Fatalf("Unexpected claim error: %v", err)
	}

	for _, delivery := range due {
		if delivery.ID == future.ID {
			t.Fatal("A future delivery must not be claimed")
		}
	}

	// Capture event identifiers before the crash-takeover cycle.
	rows, err := db.Query(`SELECT id, event_id FROM webhook_save_deliveries WHERE id = ANY($1)`, pq.Array(ownedIDList(ownedIDs)))
	if err != nil {
		t.Fatalf("Unable to read event ids: %v", err)
	}

	for rows.Next() {
		var id int64
		var eventID string
		if err := rows.Scan(&id, &eventID); err != nil {
			t.Fatalf("Unable to scan event id: %v", err)
		}

		eventIDsBeforeReclaim[id] = eventID
	}
	rows.Close()

	// Expired in-flight records are reclaimed after the lease with the same
	// event identifier.
	time.Sleep(10 * time.Millisecond)
	if _, err := store.ReclaimExpiredInFlightWebhookDeliveries(time.Nanosecond); err != nil {
		t.Fatalf("Unable to reclaim expired deliveries: %v", err)
	}

	var reclaimed int
	if err := db.QueryRow(`SELECT count(*) FROM webhook_save_deliveries WHERE status = 'retry_waiting' AND id = ANY($1)`, pq.Array(ownedIDList(ownedIDs))).Scan(&reclaimed); err != nil {
		t.Fatalf("Unable to count reclaimed deliveries: %v", err)
	}

	if reclaimed != nbDeliveries {
		t.Fatalf("Expected %d reclaimed deliveries, got %d", nbDeliveries, reclaimed)
	}

	rows, err = db.Query(`SELECT id, event_id FROM webhook_save_deliveries WHERE id = ANY($1)`, pq.Array(ownedIDList(ownedIDs)))
	if err != nil {
		t.Fatalf("Unable to re-read event ids: %v", err)
	}

	for rows.Next() {
		var id int64
		var eventID string
		if err := rows.Scan(&id, &eventID); err != nil {
			t.Fatalf("Unable to scan event id: %v", err)
		}

		if eventIDsBeforeReclaim[id] != eventID {
			t.Fatalf("Event identifier changed after reclaim for delivery #%d", id)
		}
	}
	rows.Close()

	after, err := store.WebhookDeliveryByEntry(futureFixture.userID, futureFixture.entryID)
	if err != nil {
		t.Fatalf("Unable to fetch future delivery: %v", err)
	}

	if after.Status != model.WebhookDeliveryStatusPending {
		t.Fatalf("Future delivery should stay pending, got %q", after.Status)
	}
}

func ownedIDList(ids map[int64]bool) []int64 {
	result := make([]int64, 0, len(ids))
	for id := range ids {
		result = append(result, id)
	}

	return result
}

func TestWebhookDeliveryMarkConditionalAndCleanup(t *testing.T) {
	store, db := newWebhookDeliveryTestStorage(t)

	fixture := createWebhookDeliveryFixture(t, db, "mark")

	delivery, err := store.CreateWebhookSaveDelivery(fixture.userID, fixture.entryID, "https://hook.example/mark", 1)
	if err != nil {
		t.Fatalf("Unable to create delivery: %v", err)
	}

	// Outcomes cannot be recorded before the record is claimed.
	if rows, err := store.MarkWebhookDeliverySucceeded(delivery.ID, 200); err != nil || rows != 0 {
		t.Fatalf("Succeeded before claim should affect 0 rows, got %d err=%v", rows, err)
	}

	claimed, err := store.ClaimDueWebhookDeliveries(10)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("Unable to claim the delivery: %+v err=%v", claimed, err)
	}

	statusCode := 500
	if rows, err := store.MarkWebhookDeliveryWaitingRetry(delivery.ID, &statusCode, "temporary", time.Now().Add(time.Minute)); err != nil || rows != 1 {
		t.Fatalf("Waiting retry should affect 1 row, got %d err=%v", rows, err)
	}

	refreshed, err := store.WebhookDeliveryByEntry(fixture.userID, fixture.entryID)
	if err != nil {
		t.Fatalf("Unable to refresh delivery: %v", err)
	}

	if refreshed.Status != model.WebhookDeliveryStatusRetryWaiting || refreshed.LastHTTPStatus == nil || *refreshed.LastHTTPStatus != 500 {
		t.Fatalf("Unexpected refreshed delivery: %+v", refreshed)
	}

	// Force it back to in_flight and record the terminal success.
	if _, err := db.Exec(`UPDATE webhook_save_deliveries SET status = 'in_flight' WHERE id = $1`, delivery.ID); err != nil {
		t.Fatalf("Unable to force in_flight: %v", err)
	}

	if rows, err := store.MarkWebhookDeliverySucceeded(delivery.ID, 204); err != nil || rows != 1 {
		t.Fatalf("Succeeded should affect 1 row, got %d err=%v", rows, err)
	}

	// Late outcomes cannot overwrite the terminal status.
	if rows, err := store.MarkWebhookDeliveryFailed(delivery.ID, nil, "late"); err != nil || rows != 0 {
		t.Fatalf("Late failure should affect 0 rows, got %d err=%v", rows, err)
	}

	if rows, err := store.MarkWebhookDeliverySucceeded(delivery.ID, 200); err != nil || rows != 0 {
		t.Fatalf("Second success should affect 0 rows, got %d err=%v", rows, err)
	}

	// Only terminal records older than the cutoff are cleaned.
	if _, err := db.Exec(`UPDATE webhook_save_deliveries SET updated_at = now() - interval '31 days' WHERE id = $1`, delivery.ID); err != nil {
		t.Fatalf("Unable to backdate succeeded delivery: %v", err)
	}

	oldFixture := createWebhookDeliveryFixture(t, db, "cleanup_failed")
	oldFailed, err := store.CreateWebhookSaveDelivery(oldFixture.userID, oldFixture.entryID, "https://hook.example/old", 1)
	if err != nil {
		t.Fatalf("Unable to create old failed delivery: %v", err)
	}

	if _, err := db.Exec(`UPDATE webhook_save_deliveries SET status = 'failed', updated_at = now() - interval '31 days' WHERE id = $1`, oldFailed.ID); err != nil {
		t.Fatalf("Unable to backdate failed delivery: %v", err)
	}

	recentFixture := createWebhookDeliveryFixture(t, db, "cleanup_recent")
	recent, err := store.CreateWebhookSaveDelivery(recentFixture.userID, recentFixture.entryID, "https://hook.example/recent", 1)
	if err != nil {
		t.Fatalf("Unable to create recent terminal delivery: %v", err)
	}

	if _, err := db.Exec(`UPDATE webhook_save_deliveries SET status = 'canceled' WHERE id = $1`, recent.ID); err != nil {
		t.Fatalf("Unable to mark recent delivery canceled: %v", err)
	}

	pendingFixture := createWebhookDeliveryFixture(t, db, "cleanup_pending")
	pending, err := store.CreateWebhookSaveDelivery(pendingFixture.userID, pendingFixture.entryID, "https://hook.example/pending", 1)
	if err != nil {
		t.Fatalf("Unable to create pending delivery: %v", err)
	}

	removed, err := store.CleanOldWebhookDeliveries(time.Now().Add(-30 * 24 * time.Hour))
	if err != nil {
		t.Fatalf("Cleanup failed: %v", err)
	}

	if removed < 2 {
		t.Fatalf("Expected at least 2 old terminal records removed, got %d", removed)
	}

	if exists, err := recordExists(db, recent.ID); err != nil || !exists {
		t.Fatalf("Recent terminal record should be retained, exists=%v err=%v", exists, err)
	}

	if exists, err := recordExists(db, pending.ID); err != nil || !exists {
		t.Fatalf("Pending record must never be cleaned, exists=%v err=%v", exists, err)
	}

	if exists, err := recordExists(db, delivery.ID); err != nil || exists {
		t.Fatalf("Old succeeded record should be removed, exists=%v err=%v", exists, err)
	}

	if exists, err := recordExists(db, oldFailed.ID); err != nil || exists {
		t.Fatalf("Old failed record should be removed, exists=%v err=%v", exists, err)
	}
}

func TestClaimDueSkipsDeliveriesForDisabledWebhook(t *testing.T) {
	store, db := newWebhookDeliveryTestStorage(t)
	defer db.Close()

	enabled := createWebhookDeliveryFixtureWithWebhook(t, db, "claim_on", true)
	disabled := createWebhookDeliveryFixtureWithWebhook(t, db, "claim_off", false)

	if _, err := store.CreateWebhookSaveDelivery(enabled.userID, enabled.entryID, "https://hook.example/on", 3); err != nil {
		t.Fatalf("Unable to create enabled delivery: %v", err)
	}

	if _, err := store.CreateWebhookSaveDelivery(disabled.userID, disabled.entryID, "https://hook.example/off", 3); err != nil {
		t.Fatalf("Unable to create disabled delivery: %v", err)
	}

	claimed, err := store.ClaimDueWebhookDeliveries(10)
	if err != nil {
		t.Fatalf("Claim failed: %v", err)
	}

	if len(claimed) != 1 || claimed[0].UserID != enabled.userID {
		var ids []int64
		for _, c := range claimed {
			ids = append(ids, c.UserID)
		}

		t.Fatalf("Only the enabled user's delivery should be claimed, got users %v", ids)
	}

	off, err := store.WebhookDeliveryByEntry(disabled.userID, disabled.entryID)
	if err != nil {
		t.Fatalf("Unable to load disabled delivery: %v", err)
	}

	if off.Status != model.WebhookDeliveryStatusPending || off.Attempts != 0 {
		t.Fatalf("Disabled delivery must stay pending with attempts=0, got status=%q attempts=%d", off.Status, off.Attempts)
	}

	// Re-enabling lets the row flow again without any manual intervention.
	if _, err := db.Exec(`UPDATE integrations SET webhook_enabled = true WHERE user_id = $1`, disabled.userID); err != nil {
		t.Fatalf("Unable to re-enable integration: %v", err)
	}

	claimed, err = store.ClaimDueWebhookDeliveries(10)
	if err != nil {
		t.Fatalf("Claim after re-enable failed: %v", err)
	}

	if len(claimed) != 1 || claimed[0].EntryID != disabled.entryID {
		t.Fatalf("The re-enabled delivery should now be claimable, got %+v", claimed)
	}
}

func TestResetFailedWebhookDeliveryOnlyResetsLatest(t *testing.T) {
	store, db := newWebhookDeliveryTestStorage(t)
	defer db.Close()

	fixture := createWebhookDeliveryFixture(t, db, "reset_latest")

	// Two historical failed deliveries for the same entry, then the latest
	// becomes pending via a new save.
	older, err := store.CreateWebhookSaveDelivery(fixture.userID, fixture.entryID, "https://hook.example/1", 2)
	if err != nil {
		t.Fatalf("Unable to create first delivery: %v", err)
	}

	if _, err := db.Exec(`UPDATE webhook_save_deliveries SET status = 'failed' WHERE id = $1`, older.ID); err != nil {
		t.Fatalf("Unable to fail first delivery: %v", err)
	}

	newer, err := store.CreateWebhookSaveDelivery(fixture.userID, fixture.entryID, "https://hook.example/2", 2)
	if err != nil {
		t.Fatalf("Unable to create second delivery: %v", err)
	}

	if _, err := db.Exec(`UPDATE webhook_save_deliveries SET status = 'failed', updated_at = updated_at + interval '1 second' WHERE id = $1`, newer.ID); err != nil {
		t.Fatalf("Unable to fail second delivery: %v", err)
	}

	// Reset affects only the newest failed row; the older one stays failed,
	// so the single-active-delivery constraint is not violated.
	reset, err := store.ResetFailedWebhookDelivery(fixture.userID, fixture.entryID)
	if err != nil {
		t.Fatalf("Reset should not fail with multiple historical failures: %v", err)
	}

	if reset.ID != newer.ID || reset.Status != model.WebhookDeliveryStatusPending {
		t.Fatalf("Only the latest failure should be reset, got %+v", reset)
	}

	olderRow, err := store.WebhookDeliveryByEntry(fixture.userID, fixture.entryID)
	if err != nil {
		t.Fatalf("Unable to fetch latest row: %v", err)
	}

	// Latest row is now the reset pending one (recently updated).
	if olderRow.Status != model.WebhookDeliveryStatusPending {
		t.Fatalf("Latest row should be pending, got %q", olderRow.Status)
	}

	var olderStatus string
	if err := db.QueryRow(`SELECT status FROM webhook_save_deliveries WHERE id = $1`, older.ID).Scan(&olderStatus); err != nil {
		t.Fatalf("Unable to read older row: %v", err)
	}

	if olderStatus != model.WebhookDeliveryStatusFailed {
		t.Fatalf("The older historical failure must stay failed, got %q", olderStatus)
	}

	// While the latest row is pending, retry is rejected as not-failed.
	if _, err := store.ResetFailedWebhookDelivery(fixture.userID, fixture.entryID); err != ErrWebhookDeliveryNotFailed {
		t.Fatalf("Expected ErrWebhookDeliveryNotFailed, got %v", err)
	}
}

func recordExists(db *sql.DB, id int64) (bool, error) {
	var count int
	if err := db.QueryRow(`SELECT count(*) FROM webhook_save_deliveries WHERE id = $1`, id).Scan(&count); err != nil {
		return false, err
	}

	return count == 1, nil
}
