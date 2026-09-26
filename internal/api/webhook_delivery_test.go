// SPDX-FileCopyrightText: Copyright The Miniflux Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package api

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	_ "github.com/lib/pq"

	"miniflux.app/v2/internal/config"
	"miniflux.app/v2/internal/database"
	"miniflux.app/v2/internal/http/request"
	"miniflux.app/v2/internal/storage"
)

type apiFixture struct {
	userID  int64
	entryID int64
}

func newWebhookAPITestStore(t *testing.T) (*storage.Storage, *sql.DB) {
	t.Helper()

	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		t.Skip("DATABASE_URL not set; skipping PostgreSQL-backed webhook API tests")
	}

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatalf("Unable to open database connection: %v", err)
	}

	if err := db.Ping(); err != nil {
		db.Close()
		t.Skipf("PostgreSQL is not available (%v); skipping webhook API tests", err)
	}

	if err := database.Migrate(db); err != nil {
		db.Close()
		t.Fatalf("Unable to run database migrations: %v", err)
	}

	t.Cleanup(func() { db.Close() })

	return storage.NewStorage(db), db
}

func createAPIFixture(t *testing.T, db *sql.DB) apiFixture {
	t.Helper()

	var fixture apiFixture

	username := fmt.Sprintf("wh_api_test_%d", time.Now().UnixNano())
	if err := db.QueryRow(`INSERT INTO users (username) VALUES ($1) RETURNING id`, username).Scan(&fixture.userID); err != nil {
		t.Fatalf("Unable to create user: %v", err)
	}

	t.Cleanup(func() {
		if _, err := db.Exec(`DELETE FROM users WHERE id = $1`, fixture.userID); err != nil {
			t.Errorf("Unable to delete fixture user: %v", err)
		}
	})

	if _, err := db.Exec(`INSERT INTO categories (user_id, title) VALUES ($1, 'Cat')`, fixture.userID); err != nil {
		t.Fatalf("Unable to create category: %v", err)
	}

	var categoryID int64
	if err := db.QueryRow(`SELECT id FROM categories WHERE user_id = $1`, fixture.userID).Scan(&categoryID); err != nil {
		t.Fatalf("Unable to load category: %v", err)
	}

	var feedID int64
	if err := db.QueryRow(`
		INSERT INTO feeds (user_id, category_id, title, feed_url, site_url)
		VALUES ($1, $2, 'Feed', 'https://example.com/feed.xml', 'https://example.com/')
		RETURNING id`,
		fixture.userID, categoryID,
	).Scan(&feedID); err != nil {
		t.Fatalf("Unable to create feed: %v", err)
	}

	if err := db.QueryRow(`
		INSERT INTO entries
			(user_id, feed_id, hash, published_at, changed_at, created_at, title, url, status, content, author, comments_url, share_code)
		VALUES
			($1, $2, $3, now(), now(), now(), 'Article', 'https://example.com/article', 'unread', '', '', '', '')
		RETURNING id`,
		fixture.userID, feedID, fmt.Sprintf("hash-%d", time.Now().UnixNano()),
	).Scan(&fixture.entryID); err != nil {
		t.Fatalf("Unable to create entry: %v", err)
	}

	return fixture
}

func (h *handler) webhookRetryRequest(userID, entryID int64) *httptest.ResponseRecorder {
	r := httptest.NewRequest(http.MethodPost, fmt.Sprintf("/v1/entries/%d/webhook-delivery/retry", entryID), nil)
	r = r.WithContext(context.WithValue(r.Context(), request.UserIDContextKey, userID))
	r.SetPathValue("entryID", fmt.Sprintf("%d", entryID))

	w := httptest.NewRecorder()
	h.retryWebhookDeliveryHandler(w, r)
	return w
}

func insertWebhookDelivery(t *testing.T, db *sql.DB, userID, entryID int64, status string) string {
	t.Helper()

	eventID := fmt.Sprintf("evt-%d", time.Now().UnixNano())
	_, err := db.Exec(`
		INSERT INTO webhook_save_deliveries
			(event_id, user_id, entry_id, webhook_url, status, attempts, max_attempts)
		VALUES
			($1, $2, $3, 'https://hook.example/x', $4, CASE WHEN $4 = 'pending' THEN 0 ELSE 2 END, 5)`,
		eventID, userID, entryID, status)
	if err != nil {
		t.Fatalf("Unable to insert %s delivery: %v", status, err)
	}

	return eventID
}

func TestRetryWebhookDeliveryHandler(t *testing.T) {
	store, db := newWebhookAPITestStore(t)
	fixture := createAPIFixture(t, db)
	eventID := insertWebhookDelivery(t, db, fixture.userID, fixture.entryID, "failed")

	h := &handler{store: store}

	// 200: failed record is re-queued with the same event identifier.
	w := h.webhookRetryRequest(fixture.userID, fixture.entryID)
	if w.Code != http.StatusOK {
		t.Fatalf("Expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var view map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &view); err != nil {
		t.Fatalf("Invalid JSON response: %v", err)
	}

	if view["event_id"] != eventID || view["status"] != "pending" {
		t.Fatalf("Unexpected reset view: %+v", view)
	}

	// 409: the record is now pending, not failed.
	w = h.webhookRetryRequest(fixture.userID, fixture.entryID)
	if w.Code != http.StatusConflict {
		t.Fatalf("Expected 409 after reset, got %d", w.Code)
	}

	// 404: no record at all.
	w = h.webhookRetryRequest(fixture.userID, fixture.entryID+9_999_999)
	if w.Code != http.StatusNotFound {
		t.Fatalf("Expected 404 for missing record, got %d", w.Code)
	}
}

func TestGetEntryIncludesWebhookDelivery(t *testing.T) {
	opts, err := config.NewConfigParser().ParseEnvironmentVariables()
	if err != nil {
		t.Fatalf("Unable to parse test options: %v", err)
	}

	previousOpts := config.Opts
	config.Opts = opts
	t.Cleanup(func() { config.Opts = previousOpts })

	store, db := newWebhookAPITestStore(t)
	fixture := createAPIFixture(t, db)
	eventID := insertWebhookDelivery(t, db, fixture.userID, fixture.entryID, "failed")

	h := &handler{store: store}

	r := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/v1/entries/%d", fixture.entryID), nil)
	r = r.WithContext(context.WithValue(r.Context(), request.UserIDContextKey, fixture.userID))
	r.SetPathValue("entryID", fmt.Sprintf("%d", fixture.entryID))

	w := httptest.NewRecorder()
	h.getEntryHandler(w, r)

	if w.Code != http.StatusOK {
		t.Fatalf("Expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var entry map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &entry); err != nil {
		t.Fatalf("Invalid entry JSON: %v", err)
	}

	delivery, ok := entry["webhook_delivery"].(map[string]any)
	if !ok {
		t.Fatalf("Expected webhook_delivery object, got %+v", entry["webhook_delivery"])
	}

	if delivery["event_id"] != eventID || delivery["status"] != "failed" {
		t.Fatalf("Unexpected delivery view: %+v", delivery)
	}
}
