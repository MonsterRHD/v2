// SPDX-FileCopyrightText: Copyright The Miniflux Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package opml // import "miniflux.app/v2/internal/reader/opml"

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"miniflux.app/v2/internal/database"
	"miniflux.app/v2/internal/locale"
	"miniflux.app/v2/internal/model"
	"miniflux.app/v2/internal/storage"
)

// fakeFeedCreator replaces the network-dependent feed creation in tests.
// URLs containing "/timeout" fail a configurable number of times with a
// temporary error; URLs containing "/block" block until release() is called;
// all other URLs create a real feed row through the storage layer.
type fakeFeedCreator struct {
	store         *storage.Storage
	mu            sync.Mutex
	fetched       []string
	remainingFail map[string]int
	startedCh     chan string
	blockers      map[string]chan struct{}
}

func newFakeFeedCreator(store *storage.Storage) *fakeFeedCreator {
	return &fakeFeedCreator{
		store:         store,
		remainingFail: make(map[string]int),
		startedCh:     make(chan string, 64),
		blockers:      make(map[string]chan struct{}),
	}
}

func (f *fakeFeedCreator) fail(url string, times int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.remainingFail[url] = times
}

func (f *fakeFeedCreator) block(url string) chan struct{} {
	f.mu.Lock()
	defer f.mu.Unlock()
	ch := make(chan struct{})
	f.blockers[url] = ch
	return ch
}

func (f *fakeFeedCreator) release(url string) {
	f.mu.Lock()
	ch := f.blockers[url]
	f.mu.Unlock()
	if ch != nil {
		close(ch)
	}
}

func (f *fakeFeedCreator) fetchedURLs() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	result := make([]string, len(f.fetched))
	copy(result, f.fetched)
	return result
}

func (f *fakeFeedCreator) hasFetched(url string) bool {
	for _, fetched := range f.fetchedURLs() {
		if fetched == url {
			return true
		}
	}
	return false
}

func (f *fakeFeedCreator) create(store *storage.Storage, userID int64, req *model.FeedCreationRequest) (*model.Feed, *locale.LocalizedErrorWrapper) {
	url := req.FeedURL

	f.mu.Lock()
	f.fetched = append(f.fetched, url)
	blocker := f.blockers[url]
	remaining := f.remainingFail[url]
	if remaining > 0 {
		f.remainingFail[url] = remaining - 1
	}
	f.mu.Unlock()

	f.startedCh <- url

	if blocker != nil {
		<-blocker
	}

	if remaining > 0 {
		return nil, locale.NewLocalizedErrorWrapper(
			errors.New("fetcher: simulated network timeout"),
			"error.network_timeout",
		)
	}

	feed := &model.Feed{
		UserID:  userID,
		FeedURL: url,
		SiteURL: url,
		Title:   url,
		Category: &model.Category{
			ID: req.CategoryID,
		},
	}
	feed.ScraperRules = req.ScraperRules
	feed.RewriteRules = req.RewriteRules
	feed.UrlRewriteRules = req.UrlRewriteRules
	feed.BlocklistRules = req.BlocklistRules
	feed.KeeplistRules = req.KeeplistRules
	feed.BlockFilterEntryRules = req.BlockFilterEntryRules
	feed.KeepFilterEntryRules = req.KeepFilterEntryRules
	feed.UserAgent = req.UserAgent
	feed.Crawler = req.Crawler
	feed.IgnoreEntryUpdates = req.IgnoreEntryUpdates
	feed.Disabled = req.Disabled
	feed.NoMediaPlayer = req.NoMediaPlayer
	feed.IgnoreHTTPCache = req.IgnoreHTTPCache
	feed.AllowSelfSignedCertificates = req.AllowSelfSignedCertificates
	feed.DisableHTTP2 = req.DisableHTTP2
	feed.FetchViaProxy = req.FetchViaProxy
	feed.HideGlobally = req.HideGlobally

	if err := store.CreateFeed(feed); err != nil {
		return nil, locale.NewLocalizedErrorWrapper(err, "error.database_error", err)
	}

	return feed, nil
}

func newDBTestStore(t *testing.T) (*storage.Storage, int64, func()) {
	t.Helper()

	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		dsn = "postgres://postgres:postgres@127.0.0.1/miniflux_test?sslmode=disable"
	}

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Skipf("unable to open test database: %v", err)
	}

	if err := db.Ping(); err != nil {
		db.Close()
		t.Skipf("test database is not available: %v", err)
	}

	if err := database.Migrate(db); err != nil {
		db.Close()
		t.Fatalf("unable to run migrations: %v", err)
	}

	store := storage.NewStorage(db)

	username := fmt.Sprintf("opml_batch_test_%d", time.Now().UnixNano())
	var userID int64
	if err := db.QueryRow(`INSERT INTO users (username, password) VALUES ($1, '') RETURNING id`, username).Scan(&userID); err != nil {
		db.Close()
		t.Fatalf("unable to create test user: %v", err)
	}

	cleanup := func() {
		if _, err := db.Exec(`DELETE FROM users WHERE id=$1`, userID); err != nil {
			t.Logf("unable to delete test user: %v", err)
		}
		db.Close()
	}

	return store, userID, cleanup
}

func newManualBatchHandler(store *storage.Storage) *BatchHandler {
	return &BatchHandler{store: store, autoStart: false}
}

// withFakeCreator installs the fake feed creator and restores the real one.
func withFakeCreator(t *testing.T, fake *fakeFeedCreator) {
	t.Helper()

	original := feedCreator
	feedCreator = fake.create
	t.Cleanup(func() { feedCreator = original })
}

func batchOPMLDocument(items ...string) []byte {
	var body bytes.Buffer
	body.WriteString(`<?xml version="1.0"?><opml version="2.0" xmlns:miniflux="https://miniflux.app/opml"><head><title>batch test</title></head><body>`)
	body.WriteString(`<outline text="News">`)
	for _, item := range items {
		body.WriteString(item)
	}
	body.WriteString(`</outline></body></opml>`)
	return body.Bytes()
}

func batchFlatOPMLDocument(items ...string) []byte {
	var body bytes.Buffer
	body.WriteString(`<?xml version="1.0"?><opml version="2.0" xmlns:miniflux="https://miniflux.app/opml"><head><title>flat batch test</title></head><body>`)
	for _, item := range items {
		body.WriteString(item)
	}
	body.WriteString(`</body></opml>`)
	return body.Bytes()
}

func batchOutline(feedURL string, extra ...string) string {
	attributes := ""
	if len(extra) > 0 {
		attributes = " " + extra[0]
	}
	return fmt.Sprintf(`<outline type="rss" title="%[1]s" xmlUrl="%[1]s" htmlUrl="%[1]s"%[2]s/>`, feedURL, attributes)
}

func TestDBBatchPlanResumeCancelAndRetry(t *testing.T) {
	store, userID, cleanup := newDBTestStore(t)
	defer cleanup()

	fake := newFakeFeedCreator(store)
	withFakeCreator(t, fake)

	// A first category and an existing subscription that must be merged.
	category, err := store.CreateCategory(userID, &model.CategoryCreationRequest{Title: "News"})
	if err != nil {
		t.Fatal(err)
	}

	existingURL := "http://example.test/merge/1"
	existingFeed := &model.Feed{
		UserID:   userID,
		FeedURL:  existingURL,
		SiteURL:  existingURL,
		Title:    existingURL,
		Category: category,
	}
	if err := store.CreateFeed(existingFeed); err != nil {
		t.Fatal(err)
	}

	okURL1 := "http://example.test/ok/1"
	okURL2 := "http://example.test/ok/2"
	retryURL := "http://example.test/timeout/1"
	invalidURL := "not-a-valid-url"
	badRuleURL := "http://example.test/badrule/1"

	fake.fail(retryURL, 1)

	document := batchOPMLDocument(
		batchOutline(existingURL),
		batchOutline(okURL1),
		batchOutline(retryURL),
		batchOutline(invalidURL),
		batchOutline(badRuleURL, `miniflux:blocklistRules="(["`),
		batchOutline(okURL2),
	)

	handler := newManualBatchHandler(store)

	// Resubmitting the exact same document before processing returns the
	// existing batch.
	first, alreadyExisted, err := handler.Plan(userID, bytes.NewReader(document))
	if err != nil {
		t.Fatal(err)
	}
	if alreadyExisted {
		t.Fatal("the first submission must create a new batch")
	}
	if first.Total != 6 || first.Status != model.OPMLImportStatusPending {
		t.Fatalf("unexpected frozen plan: %+v", first)
	}

	duplicate, duplicateExisted, err := handler.Plan(userID, bytes.NewReader(document))
	if err != nil {
		t.Fatal(err)
	}
	if !duplicateExisted || duplicate.ID != first.ID {
		t.Fatal("an identical active submission must return the existing batch")
	}

	// First run: 1 merge, 2 creates, 1 temporary failure, 2 permanent failures.
	handler.runImport(context.Background(), userID, first.ID, 0)

	imp, err := store.GetOPMLImport(userID, first.ID, true)
	if err != nil {
		t.Fatal(err)
	}

	if imp.Status != model.OPMLImportStatusPaused {
		t.Fatalf("a run with temporary failures should pause the batch, got %q", imp.Status)
	}

	assertCounts(t, imp, map[string]int{
		model.OPMLImportItemStatusMerged:           1,
		model.OPMLImportItemStatusCreated:          2,
		model.OPMLImportItemStatusFetchFailed:      1,
		model.OPMLImportItemStatusValidationFailed: 2,
	})

	if imp.MissingFeedCount != 0 {
		t.Errorf("created items must reference existing feeds, missing=%d", imp.MissingFeedCount)
	}

	byURL := itemsByURL(imp.Items)
	if byURL[existingURL].Status != model.OPMLImportItemStatusMerged || byURL[existingURL].FeedID != existingFeed.ID {
		t.Errorf("existing subscription must be merged with its feed ID: %+v", byURL[existingURL])
	}
	if byURL[retryURL].Status != model.OPMLImportItemStatusFetchFailed || byURL[retryURL].ErrorMessage == "" {
		t.Errorf("temporary fetch failure must be recorded with a reason: %+v", byURL[retryURL])
	}
	if byURL[invalidURL].Status != model.OPMLImportItemStatusValidationFailed {
		t.Errorf("invalid URL must be a permanent validation failure: %+v", byURL[invalidURL])
	}
	if byURL[badRuleURL].Status != model.OPMLImportItemStatusValidationFailed {
		t.Errorf("invalid rules must be a permanent validation failure: %+v", byURL[badRuleURL])
	}
	if byURL[okURL1].Position != 2 || byURL[okURL2].Position != 6 {
		t.Error("items must keep their original positions")
	}

	if fake.hasFetched(existingURL) {
		t.Error("merged subscriptions must not trigger a new fetch")
	}

	// Continue: the temporary failure is now retried and succeeds.
	if _, err := handler.Continue(userID, first.ID); err != nil {
		t.Fatal(err)
	}
	handler.runImport(context.Background(), userID, first.ID, 0)

	current, _ := store.GetOPMLImport(userID, first.ID, false)
	if current.Status != model.OPMLImportStatusCompleted {
		t.Fatalf("the batch should complete after the temporary failure is retried, got %q", current.Status)
	}

	imp, _ = store.GetOPMLImport(userID, first.ID, true)
	assertCounts(t, imp, map[string]int{
		model.OPMLImportItemStatusMerged:           1,
		model.OPMLImportItemStatusCreated:          3,
		model.OPMLImportItemStatusValidationFailed: 2,
	})
	if byURLRetry := itemsByURL(imp.Items)[retryURL]; byURLRetry.Attempts != 2 {
		t.Errorf("retried item should record 2 attempts, got %d", byURLRetry.Attempts)
	}

	// Submitting the same document after completion creates a new batch but
	// must not duplicate any feed or category.
	categoriesBefore, _ := store.Categories(userID)
	feeds, _ := store.Feeds(userID)

	second, alreadyExisted, err := handler.Plan(userID, bytes.NewReader(document))
	if err != nil {
		t.Fatal(err)
	}
	if alreadyExisted || second.ID == first.ID {
		t.Fatal("a finished batch can be submitted again as a new batch")
	}
	handler.runImport(context.Background(), userID, second.ID, 0)

	secondImport, _ := store.GetOPMLImport(userID, second.ID, true)
	if secondImport.Status != model.OPMLImportStatusCompleted {
		t.Fatalf("resubmission should complete, got %q", secondImport.Status)
	}
	assertCounts(t, secondImport, map[string]int{
		model.OPMLImportItemStatusMerged:           4,
		model.OPMLImportItemStatusValidationFailed: 2,
	})

	categoriesAfter, _ := store.Categories(userID)
	feedsAfter, _ := store.Feeds(userID)
	if len(categoriesAfter) != len(categoriesBefore) {
		t.Errorf("resubmission must not duplicate categories: before=%d after=%d", len(categoriesBefore), len(categoriesAfter))
	}
	if len(feedsAfter) != len(feeds) {
		t.Errorf("resubmission must not duplicate feeds: before=%d after=%d", len(feeds), len(feedsAfter))
	}
}

func TestDBBatchCancelStopsNewFetchesButKeepsResults(t *testing.T) {
	store, userID, cleanup := newDBTestStore(t)
	defer cleanup()

	if _, err := store.CreateCategory(userID, &model.CategoryCreationRequest{Title: "News"}); err != nil {
		t.Fatal(err)
	}

	fake := newFakeFeedCreator(store)
	withFakeCreator(t, fake)

	firstURL := "http://example.test/block/1"
	secondURL := "http://example.test/after/1"
	fake.block(firstURL)

	document := batchOPMLDocument(batchOutline(firstURL), batchOutline(secondURL))

	handler := newManualBatchHandler(store)
	imp, _, err := handler.Plan(userID, bytes.NewReader(document))
	if err != nil {
		t.Fatal(err)
	}

	runDone := make(chan struct{})
	go func() {
		started, startErr := store.TryStartOPMLImport(userID, imp.ID, false)
		if startErr != nil || !started {
			t.Errorf("unable to start import run: %v", startErr)
			close(runDone)
			return
		}
		handler.runImport(context.Background(), userID, imp.ID, 0)
		close(runDone)
	}()

	select {
	case <-fake.startedCh:
	case <-time.After(5 * time.Second):
		t.Fatal("the first fetch never started")
	}

	// Cancel while the first fetch is in flight.
	cancelled, err := handler.Cancel(userID, imp.ID)
	if err != nil {
		t.Fatal(err)
	}
	if cancelled.Status != model.OPMLImportStatusCancelling {
		t.Fatalf("expected cancelling status, got %q", cancelled.Status)
	}

	fake.release(firstURL)

	select {
	case <-runDone:
	case <-time.After(5 * time.Second):
		t.Fatal("runner did not stop after cancellation")
	}

	final, _ := store.GetOPMLImport(userID, imp.ID, true)
	if final.Status != model.OPMLImportStatusCancelled {
		t.Fatalf("expected cancelled status, got %q", final.Status)
	}
	assertCounts(t, final, map[string]int{
		model.OPMLImportItemStatusCreated: 1,
		model.OPMLImportItemStatusPending: 1,
	})
	if fake.hasFetched(secondURL) {
		t.Error("no new fetch must start after cancellation was requested")
	}
	if _, err := store.FeedByFeedURL(userID, firstURL); err != nil {
		t.Errorf("the in-flight committed result must be preserved: %v", err)
	}

	// An explicit continue resumes the remaining pending item.
	if _, err := handler.Continue(userID, imp.ID); err != nil {
		t.Fatal(err)
	}
	handler.runImport(context.Background(), userID, imp.ID, 0)
	final, _ = store.GetOPMLImport(userID, imp.ID, true)
	if final.Status != model.OPMLImportStatusCompleted {
		t.Fatalf("continued cancelled batch should complete, got %q", final.Status)
	}
	assertCounts(t, final, map[string]int{
		model.OPMLImportItemStatusCreated: 2,
	})
	if !fake.hasFetched(secondURL) {
		t.Error("the remaining item should be fetched on explicit continue")
	}
}

func TestDBBatchRetryCorrectedItemAtOriginalPosition(t *testing.T) {
	store, userID, cleanup := newDBTestStore(t)
	defer cleanup()

	fake := newFakeFeedCreator(store)
	withFakeCreator(t, fake)

	// No category exists: the item is permanently rejected.
	document := batchFlatOPMLDocument(batchOutline("http://example.test/nocat/1"))
	handler := newManualBatchHandler(store)
	imp, _, err := handler.Plan(userID, bytes.NewReader(document))
	if err != nil {
		t.Fatal(err)
	}
	handler.runImport(context.Background(), userID, imp.ID, 0)

	current, _ := store.GetOPMLImport(userID, imp.ID, true)
	if current.Status != model.OPMLImportStatusCompleted {
		t.Fatalf("batch with only permanent failures should be completed, got %q", current.Status)
	}
	item := current.Items[0]
	if item.Status != model.OPMLImportItemStatusValidationFailed {
		t.Fatalf("expected validation_failed, got %q", item.Status)
	}
	if item.Position != 1 {
		t.Fatal("item position must be frozen")
	}

	// Fix the external condition (create a category) and retry the item
	// without changing the frozen plan.
	if _, err := store.CreateCategory(userID, &model.CategoryCreationRequest{Title: "News"}); err != nil {
		t.Fatal(err)
	}
	if _, err := handler.RetryItem(userID, imp.ID, item.ID, ItemCorrection{}); err != nil {
		t.Fatal(err)
	}
	handler.runImport(context.Background(), userID, imp.ID, item.ID)

	updated, _ := store.GetOPMLImport(userID, imp.ID, true)
	if updated.Items[0].Status != model.OPMLImportItemStatusCreated {
		t.Fatal("the corrected item should be created at its original position")
	}
	if updated.Items[0].Position != 1 {
		t.Error("a retried item must keep its original position")
	}
	if updated.Status != model.OPMLImportStatusCompleted {
		t.Errorf("batch should be completed again, got %q", updated.Status)
	}

	// Correcting an invalid URL works as well.
	fixed := "http://example.test/fixed/1"
	invalidDocument := batchOPMLDocument(batchOutline("broken-url"))
	invalidImport, _, err := handler.Plan(userID, bytes.NewReader(invalidDocument))
	if err != nil {
		t.Fatal(err)
	}
	handler.runImport(context.Background(), userID, invalidImport.ID, 0)
	invalidBatch, _ := store.GetOPMLImport(userID, invalidImport.ID, true)
	invalidItem := invalidBatch.Items[0]

	if _, err := handler.RetryItem(userID, invalidImport.ID, invalidItem.ID, ItemCorrection{FeedURL: fixed}); err != nil {
		t.Fatal(err)
	}
	handler.runImport(context.Background(), userID, invalidImport.ID, invalidItem.ID)

	updated, _ = store.GetOPMLImport(userID, invalidImport.ID, true)
	if updated.Items[0].FeedURL != fixed || updated.Items[0].Status != model.OPMLImportItemStatusCreated {
		t.Fatalf("the corrected URL should be used when retrying: %+v", updated.Items[0])
	}
}

func TestDBBatchRecoversAfterInterruption(t *testing.T) {
	store, userID, cleanup := newDBTestStore(t)
	defer cleanup()

	if _, err := store.CreateCategory(userID, &model.CategoryCreationRequest{Title: "News"}); err != nil {
		t.Fatal(err)
	}

	fake := newFakeFeedCreator(store)
	withFakeCreator(t, fake)

	url := "http://example.test/recovered/1"
	document := batchOPMLDocument(batchOutline(url))
	handler := newManualBatchHandler(store)
	imp, _, err := handler.Plan(userID, bytes.NewReader(document))
	if err != nil {
		t.Fatal(err)
	}

	// Simulate a process killed while the batch was running.
	if _, err := store.TryStartOPMLImport(userID, imp.ID, false); err != nil {
		t.Fatal(err)
	}

	interrupted, err := store.RecoverInterruptedOPMLImports()
	if err != nil {
		t.Fatal(err)
	}
	if len(interrupted) != 1 || interrupted[0].ID != imp.ID {
		t.Fatalf("unexpected recovered imports: %+v", interrupted)
	}

	status, _ := store.OPMLImportStatus(userID, imp.ID)
	if status != model.OPMLImportStatusPaused {
		t.Fatalf("interrupted batch should be paused, got %q", status)
	}

	// Resuming processes the still-pending item.
	handler.runImport(context.Background(), userID, imp.ID, 0)
	final, _ := store.GetOPMLImport(userID, imp.ID, true)
	if final.Status != model.OPMLImportStatusCompleted {
		t.Fatalf("recovered batch should complete, got %q", final.Status)
	}
	assertCounts(t, final, map[string]int{
		model.OPMLImportItemStatusCreated: 1,
	})
}

func TestDBBatchSummaryMatchesDatabase(t *testing.T) {
	store, userID, cleanup := newDBTestStore(t)
	defer cleanup()

	if _, err := store.CreateCategory(userID, &model.CategoryCreationRequest{Title: "News"}); err != nil {
		t.Fatal(err)
	}

	fake := newFakeFeedCreator(store)
	withFakeCreator(t, fake)

	createdURL := "http://example.test/summary/created"
	failedURL := "http://example.test/summary/timeout"
	fake.fail(failedURL, 10)

	document := batchOPMLDocument(batchOutline(createdURL), batchOutline(failedURL))
	handler := newManualBatchHandler(store)
	imp, _, err := handler.Plan(userID, bytes.NewReader(document))
	if err != nil {
		t.Fatal(err)
	}
	handler.runImport(context.Background(), userID, imp.ID, 0)

	final, _ := store.GetOPMLImport(userID, imp.ID, true)

	sumByStatus := 0
	for _, item := range final.Items {
		sumByStatus++
		switch item.Status {
		case model.OPMLImportItemStatusCreated:
		case model.OPMLImportItemStatusFetchFailed:
		default:
			t.Fatalf("unexpected item status %q", item.Status)
		}
	}

	if sumByStatus != final.Total {
		t.Errorf("total count mismatch: items=%d total=%d", sumByStatus, final.Total)
	}
	if final.CreatedCount+final.MergedCount+final.PendingCount+final.FetchFailedCount+final.ValidationFailedCount != final.Total {
		t.Errorf("counter breakdown does not add up to the total: %+v", final)
	}

	// Every created item must point at an actual feed row.
	for _, item := range final.Items {
		if item.Status != model.OPMLImportItemStatusCreated {
			continue
		}
		feed, err := store.FeedByID(userID, item.FeedID)
		if err != nil {
			t.Fatal(err)
		}
		if feed == nil {
			t.Errorf("created item #%d references a missing feed", item.ID)
		}
	}

	// Deleting a feed is reflected by missing_feed_count, keeping the summary
	// consistent with the actual subscriptions.
	if err := store.RemoveFeed(userID, final.Items[0].FeedID); err != nil {
		t.Fatal(err)
	}
	afterDelete, _ := store.GetOPMLImport(userID, imp.ID, false)
	if afterDelete.MissingFeedCount != 1 {
		t.Errorf("missing_feed_count should be 1 after deleting the feed, got %d", afterDelete.MissingFeedCount)
	}
}

func itemsByURL(items model.OPMLImportItems) map[string]*model.OPMLImportItem {
	result := make(map[string]*model.OPMLImportItem, len(items))
	for _, item := range items {
		result[item.FeedURL] = item
	}
	return result
}

func assertCounts(t *testing.T, imp *model.OPMLImport, expected map[string]int) {
	t.Helper()

	actual := map[string]int{
		model.OPMLImportItemStatusPending:          imp.PendingCount,
		model.OPMLImportItemStatusCreated:          imp.CreatedCount,
		model.OPMLImportItemStatusMerged:           imp.MergedCount,
		model.OPMLImportItemStatusFetchFailed:      imp.FetchFailedCount,
		model.OPMLImportItemStatusValidationFailed: imp.ValidationFailedCount,
	}

	for status, expectedCount := range expected {
		if actual[status] != expectedCount {
			t.Errorf("unexpected %s count: got %d want %d (import=%+v)", status, actual[status], expectedCount, imp)
		}
	}

	// No unexpected statuses should be present.
	for status, count := range actual {
		if count == 0 {
			continue
		}
		if expectedCount, ok := expected[status]; !ok || expectedCount != count {
			t.Errorf("unexpected non-zero %s count: %d (import=%+v)", status, count, imp)
		}
	}

	if len(imp.Items) > 0 {
		fromItems := map[string]int{}
		for _, item := range imp.Items {
			fromItems[item.Status]++
		}
		for status, count := range actual {
			if fromItems[status] != count {
				t.Errorf("counter for %s (%d) does not match item rows (%d)", status, count, fromItems[status])
			}
		}
	}
}
