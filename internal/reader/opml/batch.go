// SPDX-FileCopyrightText: Copyright The Miniflux Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package opml // import "miniflux.app/v2/internal/reader/opml"

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
feedhandler "miniflux.app/v2/internal/reader/handler"
	"miniflux.app/v2/internal/model"
	"miniflux.app/v2/internal/storage"
	"miniflux.app/v2/internal/validator"
)

// ErrEmptyOPMLDocument is returned when an OPML submission contains no
// subscription at all.
var ErrEmptyOPMLDocument = errors.New("opml: no subscription found in the OPML document")

// ItemCorrection contains the user-provided fixes for a failed import item.
// A nil CategoryName leaves the frozen category untouched, while a pointer to
// an empty string forces the user's first category.
type ItemCorrection struct {
	FeedURL      string
	CategoryName *string
	Settings     *model.OPMLItemSettings
}

// feedCreator fetches, parses and creates a feed. It is a package-level
// variable so the batch processing can be exercised without network access
// in tests.
var feedCreator = feedhandler.CreateFeed

// BatchHandler implements the resumable, per-item OPML import workflow.
type BatchHandler struct {
	store     *storage.Storage
	autoStart bool
}

// NewBatchHandler creates a new batch handler. Submitted batches are processed
// in the background automatically.
func NewBatchHandler(store *storage.Storage) *BatchHandler {
	return &BatchHandler{store: store, autoStart: true}
}

// Plan parses the OPML document, freezes the category/subscription plan and
// starts processing it. When the same document was already submitted while
// the previous batch is still active, the existing batch is returned instead
// of creating a new one. The boolean result reports whether an existing batch
// was returned.
func (h *BatchHandler) Plan(userID int64, data io.Reader) (*model.OPMLImport, bool, error) {
	title, subscriptions, err := parsePlan(data)
	if err != nil {
		return nil, false, err
	}

	if len(subscriptions) == 0 {
		return nil, false, ErrEmptyOPMLDocument
	}

	contentHash := subscriptionsHash(subscriptions)

	if existing, err := h.store.ActiveOPMLImportByHash(userID, contentHash); err != nil {
		return nil, false, err
	} else if existing != nil {
		return existing, true, nil
	}

	imp := &model.OPMLImport{
		UserID:      userID,
		Title:       title,
		ContentHash: contentHash,
	}

	items := make(model.OPMLImportItems, 0, len(subscriptions))
	for index, subscription := range subscriptions {
		items = append(items, &model.OPMLImportItem{
			Position:     index + 1,
			Title:        subscription.Title,
			FeedURL:      subscription.FeedURL,
			SiteURL:      subscription.SiteURL,
			Description:  subscription.Description,
			CategoryName: subscription.CategoryName,
			Settings:     subscription.Settings,
		})
	}

	if err := h.store.CreateOPMLImport(imp, items); err != nil {
		if errors.Is(err, storage.ErrOPMLImportActiveExists) {
			existing, getErr := h.store.ActiveOPMLImportByHash(userID, contentHash)
			if getErr != nil {
				return nil, false, getErr
			}
			if existing != nil {
				return existing, true, nil
			}
		}
		return nil, false, err
	}

	h.tryRun(imp.UserID, imp.ID, 0, false)
	return imp, false, nil
}

// GetImport returns a batch with its counters, optionally with all items.
func (h *BatchHandler) GetImport(userID, importID int64, withItems bool) (*model.OPMLImport, error) {
	return h.store.GetOPMLImport(userID, importID, withItems)
}

// ListImports returns the user's import batches, newest first.
func (h *BatchHandler) ListImports(userID int64) (model.OPMLImports, error) {
	return h.store.ListOPMLImports(userID)
}

// Continue resumes a paused, pending or cancelled batch, processing pending
// and temporary (fetch_failed) items. Permanent validation failures are not
// retried automatically; use RetryItem after fixing them.
func (h *BatchHandler) Continue(userID, importID int64) (*model.OPMLImport, error) {
	started, err := h.store.TryStartOPMLImport(userID, importID, false)
	if err != nil {
		return nil, err
	}

	if started && h.autoStart {
		go h.runImport(context.Background(), userID, importID, 0)
	}

	return h.store.GetOPMLImport(userID, importID, false)
}

// Cancel requests a cancellation. A running batch stops before starting any
// new fetch while the in-flight item finishes; already committed results are
// preserved. A non-running batch is cancelled immediately.
func (h *BatchHandler) Cancel(userID, importID int64) (*model.OPMLImport, error) {
	if _, err := h.store.RequestOPMLImportCancellation(userID, importID); err != nil {
		return nil, err
	}

	return h.store.GetOPMLImport(userID, importID, false)
}

// RetryItem applies optional corrections to a failed item and retries it at
// its original position. A completed batch containing the item is reopened
// for the targeted run.
func (h *BatchHandler) RetryItem(userID, importID, itemID int64, correction ItemCorrection) (*model.OPMLImport, error) {
	item, err := h.store.CorrectOPMLImportItem(userID, importID, itemID, &storage.OPMLImportItemCorrection{
		FeedURL:      correction.FeedURL,
		CategoryName: correction.CategoryName,
		Settings:     correction.Settings,
	})
	if err != nil {
		return nil, err
	}

	started, err := h.store.TryStartOPMLImport(userID, importID, true)
	if err != nil {
		return nil, err
	}

	if started && h.autoStart {
		go h.runImport(context.Background(), userID, importID, item.ID)
	}

	return h.store.GetOPMLImport(userID, importID, true)
}

// RecoverInterrupted resets batches interrupted by a process restart and
// continues them from their unfinished items.
func (h *BatchHandler) RecoverInterrupted(ctx context.Context) error {
	imports, err := h.store.RecoverInterruptedOPMLImports()
	if err != nil {
		return err
	}

	for _, imp := range imports {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		slog.Info("Resuming interrupted OPML import after process restart",
			slog.Int64("user_id", imp.UserID),
			slog.Int64("import_id", imp.ID),
		)

		started, err := h.store.TryStartOPMLImport(imp.UserID, imp.ID, false)
		if err != nil {
			slog.Error("Unable to resume interrupted OPML import",
				slog.Int64("import_id", imp.ID),
				slog.Any("error", err),
			)
			continue
		}

		if started {
			go h.runImport(ctx, imp.UserID, imp.ID, 0)
		}
	}

	return nil
}

func (h *BatchHandler) tryRun(userID, importID, onlyItemID int64, reopenCompleted bool) {
	if !h.autoStart {
		return
	}

	go func() {
		started, err := h.store.TryStartOPMLImport(userID, importID, reopenCompleted)
		if err != nil {
			slog.Error("Unable to start OPML import processing",
				slog.Int64("import_id", importID),
				slog.Any("error", err),
			)
			return
		}

		if started {
			h.runImport(context.Background(), userID, importID, onlyItemID)
		}
	}()
}

// runImport processes eligible items in original position order. The caller
// must own the run (TryStart); item statuses are the resume cursor, so an
// interrupted run can simply call this again.
func (h *BatchHandler) runImport(ctx context.Context, userID, importID, onlyItemID int64) {
	// Items attempted during this run. A temporary failure is not retried in
	// the same run: it waits for an explicit continuation, which starts a
	// fresh run with an empty attempt set.
	attemptedIDs := make([]int64, 0)

	defer func() {
		if r := recover(); r != nil {
			slog.Error("OPML import processing panicked",
				slog.Int64("user_id", userID),
				slog.Int64("import_id", importID),
				slog.Any("panic", r),
			)
		}

		if err := h.store.FinishOPMLImportRun(userID, importID); err != nil {
			slog.Error("Unable to finalize OPML import run",
				slog.Int64("user_id", userID),
				slog.Int64("import_id", importID),
				slog.Any("error", err),
			)
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		cancelled, err := h.store.OPMLImportCancellationRequested(userID, importID)
		if err != nil {
			slog.Error("Unable to check OPML import cancellation",
				slog.Int64("import_id", importID),
				slog.Any("error", err),
			)
			return
		}
		if cancelled {
			slog.Info("OPML import cancelled, no further subscription will be fetched",
				slog.Int64("user_id", userID),
				slog.Int64("import_id", importID),
			)
			return
		}

		item, err := h.store.NextOPMLImportItem(importID, onlyItemID, attemptedIDs)
		if err != nil {
			slog.Error("Unable to fetch next OPML import item",
				slog.Int64("import_id", importID),
				slog.Any("error", err),
			)
			return
		}
		if item == nil {
			return
		}

		status, feedID, errorMessage := h.processItem(userID, item)

		slog.Debug("OPML import item processed",
			slog.Int64("import_id", importID),
			slog.Int("position", item.Position),
			slog.String("feed_url", item.FeedURL),
			slog.String("status", status),
		)

		if err := h.store.CompleteOPMLImportItem(importID, item.ID, feedID, status, errorMessage); err != nil {
			slog.Error("Unable to persist OPML import item result",
				slog.Int64("import_id", importID),
				slog.Int64("item_id", item.ID),
				slog.Any("error", err),
			)
			return
		}

		attemptedIDs = append(attemptedIDs, item.ID)
	}
}

// processItem gives a single item its definitive status:
//   - merged: the feed URL already belongs to an existing subscription (no
//     fetch is started), or the fetch redirected to an existing feed;
//   - validation_failed: a permanent, local rejection (invalid URL or feed
//     settings, or no category available);
//   - fetch_failed: a temporary failure while fetching, parsing or storing
//     the feed, which can be continued later;
//   - created: the feed was created successfully.
func (h *BatchHandler) processItem(userID int64, item *model.OPMLImportItem) (status string, feedID int64, errorMessage string) {
	if h.store.FeedURLExists(userID, item.FeedURL) {
		if feed, _ := h.store.FeedByFeedURL(userID, item.FeedURL); feed != nil {
			return model.OPMLImportItemStatusMerged, feed.ID, ""
		}
		return model.OPMLImportItemStatusMerged, 0, ""
	}

	category, err := resolveImportCategory(h.store, userID, item.CategoryName)
	if err != nil {
		if errors.Is(err, storage.ErrNoCategory) {
			return model.OPMLImportItemStatusValidationFailed, 0, "opml: no category is available for this user"
		}
		return model.OPMLImportItemStatusFetchFailed, 0, err.Error()
	}

	subscription := model.FrozenOPMLSubscription{
		Title:        item.Title,
		SiteURL:      item.SiteURL,
		FeedURL:      item.FeedURL,
		CategoryName: item.CategoryName,
		Description:  item.Description,
		Settings:     item.Settings,
	}

	feedCreationRequest := feedCreationRequestFromFrozen(subscription, category.ID)
	if validationErr := validator.ValidateFeedCreation(h.store, userID, feedCreationRequest); validationErr != nil {
		return model.OPMLImportItemStatusValidationFailed, 0, validationErr.String()
	}

	feed, localizedError := feedCreator(h.store, userID, feedCreationRequest)
	if localizedError != nil {
		originalError := localizedError.Error()

		if errors.Is(originalError, feedhandler.ErrDuplicatedFeed) {
			if existing, _ := h.store.FeedByFeedURL(userID, item.FeedURL); existing != nil {
				return model.OPMLImportItemStatusMerged, existing.ID, ""
			}
			return model.OPMLImportItemStatusMerged, 0, ""
		}

		return model.OPMLImportItemStatusFetchFailed, 0, originalError.Error()
	}

	return model.OPMLImportItemStatusCreated, feed.ID, ""
}

// subscriptionsHash computes a stable hash of the frozen subscription plan.
// Identical OPML documents always produce the same hash, which lets duplicate
// submissions reuse the same active batch.
func subscriptionsHash(subscriptions []model.FrozenOPMLSubscription) string {
	hash := sha256.New()

	for _, subscription := range subscriptions {
		fmt.Fprintf(hash, "%s\x00%s\x00%s\x00%s\x00%s\x00%s\x00",
			subscription.FeedURL,
			subscription.SiteURL,
			subscription.Title,
			subscription.Description,
			subscription.CategoryName,
			subscription.Settings.JSON(),
		)
	}

	return hex.EncodeToString(hash.Sum(nil))
}
