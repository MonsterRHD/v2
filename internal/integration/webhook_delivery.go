// SPDX-FileCopyrightText: Copyright The Miniflux Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package integration // import "miniflux.app/v2/internal/integration"

import (
	"log/slog"

	"miniflux.app/v2/internal/config"
	"miniflux.app/v2/internal/integration/webhook"
	"miniflux.app/v2/internal/model"
)

// SaveEntryStore is the subset of storage methods used when saving entries.
// *storage.Storage satisfies it.
type SaveEntryStore interface {
	CreateWebhookSaveDelivery(userID, entryID int64, webhookURL string, maxAttempts int) (*model.WebhookDelivery, error)
	SetEntriesStarredStateAndWebhookDeliveries(userID int64, entryIDs []int64, starred bool, webhookURLs map[int64]string, maxAttempts int) error
}

// resolveWebhookURL returns the feed-level webhook URL when configured,
// otherwise the user-level URL, mirroring the previous inline behavior.
func resolveWebhookURL(entry *model.Entry, userIntegrations *model.Integration) string {
	if entry.Feed != nil && entry.Feed.WebhookURL != "" {
		return entry.Feed.WebhookURL
	}

	return userIntegrations.WebhookURL
}

// EnqueueSaveEntry persists the durable save-entry webhook delivery (when the
// webhook is enabled) and then fans the entry out to the other save
// integrations using the existing fire-and-forget behavior. The webhook HTTP
// request itself is only sent by the background dispatcher after the database
// transaction commits. The returned delivery is nil when the webhook is not
// enabled.
func EnqueueSaveEntry(store SaveEntryStore, entry *model.Entry, userIntegrations *model.Integration) (*model.WebhookDelivery, error) {
	var delivery *model.WebhookDelivery

	if userIntegrations.WebhookEnabled {
		webhookURL := resolveWebhookURL(entry, userIntegrations)

		slog.Debug("Enqueuing save entry webhook delivery",
			slog.Int64("user_id", userIntegrations.UserID),
			slog.Int64("entry_id", entry.ID),
			slog.String("webhook_url", webhookURL),
		)

		var err error
		delivery, err = store.CreateWebhookSaveDelivery(
			userIntegrations.UserID,
			entry.ID,
			webhookURL,
			config.Opts.WebhookSaveMaxAttempts(),
		)
		if err != nil {
			return nil, err
		}

		webhook.NotifyPending()
	}

	go SendEntry(entry, userIntegrations)

	return delivery, nil
}

// SyncStarredSaveEntries applies a starred/unstarred batch coming from the
// Fever or Google Reader APIs: the entry state and the webhook delivery
// lifecycle are committed together in one transaction. When starring, a
// pending delivery is enqueued per entry and the other save integrations are
// still triggered asynchronously; when unstarring, only deliveries that have
// never been sent are canceled — in-flight or unknown deliveries keep their
// original event identifier and are never masked by an opposite event.
func SyncStarredSaveEntries(store SaveEntryStore, entries model.Entries, starred bool, userIntegrations *model.Integration) error {
	if len(entries) == 0 {
		return nil
	}

	entryIDs := make([]int64, 0, len(entries))
	webhookURLs := make(map[int64]string)

	for _, entry := range entries {
		entryIDs = append(entryIDs, entry.ID)
		if starred && userIntegrations.WebhookEnabled {
			webhookURLs[entry.ID] = resolveWebhookURL(entry, userIntegrations)
		}
	}

	if err := store.SetEntriesStarredStateAndWebhookDeliveries(
		userIntegrations.UserID,
		entryIDs,
		starred,
		webhookURLs,
		config.Opts.WebhookSaveMaxAttempts(),
	); err != nil {
		return err
	}

	if starred {
		if userIntegrations.WebhookEnabled {
			webhook.NotifyPending()
		}

		for _, entry := range entries {
			e := entry
			go SendEntry(e, userIntegrations)
		}
	}

	return nil
}
