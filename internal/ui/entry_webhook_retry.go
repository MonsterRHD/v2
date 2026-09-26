// SPDX-FileCopyrightText: Copyright The Miniflux Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package ui // import "miniflux.app/v2/internal/ui"

import (
	"errors"
	"log/slog"
	"net/http"

	"miniflux.app/v2/internal/http/request"
	"miniflux.app/v2/internal/http/response"
	"miniflux.app/v2/internal/integration/webhook"
	"miniflux.app/v2/internal/model"
	"miniflux.app/v2/internal/storage"
)

// attachEntryWebhookDelivery fills the read-only webhook delivery view of the
// entry rendered on the entry page. Lookup errors are non-fatal: the entry
// page must remain usable when the delivery table cannot be queried.
func (h *handler) attachEntryWebhookDelivery(userID int64, entry *model.Entry) {
	delivery, err := h.store.WebhookDeliveryByEntry(userID, entry.ID)
	if err != nil {
		slog.Warn("Unable to load webhook delivery for entry page",
			slog.Int64("user_id", userID),
			slog.Int64("entry_id", entry.ID),
			slog.Any("error", err),
		)
		return
	}

	if delivery != nil {
		entry.WebhookDelivery = delivery.View()
	}
}

func (h *handler) retryEntryWebhookDelivery(w http.ResponseWriter, r *http.Request) {
	userID := request.UserID(r)
	entryID := request.RouteInt64Param(r, "entryID")

	delivery, err := h.store.ResetFailedWebhookDelivery(userID, entryID)
	switch {
	case errors.Is(err, storage.ErrWebhookDeliveryNotFound):
		response.JSONNotFound(w, r)
		return
	case errors.Is(err, storage.ErrWebhookDeliveryNotFailed):
		response.JSONBadRequest(w, r, errors.New("webhook delivery is not in the failed status"))
		return
	case err != nil:
		response.JSONServerError(w, r, err)
		return
	}

	webhook.NotifyPending()

	response.JSON(w, r, map[string]any{"webhook_delivery": delivery.View()})
}
