// SPDX-FileCopyrightText: Copyright The Miniflux Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package api // import "miniflux.app/v2/internal/api"

import (
	"errors"
	"net/http"

	"miniflux.app/v2/internal/http/request"
	"miniflux.app/v2/internal/http/response"
	"miniflux.app/v2/internal/integration/webhook"
	"miniflux.app/v2/internal/model"
	"miniflux.app/v2/internal/storage"
)

// attachWebhookDeliveries performs a single batched lookup and fills the
// read-only webhook delivery view on each entry. Entries without a delivery
// record keep a nil field so the JSON key is omitted. It returns false after
// writing an error response when the lookup fails.
func (h *handler) attachWebhookDeliveries(w http.ResponseWriter, r *http.Request, userID int64, entries model.Entries) bool {
	if len(entries) == 0 {
		return true
	}

	entryIDs := make([]int64, 0, len(entries))
	for _, entry := range entries {
		entryIDs = append(entryIDs, entry.ID)
	}

	deliveries, err := h.store.WebhookDeliveriesByEntries(userID, entryIDs)
	if err != nil {
		response.JSONServerError(w, r, err)
		return false
	}

	for _, entry := range entries {
		if delivery, found := deliveries[entry.ID]; found {
			entry.WebhookDelivery = delivery.View()
		}
	}

	return true
}

// retryWebhookDeliveryHandler re-queues a failed save-entry webhook delivery
// for a new attempt, preserving its stable event identifier.
func (h *handler) retryWebhookDeliveryHandler(w http.ResponseWriter, r *http.Request) {
	userID := request.UserID(r)
	entryID := request.RouteInt64Param(r, "entryID")
	if entryID == 0 {
		response.JSONBadRequest(w, r, errors.New("invalid entry ID"))
		return
	}

	delivery, err := h.store.ResetFailedWebhookDelivery(userID, entryID)
	switch {
	case errors.Is(err, storage.ErrWebhookDeliveryNotFound):
		response.JSONNotFound(w, r)
		return
	case errors.Is(err, storage.ErrWebhookDeliveryNotFailed):
		response.JSONConflict(w, r, errors.New("webhook delivery is not in the failed status"))
		return
	case err != nil:
		response.JSONServerError(w, r, err)
		return
	}

	webhook.NotifyPending()

	response.JSON(w, r, delivery.View())
}
