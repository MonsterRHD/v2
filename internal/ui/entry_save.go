// SPDX-FileCopyrightText: Copyright The Miniflux Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package ui // import "miniflux.app/v2/internal/ui"

import (
	"net/http"

	"miniflux.app/v2/internal/http/request"
	"miniflux.app/v2/internal/http/response"
	"miniflux.app/v2/internal/integration"
)

func (h *handler) saveEntry(w http.ResponseWriter, r *http.Request) {
	entryID := request.RouteInt64Param(r, "entryID")

	entry, err := h.store.NewEntryQueryBuilder(request.UserID(r)).
		WithEntryIDs(entryID).
		GetEntry()
	if err != nil {
		response.JSONServerError(w, r, err)
		return
	}

	if entry == nil {
		response.JSONNotFound(w, r)
		return
	}

	userIntegrations, err := h.store.Integration(request.UserID(r))
	if err != nil {
		response.JSONServerError(w, r, err)
		return
	}

	delivery, err := integration.EnqueueSaveEntry(h.store, entry, userIntegrations)
	if err != nil {
		response.JSONServerError(w, r, err)
		return
	}

	body := map[string]any{"message": "saved"}
	if delivery != nil {
		body["webhook_delivery"] = delivery.View()
	}

	response.JSONCreated(w, r, body)
}
