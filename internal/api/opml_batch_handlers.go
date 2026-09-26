// SPDX-FileCopyrightText: Copyright The Miniflux Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package api // import "miniflux.app/v2/internal/api"

import (
	json_parser "encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"

	"miniflux.app/v2/internal/http/request"
	"miniflux.app/v2/internal/http/response"
	"miniflux.app/v2/internal/model"
	"miniflux.app/v2/internal/reader/opml"
	"miniflux.app/v2/internal/storage"
	"miniflux.app/v2/internal/urllib"
)

type createOPMLImportResponse struct {
	ImportID       int64  `json:"import_id"`
	Status         string `json:"status"`
	Total          int    `json:"total"`
	AlreadyExisted bool   `json:"already_existed"`
}

type retryOPMLImportItemRequest struct {
	FeedURL      string                  `json:"feed_url"`
	CategoryName *string                 `json:"category_name"`
	Settings     *model.OPMLItemSettings `json:"settings"`
}

func (h *handler) createOPMLImportHandler(w http.ResponseWriter, r *http.Request) {
	userID := request.UserID(r)
	defer r.Body.Close()

	batchHandler := opml.NewBatchHandler(h.store)
	importBatch, alreadyExisted, err := batchHandler.Plan(userID, r.Body)
	if err != nil {
		if errors.Is(err, opml.ErrEmptyOPMLDocument) || strings.Contains(err.Error(), "unable to parse document") {
			response.JSONBadRequest(w, r, err)
			return
		}
		response.JSONServerError(w, r, err)
		return
	}

	result := &createOPMLImportResponse{
		ImportID:       importBatch.ID,
		Status:         importBatch.Status,
		Total:          importBatch.Total,
		AlreadyExisted: alreadyExisted,
	}

	if alreadyExisted {
		response.JSON(w, r, result)
		return
	}

	response.JSONCreated(w, r, result)
}

func (h *handler) getOPMLImportsHandler(w http.ResponseWriter, r *http.Request) {
	batchHandler := opml.NewBatchHandler(h.store)
	imports, err := batchHandler.ListImports(request.UserID(r))
	if err != nil {
		response.JSONServerError(w, r, err)
		return
	}

	response.JSON(w, r, imports)
}

func (h *handler) getOPMLImportHandler(w http.ResponseWriter, r *http.Request) {
	importID := request.RouteInt64Param(r, "importID")
	if importID == 0 {
		response.JSONBadRequest(w, r, errors.New("invalid import ID"))
		return
	}

	batchHandler := opml.NewBatchHandler(h.store)
	importBatch, err := batchHandler.GetImport(request.UserID(r), importID, true)
	if err != nil {
		if errors.Is(err, storage.ErrOPMLImportNotFound) {
			response.JSONNotFound(w, r)
			return
		}
		response.JSONServerError(w, r, err)
		return
	}

	response.JSON(w, r, importBatch)
}

func (h *handler) continueOPMLImportHandler(w http.ResponseWriter, r *http.Request) {
	importID := request.RouteInt64Param(r, "importID")
	if importID == 0 {
		response.JSONBadRequest(w, r, errors.New("invalid import ID"))
		return
	}

	batchHandler := opml.NewBatchHandler(h.store)
	importBatch, err := batchHandler.Continue(request.UserID(r), importID)
	if err != nil {
		if errors.Is(err, storage.ErrOPMLImportNotFound) {
			response.JSONNotFound(w, r)
			return
		}
		response.JSONServerError(w, r, err)
		return
	}

	response.JSON(w, r, importBatch)
}

func (h *handler) cancelOPMLImportHandler(w http.ResponseWriter, r *http.Request) {
	importID := request.RouteInt64Param(r, "importID")
	if importID == 0 {
		response.JSONBadRequest(w, r, errors.New("invalid import ID"))
		return
	}

	batchHandler := opml.NewBatchHandler(h.store)
	importBatch, err := batchHandler.Cancel(request.UserID(r), importID)
	if err != nil {
		if errors.Is(err, storage.ErrOPMLImportNotFound) {
			response.JSONNotFound(w, r)
			return
		}
		response.JSONServerError(w, r, err)
		return
	}

	if importBatch.Status == model.OPMLImportStatusCompleted {
		response.JSONConflict(w, r, errors.New("a completed OPML import cannot be cancelled"))
		return
	}

	response.JSON(w, r, importBatch)
}

func (h *handler) retryOPMLImportItemHandler(w http.ResponseWriter, r *http.Request) {
	importID := request.RouteInt64Param(r, "importID")
	itemID := request.RouteInt64Param(r, "itemID")
	if importID == 0 || itemID == 0 {
		response.JSONBadRequest(w, r, errors.New("invalid import or item ID"))
		return
	}

	defer r.Body.Close()

	// An empty body is valid: the item is retried without changing its frozen
	// plan.
	var retryRequest retryOPMLImportItemRequest
	if body, err := io.ReadAll(r.Body); err == nil && len(strings.TrimSpace(string(body))) > 0 {
		if err := json_parser.Unmarshal(body, &retryRequest); err != nil {
			response.JSONBadRequest(w, r, err)
			return
		}
	}

	if retryRequest.FeedURL != "" && !urllib.IsAbsoluteURL(retryRequest.FeedURL) {
		response.JSONBadRequest(w, r, errors.New("invalid feed URL"))
		return
	}

	correction := opml.ItemCorrection{
		FeedURL:      retryRequest.FeedURL,
		CategoryName: retryRequest.CategoryName,
		Settings:     retryRequest.Settings,
	}

	batchHandler := opml.NewBatchHandler(h.store)
	importBatch, err := batchHandler.RetryItem(request.UserID(r), importID, itemID, correction)
	if err != nil {
		switch {
		case errors.Is(err, storage.ErrOPMLImportNotFound), errors.Is(err, storage.ErrOPMLImportItemNotFound):
			response.JSONNotFound(w, r)
		case strings.Contains(err.Error(), "cannot be modified"):
			response.JSONConflict(w, r, err)
		default:
			response.JSONServerError(w, r, err)
		}
		return
	}

	response.JSON(w, r, importBatch)
}
