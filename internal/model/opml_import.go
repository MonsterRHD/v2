// SPDX-FileCopyrightText: Copyright The Miniflux Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package model // import "miniflux.app/v2/internal/model"

import (
	"encoding/json"
	"time"
)

// OPML import batch statuses.
const (
	OPMLImportStatusPending     = "pending"
	OPMLImportStatusInProgress  = "in_progress"
	OPMLImportStatusPaused      = "paused"
	OPMLImportStatusCancelling  = "cancelling"
	OPMLImportStatusCancelled   = "cancelled"
	OPMLImportStatusCompleted   = "completed"
)

// OPML import item statuses. A terminal status describes a definitive,
// user-visible outcome; pending and fetch_failed items can be processed
// again when the batch is continued.
const (
	OPMLImportItemStatusPending          = "pending"
	OPMLImportItemStatusCreated          = "created"
	OPMLImportItemStatusMerged           = "merged"
	OPMLImportItemStatusFetchFailed      = "fetch_failed"
	OPMLImportItemStatusValidationFailed = "validation_failed"
)

// OPMLItemSettings contains the frozen, Miniflux-specific feed settings of a
// single OPML subscription. They are persisted as JSON when the import plan
// is created so that later retries use exactly the settings that were
// submitted, even if the original OPML document is no longer available.
type OPMLItemSettings struct {
	ScraperRules                string `json:"scraper_rules,omitempty"`
	RewriteRules                string `json:"rewrite_rules,omitempty"`
	UrlRewriteRules             string `json:"url_rewrite_rules,omitempty"`
	BlocklistRules              string `json:"blocklist_rules,omitempty"`
	KeeplistRules               string `json:"keeplist_rules,omitempty"`
	BlockFilterEntryRules       string `json:"block_filter_entry_rules,omitempty"`
	KeepFilterEntryRules        string `json:"keep_filter_entry_rules,omitempty"`
	UserAgent                   string `json:"user_agent,omitempty"`
	Crawler                     bool   `json:"crawler,omitempty"`
	IgnoreHTTPCache             bool   `json:"ignore_http_cache,omitempty"`
	FetchViaProxy               bool   `json:"fetch_via_proxy,omitempty"`
	Disabled                    bool   `json:"disabled,omitempty"`
	NoMediaPlayer               bool   `json:"no_media_player,omitempty"`
	HideGlobally                bool   `json:"hide_globally,omitempty"`
	AllowSelfSignedCertificates bool   `json:"allow_self_signed_certificates,omitempty"`
	DisableHTTP2                bool   `json:"disable_http2,omitempty"`
	IgnoreEntryUpdates          bool   `json:"ignore_entry_updates,omitempty"`
}

// JSON serializes the frozen settings for database storage.
func (s OPMLItemSettings) JSON() []byte {
	b, err := json.Marshal(s)
	if err != nil {
		return []byte("{}")
	}
	return b
}

// DecodeOPMLItemSettings parses frozen settings stored in the database.
func DecodeOPMLItemSettings(data []byte) OPMLItemSettings {
	var settings OPMLItemSettings
	if len(data) == 0 {
		return settings
	}
	_ = json.Unmarshal(data, &settings)
	return settings
}

// OPMLImportItem is a single frozen subscription of an OPML import batch.
type OPMLImportItem struct {
	ID           int64            `json:"id"`
	ImportID     int64            `json:"import_id"`
	Position     int              `json:"position"`
	Title        string           `json:"title"`
	FeedURL      string           `json:"feed_url"`
	SiteURL      string           `json:"site_url"`
	Description  string           `json:"description"`
	CategoryName string           `json:"category_name"`
	Settings     OPMLItemSettings `json:"settings,omitempty"`
	Status       string           `json:"status"`
	FeedID       int64            `json:"feed_id,omitempty"`
	ErrorMessage string           `json:"error_message,omitempty"`
	Attempts     int              `json:"attempts"`
	CreatedAt    time.Time        `json:"created_at"`
	UpdatedAt    time.Time        `json:"updated_at"`
}

// OPMLImportItems is a list of OPML import items.
type OPMLImportItems []*OPMLImportItem

// OPMLImport is a frozen, resumable OPML import batch.
type OPMLImport struct {
	ID                    int64              `json:"id"`
	UserID                int64              `json:"user_id"`
	Title                 string             `json:"title"`
	ContentHash           string             `json:"-"`
	Status                string             `json:"status"`
	ErrorMessage          string             `json:"error_message,omitempty"`
	Total                 int                `json:"total"`
	CreatedCount          int                `json:"created_count"`
	MergedCount           int                `json:"merged_count"`
	PendingCount          int                `json:"pending_count"`
	FetchFailedCount      int                `json:"fetch_failed_count"`
	ValidationFailedCount int                `json:"validation_failed_count"`
	MissingFeedCount      int                `json:"missing_feed_count"`
	CreatedAt             time.Time          `json:"created_at"`
	StartedAt             *time.Time         `json:"started_at,omitempty"`
	FinishedAt            *time.Time         `json:"finished_at,omitempty"`
	UpdatedAt             time.Time          `json:"updated_at"`
	Items                 OPMLImportItems    `json:"items,omitempty"`
}

// OPMLImports is a list of OPML import batches.
type OPMLImports []*OPMLImport

// FrozenOPMLSubscription is a subscription extracted from an OPML document
// before the import plan is persisted.
type FrozenOPMLSubscription struct {
	Title        string
	SiteURL      string
	FeedURL      string
	CategoryName string
	Description  string
	Settings     OPMLItemSettings
}
