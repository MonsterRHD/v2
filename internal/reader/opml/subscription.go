// SPDX-FileCopyrightText: Copyright The Miniflux Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package opml // import "miniflux.app/v2/internal/reader/opml"

import "miniflux.app/v2/internal/model"

// subcription represents a feed that will be imported or exported.
type subcription struct {
	Title        string
	SiteURL      string
	FeedURL      string
	CategoryName string
	Description  string

	// Miniflux-specific feed settings
	ScraperRules                string
	RewriteRules                string
	UrlRewriteRules             string
	BlocklistRules              string
	KeeplistRules               string
	BlockFilterEntryRules       string
	KeepFilterEntryRules        string
	UserAgent                   string
	Crawler                     bool
	IgnoreHTTPCache             bool
	FetchViaProxy               bool
	Disabled                    bool
	NoMediaPlayer               bool
	HideGlobally                bool
	AllowSelfSignedCertificates bool
	DisableHTTP2                bool
	IgnoreEntryUpdates          bool
}

func (s subcription) toFrozen() model.FrozenOPMLSubscription {
	return model.FrozenOPMLSubscription{
		Title:        s.Title,
		SiteURL:      s.SiteURL,
		FeedURL:      s.FeedURL,
		CategoryName: s.CategoryName,
		Description:  s.Description,
		Settings: model.OPMLItemSettings{
			ScraperRules:                s.ScraperRules,
			RewriteRules:                s.RewriteRules,
			UrlRewriteRules:             s.UrlRewriteRules,
			BlocklistRules:              s.BlocklistRules,
			KeeplistRules:               s.KeeplistRules,
			BlockFilterEntryRules:       s.BlockFilterEntryRules,
			KeepFilterEntryRules:        s.KeepFilterEntryRules,
			UserAgent:                   s.UserAgent,
			Crawler:                     s.Crawler,
			IgnoreHTTPCache:             s.IgnoreHTTPCache,
			FetchViaProxy:               s.FetchViaProxy,
			Disabled:                    s.Disabled,
			NoMediaPlayer:               s.NoMediaPlayer,
			HideGlobally:                s.HideGlobally,
			AllowSelfSignedCertificates: s.AllowSelfSignedCertificates,
			DisableHTTP2:                s.DisableHTTP2,
			IgnoreEntryUpdates:          s.IgnoreEntryUpdates,
		},
	}
}

// feedCreationRequestFromFrozen builds the feed creation request used for
// validation and for the fetch-based creation of a frozen import item.
func feedCreationRequestFromFrozen(s model.FrozenOPMLSubscription, categoryID int64) *model.FeedCreationRequest {
	return &model.FeedCreationRequest{
		FeedURL:                     s.FeedURL,
		CategoryID:                  categoryID,
		UserAgent:                   s.Settings.UserAgent,
		Crawler:                     s.Settings.Crawler,
		IgnoreEntryUpdates:          s.Settings.IgnoreEntryUpdates,
		Disabled:                    s.Settings.Disabled,
		NoMediaPlayer:               s.Settings.NoMediaPlayer,
		IgnoreHTTPCache:             s.Settings.IgnoreHTTPCache,
		AllowSelfSignedCertificates: s.Settings.AllowSelfSignedCertificates,
		FetchViaProxy:               s.Settings.FetchViaProxy,
		HideGlobally:                s.Settings.HideGlobally,
		DisableHTTP2:                s.Settings.DisableHTTP2,
		ScraperRules:                s.Settings.ScraperRules,
		RewriteRules:                s.Settings.RewriteRules,
		BlocklistRules:              s.Settings.BlocklistRules,
		KeeplistRules:               s.Settings.KeeplistRules,
		BlockFilterEntryRules:       s.Settings.BlockFilterEntryRules,
		KeepFilterEntryRules:        s.Settings.KeepFilterEntryRules,
		UrlRewriteRules:             s.Settings.UrlRewriteRules,
	}
}

