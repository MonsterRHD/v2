// SPDX-FileCopyrightText: Copyright The Miniflux Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package opml // import "miniflux.app/v2/internal/reader/opml"

import (
	"encoding/xml"
	"fmt"
	"io"

	"miniflux.app/v2/internal/model"
	"miniflux.app/v2/internal/reader/encoding"
)

// parse reads an OPML file and returns a list of subscription.
func parse(data io.Reader) ([]subcription, error) {
	_, subscriptions, err := parsePlan(data)
	if err != nil {
		return nil, err
	}

	result := make([]subcription, 0, len(subscriptions))
	for _, subscription := range subscriptions {
		result = append(result, frozenToSubcription(subscription))
	}

	return result, nil
}

func frozenToSubcription(s model.FrozenOPMLSubscription) subcription {
	return subcription{
		Title:                       s.Title,
		SiteURL:                     s.SiteURL,
		FeedURL:                     s.FeedURL,
		CategoryName:                s.CategoryName,
		Description:                 s.Description,
		ScraperRules:                s.Settings.ScraperRules,
		RewriteRules:                s.Settings.RewriteRules,
		UrlRewriteRules:             s.Settings.UrlRewriteRules,
		BlocklistRules:              s.Settings.BlocklistRules,
		KeeplistRules:               s.Settings.KeeplistRules,
		BlockFilterEntryRules:       s.Settings.BlockFilterEntryRules,
		KeepFilterEntryRules:        s.Settings.KeepFilterEntryRules,
		UserAgent:                   s.Settings.UserAgent,
		Crawler:                     s.Settings.Crawler,
		IgnoreHTTPCache:             s.Settings.IgnoreHTTPCache,
		FetchViaProxy:               s.Settings.FetchViaProxy,
		Disabled:                    s.Settings.Disabled,
		NoMediaPlayer:               s.Settings.NoMediaPlayer,
		HideGlobally:                s.Settings.HideGlobally,
		AllowSelfSignedCertificates: s.Settings.AllowSelfSignedCertificates,
		DisableHTTP2:                s.Settings.DisableHTTP2,
		IgnoreEntryUpdates:          s.Settings.IgnoreEntryUpdates,
	}
}

// parsePlan reads an OPML file and returns the document title together with
// the frozen subscription plan in document order.
func parsePlan(data io.Reader) (string, []model.FrozenOPMLSubscription, error) {
	opmlDocument := &opmlDocument{}
	decoder := xml.NewDecoder(data)
	decoder.Entity = xml.HTMLEntity
	decoder.Strict = false
	decoder.CharsetReader = encoding.CharsetReader

	err := decoder.Decode(opmlDocument)
	if err != nil {
		return "", nil, fmt.Errorf("opml: unable to parse document: %w", err)
	}

	subscriptions := getSubscriptionsFromOutlines(opmlDocument.Outlines, "")

	frozen := make([]model.FrozenOPMLSubscription, 0, len(subscriptions))
	for _, subscription := range subscriptions {
		frozen = append(frozen, subscription.toFrozen())
	}

	return opmlDocument.Header.Title, frozen, nil
}

func getSubscriptionsFromOutlines(outlines opmlOutlineCollection, category string) []subcription {
	subscriptions := make([]subcription, 0, len(outlines))

	for _, outline := range outlines {
		if outline.IsSubscription() {
			subscriptions = append(subscriptions, subcription{
				Title:        outline.GetTitle(),
				FeedURL:      outline.FeedURL,
				SiteURL:      outline.GetSiteURL(),
				Description:  outline.Description,
				CategoryName: category,

				ScraperRules:                outline.ScraperRules,
				RewriteRules:                outline.RewriteRules,
				UrlRewriteRules:             outline.UrlRewriteRules,
				BlocklistRules:              outline.BlocklistRules,
				KeeplistRules:               outline.KeeplistRules,
				BlockFilterEntryRules:       outline.BlockFilterEntryRules,
				KeepFilterEntryRules:        outline.KeepFilterEntryRules,
				UserAgent:                   outline.UserAgent,
				Crawler:                     outline.Crawler,
				IgnoreHTTPCache:             outline.IgnoreHTTPCache,
				FetchViaProxy:               outline.FetchViaProxy,
				Disabled:                    outline.Disabled,
				NoMediaPlayer:               outline.NoMediaPlayer,
				HideGlobally:                outline.HideGlobally,
				AllowSelfSignedCertificates: outline.AllowSelfSignedCertificates,
				DisableHTTP2:                outline.DisableHTTP2,
				IgnoreEntryUpdates:          outline.IgnoreEntryUpdates,
			})
		} else if outline.Outlines.HasChildren() {
			subscriptions = append(subscriptions, getSubscriptionsFromOutlines(outline.Outlines, outline.GetTitle())...)
		}
	}
	return subscriptions
}
