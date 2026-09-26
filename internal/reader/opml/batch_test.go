// SPDX-FileCopyrightText: Copyright The Miniflux Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package opml // import "miniflux.app/v2/internal/reader/opml"

import (
	"bytes"
	"testing"

	"miniflux.app/v2/internal/model"
)

func TestSubscriptionsHashIsStableAndOrderSensitive(t *testing.T) {
	subscriptions := []model.FrozenOPMLSubscription{
		{FeedURL: "http://example.org/feed1", CategoryName: "News", Settings: model.OPMLItemSettings{Crawler: true}},
		{FeedURL: "http://example.org/feed2", CategoryName: "Tech"},
	}

	first := subscriptionsHash(subscriptions)
	second := subscriptionsHash(subscriptions)

	if first == "" {
		t.Fatal("The subscription hash must not be empty")
	}

	if first != second {
		t.Fatal("The same frozen plan must always produce the same hash")
	}

	reversed := []model.FrozenOPMLSubscription{subscriptions[1], subscriptions[0]}
	if subscriptionsHash(reversed) == first {
		t.Fatal("Plans with a different item order must produce a different hash")
	}

	modified := []model.FrozenOPMLSubscription{
		{FeedURL: "http://example.org/feed1", CategoryName: "Different"},
		{FeedURL: "http://example.org/feed2"},
	}
	if subscriptionsHash(modified) == first {
		t.Fatal("Plans with different categories must produce a different hash")
	}
}

func TestParsePlanFreezesTitleOrderAndSettings(t *testing.T) {
	data := `<?xml version="1.0"?>
	<opml version="2.0" xmlns:miniflux="https://miniflux.app/opml">
		<head><title>Migrated subscriptions</title></head>
		<body>
			<outline text="News">
				<outline type="rss" title="Feed 1" xmlUrl="http://example.org/feed1/" htmlUrl="http://example.org/1" miniflux:crawler="true"/>
				<outline type="rss" title="Feed 2" xmlUrl="http://example.org/feed2/" htmlUrl="http://example.org/2"/>
			</outline>
			<outline type="rss" title="Feed 3" xmlUrl="http://example.org/feed3/" htmlUrl="http://example.org/3"/>
		</body>
	</opml>`

	title, subscriptions, err := parsePlan(bytes.NewBufferString(data))
	if err != nil {
		t.Fatal(err)
	}

	if title != "Migrated subscriptions" {
		t.Fatalf("Unexpected title: %q", title)
	}

	if len(subscriptions) != 3 {
		t.Fatalf("Wrong number of subscriptions: %d", len(subscriptions))
	}

	// Document order is preserved, including nesting.
	expectedOrder := []string{
		"http://example.org/feed1/",
		"http://example.org/feed2/",
		"http://example.org/feed3/",
	}
	for i, expected := range expectedOrder {
		if subscriptions[i].FeedURL != expected {
			t.Errorf("Item #%d: got %q want %q", i+1, subscriptions[i].FeedURL, expected)
		}
	}

	if subscriptions[0].CategoryName != "News" {
		t.Errorf("Nested subscriptions must keep their frozen category: %q", subscriptions[0].CategoryName)
	}

	if !subscriptions[0].Settings.Crawler {
		t.Error("Miniflux settings must be frozen in the plan")
	}

	if subscriptions[1].Settings.Crawler {
		t.Error("Settings must not leak from one subscription to another")
	}
}

func TestParsePlanRejectsInvalidXML(t *testing.T) {
	if _, _, err := parsePlan(bytes.NewBufferString("garbage")); err == nil {
		t.Error("An invalid OPML document must produce an error")
	}
}
