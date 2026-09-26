// SPDX-FileCopyrightText: Copyright The Miniflux Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package webhook

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"miniflux.app/v2/internal/config"
	"miniflux.app/v2/internal/crypto"
	"miniflux.app/v2/internal/model"
)

func configureIntegrationAllowPrivateNetworksOption(t *testing.T) {
	t.Helper()

	t.Setenv("INTEGRATION_ALLOW_PRIVATE_NETWORKS", "1")

	configParser := config.NewConfigParser()
	parsedOptions, err := configParser.ParseEnvironmentVariables()
	if err != nil {
		t.Fatalf("Unable to configure test options: %v", err)
	}

	previousOptions := config.Opts
	config.Opts = parsedOptions
	t.Cleanup(func() {
		config.Opts = previousOptions
	})
}

func newTestEntry() *model.Entry {
	entry := model.NewEntry()
	entry.ID = 42
	entry.UserID = 7
	entry.FeedID = 9
	entry.Title = "Test entry"
	entry.URL = "https://example.com/article.html"
	entry.Feed.ID = 9
	entry.Feed.UserID = 7
	entry.Feed.Category.ID = 3
	entry.Feed.Category.Title = "Test category"
	entry.Feed.FeedURL = "https://example.com/feed.xml"
	entry.Feed.SiteURL = "https://example.com/"
	entry.Feed.Title = "Test feed"

	return entry
}

func TestSendSaveEntryWebhookEventHeadersSignatureAndPayload(t *testing.T) {
	configureIntegrationAllowPrivateNetworksOption(t)

	const eventID = "11111111-2222-4333-8444-555555555555"
	const secret = "topsecret"

	var gotMethod, gotEventType, gotEventID, gotSignature string
	var rawBody []byte

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotEventType = r.Header.Get(EventTypeHeader)
		gotEventID = r.Header.Get(EventIDHeader)
		gotSignature = r.Header.Get(SignatureHeader)
		rawBody, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	client := NewClient(server.URL, secret)
	result, err := client.SendSaveEntryWebhookEvent(newTestEntry(), eventID)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if result == nil || result.StatusCode != http.StatusNoContent {
		t.Fatalf("Unexpected result: %+v", result)
	}

	if gotMethod != http.MethodPost {
		t.Fatalf("Expected POST, got %q", gotMethod)
	}

	if gotEventType != SaveEntryEventType {
		t.Fatalf("Expected event type %q, got %q", SaveEntryEventType, gotEventType)
	}

	if gotEventID != eventID {
		t.Fatalf("Expected event id %q, got %q", eventID, gotEventID)
	}

	expectedSignature := crypto.GenerateSHA256Hmac(secret, rawBody)
	if gotSignature != expectedSignature {
		t.Fatalf("Signature mismatch: got %q want %q", gotSignature, expectedSignature)
	}

	var payload WebhookSaveEntryEvent
	if err := json.Unmarshal(rawBody, &payload); err != nil {
		t.Fatalf("Invalid JSON payload: %v", err)
	}

	if payload.EventType != SaveEntryEventType || payload.Entry == nil || payload.Entry.ID != 42 || payload.Entry.Feed == nil || payload.Entry.Feed.ID != 9 {
		t.Fatalf("Unexpected payload: %+v", payload)
	}
}

func TestSendSaveEntryWebhookEventEventIDStaysStableAcrossAttempts(t *testing.T) {
	configureIntegrationAllowPrivateNetworksOption(t)

	const eventID = "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee"
	seenEventIDs := make(map[string]struct{})

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seenEventIDs[r.Header.Get(EventIDHeader)] = struct{}{}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client := NewClient(server.URL, "")

	for range 3 {
		if _, err := client.SendSaveEntryWebhookEvent(newTestEntry(), eventID); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
	}

	if len(seenEventIDs) != 1 {
		t.Fatalf("Expected exactly one distinct event id across attempts, got %v", seenEventIDs)
	}

	if _, ok := seenEventIDs[eventID]; !ok {
		t.Fatalf("Expected event id %q to be the only one seen, got %v", eventID, seenEventIDs)
	}
}

func TestSendSaveEntryWebhookEventSuccessAndMissingURL(t *testing.T) {
	configureIntegrationAllowPrivateNetworksOption(t)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	result, err := NewClient(server.URL, "").SendSaveEntryWebhookEvent(newTestEntry(), "evt-1")
	if err != nil {
		t.Fatalf("Expected success, got %v", err)
	}

	if result.StatusCode != http.StatusOK {
		t.Fatalf("Expected status 200, got %d", result.StatusCode)
	}

	if _, err := NewClient("", "").SendSaveEntryWebhookEvent(newTestEntry(), "evt-1"); err == nil {
		t.Fatal("Expected an error with an empty webhook URL")
	}
}

func TestSendSaveEntryWebhookEventPermanentRejectionCapturesSnippet(t *testing.T) {
	configureIntegrationAllowPrivateNetworksOption(t)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte("bookmark not configured"))
	}))
	defer server.Close()

	result, err := NewClient(server.URL, "").SendSaveEntryWebhookEvent(newTestEntry(), "evt-404")
	if err == nil {
		t.Fatal("Expected a request error for HTTP 404")
	}

	var requestError *RequestError
	if !errors.As(err, &requestError) {
		t.Fatalf("Expected *RequestError, got %T: %v", err, err)
	}

	if requestError.StatusCode != http.StatusNotFound {
		t.Fatalf("Expected status 404 in error, got %d", requestError.StatusCode)
	}

	if result == nil || result.StatusCode != http.StatusNotFound || strings.TrimSpace(result.ResponseSnippet) != "bookmark not configured" {
		t.Fatalf("Unexpected result: %+v", result)
	}
}

func TestSendSaveEntryWebhookEventTransientStatusAndRetryAfterSeconds(t *testing.T) {
	configureIntegrationAllowPrivateNetworksOption(t)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Retry-After", "30")
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer server.Close()

	result, err := NewClient(server.URL, "").SendSaveEntryWebhookEvent(newTestEntry(), "evt-429")
	if err == nil {
		t.Fatal("Expected a request error for HTTP 429")
	}

	if result == nil || result.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("Unexpected result: %+v", result)
	}

	if result.RetryAfter != 30*time.Second {
		t.Fatalf("Expected 30s retry-after, got %s", result.RetryAfter)
	}
}

func TestParseRetryAfter(t *testing.T) {
	futureDate := time.Now().Add(45 * time.Second).UTC().Format(http.TimeFormat)

	tests := []struct {
		name  string
		value string
		want  time.Duration
	}{
		{"empty", "", 0},
		{"seconds", "12", 12 * time.Second},
		{"http date", futureDate, 45 * time.Second},
		{"garbage", "soon", 0},
		{"negative seconds", "-5", 0},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := ParseRetryAfter(tc.value)
			diff := got - tc.want
			if diff < 0 {
				diff = -diff
			}

			if diff > 2*time.Second {
				t.Fatalf("ParseRetryAfter(%q) = %s, want ~%s", tc.value, got, tc.want)
			}
		})
	}
}

func TestStatusClassification(t *testing.T) {
	permanent := []int{http.StatusBadRequest, http.StatusUnauthorized, http.StatusForbidden, http.StatusNotFound, http.StatusConflict, http.StatusGone, http.StatusUnprocessableEntity}
	for _, status := range permanent {
		if !IsPermanentStatus(status) {
			t.Errorf("Status %d should be permanent", status)
		}

		if IsTransientStatus(status) {
			t.Errorf("Status %d should not be transient", status)
		}
	}

	transient := []int{http.StatusRequestTimeout, http.StatusTooManyRequests, http.StatusInternalServerError, http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout}
	for _, status := range transient {
		if !IsTransientStatus(status) {
			t.Errorf("Status %d should be transient", status)
		}

		if IsPermanentStatus(status) {
			t.Errorf("Status %d should not be permanent", status)
		}
	}

	if IsPermanentStatus(http.StatusOK) || IsTransientStatus(http.StatusOK) {
		t.Error("Status 200 must be neither permanent nor transient")
	}
}

func TestSendSaveEntryWebhookEventTransportErrorIsTransient(t *testing.T) {
	configureIntegrationAllowPrivateNetworksOption(t)

	// Nothing listens on this port.
	client := NewClient("http://127.0.0.1:1/hook", "")

	result, err := client.SendSaveEntryWebhookEvent(newTestEntry(), "evt-down")
	if err == nil {
		t.Fatal("Expected a transport error")
	}

	if result != nil {
		t.Fatalf("Transport errors must not carry an HTTP result, got %+v", result)
	}

	var requestError *RequestError
	if errors.As(err, &requestError) {
		t.Fatalf("Transport errors must not be classified as request errors, got %v", err)
	}
}

func TestSendNewEntriesWebhookEventHasNoEventIDHeader(t *testing.T) {
	configureIntegrationAllowPrivateNetworksOption(t)

	var gotEventID string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotEventID = r.Header.Get(EventIDHeader)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	feed := &model.Feed{ID: 9, WebhookURL: server.URL, Category: &model.Category{ID: 3, Title: "Test category"}}
	entries := model.Entries{newTestEntry()}

	if err := NewClient(server.URL, "").SendNewEntriesWebhookEvent(feed, entries); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if gotEventID != "" {
		t.Fatalf("new_entries events must not carry an event id header, got %q", gotEventID)
	}
}

func TestSendSaveEntryWebhookEventSnippetTruncation(t *testing.T) {
	configureIntegrationAllowPrivateNetworksOption(t)

	longBody := strings.Repeat("x", maxResponseSnippetLength*2)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(longBody))
	}))
	defer server.Close()

	result, err := NewClient(server.URL, "").SendSaveEntryWebhookEvent(newTestEntry(), "evt-long")
	if err == nil {
		t.Fatal("Expected a request error")
	}

	if len(result.ResponseSnippet) > maxResponseSnippetLength+len("…") {
		t.Fatalf("Response snippet should be bounded, got %d bytes", len(result.ResponseSnippet))
	}
}
