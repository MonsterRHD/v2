// SPDX-FileCopyrightText: Copyright The Miniflux Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package webhook // import "miniflux.app/v2/internal/integration/webhook"

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"miniflux.app/v2/internal/crypto"
	"miniflux.app/v2/internal/http/client"
	"miniflux.app/v2/internal/model"
)

const (
	NewEntriesEventType = "new_entries"
	SaveEntryEventType  = "save_entry"
)

// Webhook HTTP headers.
const (
	SignatureHeader = "X-Miniflux-Signature"
	EventTypeHeader = "X-Miniflux-Event-Type"
	EventIDHeader   = "X-Miniflux-Event-ID"
)

// maxResponseSnippetLength bounds how much of an error response body is kept
// as a human-readable failure reason.
const maxResponseSnippetLength = 1024

type Client struct {
	webhookURL    string
	webhookSecret string
}

func NewClient(webhookURL, webhookSecret string) *Client {
	return &Client{webhookURL, webhookSecret}
}

// AttemptResult describes the HTTP outcome of a single delivery attempt. It is
// returned even when the response is an error status code, so the dispatcher
// can distinguish transient from permanent rejections.
type AttemptResult struct {
	// StatusCode is the HTTP status code returned by the remote endpoint. It
	// is zero when no response was received (transport error/timeout).
	StatusCode int
	// RetryAfter is the parsed Retry-After delay for 413/429/503 responses.
	// It is zero when the header is absent or invalid.
	RetryAfter time.Duration
	// ResponseSnippet is the truncated response body captured for error
	// status codes, used as the failure reason shown to the user.
	ResponseSnippet string
}

// RequestError is returned when the remote endpoint answers with an HTTP
// error status code (>= 400).
type RequestError struct {
	StatusCode int
	RetryAfter time.Duration
}

func (e *RequestError) Error() string {
	return fmt.Sprintf("webhook: unexpected response status code %d", e.StatusCode)
}

// IsPermanentStatus reports whether an HTTP status code is an explicit
// permanent rejection: 4xx responses, except 408 (request timeout) and 429
// (rate limited), which are retryable.
func IsPermanentStatus(statusCode int) bool {
	return statusCode >= 400 && statusCode < 500 &&
		statusCode != http.StatusRequestTimeout &&
		statusCode != http.StatusTooManyRequests
}

// IsTransientStatus reports whether an HTTP status code may succeed on retry:
// 408, 429 and every 5xx server error.
func IsTransientStatus(statusCode int) bool {
	return statusCode == http.StatusRequestTimeout ||
		statusCode == http.StatusTooManyRequests ||
		statusCode >= 500
}

// ParseRetryAfter parses a Retry-After header value expressed either as a
// number of seconds or an HTTP date. Invalid, zero or past values yield 0.
func ParseRetryAfter(value string) time.Duration {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0
	}

	if seconds, err := strconv.Atoi(value); err == nil {
		if seconds < 0 {
			return 0
		}

		return time.Duration(seconds) * time.Second
	}

	if date, err := http.ParseTime(value); err == nil {
		delay := time.Until(date)
		if delay < 0 {
			return 0
		}

		return delay
	}

	return 0
}

func readResponseSnippet(body io.Reader) string {
	data, err := io.ReadAll(io.LimitReader(body, maxResponseSnippetLength+1))
	if err != nil {
		return ""
	}

	snippet := strings.TrimSpace(strings.ToValidUTF8(string(data), ""))
	if len(snippet) > maxResponseSnippetLength {
		snippet = snippet[:maxResponseSnippetLength] + "…"
	}

	return snippet
}

// SendSaveEntryWebhookEvent sends a save_entry event. The eventID is the stable
// delivery identifier: it is sent unchanged on every attempt (including
// retries after a crash) so the remote endpoint can deduplicate them.
func (c *Client) SendSaveEntryWebhookEvent(entry *model.Entry, eventID string) (*AttemptResult, error) {
	return c.makeRequest(SaveEntryEventType, eventID, &WebhookSaveEntryEvent{
		EventType: SaveEntryEventType,
		Entry: &WebhookEntry{
			ID:          entry.ID,
			UserID:      entry.UserID,
			FeedID:      entry.FeedID,
			Status:      entry.Status,
			Hash:        entry.Hash,
			Title:       entry.Title,
			URL:         entry.URL,
			CommentsURL: entry.CommentsURL,
			Date:        entry.Date,
			CreatedAt:   entry.CreatedAt,
			ChangedAt:   entry.ChangedAt,
			Content:     entry.Content,
			Author:      entry.Author,
			ShareCode:   entry.ShareCode,
			Starred:     entry.Starred,
			ReadingTime: entry.ReadingTime,
			Enclosures:  entry.Enclosures,
			Tags:        entry.Tags,
			Feed: &WebhookFeed{
				ID:         entry.Feed.ID,
				UserID:     entry.Feed.UserID,
				CategoryID: entry.Feed.Category.ID,
				Category:   &WebhookCategory{ID: entry.Feed.Category.ID, Title: entry.Feed.Category.Title},
				FeedURL:    entry.Feed.FeedURL,
				SiteURL:    entry.Feed.SiteURL,
				Title:      entry.Feed.Title,
				CheckedAt:  entry.Feed.CheckedAt,
			},
		},
	})
}

func (c *Client) SendNewEntriesWebhookEvent(feed *model.Feed, entries model.Entries) error {
	if len(entries) == 0 {
		return nil
	}

	webhookEntries := make([]*WebhookEntry, 0, len(entries))
	for _, entry := range entries {
		webhookEntries = append(webhookEntries, &WebhookEntry{
			ID:          entry.ID,
			UserID:      entry.UserID,
			FeedID:      entry.FeedID,
			Status:      entry.Status,
			Hash:        entry.Hash,
			Title:       entry.Title,
			URL:         entry.URL,
			CommentsURL: entry.CommentsURL,
			Date:        entry.Date,
			CreatedAt:   entry.CreatedAt,
			ChangedAt:   entry.ChangedAt,
			Content:     entry.Content,
			Author:      entry.Author,
			ShareCode:   entry.ShareCode,
			Starred:     entry.Starred,
			ReadingTime: entry.ReadingTime,
			Enclosures:  entry.Enclosures,
			Tags:        entry.Tags,
		})
	}

	_, err := c.makeRequest(NewEntriesEventType, "", &WebhookNewEntriesEvent{
		EventType: NewEntriesEventType,
		Feed: &WebhookFeed{
			ID:         feed.ID,
			UserID:     feed.UserID,
			CategoryID: feed.Category.ID,
			Category:   &WebhookCategory{ID: feed.Category.ID, Title: feed.Category.Title},
			FeedURL:    feed.FeedURL,
			SiteURL:    feed.SiteURL,
			Title:      feed.Title,
			CheckedAt:  feed.CheckedAt,
		},
		Entries: webhookEntries,
	})
	return err
}

func (c *Client) makeRequest(eventType, eventID string, payload any) (*AttemptResult, error) {
	if c.webhookURL == "" {
		return nil, errors.New(`webhook: missing webhook URL`)
	}

	requestBody, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("webhook: unable to encode request body: %v", err)
	}

	requestBuilder := client.NewRequestBuilder(c.webhookURL).
		WithMethod(http.MethodPost).
		WithJSONBody(requestBody).
		WithHeader(SignatureHeader, crypto.GenerateSHA256Hmac(c.webhookSecret, requestBody)).
		WithHeader(EventTypeHeader, eventType)

	// The save event carries its stable delivery identifier; the bulk
	// new_entries event keeps its historical headers unchanged.
	if eventID != "" {
		requestBuilder = requestBuilder.WithHeader(EventIDHeader, eventID)
	}

	response, err := requestBuilder.Do()
	if err != nil {
		return nil, fmt.Errorf("webhook: %w", err)
	}
	defer response.Body.Close()

	result := &AttemptResult{
		StatusCode: response.StatusCode,
		RetryAfter: ParseRetryAfter(response.Header.Get("Retry-After")),
	}

	if response.StatusCode >= 400 {
		result.ResponseSnippet = readResponseSnippet(response.Body)
		return result, &RequestError{StatusCode: response.StatusCode, RetryAfter: result.RetryAfter}
	}

	return result, nil
}

type WebhookFeed struct {
	ID         int64            `json:"id"`
	UserID     int64            `json:"user_id"`
	CategoryID int64            `json:"category_id"`
	Category   *WebhookCategory `json:"category,omitempty"`
	FeedURL    string           `json:"feed_url"`
	SiteURL    string           `json:"site_url"`
	Title      string           `json:"title"`
	CheckedAt  time.Time        `json:"checked_at"`
}

type WebhookCategory struct {
	ID    int64  `json:"id"`
	Title string `json:"title"`
}

type WebhookEntry struct {
	ID          int64               `json:"id"`
	UserID      int64               `json:"user_id"`
	FeedID      int64               `json:"feed_id"`
	Status      string              `json:"status"`
	Hash        string              `json:"hash"`
	Title       string              `json:"title"`
	URL         string              `json:"url"`
	CommentsURL string              `json:"comments_url"`
	Date        time.Time           `json:"published_at"`
	CreatedAt   time.Time           `json:"created_at"`
	ChangedAt   time.Time           `json:"changed_at"`
	Content     string              `json:"content"`
	Author      string              `json:"author"`
	ShareCode   string              `json:"share_code"`
	Starred     bool                `json:"starred"`
	ReadingTime int                 `json:"reading_time"`
	Enclosures  model.EnclosureList `json:"enclosures"`
	Tags        []string            `json:"tags"`
	Feed        *WebhookFeed        `json:"feed,omitempty"`
}

type WebhookNewEntriesEvent struct {
	EventType string          `json:"event_type"`
	Feed      *WebhookFeed    `json:"feed"`
	Entries   []*WebhookEntry `json:"entries"`
}

type WebhookSaveEntryEvent struct {
	EventType string        `json:"event_type"`
	Entry     *WebhookEntry `json:"entry"`
}
