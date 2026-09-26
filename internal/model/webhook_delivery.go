// SPDX-FileCopyrightText: Copyright The Miniflux Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package model // import "miniflux.app/v2/internal/model"

import "time"

// Webhook delivery record statuses.
const (
	// WebhookDeliveryStatusPending is a delivery that has been committed but
	// has never been sent.
	WebhookDeliveryStatusPending = "pending"
	// WebhookDeliveryStatusInFlight is a delivery currently being sent.
	WebhookDeliveryStatusInFlight = "in_flight"
	// WebhookDeliveryStatusRetryWaiting is a delivery that was sent at least
	// once with a transient failure and is waiting for the backoff delay.
	WebhookDeliveryStatusRetryWaiting = "retry_waiting"
	// WebhookDeliveryStatusSucceeded is a terminal status: the remote endpoint
	// accepted the event.
	WebhookDeliveryStatusSucceeded = "succeeded"
	// WebhookDeliveryStatusFailed is a terminal status: the remote endpoint
	// permanently rejected the event, or the retry budget was exhausted.
	WebhookDeliveryStatusFailed = "failed"
	// WebhookDeliveryStatusCanceled is a terminal status for a never-sent
	// delivery that was revoked when the user canceled the save.
	WebhookDeliveryStatusCanceled = "canceled"
)

// WebhookDelivery is a persistent delivery record for a "save entry" webhook
// event. It implements the transactional outbox pattern: the record is
// committed in the same transaction as the entry state change, and a
// background dispatcher sends it after the commit.
type WebhookDelivery struct {
	ID             int64
	EventID        string
	UserID         int64
	EntryID        int64
	WebhookURL     string
	Status         string
	Attempts       int
	MaxAttempts    int
	LastHTTPStatus *int
	LastError      string
	CreatedAt      time.Time
	LastAttemptAt  *time.Time
	ClaimedAt      *time.Time
	NextAttemptAt  time.Time
	UpdatedAt      time.Time
}

// IsTerminal returns true if the delivery reached a terminal status.
func (d *WebhookDelivery) IsTerminal() bool {
	switch d.Status {
	case WebhookDeliveryStatusSucceeded, WebhookDeliveryStatusFailed, WebhookDeliveryStatusCanceled:
		return true
	default:
		return false
	}
}

// WebhookDeliveryView is the read-only representation of a delivery record
// exposed in the API and consumed by the web UI.
type WebhookDeliveryView struct {
	EventID        string     `json:"event_id"`
	Status         string     `json:"status"`
	Attempts       int        `json:"attempts"`
	MaxAttempts    int        `json:"max_attempts"`
	LastHTTPStatus *int       `json:"last_http_status,omitempty"`
	LastError      string     `json:"last_error,omitempty"`
	CreatedAt      time.Time  `json:"created_at"`
	LastAttemptAt  *time.Time `json:"last_attempt_at,omitempty"`
	NextAttemptAt  time.Time  `json:"next_attempt_at"`
	UpdatedAt      time.Time  `json:"updated_at"`
}

// View returns the read-only representation of the delivery record.
func (d *WebhookDelivery) View() *WebhookDeliveryView {
	return &WebhookDeliveryView{
		EventID:        d.EventID,
		Status:         d.Status,
		Attempts:       d.Attempts,
		MaxAttempts:    d.MaxAttempts,
		LastHTTPStatus: d.LastHTTPStatus,
		LastError:      d.LastError,
		CreatedAt:      d.CreatedAt,
		LastAttemptAt:  d.LastAttemptAt,
		NextAttemptAt:  d.NextAttemptAt,
		UpdatedAt:      d.UpdatedAt,
	}
}

// WebhookDeliveryList represents a list of webhook delivery records.
type WebhookDeliveryList []*WebhookDelivery
