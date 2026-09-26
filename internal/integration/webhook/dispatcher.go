// SPDX-FileCopyrightText: Copyright The Miniflux Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package webhook // import "miniflux.app/v2/internal/integration/webhook"

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"miniflux.app/v2/internal/config"
	"miniflux.app/v2/internal/model"
	"miniflux.app/v2/internal/storage"
)

// maxReasonLength bounds the error text persisted on a delivery record.
const maxReasonLength = 2048

// DeliveryStore is the subset of storage methods required by the dispatcher.
// *storage.Storage satisfies it.
type DeliveryStore interface {
	ReclaimExpiredInFlightWebhookDeliveries(lease time.Duration) (int64, error)
	ClaimDueWebhookDeliveries(batchSize int) (model.WebhookDeliveryList, error)
	MarkWebhookDeliverySucceeded(id int64, httpStatus int) (int64, error)
	MarkWebhookDeliveryWaitingRetry(id int64, httpStatus *int, errText string, nextAttemptAt time.Time) (int64, error)
	MarkWebhookDeliveryFailed(id int64, httpStatus *int, errText string) (int64, error)
	EntryForWebhookDelivery(userID, entryID int64) (*model.Entry, error)
	Integration(userID int64) (*model.Integration, error)
}

var _ DeliveryStore = (*storage.Storage)(nil)

// Clock abstracts time for deterministic dispatcher tests.
type Clock interface {
	Now() time.Time
}

type wallClock struct{}

func (wallClock) Now() time.Time { return time.Now() }

// Dispatcher polls the delivery outbox and sends due save_entry webhook
// events. Rows are claimed with SELECT ... FOR UPDATE SKIP LOCKED so multiple
// instances can run concurrently without ever sending the same attempt twice,
// and records stuck in_flight after a process crash are reclaimed after the
// configured lease.
type Dispatcher struct {
	store             DeliveryStore
	pollInterval      time.Duration
	lease             time.Duration
	initialBackoff    time.Duration
	maxBackoff        time.Duration
	backoffMultiplier int
	batchSize         int
	clock             Clock
	kick              chan struct{}
}

// NewDispatcher builds a dispatcher from the global configuration.
func NewDispatcher(store DeliveryStore) *Dispatcher {
	return &Dispatcher{
		store:             store,
		pollInterval:      config.Opts.WebhookSavePollingFrequency(),
		lease:             config.Opts.WebhookSaveClaimLease(),
		initialBackoff:    config.Opts.WebhookSaveInitialBackoff(),
		maxBackoff:        config.Opts.WebhookSaveMaxBackoff(),
		backoffMultiplier: config.Opts.WebhookSaveBackoffMultiplier(),
		batchSize:         config.Opts.WorkerPoolSize(),
		clock:             wallClock{},
		kick:              make(chan struct{}, 1),
	}
}

// backoffDelay returns the wait before attempt number attempt (1-based,
// already incremented on the record): initial * multiplier^(attempt-1),
// capped at maxBackoff.
func backoffDelay(attempt int, initial, maxBackoff time.Duration, multiplier int) time.Duration {
	delay := initial

	for i := 1; i < attempt; i++ {
		next := delay * time.Duration(multiplier)
		// Defensive overflow guard.
		if next <= delay || next >= maxBackoff {
			return maxBackoff
		}

		delay = next
	}

	if delay > maxBackoff {
		return maxBackoff
	}

	return delay
}

// dispatcherKick is the optional in-process wakeup channel registered by the
// running dispatcher. Enqueue paths use NotifyPending for immediate dispatch;
// the polling ticker remains the cross-instance and restart fallback.
var (
	dispatcherKickMu sync.Mutex
	dispatcherKick   chan<- struct{}
)

// NotifyPending wakes up the local dispatcher when new delivery records have
// been committed. It is safe to call when no dispatcher is running.
func NotifyPending() {
	dispatcherKickMu.Lock()
	kick := dispatcherKick
	dispatcherKickMu.Unlock()

	if kick != nil {
		select {
		case kick <- struct{}{}:
		default:
		}
	}
}

func (d *Dispatcher) registerKick() {
	dispatcherKickMu.Lock()
	dispatcherKick = d.kick
	dispatcherKickMu.Unlock()
}

func (d *Dispatcher) unregisterKick() {
	dispatcherKickMu.Lock()
	if dispatcherKick == d.kick {
		dispatcherKick = nil
	}
	dispatcherKickMu.Unlock()
}

// Run starts the dispatch loop until the context is canceled.
func (d *Dispatcher) Run(ctx context.Context) {
	slog.Debug("Starting webhook save delivery dispatcher",
		slog.Duration("polling_frequency", d.pollInterval),
		slog.Duration("claim_lease", d.lease),
		slog.Int("batch_size", d.batchSize),
	)

	d.registerKick()
	defer d.unregisterKick()

	ticker := time.NewTicker(d.pollInterval)
	defer ticker.Stop()

	// Catch records committed while the process was down.
	d.processOnce(ctx)

	for {
		select {
		case <-ctx.Done():
			slog.Debug("Stopping webhook save delivery dispatcher")
			return
		case <-ticker.C:
			d.processOnce(ctx)
		case <-d.kick:
			d.processOnce(ctx)
		}
	}
}

// processOnce reclaims stale in-flight records and then drains every due
// batch. It is split out for unit testing.
func (d *Dispatcher) processOnce(ctx context.Context) {
	reclaimed, err := d.store.ReclaimExpiredInFlightWebhookDeliveries(d.lease)
	if err != nil {
		slog.Error("Unable to reclaim expired webhook deliveries", slog.Any("error", err))
		return
	}

	if reclaimed > 0 {
		slog.Info("Reclaimed stale in-flight webhook deliveries after lease timeout",
			slog.Int64("count", reclaimed),
			slog.Duration("lease", d.lease),
		)
	}

	for {
		if ctx.Err() != nil {
			return
		}

		deliveries, err := d.store.ClaimDueWebhookDeliveries(d.batchSize)
		if err != nil {
			slog.Error("Unable to claim due webhook deliveries", slog.Any("error", err))
			return
		}

		if len(deliveries) == 0 {
			return
		}

		var wg sync.WaitGroup
		for _, delivery := range deliveries {
			wg.Add(1)
			go func() {
				defer wg.Done()
				d.deliver(ctx, delivery)
			}()
		}
		wg.Wait()
	}
}

func (d *Dispatcher) deliver(ctx context.Context, delivery *model.WebhookDelivery) {
	entry, err := d.store.EntryForWebhookDelivery(delivery.UserID, delivery.EntryID)
	if err != nil {
		slog.Error("Unable to load entry for webhook delivery",
			slog.Int64("delivery_id", delivery.ID),
			slog.String("event_id", delivery.EventID),
			slog.Any("error", err),
		)
		// Leave the record in_flight: the lease-based reclaim will retry it.
		return
	}

	if entry == nil {
		// The entry (and normally the delivery row via cascade) is gone.
		reason := "entry no longer exists"
		if _, err := d.store.MarkWebhookDeliveryFailed(delivery.ID, nil, reason); err != nil {
			slog.Error("Unable to mark orphan webhook delivery as failed",
				slog.Int64("delivery_id", delivery.ID),
				slog.Any("error", err),
			)
		}
		return
	}

	settings, err := d.store.Integration(delivery.UserID)
	if err != nil {
		slog.Error("Unable to load integration settings for webhook delivery",
			slog.Int64("delivery_id", delivery.ID),
			slog.Int64("user_id", delivery.UserID),
			slog.Any("error", err),
		)
		// Leave the record in_flight for lease-based reclaim.
		return
	}

	// Defense in depth: the claim query already filters on
	// integrations.webhook_enabled, so this only triggers when the setting
	// was toggled between claim and send. The row stays in_flight here and
	// is reclaimed by the lease; while disabled it is not re-claimed, so no
	// retry budget is burned.
	if !settings.WebhookEnabled {
		slog.Debug("Webhook integration disabled, pausing delivery",
			slog.Int64("user_id", delivery.UserID),
			slog.Int64("entry_id", delivery.EntryID),
			slog.String("event_id", delivery.EventID),
		)
		return
	}

	slog.Debug("Sending save entry webhook delivery",
		slog.Int64("user_id", delivery.UserID),
		slog.Int64("entry_id", delivery.EntryID),
		slog.String("event_id", delivery.EventID),
		slog.Int("attempts", delivery.Attempts),
		slog.String("webhook_url", delivery.WebhookURL),
	)

	result, sendErr := NewClient(delivery.WebhookURL, settings.WebhookSecret).
		SendSaveEntryWebhookEvent(entry, delivery.EventID)

	d.recordOutcome(delivery, result, sendErr)
}

func (d *Dispatcher) recordOutcome(delivery *model.WebhookDelivery, result *AttemptResult, sendErr error) {
	switch {
	case sendErr == nil:
		if rows, err := d.store.MarkWebhookDeliverySucceeded(delivery.ID, result.StatusCode); err != nil {
			slog.Error("Unable to mark webhook delivery as succeeded",
				slog.Int64("delivery_id", delivery.ID),
				slog.Any("error", err),
			)
		} else if rows == 1 {
			slog.Info("Save entry webhook delivery accepted",
				slog.Int64("user_id", delivery.UserID),
				slog.Int64("entry_id", delivery.EntryID),
				slog.String("event_id", delivery.EventID),
				slog.Int("attempts", delivery.Attempts),
				slog.Int("http_status", result.StatusCode),
			)
		}

	case isTransportError(result, sendErr):
		// No response was received: the remote outcome is unknown, so the
		// record must be retried with the same event identifier.
		d.scheduleRetryOrFail(delivery, nil, truncateReason(sendErr.Error()), 0)

	default:
		// We received an HTTP error response.
		var requestError *RequestError
		if errors.As(sendErr, &requestError) {
			reason := truncateReason(fmt.Sprintf("webhook endpoint rejected the event with HTTP %d", requestError.StatusCode))
			if result != nil && result.ResponseSnippet != "" {
				reason = truncateReason(fmt.Sprintf("%s: %s", reason, result.ResponseSnippet))
			}

			if IsPermanentStatus(requestError.StatusCode) {
				d.failDelivery(delivery, &requestError.StatusCode, reason)
				return
			}

			retryAfter := time.Duration(0)
			if result != nil {
				retryAfter = result.RetryAfter
			}

			d.scheduleRetryOrFail(delivery, &requestError.StatusCode, reason, retryAfter)
			return
		}

		// Defensive: any other error is treated as transient.
		d.scheduleRetryOrFail(delivery, nil, truncateReason(sendErr.Error()), 0)
	}
}

func isTransportError(result *AttemptResult, err error) bool {
	return result == nil && err != nil
}

func (d *Dispatcher) failDelivery(delivery *model.WebhookDelivery, httpStatus *int, reason string) {
	if rows, err := d.store.MarkWebhookDeliveryFailed(delivery.ID, httpStatus, reason); err != nil {
		slog.Error("Unable to mark webhook delivery as permanently failed",
			slog.Int64("delivery_id", delivery.ID),
			slog.Any("error", err),
		)
		return
	} else if rows == 1 {
		slog.Warn("Save entry webhook delivery permanently rejected",
			slog.Int64("user_id", delivery.UserID),
			slog.Int64("entry_id", delivery.EntryID),
			slog.String("event_id", delivery.EventID),
			slog.Int("attempts", delivery.Attempts),
			slog.String("reason", reason),
		)
	}
}

// scheduleRetryOrFail records either the next retry_waiting deadline or, when
// the (snapshot) retry budget is exhausted, the terminal failure. A Retry-After
// hint takes precedence over the exponential backoff but is never allowed to
// exceed the configured maximum backoff.
func (d *Dispatcher) scheduleRetryOrFail(delivery *model.WebhookDelivery, httpStatus *int, reason string, retryAfter time.Duration) {
	if delivery.MaxAttempts > 0 && delivery.Attempts >= delivery.MaxAttempts {
		exhaustedReason := truncateReason(fmt.Sprintf("%s (max attempts %d reached)", reason, delivery.MaxAttempts))
		d.failDelivery(delivery, httpStatus, exhaustedReason)
		return
	}

	delay := d.retryDelay(delivery.Attempts)
	if retryAfter > 0 {
		if retryAfter > d.maxBackoff {
			retryAfter = d.maxBackoff
		}

		delay = retryAfter
	}

	nextAttemptAt := d.clock.Now().Add(delay)

	if rows, err := d.store.MarkWebhookDeliveryWaitingRetry(delivery.ID, httpStatus, reason, nextAttemptAt); err != nil {
		slog.Error("Unable to schedule webhook delivery retry",
			slog.Int64("delivery_id", delivery.ID),
			slog.Any("error", err),
		)
		return
	} else if rows == 1 {
		slog.Warn("Save entry webhook delivery will be retried",
			slog.Int64("user_id", delivery.UserID),
			slog.Int64("entry_id", delivery.EntryID),
			slog.String("event_id", delivery.EventID),
			slog.Int("attempts", delivery.Attempts),
			slog.Time("next_attempt_at", nextAttemptAt),
			slog.String("reason", reason),
		)
	}
}

func (d *Dispatcher) retryDelay(attempts int) time.Duration {
	return backoffDelay(attempts, d.initialBackoff, d.maxBackoff, d.backoffMultiplier)
}

func truncateReason(reason string) string {
	if len(reason) > maxReasonLength {
		return reason[:maxReasonLength] + "…"
	}

	return reason
}
