// SPDX-FileCopyrightText: Copyright The Miniflux Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package storage // import "miniflux.app/v2/internal/storage"

import (
	"crypto/rand"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/lib/pq"

	"miniflux.app/v2/internal/model"
)

// Sentinel errors for the manual retry operation.
var (
	ErrWebhookDeliveryNotFound  = errors.New("store: webhook delivery record not found")
	ErrWebhookDeliveryNotFailed = errors.New("store: webhook delivery record is not in the failed status")
)

// webhookDeliveryColumns lists the persisted columns of a delivery record, in
// the exact scan order expected by scanWebhookDelivery.
const webhookDeliveryColumns = `
	id,
	event_id,
	user_id,
	entry_id,
	webhook_url,
	status,
	attempts,
	max_attempts,
	last_http_status,
	last_error,
	created_at,
	last_attempt_at,
	claimed_at,
	next_attempt_at,
	updated_at
`

// sqlExecutor is satisfied by both *sql.DB and *sql.Tx so the same statements
// can run inside or outside a transaction.
type sqlExecutor interface {
	Exec(query string, args ...any) (sql.Result, error)
	Query(query string, args ...any) (*sql.Rows, error)
	QueryRow(query string, args ...any) *sql.Row
}

type rowScanner interface {
	Scan(dest ...any) error
}

// newWebhookEventID returns a random RFC 4122 version 4 UUID string used as
// the stable event identifier of a delivery record. The same value is sent on
// every delivery attempt so the remote endpoint can deduplicate retries.
func newWebhookEventID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("store: unable to generate webhook event id: %v", err)
	}

	// Set the version (4) and variant (RFC 4122) bits.
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80

	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16]), nil
}

func scanWebhookDelivery(scanner rowScanner) (*model.WebhookDelivery, error) {
	var delivery model.WebhookDelivery
	var lastHTTPStatus sql.NullInt64
	var lastAttemptAt sql.NullTime
	var claimedAt sql.NullTime

	err := scanner.Scan(
		&delivery.ID,
		&delivery.EventID,
		&delivery.UserID,
		&delivery.EntryID,
		&delivery.WebhookURL,
		&delivery.Status,
		&delivery.Attempts,
		&delivery.MaxAttempts,
		&lastHTTPStatus,
		&delivery.LastError,
		&delivery.CreatedAt,
		&lastAttemptAt,
		&claimedAt,
		&delivery.NextAttemptAt,
		&delivery.UpdatedAt,
	)
	if err != nil {
		return nil, err
	}

	if lastHTTPStatus.Valid {
		statusCode := int(lastHTTPStatus.Int64)
		delivery.LastHTTPStatus = &statusCode
	}

	if lastAttemptAt.Valid {
		attemptTime := lastAttemptAt.Time
		delivery.LastAttemptAt = &attemptTime
	}

	if claimedAt.Valid {
		claimTime := claimedAt.Time
		delivery.ClaimedAt = &claimTime
	}

	return &delivery, nil
}

// insertPendingWebhookDelivery inserts a pending delivery record inside the
// given executor. The partial unique index guarantees that a duplicate save
// for the same user/entry reuses the existing active record instead of
// producing a second row.
func insertPendingWebhookDelivery(exec sqlExecutor, userID, entryID int64, webhookURL string, maxAttempts int) error {
	eventID, err := newWebhookEventID()
	if err != nil {
		return err
	}

	query := `
		INSERT INTO webhook_save_deliveries
			(event_id, user_id, entry_id, webhook_url, max_attempts)
		VALUES
			($1, $2, $3, $4, $5)
		ON CONFLICT (user_id, entry_id)
			WHERE status IN ('pending', 'in_flight', 'retry_waiting')
		DO NOTHING
	`

	if _, err := exec.Exec(query, eventID, userID, entryID, webhookURL, maxAttempts); err != nil {
		return fmt.Errorf("store: unable to create webhook delivery for user_id=%d entry_id=%d: %v", userID, entryID, err)
	}

	return nil
}

// cancelUnsentWebhookDeliveries revokes only the deliveries that have never
// been sent (pending with zero attempts). Records already on the wire
// (in_flight/retry_waiting) keep their original event identifier and continue
// to their terminal status; canceling them would mask an unknown outcome.
func cancelUnsentWebhookDeliveries(exec sqlExecutor, userID int64, entryIDs []int64) (int64, error) {
	query := `
		UPDATE
			webhook_save_deliveries
		SET
			status = 'canceled',
			updated_at = now()
		WHERE
			user_id = $1
			AND entry_id = ANY($2)
			AND status = 'pending'
			AND attempts = 0
	`

	result, err := exec.Exec(query, userID, pq.Array(entryIDs))
	if err != nil {
		return 0, fmt.Errorf("store: unable to cancel unsent webhook deliveries for user_id=%d: %v", userID, err)
	}

	count, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("store: unable to count canceled webhook deliveries for user_id=%d: %v", userID, err)
	}

	return count, nil
}

// CreateWebhookSaveDelivery persists a new pending delivery for a save action.
// If an active delivery already exists for the user/entry, it is returned
// unchanged. maxAttempts is the configured retry budget snapshot (0 means
// unlimited).
func (s *Storage) CreateWebhookSaveDelivery(userID, entryID int64, webhookURL string, maxAttempts int) (*model.WebhookDelivery, error) {
	eventID, err := newWebhookEventID()
	if err != nil {
		return nil, err
	}

	query := `
		INSERT INTO webhook_save_deliveries
			(event_id, user_id, entry_id, webhook_url, max_attempts)
		VALUES
			($1, $2, $3, $4, $5)
		ON CONFLICT (user_id, entry_id)
			WHERE status IN ('pending', 'in_flight', 'retry_waiting')
		DO NOTHING
		RETURNING ` + webhookDeliveryColumns

	delivery, err := scanWebhookDelivery(s.db.QueryRow(query, eventID, userID, entryID, webhookURL, maxAttempts))
	if errors.Is(err, sql.ErrNoRows) {
		existing, getErr := s.activeWebhookDelivery(userID, entryID)
		if getErr != nil {
			return nil, getErr
		}

		if existing == nil {
			return nil, fmt.Errorf("store: unable to create or find webhook delivery for user_id=%d entry_id=%d", userID, entryID)
		}

		return existing, nil
	}

	if err != nil {
		return nil, fmt.Errorf("store: unable to create webhook delivery for user_id=%d entry_id=%d: %v", userID, entryID, err)
	}

	return delivery, nil
}

// EntryForWebhookDelivery loads the entry (with its feed and category)
// required to (re)build a save_entry webhook payload. It returns nil without
// error when the entry no longer exists; in that case the delivery row itself
// is normally already gone because of the cascading foreign key.
func (s *Storage) EntryForWebhookDelivery(userID, entryID int64) (*model.Entry, error) {
	entry, err := s.NewEntryQueryBuilder(userID).
		WithEntryIDs(entryID).
		GetEntry()
	if err != nil {
		return nil, fmt.Errorf("store: unable to load entry %d for webhook delivery: %v", entryID, err)
	}

	return entry, nil
}

func (s *Storage) activeWebhookDelivery(userID, entryID int64) (*model.WebhookDelivery, error) {
	query := `
		SELECT
			` + webhookDeliveryColumns + `
		FROM
			webhook_save_deliveries
		WHERE
			user_id = $1
			AND entry_id = $2
			AND status IN ('pending', 'in_flight', 'retry_waiting')
		LIMIT 1
	`

	delivery, err := scanWebhookDelivery(s.db.QueryRow(query, userID, entryID))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}

	if err != nil {
		return nil, fmt.Errorf("store: unable to fetch active webhook delivery for user_id=%d entry_id=%d: %v", userID, entryID, err)
	}

	return delivery, nil
}

// SetEntriesStarredStateAndWebhookDeliveries updates the starred state of the
// given entries and synchronizes webhook deliveries in the same transaction:
// when starring, a pending delivery is enqueued for each entry (using the
// provided per-entry webhook URL snapshot); when unstarring, never-sent
// deliveries are canceled. The outbox rows are only visible to the dispatcher
// after the transaction commits, so no request can precede the state change.
func (s *Storage) SetEntriesStarredStateAndWebhookDeliveries(userID int64, entryIDs []int64, starred bool, webhookURLs map[int64]string, maxAttempts int) error {
	if len(entryIDs) == 0 {
		return nil
	}

	tx, err := s.db.Begin()
	if err != nil {
		return fmt.Errorf("store: unable to start transaction for starred state update: %v", err)
	}
	defer tx.Rollback()

	if _, err := tx.Exec(`UPDATE entries SET starred = $1, changed_at = now() WHERE user_id = $2 AND id = ANY($3)`, starred, userID, pq.Array(entryIDs)); err != nil {
		return fmt.Errorf("store: unable to update starred state for entries %v: %v", entryIDs, err)
	}

	if starred {
		for _, entryID := range entryIDs {
			webhookURL := webhookURLs[entryID]
			if webhookURL == "" {
				continue
			}

			if err := insertPendingWebhookDelivery(tx, userID, entryID, webhookURL, maxAttempts); err != nil {
				return err
			}
		}
	} else if _, err := cancelUnsentWebhookDeliveries(tx, userID, entryIDs); err != nil {
		return err
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("store: unable to commit starred state update: %v", err)
	}

	return nil
}

// WebhookDeliveryByEntry returns the most recent delivery record for a single
// user/entry pair, or nil if none exists.
func (s *Storage) WebhookDeliveryByEntry(userID, entryID int64) (*model.WebhookDelivery, error) {
	query := `
		SELECT
			` + webhookDeliveryColumns + `
		FROM
			webhook_save_deliveries
		WHERE
			user_id = $1
			AND entry_id = $2
		ORDER BY
			updated_at DESC,
			id DESC
		LIMIT 1
	`

	delivery, err := scanWebhookDelivery(s.db.QueryRow(query, userID, entryID))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}

	if err != nil {
		return nil, fmt.Errorf("store: unable to fetch webhook delivery for user_id=%d entry_id=%d: %v", userID, entryID, err)
	}

	return delivery, nil
}

// WebhookDeliveriesByEntries returns the most recent delivery record for each
// of the given entries in a single query (empty entries yield an empty map).
func (s *Storage) WebhookDeliveriesByEntries(userID int64, entryIDs []int64) (map[int64]*model.WebhookDelivery, error) {
	deliveries := make(map[int64]*model.WebhookDelivery)
	if len(entryIDs) == 0 {
		return deliveries, nil
	}

	query := `
		SELECT DISTINCT ON (entry_id)
			` + webhookDeliveryColumns + `
		FROM
			webhook_save_deliveries
		WHERE
			user_id = $1
			AND entry_id = ANY($2)
		ORDER BY
			entry_id,
			updated_at DESC,
			id DESC
	`

	rows, err := s.db.Query(query, userID, pq.Array(entryIDs))
	if err != nil {
		return nil, fmt.Errorf("store: unable to fetch webhook deliveries for user_id=%d: %v", userID, err)
	}
	defer rows.Close()

	for rows.Next() {
		delivery, err := scanWebhookDelivery(rows)
		if err != nil {
			return nil, fmt.Errorf("store: unable to scan webhook delivery: %v", err)
		}

		deliveries[delivery.EntryID] = delivery
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: unable to iterate webhook deliveries: %v", err)
	}

	return deliveries, nil
}

// ResetFailedWebhookDelivery moves the most recent failed delivery for a
// user/entry back to pending for a manual retry. The original event
// identifier is preserved so the remote endpoint can deduplicate the new
// attempt; the retry budget is restored. Only the latest record is reset so
// older historical failures cannot be reactivated simultaneously (which
// would violate the single-active-delivery constraint). It returns
// ErrWebhookDeliveryNotFound when no record exists and
// ErrWebhookDeliveryNotFailed when the latest record is not failed.
func (s *Storage) ResetFailedWebhookDelivery(userID, entryID int64) (*model.WebhookDelivery, error) {
	query := `
		UPDATE
			webhook_save_deliveries
		SET
			status = 'pending',
			attempts = 0,
			last_http_status = NULL,
			last_error = '',
			claimed_at = NULL,
			next_attempt_at = now(),
			updated_at = now()
		WHERE
			id = (
				SELECT
					id
				FROM
					webhook_save_deliveries
				WHERE
					user_id = $1
					AND entry_id = $2
				ORDER BY
					updated_at DESC,
					id DESC
				LIMIT 1
			)
			AND status = 'failed'
		RETURNING ` + webhookDeliveryColumns

	delivery, err := scanWebhookDelivery(s.db.QueryRow(query, userID, entryID))
	if errors.Is(err, sql.ErrNoRows) {
		existing, getErr := s.WebhookDeliveryByEntry(userID, entryID)
		if getErr != nil {
			return nil, getErr
		}

		if existing == nil {
			return nil, ErrWebhookDeliveryNotFound
		}

		return nil, ErrWebhookDeliveryNotFailed
	}

	if err != nil {
		return nil, fmt.Errorf("store: unable to reset failed webhook delivery for user_id=%d entry_id=%d: %v", userID, entryID, err)
	}

	return delivery, nil
}

// CleanOldWebhookDeliveries deletes terminal delivery records whose last
// update is older than the given cutoff and returns the number of deleted
// rows. Non-terminal records are always retained.
func (s *Storage) CleanOldWebhookDeliveries(cutoff time.Time) (int64, error) {
	query := `
		DELETE FROM
			webhook_save_deliveries
		WHERE
			status IN ('succeeded', 'failed', 'canceled')
			AND updated_at < $1
	`

	result, err := s.db.Exec(query, cutoff)
	if err != nil {
		return 0, fmt.Errorf("store: unable to clean old webhook deliveries: %v", err)
	}

	count, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("store: unable to count cleaned webhook deliveries: %v", err)
	}

	return count, nil
}

// ReclaimExpiredInFlightWebhookDeliveries turns in-flight records older than
// the claim lease back into due retry_waiting records. A record reaches that
// state when the sender process died (or was restarted) while the HTTP
// request was in flight; the retry keeps the original event identifier.
func (s *Storage) ReclaimExpiredInFlightWebhookDeliveries(lease time.Duration) (int64, error) {
	cutoff := time.Now().Add(-lease)

	query := `
		UPDATE
			webhook_save_deliveries
		SET
			status = 'retry_waiting',
			next_attempt_at = now(),
			updated_at = now()
		WHERE
			status = 'in_flight'
			AND claimed_at < $1
	`

	result, err := s.db.Exec(query, cutoff)
	if err != nil {
		return 0, fmt.Errorf("store: unable to reclaim expired in-flight webhook deliveries: %v", err)
	}

	count, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("store: unable to count reclaimed webhook deliveries: %v", err)
	}

	return count, nil
}

// ClaimDueWebhookDeliveries atomically claims a batch of due deliveries using
// FOR UPDATE SKIP LOCKED so that multiple instances can never send the same
// attempt concurrently. Claimed records become in_flight with their attempt
// counter incremented; the caller owns the HTTP attempt and must record its
// outcome with one of the Mark methods.
func (s *Storage) ClaimDueWebhookDeliveries(batchSize int) (model.WebhookDeliveryList, error) {
	tx, err := s.db.Begin()
	if err != nil {
		return nil, fmt.Errorf("store: unable to start transaction to claim webhook deliveries: %v", err)
	}
	defer tx.Rollback()

	// Only claim deliveries of users whose webhook integration is enabled.
	// Rows for disabled users stay untouched (attempts are not burned while
	// the integration is switched off) and resume naturally once it is
	// enabled again. Every user has an integrations row by construction.
	selectQuery := `
		SELECT
			d.id
		FROM
			webhook_save_deliveries AS d
			JOIN integrations AS i ON i.user_id = d.user_id
		WHERE
			i.webhook_enabled = true
			AND d.status IN ('pending', 'retry_waiting')
			AND d.next_attempt_at <= now()
		ORDER BY
			d.next_attempt_at
		LIMIT $1
		FOR UPDATE OF d SKIP LOCKED
	`

	rows, err := tx.Query(selectQuery, batchSize)
	if err != nil {
		return nil, fmt.Errorf("store: unable to select due webhook deliveries: %v", err)
	}

	var ids []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return nil, fmt.Errorf("store: unable to scan claimed webhook delivery id: %v", err)
		}

		ids = append(ids, id)
	}
	rows.Close()

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: unable to iterate claimed webhook deliveries: %v", err)
	}

	if len(ids) == 0 {
		return nil, nil
	}

	updateQuery := `
		UPDATE
			webhook_save_deliveries
		SET
			status = 'in_flight',
			attempts = attempts + 1,
			last_attempt_at = now(),
			claimed_at = now(),
			updated_at = now()
		WHERE
			id = ANY($1)
			AND status IN ('pending', 'retry_waiting')
		RETURNING ` + webhookDeliveryColumns

	deliveryRows, err := tx.Query(updateQuery, pq.Array(ids))
	if err != nil {
		return nil, fmt.Errorf("store: unable to mark webhook deliveries as in_flight: %v", err)
	}
	defer deliveryRows.Close()

	deliveries := make(model.WebhookDeliveryList, 0, len(ids))
	for deliveryRows.Next() {
		delivery, err := scanWebhookDelivery(deliveryRows)
		if err != nil {
			return nil, fmt.Errorf("store: unable to scan in_flight webhook delivery: %v", err)
		}

		deliveries = append(deliveries, delivery)
	}

	if err := deliveryRows.Err(); err != nil {
		return nil, fmt.Errorf("store: unable to iterate in_flight webhook deliveries: %v", err)
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("store: unable to commit claimed webhook deliveries: %v", err)
	}

	return deliveries, nil
}

// MarkWebhookDeliverySucceeded records the final successful outcome of an
// in_flight delivery. Only an in_flight record can be advanced, so a late
// response after crash takeover cannot overwrite the terminal status.
func (s *Storage) MarkWebhookDeliverySucceeded(id int64, httpStatus int) (int64, error) {
	query := `
		UPDATE
			webhook_save_deliveries
		SET
			status = 'succeeded',
			last_http_status = $2,
			updated_at = now()
		WHERE
			id = $1
			AND status = 'in_flight'
	`

	result, err := s.db.Exec(query, id, httpStatus)
	if err != nil {
		return 0, fmt.Errorf("store: unable to mark webhook delivery #%d as succeeded: %v", id, err)
	}

	return result.RowsAffected()
}

// MarkWebhookDeliveryWaitingRetry moves an in_flight delivery back to
// retry_waiting until nextAttemptAt. httpStatus may be nil when no response
// was received (transport error).
func (s *Storage) MarkWebhookDeliveryWaitingRetry(id int64, httpStatus *int, errText string, nextAttemptAt time.Time) (int64, error) {
	query := `
		UPDATE
			webhook_save_deliveries
		SET
			status = 'retry_waiting',
			last_http_status = $2,
			last_error = $3,
			next_attempt_at = $4,
			updated_at = now()
		WHERE
			id = $1
			AND status = 'in_flight'
	`

	result, err := s.db.Exec(query, id, httpStatus, errText, nextAttemptAt)
	if err != nil {
		return 0, fmt.Errorf("store: unable to mark webhook delivery #%d as waiting retry: %v", id, err)
	}

	return result.RowsAffected()
}

// MarkWebhookDeliveryFailed records the terminal failure of an in_flight
// delivery (permanent rejection or exhausted retry budget).
func (s *Storage) MarkWebhookDeliveryFailed(id int64, httpStatus *int, errText string) (int64, error) {
	query := `
		UPDATE
			webhook_save_deliveries
		SET
			status = 'failed',
			last_http_status = $2,
			last_error = $3,
			updated_at = now()
		WHERE
			id = $1
			AND status = 'in_flight'
	`

	result, err := s.db.Exec(query, id, httpStatus, errText)
	if err != nil {
		return 0, fmt.Errorf("store: unable to mark webhook delivery #%d as failed: %v", id, err)
	}

	return result.RowsAffected()
}
