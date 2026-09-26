// SPDX-FileCopyrightText: Copyright The Miniflux Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package storage // import "miniflux.app/v2/internal/storage

import (
	"database/sql"
	"errors"
	"fmt"

	"github.com/lib/pq"
	"miniflux.app/v2/internal/model"
)

// Errors returned by the OPML import storage.
var (
	ErrOPMLImportNotFound     = errors.New("store: OPML import not found")
	ErrOPMLImportItemNotFound = errors.New("store: OPML import item not found")
	ErrOPMLImportActiveExists = errors.New("store: an active OPML import with the same content already exists")
)

const opmlImportItemColumns = `id, import_id, position, title, feed_url, site_url, description, category_name, settings, status, COALESCE(feed_id, 0), error_message, attempts, created_at, updated_at`

// CreateOPMLImport persists a frozen import plan together with its items in a
// single transaction. If an active import with the same content hash already
// exists for the user, ErrOPMLImportActiveExists is returned.
func (s *Storage) CreateOPMLImport(imp *model.OPMLImport, items model.OPMLImportItems) error {
	tx, err := s.db.Begin()
	if err != nil {
		return fmt.Errorf("store: unable to start transaction: %w", err)
	}

	query := `
		INSERT INTO opml_imports
			(user_id, title, content_hash, status)
		VALUES
			($1, $2, $3, $4::opml_import_status)
		RETURNING
			id, created_at, updated_at
	`
	err = tx.QueryRow(query, imp.UserID, imp.Title, imp.ContentHash, model.OPMLImportStatusPending).Scan(&imp.ID, &imp.CreatedAt, &imp.UpdatedAt)
	if err != nil {
		if rollbackErr := tx.Rollback(); rollbackErr != nil {
			return fmt.Errorf("store: unable to rollback transaction: %v (rolled back due to: %v)", rollbackErr, err)
		}
		var pqErr *pq.Error
		if errors.As(err, &pqErr) && pqErr.Code == "23505" {
			return ErrOPMLImportActiveExists
		}
		return fmt.Errorf("store: unable to create OPML import: %w", err)
	}

	itemQuery := `
		INSERT INTO opml_import_items
			(import_id, position, title, feed_url, site_url, description, category_name, settings)
		VALUES
			($1, $2, $3, $4, $5, $6, $7, $8::jsonb)
		RETURNING
			id, created_at, updated_at
	`

	for _, item := range items {
		item.ImportID = imp.ID
		err = tx.QueryRow(
			itemQuery,
			imp.ID,
			item.Position,
			item.Title,
			item.FeedURL,
			item.SiteURL,
			item.Description,
			item.CategoryName,
			string(item.Settings.JSON()),
		).Scan(&item.ID, &item.CreatedAt, &item.UpdatedAt)
		if err != nil {
			if rollbackErr := tx.Rollback(); rollbackErr != nil {
				return fmt.Errorf("store: unable to rollback transaction: %v (rolled back due to: %v)", rollbackErr, err)
			}
			return fmt.Errorf("store: unable to create OPML import item: %w", err)
		}
		item.Status = model.OPMLImportItemStatusPending
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("store: unable to commit transaction: %w", err)
	}

	imp.Status = model.OPMLImportStatusPending
	imp.Total = len(items)
	imp.PendingCount = len(items)
	imp.Items = items
	return nil
}

// ActiveOPMLImportByHash returns the non-terminal import matching the given
// content hash for the user, or nil if none exists.
func (s *Storage) ActiveOPMLImportByHash(userID int64, contentHash string) (*model.OPMLImport, error) {
	row := s.db.QueryRow(opmlImportSelectPrefix+`
		LEFT JOIN opml_import_items AS items ON items.import_id = opml_imports.id
		WHERE
			opml_imports.user_id=$1 AND opml_imports.content_hash=$2
			AND opml_imports.status IN ('pending', 'in_progress', 'paused', 'cancelling')
		GROUP BY opml_imports.id
	`, userID, contentHash)

	imp, err := scanOPMLImport(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return imp, nil
}

// GetOPMLImport returns the import with the per-status item counters. When
// withItems is true, all frozen items are returned as well, ordered by their
// original position.
func (s *Storage) GetOPMLImport(userID, importID int64, withItems bool) (*model.OPMLImport, error) {
	imp, err := s.fetchOPMLImport(`
		LEFT JOIN opml_import_items AS items ON items.import_id = opml_imports.id
		WHERE
			opml_imports.user_id=$1 AND opml_imports.id=$2
		GROUP BY opml_imports.id
	`, userID, importID)
	if err != nil {
		return nil, err
	}

	if withItems {
		items, err := s.getOPMLImportItems(importID)
		if err != nil {
			return nil, err
		}
		imp.Items = items
	}

	return imp, nil
}

// ListOPMLImports returns all imports belonging to the user, newest first.
func (s *Storage) ListOPMLImports(userID int64) (model.OPMLImports, error) {
	query := opmlImportSelectPrefix + `
		LEFT JOIN opml_import_items AS items ON items.import_id = opml_imports.id
		WHERE
			opml_imports.user_id=$1
		GROUP BY opml_imports.id
		ORDER BY opml_imports.created_at DESC
	`
	rows, err := s.db.Query(query, userID)
	if err != nil {
		return nil, fmt.Errorf("store: unable to fetch OPML imports: %w", err)
	}
	defer rows.Close()

	imports := make(model.OPMLImports, 0)
	for rows.Next() {
		imp, err := scanOPMLImport(rows)
		if err != nil {
			return nil, err
		}
		imports = append(imports, imp)
	}

	return imports, nil
}

const opmlImportSelectPrefix = `
	SELECT
		opml_imports.id,
		opml_imports.user_id,
		opml_imports.title,
		opml_imports.content_hash,
		opml_imports.status,
		opml_imports.error_message,
		opml_imports.created_at,
		opml_imports.started_at,
		opml_imports.finished_at,
		opml_imports.updated_at,
		count(items.id),
		count(items.id) FILTER (WHERE items.status = 'created'),
		count(items.id) FILTER (WHERE items.status = 'merged'),
		count(items.id) FILTER (WHERE items.status = 'pending'),
		count(items.id) FILTER (WHERE items.status = 'fetch_failed'),
		count(items.id) FILTER (WHERE items.status = 'validation_failed'),
		count(items.id) FILTER (WHERE items.status = 'created' AND items.feed_id IS NULL)
	FROM opml_imports
`

type opmlImportRowScanner interface {
	Scan(dest ...any) error
}

func (s *Storage) fetchOPMLImport(whereClause string, args ...any) (*model.OPMLImport, error) {
	row := s.db.QueryRow(opmlImportSelectPrefix+whereClause, args...)
	imp, err := scanOPMLImport(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrOPMLImportNotFound
	}
	if err != nil {
		return nil, err
	}
	return imp, nil
}

func scanOPMLImport(scanner opmlImportRowScanner) (*model.OPMLImport, error) {
	var imp model.OPMLImport
	err := scanner.Scan(
		&imp.ID,
		&imp.UserID,
		&imp.Title,
		&imp.ContentHash,
		&imp.Status,
		&imp.ErrorMessage,
		&imp.CreatedAt,
		&imp.StartedAt,
		&imp.FinishedAt,
		&imp.UpdatedAt,
		&imp.Total,
		&imp.CreatedCount,
		&imp.MergedCount,
		&imp.PendingCount,
		&imp.FetchFailedCount,
		&imp.ValidationFailedCount,
		&imp.MissingFeedCount,
	)
	if err != nil {
		return nil, fmt.Errorf("store: unable to scan OPML import: %w", err)
	}
	return &imp, nil
}

func (s *Storage) getOPMLImportItems(importID int64) (model.OPMLImportItems, error) {
	query := `
		SELECT ` + opmlImportItemColumns + `
		FROM opml_import_items
		WHERE import_id=$1
		ORDER BY position ASC
	`
	rows, err := s.db.Query(query, importID)
	if err != nil {
		return nil, fmt.Errorf("store: unable to fetch OPML import items: %w", err)
	}
	defer rows.Close()

	return scanOPMLImportItems(rows)
}

func scanOPMLImportItems(rows *sql.Rows) (model.OPMLImportItems, error) {
	items := make(model.OPMLImportItems, 0)
	for rows.Next() {
		item, err := scanOPMLImportItem(rows)
		if err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, nil
}

func scanOPMLImportItem(scanner opmlImportRowScanner) (*model.OPMLImportItem, error) {
	var item model.OPMLImportItem
	var settings []byte
	err := scanner.Scan(
		&item.ID,
		&item.ImportID,
		&item.Position,
		&item.Title,
		&item.FeedURL,
		&item.SiteURL,
		&item.Description,
		&item.CategoryName,
		&settings,
		&item.Status,
		&item.FeedID,
		&item.ErrorMessage,
		&item.Attempts,
		&item.CreatedAt,
		&item.UpdatedAt,
	)
	if err != nil {
		return nil, fmt.Errorf("store: unable to scan OPML import item: %w", err)
	}
	item.Settings = model.DecodeOPMLItemSettings(settings)
	return &item, nil
}

// OPMLImportStatus returns the current status of an import owned by the user.
func (s *Storage) OPMLImportStatus(userID, importID int64) (string, error) {
	var status string
	query := `SELECT status FROM opml_imports WHERE id=$1 AND user_id=$2`
	if err := s.db.QueryRow(query, importID, userID).Scan(&status); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", ErrOPMLImportNotFound
		}
		return "", fmt.Errorf("store: unable to fetch OPML import status: %w", err)
	}
	return status, nil
}

// OPMLImportCancellationRequested reports whether a cancel has been requested
// or confirmed for the import.
func (s *Storage) OPMLImportCancellationRequested(userID, importID int64) (bool, error) {
	status, err := s.OPMLImportStatus(userID, importID)
	if err != nil {
		return false, err
	}
	return status == model.OPMLImportStatusCancelling || status == model.OPMLImportStatusCancelled, nil
}

// TryStartOPMLImport transitions a non-running import to in_progress so that
// only one runner processes the batch. It returns true when the caller has
// won the run and false when the import is already running. Completed imports
// can only be restarted when reopenCompleted is true, which is used to retry a
// specific corrected item.
func (s *Storage) TryStartOPMLImport(userID, importID int64, reopenCompleted bool) (bool, error) {
	allowedStatuses := []string{
		model.OPMLImportStatusPending,
		model.OPMLImportStatusPaused,
		model.OPMLImportStatusCancelled,
	}
	if reopenCompleted {
		allowedStatuses = append(allowedStatuses, model.OPMLImportStatusCompleted)
	}

	query := `
		UPDATE opml_imports
		SET status='in_progress',
			started_at=COALESCE(started_at, now()),
			finished_at=NULL,
			updated_at=now()
		WHERE id=$1 AND user_id=$2 AND status = ANY($3)
	`
	result, err := s.db.Exec(query, importID, userID, pq.Array(allowedStatuses))
	if err != nil {
		return false, fmt.Errorf("store: unable to start OPML import: %w", err)
	}

	count, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("store: unable to check OPML import start result: %w", err)
	}

	return count == 1, nil
}

// RequestOPMLImportCancellation marks an import for cancellation. Running
// imports switch to cancelling (the runner stops before the next fetch);
// non-running imports are cancelled immediately. It returns the resulting
// status.
func (s *Storage) RequestOPMLImportCancellation(userID, importID int64) (string, error) {
	query := `
		UPDATE opml_imports
		SET
			status = (CASE
				WHEN status IN ('in_progress', 'cancelling') THEN 'cancelling'
				WHEN status IN ('pending', 'paused', 'cancelled') THEN 'cancelled'
				ELSE status
			END)::opml_import_status,
			finished_at = CASE
				WHEN status IN ('pending', 'paused') THEN COALESCE(finished_at, now())
				ELSE finished_at
			END,
			updated_at=now()
		WHERE id=$1 AND user_id=$2
		RETURNING status
	`
	var status string
	if err := s.db.QueryRow(query, importID, userID).Scan(&status); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", ErrOPMLImportNotFound
		}
		return "", fmt.Errorf("store: unable to cancel OPML import: %w", err)
	}
	return status, nil
}

// FinishOPMLImportRun recomputes the final status of a run: cancelled when a
// cancellation was requested, paused when retryable items remain, completed
// otherwise.
func (s *Storage) FinishOPMLImportRun(userID, importID int64) error {
	query := `
		UPDATE opml_imports
		SET
			status=(CASE
				WHEN status IN ('cancelling', 'cancelled') THEN 'cancelled'
				WHEN EXISTS (
					SELECT 1 FROM opml_import_items
					WHERE import_id=opml_imports.id AND status IN ('pending', 'fetch_failed')
				) THEN 'paused'
				ELSE 'completed'
			END)::opml_import_status,
			finished_at = CASE
				WHEN status IN ('cancelling', 'cancelled') THEN COALESCE(finished_at, now())
				WHEN EXISTS (
					SELECT 1 FROM opml_import_items
					WHERE import_id=opml_imports.id AND status IN ('pending', 'fetch_failed')
				) THEN NULL
				ELSE COALESCE(finished_at, now())
			END,
			updated_at=now()
		WHERE id=$1 AND user_id=$2
	`
	result, err := s.db.Exec(query, importID, userID)
	if err != nil {
		return fmt.Errorf("store: unable to finish OPML import run: %w", err)
	}

	count, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("store: unable to check OPML import finish result: %w", err)
	}
	if count == 0 {
		return ErrOPMLImportNotFound
	}
	return nil
}

// NextOPMLImportItem returns the next item to process in original position
// order. Only pending and fetch_failed items are eligible; attemptedIDs lists
// items already attempted by the current run (a temporary failure is only
// retried on an explicit continuation, never in a tight loop). When onlyItemID
// is non-zero, the selection is restricted to that item (used when retrying a
// single corrected item).
func (s *Storage) NextOPMLImportItem(importID, onlyItemID int64, attemptedIDs []int64) (*model.OPMLImportItem, error) {
	if attemptedIDs == nil {
		attemptedIDs = []int64{}
	}

	query := `
		SELECT ` + opmlImportItemColumns + `
		FROM opml_import_items
		WHERE import_id=$1
			AND status IN ('pending', 'fetch_failed')
			AND NOT (id = ANY($2))
			AND ($3 = 0 OR id = $3)
		ORDER BY position ASC
		LIMIT 1
	`
	row := s.db.QueryRow(query, importID, pq.Array(attemptedIDs), onlyItemID)
	item, err := scanOPMLImportItem(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return item, nil
}

// CompleteOPMLImportItem persists the definitive outcome of an item attempt.
func (s *Storage) CompleteOPMLImportItem(importID, itemID, feedID int64, status, errorMessage string) error {
	query := `
		UPDATE opml_import_items
		SET
			status=$3::opml_import_item_status,
			feed_id=NULLIF($4, 0),
			error_message=$5,
			attempts=attempts+1,
			updated_at=now()
		WHERE id=$1 AND import_id=$2
	`
	result, err := s.db.Exec(query, itemID, importID, status, feedID, errorMessage)
	if err != nil {
		return fmt.Errorf("store: unable to update OPML import item: %w", err)
	}

	count, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("store: unable to check OPML import item update result: %w", err)
	}
	if count == 0 {
		return ErrOPMLImportItemNotFound
	}
	return nil
}

// GetOPMLImportItem returns a specific item, scoped to the import owner.
func (s *Storage) GetOPMLImportItem(userID, importID, itemID int64) (*model.OPMLImportItem, error) {
	query := `
		SELECT ` + opmlImportItemSelectColumns() + `
		FROM opml_import_items
		WHERE opml_import_items.id=$1
			AND opml_import_items.import_id=$2
			AND EXISTS (SELECT 1 FROM opml_imports WHERE opml_imports.id=opml_import_items.import_id AND opml_imports.user_id=$3)
	`
	row := s.db.QueryRow(query, itemID, importID, userID)
	item, err := scanOPMLImportItem(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrOPMLImportItemNotFound
	}
	if err != nil {
		return nil, err
	}
	return item, nil
}

func opmlImportItemSelectColumns() string {
	return `
		opml_import_items.id,
		opml_import_items.import_id,
		opml_import_items.position,
		opml_import_items.title,
		opml_import_items.feed_url,
		opml_import_items.site_url,
		opml_import_items.description,
		opml_import_items.category_name,
		opml_import_items.settings,
		opml_import_items.status,
		COALESCE(opml_import_items.feed_id, 0),
		opml_import_items.error_message,
		opml_import_items.attempts,
		opml_import_items.created_at,
		opml_import_items.updated_at
	`
}

// OPMLImportItemCorrection contains user-provided fixes for a failed item.
type OPMLImportItemCorrection struct {
	FeedURL      string
	CategoryName *string
	Settings     *model.OPMLItemSettings
}

// CorrectOPMLImportItem updates the frozen data of a failed item and resets it
// to pending so it can be retried at its original position.
func (s *Storage) CorrectOPMLImportItem(userID, importID, itemID int64, correction *OPMLImportItemCorrection) (*model.OPMLImportItem, error) {
	settingsJSON := "{}"
	if correction.Settings != nil {
		settingsJSON = string(correction.Settings.JSON())
	}

	query := `
		UPDATE opml_import_items
		SET
			feed_url=CASE WHEN $4 <> '' THEN $4 ELSE feed_url END,
			category_name=CASE WHEN $5 THEN $6 ELSE category_name END,
			settings=CASE WHEN $7 THEN $8::jsonb ELSE settings END,
			status='pending',
			feed_id=NULL,
			error_message='',
			updated_at=now()
		WHERE id=$1
			AND import_id=$2
			AND status IN ('fetch_failed', 'validation_failed', 'pending')
			AND EXISTS (SELECT 1 FROM opml_imports WHERE opml_imports.id=opml_import_items.import_id AND opml_imports.user_id=$3)
		RETURNING ` + opmlImportItemSelectColumns()

	row := s.db.QueryRow(
		query,
		itemID,
		importID,
		userID,
		correction.FeedURL,
		correction.CategoryName != nil,
		derefString(correction.CategoryName),
		correction.Settings != nil,
		settingsJSON,
	)
	item, err := scanOPMLImportItem(row)
	if errors.Is(err, sql.ErrNoRows) {
		// Distinguish an unknown item from an item that is not retryable.
		if _, getErr := s.GetOPMLImportItem(userID, importID, itemID); getErr != nil {
			return nil, getErr
		}
		return nil, fmt.Errorf("store: OPML import item #%d cannot be modified in its current state", itemID)
	}
	if err != nil {
		return nil, err
	}
	return item, nil
}

func derefString(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

// RecoverInterruptedOPMLImports resets batches left in a running state by a
// process crash and returns their (import ID, user ID) pairs so they can be
// continued. Item rows do not carry an in-progress state: an item is only
// marked after its database result is committed, so unfinished items are
// still pending and will be retried.
func (s *Storage) RecoverInterruptedOPMLImports() ([]model.OPMLImport, error) {
	query := `
		UPDATE opml_imports
		SET status='paused', updated_at=now()
		WHERE status IN ('in_progress', 'cancelling')
		RETURNING id, user_id
	`
	rows, err := s.db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("store: unable to recover interrupted OPML imports: %w", err)
	}
	defer rows.Close()

	imports := make([]model.OPMLImport, 0)
	for rows.Next() {
		var imp model.OPMLImport
		if err := rows.Scan(&imp.ID, &imp.UserID); err != nil {
			return nil, fmt.Errorf("store: unable to scan recovered OPML import: %w", err)
		}
		imports = append(imports, imp)
	}

	return imports, nil
}
