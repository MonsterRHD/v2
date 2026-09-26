// SPDX-FileCopyrightText: Copyright The Miniflux Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package storage // import "miniflux.app/v2/internal/storage"

import (
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"time"

	"miniflux.app/v2/internal/model"
)

// ErrWebSessionNotFound is returned when a web session row no longer exists,
// typically because "flush-sessions" revoked it while a request was in flight.
var ErrWebSessionNotFound = errors.New(`store: web session not found`)

// CreateWebSession persists a new web session built via model.NewWebSession.
//
// The session is stamped with the current global generation. The generation
// row is locked for the duration of the transaction, which serializes session
// creation against a concurrent global flush: either the flush happens first
// and the new session is stamped with the new generation, or the new row is
// created first and the flush deletes it. A session that survived a flush with
// a stale generation can therefore never be produced.
func (s *Storage) CreateWebSession(session *model.WebSession) error {
	if session == nil {
		return errors.New(`store: web session is nil`)
	}

	stateJSON, err := session.MarshalState()
	if err != nil {
		return fmt.Errorf(`store: unable to serialize web session state: %v`, err)
	}

	tx, err := s.db.Begin()
	if err != nil {
		return fmt.Errorf(`store: unable to create web session: %v`, err)
	}
	defer tx.Rollback()

	var generation int64
	if err := tx.QueryRow(
		`SELECT generation FROM web_session_generations WHERE id = $1 FOR UPDATE`,
		webSessionGenerationSingletonID,
	).Scan(&generation); err != nil {
		return fmt.Errorf(`store: unable to create web session: %v`, err)
	}

	err = tx.QueryRow(`
		INSERT INTO web_sessions (
			id,
			secret_hash,
			user_agent,
			ip,
			state,
			generation
		)
		VALUES ($1, $2, $3, $4, $5, $6)
		RETURNING created_at
	`,
		session.ID,
		session.SecretHash,
		session.UserAgent,
		sql.NullString{String: session.IP, Valid: session.IP != ""},
		stateJSON,
		generation,
	).Scan(&session.CreatedAt)
	if err != nil {
		return fmt.Errorf(`store: unable to create web session: %v`, err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf(`store: unable to create web session: %v`, err)
	}

	session.Generation = generation
	return nil
}

// WebSessionsByUserID returns web sessions for the given user.
func (s *Storage) WebSessionsByUserID(userID int64) ([]model.WebSession, error) {
	query := `
		SELECT
			id,
			secret_hash,
			user_id,
			created_at,
			generation,
			user_agent,
			ip,
			state
		FROM
			web_sessions
		WHERE
			user_id=$1
		ORDER BY
			created_at DESC
	`

	rows, err := s.db.Query(query, userID)
	if err != nil {
		return nil, fmt.Errorf(`store: unable to fetch web sessions: %v`, err)
	}
	defer rows.Close()

	var sessions []model.WebSession

	for rows.Next() {
		session, err := scanWebSession(rows)
		if err != nil {
			return nil, fmt.Errorf(`store: unable to fetch web session row: %v`, err)
		}

		sessions = append(sessions, *session)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf(`store: unable to fetch web sessions: %v`, err)
	}

	return sessions, nil
}

// WebSessionByID returns the web session identified by id, or nil if not found.
func (s *Storage) WebSessionByID(sessionID string) (*model.WebSession, error) {
	if sessionID == "" {
		return nil, nil
	}

	row := s.db.QueryRow(`
		SELECT
			id,
			secret_hash,
			user_id,
			created_at,
			generation,
			user_agent,
			ip,
			state
		FROM
			web_sessions
		WHERE
			id=$1
	`, sessionID)

	session, err := scanWebSession(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf(`store: unable to fetch web session: %v`, err)
	}

	return session, nil
}

// RotateWebSession persists a session whose identity has been rotated via
// (*model.WebSession).Rotate(), updating the row previously keyed by oldID.
//
// The rotation runs while holding the global generation row lock, so it cannot
// interleave with a global flush: either the rotation commits first and the
// flush deletes the rotated row (the new cookie is then treated as
// unauthenticated), or the flush commits first and ErrWebSessionNotFound is
// returned so the caller can fail the login explicitly. A rotated session is
// always stamped with the current generation.
func (s *Storage) RotateWebSession(oldID string, session *model.WebSession) error {
	if session == nil {
		return errors.New(`store: web session is nil`)
	}

	if oldID == "" || session.ID == "" {
		return errors.New(`store: web session ID cannot be empty`)
	}

	stateJSON, err := session.MarshalState()
	if err != nil {
		return fmt.Errorf(`store: unable to serialize web session state: %v`, err)
	}

	tx, err := s.db.Begin()
	if err != nil {
		return fmt.Errorf(`store: unable to rotate web session: %v`, err)
	}
	defer tx.Rollback()

	var generation int64
	if err := tx.QueryRow(
		`SELECT generation FROM web_session_generations WHERE id = $1 FOR UPDATE`,
		webSessionGenerationSingletonID,
	).Scan(&generation); err != nil {
		return fmt.Errorf(`store: unable to rotate web session: %v`, err)
	}

	err = tx.QueryRow(`
		UPDATE
			web_sessions
		SET
			id=$2,
			secret_hash=$3,
			user_id=$4,
			state=$5,
			created_at=now(),
			generation=$6
		WHERE
			id=$1
		RETURNING created_at
	`,
		oldID,
		session.ID,
		session.SecretHash,
		session.NullUserID(),
		stateJSON,
		generation,
	).Scan(&session.CreatedAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ErrWebSessionNotFound
		}
		return fmt.Errorf(`store: unable to rotate web session: %v`, err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf(`store: unable to rotate web session: %v`, err)
	}

	session.Generation = generation
	return nil
}

// UpdateWebSession updates the mutable fields of a web session. It never
// resurrects a deleted row: if the session was flushed while the request was
// in flight, ErrWebSessionNotFound is returned.
func (s *Storage) UpdateWebSession(session *model.WebSession) error {
	if session == nil {
		return errors.New(`store: web session is nil`)
	}

	if session.ID == "" {
		return errors.New(`store: web session ID cannot be empty`)
	}

	query := `
		UPDATE
			web_sessions
		SET
			user_id=$2,
			state=$3
		WHERE
			id=$1
	`

	stateJSON, err := session.MarshalState()
	if err != nil {
		return fmt.Errorf(`store: unable to serialize web session state: %v`, err)
	}

	result, err := s.db.Exec(
		query,
		session.ID,
		session.NullUserID(),
		stateJSON,
	)
	if err != nil {
		return fmt.Errorf(`store: unable to update web session: %v`, err)
	}

	count, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf(`store: unable to update web session: %v`, err)
	}

	if count != 1 {
		return ErrWebSessionNotFound
	}

	return nil
}

// ValidateWebSessionGeneration is the database-authoritative check performed
// before a sensitive write. It succeeds only when the session row still exists
// and belongs to the very generation the request was authenticated against at
// entry time.
func (s *Storage) ValidateWebSessionGeneration(sessionID string, generation int64) (bool, error) {
	if sessionID == "" {
		return false, nil
	}

	var valid bool
	err := s.db.QueryRow(`
		SELECT EXISTS (
			SELECT
				1
			FROM
				web_sessions AS s
			JOIN
				web_session_generations AS g ON g.id = $1
			WHERE
				s.id = $2
				AND s.generation = $3
				AND s.generation = g.generation
		)
	`,
		webSessionGenerationSingletonID,
		sessionID,
		generation,
	).Scan(&valid)
	if err != nil {
		return false, fmt.Errorf(`store: unable to validate web session generation: %v`, err)
	}

	return valid, nil
}

// RemoveUserWebSession removes a web session for the given user if present.
func (s *Storage) RemoveUserWebSession(userID int64, sessionID string) error {
	if _, err := s.db.Exec(`DELETE FROM web_sessions WHERE user_id=$1 AND id=$2`, userID, sessionID); err != nil {
		return fmt.Errorf(`store: unable to remove this web session: %v`, err)
	}

	return nil
}

// CleanOldWebSessions removes web sessions older than the specified interval (24h minimum).
func (s *Storage) CleanOldWebSessions(interval time.Duration) (int64, error) {
	query := `
		DELETE FROM
			web_sessions
		WHERE
			created_at < now() - $1::interval
	`

	days := max(int(interval/(24*time.Hour)), 1)

	result, err := s.db.Exec(query, fmt.Sprintf("%d days", days))
	if err != nil {
		return 0, fmt.Errorf(`store: unable to clean old web sessions: %v`, err)
	}

	n, _ := result.RowsAffected()
	return n, nil
}

// FlushAllSessions performs a global revocation of every web session: all
// session rows are deleted and the persistent generation is bumped in the same
// transaction, so the new generation is only observable once the deletion is
// durable. Other instances learn about it through the notification queued at
// commit, or through their bounded generation polling. Running it repeatedly
// is idempotent and always succeeds even when no session exists.
func (s *Storage) FlushAllSessions() (generation int64, err error) {
	tx, err := s.db.Begin()
	if err != nil {
		return 0, fmt.Errorf(`store: unable to flush web sessions: %v`, err)
	}
	defer tx.Rollback()

	err = tx.QueryRow(`
		INSERT INTO web_session_generations (id, generation)
		VALUES ($1, 1)
		ON CONFLICT (id) DO UPDATE
			SET generation = web_session_generations.generation + 1
		RETURNING generation
	`, webSessionGenerationSingletonID).Scan(&generation)
	if err != nil {
		return 0, fmt.Errorf(`store: unable to bump web session generation: %v`, err)
	}

	if _, err := tx.Exec(`DELETE FROM web_sessions`); err != nil {
		return 0, fmt.Errorf(`store: unable to delete all web sessions: %v`, err)
	}

	// NOTIFY is only delivered to listeners after this transaction commits,
	// so a notification can never announce a generation that is not durable.
	if _, err := tx.Exec(
		`SELECT pg_notify($1, $2)`,
		webSessionGenerationChannel,
		strconv.FormatInt(generation, 10),
	); err != nil {
		return 0, fmt.Errorf(`store: unable to notify web session flush: %v`, err)
	}

	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf(`store: unable to flush web sessions: %v`, err)
	}

	return generation, nil
}

type webSessionScanner interface {
	Scan(dest ...any) error
}

func scanWebSession(scanner webSessionScanner) (*model.WebSession, error) {
	var session model.WebSession
	var userID sql.NullInt64
	var ip sql.NullString
	var stateRaw []byte

	err := scanner.Scan(
		&session.ID,
		&session.SecretHash,
		&userID,
		&session.CreatedAt,
		&session.Generation,
		&session.UserAgent,
		&ip,
		&stateRaw,
	)
	if err != nil {
		return nil, err
	}

	session.ScanUserID(userID)

	if ip.Valid {
		session.IP = ip.String
	}

	if err := session.UnmarshalState(stateRaw); err != nil {
		return nil, err
	}

	return &session, nil
}
