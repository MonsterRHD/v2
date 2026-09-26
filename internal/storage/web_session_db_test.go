// SPDX-FileCopyrightText: Copyright The Miniflux Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package storage

import (
	"context"
	"database/sql"
	"errors"
	"net/url"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"miniflux.app/v2/internal/database"
	"miniflux.app/v2/internal/model"
)

const databaseURLEnvVar = "DATABASE_URL"

// scratchTestDatabase provisions a package-private database derived from
// DATABASE_URL (expected as a postgres:// URL) so concurrently running test
// packages never flush each other's sessions.
func scratchTestDatabase(t *testing.T, suffix string) string {
	t.Helper()

	dsn := os.Getenv(databaseURLEnvVar)
	if dsn == "" {
		t.Skipf("%s not set; skipping database-backed web session tests", databaseURLEnvVar)
	}

	parsed, err := url.Parse(dsn)
	if err != nil || (parsed.Scheme != "postgres" && parsed.Scheme != "postgresql") {
		t.Fatalf("%s must be a postgres:// URL, got %q", databaseURLEnvVar, dsn)
	}

	databaseName := "miniflux_test_" + suffix

	adminURL := *parsed
	adminURL.Path = "/postgres"
	adminDB, err := sql.Open("postgres", adminURL.String())
	if err != nil {
		t.Fatalf("connecting to maintenance database: %v", err)
	}

	var exists bool
	if err := adminDB.QueryRow(`SELECT EXISTS (SELECT 1 FROM pg_database WHERE datname = $1)`, databaseName).Scan(&exists); err != nil {
		t.Fatalf("checking scratch database: %v", err)
	}
	if !exists {
		if _, err := adminDB.Exec(`CREATE DATABASE ` + databaseName); err != nil {
			t.Fatalf("creating scratch database: %v", err)
		}
	}
	adminDB.Close()

	scratchURL := *parsed
	scratchURL.Path = "/" + databaseName
	return scratchURL.String()
}

func newDBBackedTestStorage(t *testing.T) *Storage {
	t.Helper()

	dsn := scratchTestDatabase(t, "websession_storage")

	db, err := database.NewConnectionPool(dsn, 2, 10, time.Minute)
	if err != nil {
		t.Fatalf("NewConnectionPool() error: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	if err := database.Migrate(db); err != nil {
		t.Fatalf("Migrate() error: %v", err)
	}

	return NewStorage(db)
}

func TestWebSessionGeneration_StampingAndValidation(t *testing.T) {
	store := newDBBackedTestStorage(t)
	ctx := context.Background()

	if _, err := store.FlushAllSessions(); err != nil {
		t.Fatalf("baseline FlushAllSessions() error: %v", err)
	}

	currentGeneration, err := store.CurrentWebSessionGeneration(ctx)
	if err != nil {
		t.Fatalf("CurrentWebSessionGeneration() error: %v", err)
	}

	session, _ := model.NewWebSession("test-agent", "127.0.0.1")
	if err := store.CreateWebSession(session); err != nil {
		t.Fatalf("CreateWebSession() error: %v", err)
	}

	if session.Generation != currentGeneration {
		t.Fatalf("new session generation = %d, want current generation %d", session.Generation, currentGeneration)
	}

	valid, err := store.ValidateWebSessionGeneration(session.ID, session.Generation)
	if err != nil {
		t.Fatalf("ValidateWebSessionGeneration() error: %v", err)
	}
	if !valid {
		t.Fatal("a freshly created session must validate against the current generation")
	}

	// A session from a generation that never existed cannot validate.
	valid, err = store.ValidateWebSessionGeneration(session.ID, session.Generation+1)
	if err != nil {
		t.Fatalf("ValidateWebSessionGeneration(future) error: %v", err)
	}
	if valid {
		t.Fatal("a session must not validate against a different generation")
	}

	if valid, _ := store.ValidateWebSessionGeneration("", currentGeneration); valid {
		t.Fatal("an empty session ID must never validate")
	}
}

func TestWebSessionGeneration_RotateKeepsCurrentGeneration(t *testing.T) {
	store := newDBBackedTestStorage(t)

	if _, err := store.FlushAllSessions(); err != nil {
		t.Fatalf("baseline FlushAllSessions() error: %v", err)
	}

	currentGeneration, _ := store.CurrentWebSessionGeneration(context.Background())

	session, _ := model.NewWebSession("test-agent", "")
	if err := store.CreateWebSession(session); err != nil {
		t.Fatalf("CreateWebSession() error: %v", err)
	}

	oldID, _ := session.Rotate()
	if err := store.RotateWebSession(oldID, session); err != nil {
		t.Fatalf("RotateWebSession() error: %v", err)
	}

	if session.Generation != currentGeneration {
		t.Fatalf("rotated session generation = %d, want %d", session.Generation, currentGeneration)
	}

	if loaded, err := store.WebSessionByID(session.ID); err != nil {
		t.Fatalf("WebSessionByID(new ID) error: %v", err)
	} else if loaded == nil {
		t.Fatal("rotated session must be readable under its new ID")
	} else if loaded.Generation != currentGeneration {
		t.Fatalf("persisted rotated generation = %d, want %d", loaded.Generation, currentGeneration)
	}

	if old, err := store.WebSessionByID(oldID); err != nil {
		t.Fatalf("WebSessionByID(old ID) error: %v", err)
	} else if old != nil {
		t.Fatal("the pre-rotation session row must be gone")
	}
}

func TestFlushAllSessions_RevokesAndBumpsGeneration(t *testing.T) {
	store := newDBBackedTestStorage(t)
	ctx := context.Background()

	if _, err := store.FlushAllSessions(); err != nil {
		t.Fatalf("baseline FlushAllSessions() error: %v", err)
	}

	before, err := store.CurrentWebSessionGeneration(ctx)
	if err != nil {
		t.Fatalf("CurrentWebSessionGeneration() error: %v", err)
	}

	session, _ := model.NewWebSession("test-agent", "")
	if err := store.CreateWebSession(session); err != nil {
		t.Fatalf("CreateWebSession() error: %v", err)
	}

	newGeneration, err := store.FlushAllSessions()
	if err != nil {
		t.Fatalf("FlushAllSessions() error: %v", err)
	}
	if newGeneration != before+1 {
		t.Fatalf("generation after flush = %d, want %d", newGeneration, before+1)
	}

	if loaded, err := store.WebSessionByID(session.ID); err != nil {
		t.Fatalf("WebSessionByID() after flush error: %v", err)
	} else if loaded != nil {
		t.Fatal("flushed session must no longer be readable")
	}

	valid, err := store.ValidateWebSessionGeneration(session.ID, session.Generation)
	if err != nil {
		t.Fatalf("ValidateWebSessionGeneration() after flush error: %v", err)
	}
	if valid {
		t.Fatal("a flushed session must not validate against the new generation")
	}

	fresh, _ := model.NewWebSession("test-agent", "")
	if err := store.CreateWebSession(fresh); err != nil {
		t.Fatalf("CreateWebSession() after flush error: %v", err)
	}
	if fresh.Generation != newGeneration {
		t.Fatalf("post-flush session generation = %d, want %d", fresh.Generation, newGeneration)
	}
}

func TestFlushAllSessions_IsIdempotent(t *testing.T) {
	store := newDBBackedTestStorage(t)

	first, err := store.FlushAllSessions()
	if err != nil {
		t.Fatalf("first FlushAllSessions() error: %v", err)
	}

	second, err := store.FlushAllSessions()
	if err != nil {
		t.Fatalf("second FlushAllSessions() error: %v", err)
	}

	if second != first+1 {
		t.Fatalf("generation after repeated flush = %d, want %d", second, first+1)
	}

	var count int
	if err := store.db.QueryRow(`SELECT count(*) FROM web_sessions`).Scan(&count); err != nil {
		t.Fatalf("counting web sessions error: %v", err)
	}
	if count != 0 {
		t.Fatalf("repeated flush left %d sessions, want 0", count)
	}
}

func TestRotateWebSession_AfterFlushFailsExplicitly(t *testing.T) {
	store := newDBBackedTestStorage(t)

	if _, err := store.FlushAllSessions(); err != nil {
		t.Fatalf("baseline FlushAllSessions() error: %v", err)
	}

	session, _ := model.NewWebSession("test-agent", "")
	if err := store.CreateWebSession(session); err != nil {
		t.Fatalf("CreateWebSession() error: %v", err)
	}

	if _, err := store.FlushAllSessions(); err != nil {
		t.Fatalf("FlushAllSessions() error: %v", err)
	}

	// Simulates a login whose session was revoked between request entry and
	// the rotate: it must fail explicitly instead of resurrecting the row.
	oldID, _ := session.Rotate()
	err := store.RotateWebSession(oldID, session)
	if !errors.Is(err, ErrWebSessionNotFound) {
		t.Fatalf("RotateWebSession() after flush error = %v, want ErrWebSessionNotFound", err)
	}

	if loaded, _ := store.WebSessionByID(session.ID); loaded != nil {
		t.Fatal("a failed rotation after flush must not create a stale-generation row")
	}
}

func TestUpdateWebSession_AfterFlushDoesNotResurrect(t *testing.T) {
	store := newDBBackedTestStorage(t)

	if _, err := store.FlushAllSessions(); err != nil {
		t.Fatalf("baseline FlushAllSessions() error: %v", err)
	}

	session, _ := model.NewWebSession("test-agent", "")
	if err := store.CreateWebSession(session); err != nil {
		t.Fatalf("CreateWebSession() error: %v", err)
	}
	session.SetSuccessMessage("in flight")

	if _, err := store.FlushAllSessions(); err != nil {
		t.Fatalf("FlushAllSessions() error: %v", err)
	}

	err := store.UpdateWebSession(session)
	if !errors.Is(err, ErrWebSessionNotFound) {
		t.Fatalf("UpdateWebSession() after flush error = %v, want ErrWebSessionNotFound", err)
	}

	if loaded, _ := store.WebSessionByID(session.ID); loaded != nil {
		t.Fatal("the flushed session row must not be resurrected by the tail update")
	}
}

func TestConcurrentFlushAndCreate_LeavesNoStaleGenerationRows(t *testing.T) {
	store := newDBBackedTestStorage(t)

	if _, err := store.FlushAllSessions(); err != nil {
		t.Fatalf("baseline FlushAllSessions() error: %v", err)
	}

	const (
		creatorCount = 8
		flushCount   = 25
	)

	var wg sync.WaitGroup
	stop := make(chan struct{})
	var creatorFailures atomic.Int64

	for range creatorCount {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}

				session, _ := model.NewWebSession("concurrent", "")
				if err := store.CreateWebSession(session); err != nil {
					creatorFailures.Add(1)
				}
			}
		}()
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		for range flushCount {
			if _, err := store.FlushAllSessions(); err != nil {
				t.Errorf("concurrent FlushAllSessions() error: %v", err)
				return
			}
			time.Sleep(time.Millisecond)
		}
		close(stop)
	}()

	wg.Wait()

	if failures := creatorFailures.Load(); failures != 0 {
		t.Fatalf("concurrent session creation produced %d errors", failures)
	}

	// The core invariant: creation and flush interleave, but a session row
	// that survived must always belong to the current generation.
	var staleRows int
	if err := store.db.QueryRow(`
		SELECT count(*)
		FROM web_sessions AS s
		JOIN web_session_generations AS g ON g.id = $1
		WHERE s.generation <> g.generation
	`, webSessionGenerationSingletonID).Scan(&staleRows); err != nil {
		t.Fatalf("stale generation row check error: %v", err)
	}
	if staleRows != 0 {
		t.Fatalf("found %d session rows stamped with a revoked generation", staleRows)
	}

	if _, err := store.FlushAllSessions(); err != nil {
		t.Fatalf("final FlushAllSessions() error: %v", err)
	}
}

func TestWebSessionGenerationMonitor_ReceivesFlushNotification(t *testing.T) {
	dsn := scratchTestDatabase(t, "websession_storage")
	db, err := database.NewConnectionPool(dsn, 2, 10, time.Minute)
	if err != nil {
		t.Fatalf("NewConnectionPool() error: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	if err := database.Migrate(db); err != nil {
		t.Fatalf("Migrate() error: %v", err)
	}
	store := NewStorage(db)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Only flush once the LISTEN subscription is acknowledged, otherwise the
	// notification could be emitted before the listener registered.
	ready := store.genMonitor.start(ctx, dsn)
	select {
	case <-ready:
	case <-time.After(10 * time.Second):
		t.Fatal("web session generation monitor did not become ready")
	}

	expected, err := store.FlushAllSessions()
	if err != nil {
		t.Fatalf("FlushAllSessions() error: %v", err)
	}

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		observed, err := store.CurrentWebSessionGeneration(ctx)
		if err == nil && observed == expected {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}

	t.Fatalf("monitor did not observe flushed generation %d via LISTEN/NOTIFY", expected)
}

func TestWebSessionGenerationMonitor_RestartLoadsFromDatabase(t *testing.T) {
	store := newDBBackedTestStorage(t)

	expected, err := store.FlushAllSessions()
	if err != nil {
		t.Fatalf("FlushAllSessions() error: %v", err)
	}

	// A brand new monitor simulates an instance restart: cold cache is loaded
	// straight from the database.
	monitor := newWebSessionGenerationMonitor(store.db)
	got, err := monitor.Current(context.Background())
	if err != nil {
		t.Fatalf("Current() on a cold monitor error: %v", err)
	}
	if got != expected {
		t.Fatalf("cold monitor generation = %d, want database generation %d", got, expected)
	}
}
