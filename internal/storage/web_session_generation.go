// SPDX-FileCopyrightText: Copyright The Miniflux Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package storage // import "miniflux.app/v2/internal/storage"

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"sync"
	"time"

	"github.com/lib/pq"
)

const (
	webSessionGenerationSingletonID = 1
	webSessionGenerationChannel     = "miniflux_web_sessions_flush"
	// webSessionGenerationCacheTTL bounds how long an instance may rely on a
	// locally cached generation when notifications are lost.
	webSessionGenerationCacheTTL = 30 * time.Second
	listenerMinReconnect         = 5 * time.Second
	listenerMaxReconnect         = time.Minute
)

// webSessionGenerationMonitor keeps a process-local view of the persistent
// global web session generation. The database is always the source of truth:
// notifications invalidate the cache immediately, bounded polling covers
// lost notifications, and a cold cache (including after a restart) is loaded
// directly from the database.
type webSessionGenerationMonitor struct {
	db *sql.DB

	// lookup fetches the generation from the database. It is a field so unit
	// tests can exercise the caching logic without a database.
	lookup func(ctx context.Context) (int64, error)

	mu         sync.RWMutex
	generation int64
	fetchedAt  time.Time
	loaded     bool
}

func newWebSessionGenerationMonitor(db *sql.DB) *webSessionGenerationMonitor {
	m := &webSessionGenerationMonitor{db: db}
	m.lookup = m.lookupFromDatabase
	return m
}

// Current returns the generation most recently confirmed by the database.
// The cached value is reused for at most webSessionGenerationCacheTTL and is
// refreshed immediately when a flush notification arrives.
func (m *webSessionGenerationMonitor) Current(ctx context.Context) (int64, error) {
	m.mu.RLock()
	fresh := m.loaded && time.Since(m.fetchedAt) < webSessionGenerationCacheTTL
	generation := m.generation
	m.mu.RUnlock()

	if fresh {
		return generation, nil
	}

	return m.Refresh(ctx)
}

// Refresh reloads the generation from the database and updates the cache.
// When the database cannot be reached, the last known generation is served
// instead of failing open.
func (m *webSessionGenerationMonitor) Refresh(ctx context.Context) (int64, error) {
	generation, err := m.lookup(ctx)
	if err != nil {
		m.mu.RLock()
		defer m.mu.RUnlock()
		if m.loaded {
			slog.Warn("Unable to refresh web session generation, keeping last known value",
				slog.Int64("last_known_generation", m.generation),
				slog.Any("error", err),
			)
			return m.generation, nil
		}
		return 0, err
	}

	m.mu.Lock()
	if !m.loaded || generation > m.generation {
		m.generation = generation
	}
	current := m.generation
	m.fetchedAt = time.Now()
	m.loaded = true
	m.mu.Unlock()

	return current, nil
}

// Adopt records a generation pushed through LISTEN/NOTIFY. Values lower than
// the one already observed are ignored.
func (m *webSessionGenerationMonitor) Adopt(generation int64) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if !m.loaded || generation > m.generation {
		m.generation = generation
	}
	m.fetchedAt = time.Now()
	m.loaded = true
}

func (m *webSessionGenerationMonitor) lookupFromDatabase(ctx context.Context) (int64, error) {
	var generation int64
	err := m.db.QueryRowContext(ctx,
		`SELECT generation FROM web_session_generations WHERE id = $1`,
		webSessionGenerationSingletonID,
	).Scan(&generation)
	if err != nil {
		return 0, fmt.Errorf(`store: unable to fetch current web session generation: %v`, err)
	}
	return generation, nil
}

// CurrentWebSessionGeneration returns the current global web session
// generation, using the locally cached value maintained by the monitor.
func (s *Storage) CurrentWebSessionGeneration(ctx context.Context) (int64, error) {
	return s.genMonitor.Current(ctx)
}

// StartWebSessionGenerationMonitor keeps the local generation view in sync
// with the database: flush notifications invalidate it immediately, periodic
// polling bounds the delay when notifications are lost, and a restart simply
// reloads from the database.
func (s *Storage) StartWebSessionGenerationMonitor(ctx context.Context, databaseURL string) {
	s.genMonitor.start(ctx, databaseURL)
}

// start launches the monitor goroutine and returns a channel that is closed
// once the LISTEN subscription is established (or once the monitor has
// deterministically fallen back to bounded polling).
func (m *webSessionGenerationMonitor) start(ctx context.Context, databaseURL string) <-chan struct{} {
	ready := make(chan struct{})
	go m.run(ctx, databaseURL, ready)
	return ready
}

func (m *webSessionGenerationMonitor) run(ctx context.Context, databaseURL string, ready chan<- struct{}) {
	if _, err := m.Refresh(ctx); err != nil {
		slog.Warn("Unable to load current web session generation", slog.Any("error", err))
	}

	dsn, err := pq.ParseURL(databaseURL)
	if err != nil {
		dsn = databaseURL
	}

	listener := pq.NewListener(dsn, listenerMinReconnect, listenerMaxReconnect, func(event pq.ListenerEventType, eventErr error) {
		if event == pq.ListenerEventConnectionAttemptFailed {
			slog.Warn("Web session generation listener reconnect attempt failed", slog.Any("error", eventErr))
		}
	})

	if err := listener.Listen(webSessionGenerationChannel); err != nil && !errors.Is(err, pq.ErrChannelAlreadyOpen) {
		// Bounded polling still converges without a working listener.
		slog.Warn("Unable to listen for web session flush notifications, falling back to bounded polling",
			slog.Any("error", err),
		)
	}
	// LISTEN blocks until the server acknowledges it, so any flush committed
	// after this point is guaranteed to be delivered to the Notify channel.
	close(ready)
	defer listener.Close()

	pollTicker := time.NewTicker(webSessionGenerationCacheTTL)
	defer pollTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case notification := <-listener.Notify:
			// A nil notification is delivered after a reconnect: notifications
			// emitted while the connection was down were missed, so resync.
			if notification == nil {
				if _, err := m.Refresh(ctx); err != nil {
					slog.Warn("Unable to resync web session generation after listener reconnect", slog.Any("error", err))
				}
				continue
			}

			generation, convErr := strconv.ParseInt(notification.Extra, 10, 64)
			if convErr != nil {
				if _, err := m.Refresh(ctx); err != nil {
					slog.Warn("Unable to resync web session generation after a malformed notification", slog.Any("error", err))
				}
				continue
			}

			slog.Info("Received web session flush notification", slog.Int64("generation", generation))
			m.Adopt(generation)
		case <-pollTicker.C:
			if err := listener.Ping(); err != nil {
				slog.Debug("Web session generation listener ping failed", slog.Any("error", err))
			}
			if _, err := m.Refresh(ctx); err != nil {
				slog.Debug("Web session generation polling refresh failed", slog.Any("error", err))
			}
		}
	}
}
