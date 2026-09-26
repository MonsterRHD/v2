// SPDX-FileCopyrightText: Copyright The Miniflux Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package ui

import (
	"context"
	"database/sql"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"miniflux.app/v2/internal/config"
	"miniflux.app/v2/internal/database"
	"miniflux.app/v2/internal/model"
	"miniflux.app/v2/internal/storage"
)

const testDatabaseURLEnvVar = "DATABASE_URL"

var testUserSerial atomic.Int64

func uniqueTestUsername(prefix string) string {
	return prefix + "_" + strings.ReplaceAll(time.Now().Format("150405.000000"), ".", "") + "_" +
		strconv.FormatInt(testUserSerial.Add(1), 10)
}

// middlewareTestDatabase provisions a package-private scratch database so
// concurrently running test packages never flush each other's sessions.
func middlewareTestDatabase(t *testing.T) string {
	t.Helper()

	dsn := os.Getenv(testDatabaseURLEnvVar)
	if dsn == "" {
		t.Skipf("%s not set; skipping database-backed web session middleware tests", testDatabaseURLEnvVar)
	}

	parsed, err := url.Parse(dsn)
	if err != nil || (parsed.Scheme != "postgres" && parsed.Scheme != "postgresql") {
		t.Fatalf("%s must be a postgres:// URL, got %q", testDatabaseURLEnvVar, dsn)
	}

	const databaseName = "miniflux_test_websession_ui"

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

func newMiddlewareTestStore(t *testing.T) *storage.Storage {
	t.Helper()

	dsn := middlewareTestDatabase(t)

	db, err := database.NewConnectionPool(dsn, 2, 10, time.Minute)
	if err != nil {
		t.Fatalf("NewConnectionPool() error: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	if err := database.Migrate(db); err != nil {
		t.Fatalf("Migrate() error: %v", err)
	}

	parsedOptions, err := config.NewConfigParser().ParseEnvironmentVariables()
	if err != nil {
		t.Fatalf("ParseEnvironmentVariables() error: %v", err)
	}
	previousOptions := config.Opts
	config.Opts = parsedOptions
	t.Cleanup(func() { config.Opts = previousOptions })

	store := storage.NewStorage(db)

	if _, err := store.FlushAllSessions(); err != nil {
		t.Fatalf("baseline FlushAllSessions() error: %v", err)
	}

	return store
}

func newAuthenticatedTestSession(t *testing.T, store *storage.Storage) (*model.WebSession, string) {
	t.Helper()

	user, err := store.CreateUser(&model.UserCreationRequest{
		Username: uniqueTestUsername("gen_middleware"),
		Password: "test-password",
	})
	if err != nil {
		t.Fatalf("CreateUser() error: %v", err)
	}

	session, _ := model.NewWebSession("middleware-test", "")
	if err := store.CreateWebSession(session); err != nil {
		t.Fatalf("CreateWebSession() error: %v", err)
	}

	session.SetUser(user)
	oldID, secret := session.Rotate()
	if err := store.RotateWebSession(oldID, session); err != nil {
		t.Fatalf("RotateWebSession() error: %v", err)
	}

	return session, session.ID + "." + secret
}

func runThroughMiddleware(t *testing.T, store *storage.Storage, method, target, cookie string) (*httptest.ResponseRecorder, *atomic.Int32) {
	t.Helper()

	var handlerCalls atomic.Int32
	middleware := newWebSessionMiddleware("", store)
	handler := middleware.handle(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handlerCalls.Add(1)
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(method, target, nil)
	if cookie != "" {
		req.Header.Set("Cookie", sessionCookieName+"="+cookie)
	}

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, req)

	return recorder, &handlerCalls
}

func TestWebSessionMiddleware_AuthenticatedRequestSucceeds(t *testing.T) {
	store := newMiddlewareTestStore(t)
	_, cookie := newAuthenticatedTestSession(t, store)

	recorder, calls := runThroughMiddleware(t, store, http.MethodGet, "/unread", cookie)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", recorder.Code)
	}
	if calls.Load() != 1 {
		t.Fatalf("handler calls = %d, want 1", calls.Load())
	}
}

func TestWebSessionMiddleware_OldCookieAfterFlushGetsUnauthenticatedResponse(t *testing.T) {
	store := newMiddlewareTestStore(t)
	_, cookie := newAuthenticatedTestSession(t, store)

	if _, err := store.FlushAllSessions(); err != nil {
		t.Fatalf("FlushAllSessions() error: %v", err)
	}

	recorder, calls := runThroughMiddleware(t, store, http.MethodGet, "/unread", cookie)

	if recorder.Code != http.StatusFound {
		t.Fatalf("status = %d, want 302 redirect to the login page", recorder.Code)
	}

	location := recorder.Header().Get("Location")
	if !strings.HasPrefix(location, "/?redirect_url=") {
		t.Fatalf("Location = %q, want the existing login redirect", location)
	}

	if calls.Load() != 0 {
		t.Fatal("the protected handler must not run for a flushed session cookie")
	}

	if setCookie := recorder.Header().Get("Set-Cookie"); !strings.Contains(setCookie, sessionCookieName+"=") {
		t.Fatalf("a flushed cookie must get the same fresh unauthenticated session response, Set-Cookie = %q", setCookie)
	}
}

func TestWebSessionMiddleware_FlushDuringRequestBlocksSensitiveWrite(t *testing.T) {
	store := newMiddlewareTestStore(t)
	_, cookie := newAuthenticatedTestSession(t, store)

	t.Run("mutating request is allowed before the flush", func(t *testing.T) {
		recorder, calls := runThroughMiddleware(t, store, http.MethodPost, "/mark-all-as-read", cookie)
		if recorder.Code != http.StatusOK || calls.Load() != 1 {
			t.Fatalf("status = %d, calls = %d; want 200 and 1 call", recorder.Code, calls.Load())
		}
	})

	if _, err := store.FlushAllSessions(); err != nil {
		t.Fatalf("FlushAllSessions() error: %v", err)
	}

	t.Run("mutating request is blocked after the flush", func(t *testing.T) {
		recorder, calls := runThroughMiddleware(t, store, http.MethodPost, "/mark-all-as-read", cookie)
		if recorder.Code != http.StatusFound {
			t.Fatalf("status = %d, want 302 login redirect", recorder.Code)
		}
		if calls.Load() != 0 {
			t.Fatal("a state-changing handler must not run once the session generation was revoked")
		}
	})

	t.Run("safe GET request is rejected at entry too", func(t *testing.T) {
		recorder, calls := runThroughMiddleware(t, store, http.MethodGet, "/settings", cookie)
		if recorder.Code != http.StatusFound {
			t.Fatalf("status = %d, want 302 login redirect", recorder.Code)
		}
		if calls.Load() != 0 {
			t.Fatal("the protected handler must not run for a revoked session")
		}
	})
}

func TestWebSessionMiddleware_PublicPagesAndLoginUnaffectedByFlush(t *testing.T) {
	store := newMiddlewareTestStore(t)

	// A flush just before browsing the public login page changes nothing:
	// a brand-new current-generation session is created and the page renders.
	if _, err := store.FlushAllSessions(); err != nil {
		t.Fatalf("FlushAllSessions() error: %v", err)
	}

	recorder, calls := runThroughMiddleware(t, store, http.MethodGet, "/", "")
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 on the public login page", recorder.Code)
	}
	if calls.Load() != 1 {
		t.Fatal("the public login page handler must run without a session")
	}

	newCookie := ""
	for _, c := range recorder.Result().Cookies() {
		if c.Name == sessionCookieName {
			newCookie = c.Value
		}
	}
	if newCookie == "" {
		t.Fatal("a fresh unauthenticated session cookie must be issued")
	}

	// Submitting the login form is an unauthenticated request at entry: the
	// pre-write generation gate must not block it; the atomic rotation itself
	// decides whether a new-generation session can be granted.
	recorder, calls = runThroughMiddleware(t, store, http.MethodPost, "/login", newCookie)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 for the login submission", recorder.Code)
	}
	if calls.Load() != 1 {
		t.Fatal("the login handler must remain reachable right after a flush")
	}
}

func TestWebSessionMiddleware_NewLoginAfterFlushGetsNewGenerationSession(t *testing.T) {
	store := newMiddlewareTestStore(t)

	if _, err := store.FlushAllSessions(); err != nil {
		t.Fatalf("FlushAllSessions() error: %v", err)
	}

	recorder, _ := runThroughMiddleware(t, store, http.MethodGet, "/", "")

	var newCookie string
	for _, c := range recorder.Result().Cookies() {
		if c.Name == sessionCookieName {
			newCookie = c.Value
		}
	}
	if newCookie == "" {
		t.Fatal("expected a fresh session cookie")
	}

	sessionID, _, ok := strings.Cut(newCookie, ".")
	if !ok {
		t.Fatalf("malformed session cookie: %q", newCookie)
	}

	// Complete the login against the freshly created, current-generation row.
	user, err := store.CreateUser(&model.UserCreationRequest{
		Username: uniqueTestUsername("gen_login"),
		Password: "test-password",
	})
	if err != nil {
		t.Fatalf("CreateUser() error: %v", err)
	}

	session, err := store.WebSessionByID(sessionID)
	if err != nil {
		t.Fatalf("WebSessionByID() error: %v", err)
	}
	if session == nil {
		t.Fatal("freshly created session not found")
	}

	session.SetUser(user)
	oldID, secret := session.Rotate()
	if err := store.RotateWebSession(oldID, session); err != nil {
		t.Fatalf("RotateWebSession() on a new-generation session error: %v", err)
	}

	authenticatedCookie := session.ID + "." + secret
	recorder, calls := runThroughMiddleware(t, store, http.MethodGet, "/unread", authenticatedCookie)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 after a fresh post-flush login", recorder.Code)
	}
	if calls.Load() != 1 {
		t.Fatal("the authenticated handler must run with the new-generation session")
	}
}

func TestWebSessionMiddleware_ConcurrentFlushFailsOrRevokesLogin(t *testing.T) {
	store := newMiddlewareTestStore(t)

	session, _ := model.NewWebSession("race", "")
	if err := store.CreateWebSession(session); err != nil {
		t.Fatalf("CreateWebSession() error: %v", err)
	}

	user, err := store.CreateUser(&model.UserCreationRequest{
		Username: uniqueTestUsername("gen_race"),
		Password: "test-password",
	})
	if err != nil {
		t.Fatalf("CreateUser() error: %v", err)
	}
	session.SetUser(user)
	oldID, _ := session.Rotate()

	if _, err := store.FlushAllSessions(); err != nil {
		t.Fatalf("FlushAllSessions() error: %v", err)
	}

	err = store.RotateWebSession(oldID, session)
	if !errors.Is(err, storage.ErrWebSessionNotFound) {
		t.Fatalf("RotateWebSession() racing the flush error = %v, want ErrWebSessionNotFound", err)
	}

	if _, err := store.CurrentWebSessionGeneration(context.Background()); err != nil {
		t.Fatalf("CurrentWebSessionGeneration() error: %v", err)
	}
}
