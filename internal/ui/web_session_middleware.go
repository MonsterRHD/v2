// SPDX-FileCopyrightText: Copyright The Miniflux Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package ui // import "miniflux.app/v2/internal/ui"

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"strings"

	"miniflux.app/v2/internal/http/request"
	"miniflux.app/v2/internal/http/response"
	"miniflux.app/v2/internal/model"
	"miniflux.app/v2/internal/storage"
)

type webSessionMiddleware struct {
	basePath string
	store    *storage.Storage
}

func newWebSessionMiddleware(basePath string, store *storage.Storage) *webSessionMiddleware {
	return &webSessionMiddleware{basePath: basePath, store: store}
}

func (m *webSessionMiddleware) handle(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if isStaticAssetRoute(r) {
			next.ServeHTTP(w, r)
			return
		}

		session, err := m.loadWebSessionFromCookie(r)
		if err != nil {
			response.HTMLServerError(w, r, err)
			return
		}

		if session != nil {
			// Authentication entry point: the session must belong to the
			// current global generation. A session stamped with an older
			// generation has been revoked by "flush-sessions" and is handled
			// exactly like a missing or invalid cookie.
			currentGeneration, err := m.store.CurrentWebSessionGeneration(r.Context())
			if err != nil {
				response.HTMLServerError(w, r, err)
				return
			}

			if session.Generation != currentGeneration {
				slog.Info("Rejecting web session from a revoked generation",
					slog.String("session_id", session.ID),
					slog.Int64("session_generation", session.Generation),
					slog.Int64("current_generation", currentGeneration),
					slog.String("client_ip", request.ClientIP(r)),
					slog.String("request_uri", r.RequestURI),
				)
				session = nil
			}
		}

		if session == nil {
			var secret string
			session, secret = model.NewWebSession(r.UserAgent(), request.ClientIP(r))
			if err := m.store.CreateWebSession(session); err != nil {
				response.HTMLServerError(w, r, err)
				return
			}
			setSessionCookie(w, session, secret)
		}

		ctx := context.WithValue(r.Context(), request.WebSessionContextKey, session)
		r = r.WithContext(ctx)

		if !request.IsAuthenticated(r) && !isPublicRoute(r) {
			response.HTMLRedirect(w, r, loginRedirectURL(m.basePath, r.RequestURI))
			return
		}

		// Before any sensitive write reaches a handler, re-confirm against the
		// database that the session still exists in the generation observed
		// when the request entered. A flush committed on another instance
		// while this request was already being handled therefore cannot turn
		// into user data writes or extend the revoked session.
		if request.IsAuthenticated(r) && isStateChangingMethod(r.Method) {
			valid, err := m.store.ValidateWebSessionGeneration(session.ID, session.Generation)
			if err != nil {
				response.HTMLServerError(w, r, err)
				return
			}

			if !valid {
				slog.Info("Aborting state-changing request: web session was revoked while the request was in flight",
					slog.String("session_id", session.ID),
					slog.Int64("session_generation", session.Generation),
					slog.String("method", r.Method),
					slog.String("client_ip", request.ClientIP(r)),
					slog.String("request_uri", r.RequestURI),
				)
				response.HTMLRedirect(w, r, loginRedirectURL(m.basePath, r.RequestURI))
				return
			}
		}

		next.ServeHTTP(w, r)

		if session.IsDirty() {
			if err := m.store.UpdateWebSession(session); err != nil {
				if errors.Is(err, storage.ErrWebSessionNotFound) {
					// The session was flushed while the request was in flight.
					// The UPDATE matches no row and cannot resurrect anything.
					slog.Debug("Skipping web session update: session was revoked during the request",
						slog.String("session_id", session.ID),
					)
					return
				}

				slog.Error("Unable to persist web session changes",
					slog.String("session_id", session.ID),
					slog.Any("error", err),
				)
			}
		}
	})
}

func (m *webSessionMiddleware) loadWebSessionFromCookie(r *http.Request) (*model.WebSession, error) {
	cookieValue := request.CookieValue(r, sessionCookieName)
	if cookieValue == "" {
		return nil, nil
	}

	sessionID, secret, ok := strings.Cut(cookieValue, ".")
	if !ok || sessionID == "" || secret == "" {
		return nil, nil
	}

	session, err := m.store.WebSessionByID(sessionID)
	if err != nil {
		return nil, err
	}

	if session == nil || !session.VerifySecret(secret) {
		return nil, nil
	}

	return session, nil
}

// isStateChangingMethod reports whether the HTTP method may mutate server
// state. Safe methods do not need the pre-write generation re-validation.
func isStateChangingMethod(method string) bool {
	switch method {
	case http.MethodGet, http.MethodHead, http.MethodOptions, http.MethodTrace:
		return false
	default:
		return true
	}
}
