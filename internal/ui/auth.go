// SPDX-FileCopyrightText: Copyright The Miniflux Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package ui // import "miniflux.app/v2/internal/ui"

import (
	"context"
	"errors"
	"net/http"
	"time"

	"miniflux.app/v2/internal/config"
	"miniflux.app/v2/internal/http/request"
	"miniflux.app/v2/internal/model"
	"miniflux.app/v2/internal/oauth2"
	"miniflux.app/v2/internal/storage"
)

const sessionCookieName = "MinifluxSessionID"

// isWebSessionRevoked reports whether authentication failed because a global
// session flush revoked the browser session concurrently with the login. The
// caller must answer with an explicit, unauthenticated response instead of a
// generic server error: no session is granted and no stale-generation row is
// produced.
func isWebSessionRevoked(err error) bool {
	return errors.Is(err, storage.ErrWebSessionNotFound)
}

// authenticateWebSession binds the current browser session to the given user,
// rotates its identifier and secret, and refreshes the client cookie.
func authenticateWebSession(w http.ResponseWriter, r *http.Request, store *storage.Storage, user *model.User) error {
	session := request.WebSession(r)
	session.SetUser(user)

	oldID, secret := session.Rotate()
	if err := store.RotateWebSession(oldID, session); err != nil {
		return err
	}

	setSessionCookie(w, session, secret)
	return nil
}

// setSessionCookie writes the session cookie to the response with the
// security attributes used by miniflux (HttpOnly, SameSite=Lax, Secure
// when HTTPS).
func setSessionCookie(w http.ResponseWriter, session *model.WebSession, secret string) {
	path := config.Opts.BasePath()
	if path == "" {
		path = "/"
	}

	http.SetCookie(w, &http.Cookie{
		Name:     sessionCookieName,
		Value:    session.ID + "." + secret,
		Path:     path,
		Secure:   config.Opts.HTTPS(),
		HttpOnly: true,
		Expires:  time.Now().Add(config.Opts.CleanupRemoveSessionsInterval()),
		SameSite: http.SameSiteLaxMode,
	})
}

func getOAuth2Manager(ctx context.Context) *oauth2.Manager {
	return oauth2.NewManager(
		ctx,
		config.Opts.OAuth2Provider(),
		config.Opts.OAuth2ClientID(),
		config.Opts.OAuth2ClientSecret(),
		config.Opts.OAuth2RedirectURL(),
		config.Opts.OAuth2OIDCDiscoveryEndpoint(),
	)
}
