// SPDX-FileCopyrightText: Copyright The Miniflux Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package template

import (
	"bytes"
	"html/template"
	"strings"
	"testing"
	"time"

	"miniflux.app/v2/internal/locale"
	"miniflux.app/v2/internal/model"
)

func renderWebhookDeliveryPartial(t *testing.T, status, lastError string, nextAttemptAt time.Time) string {
	t.Helper()

	funcs := (&funcMap{}).Map()
	tpl := template.Must(template.New("").Funcs(funcs).ParseFS(commonTemplateFiles, "templates/common/webhook_delivery.html"))

	printer := locale.NewPrinter("en_US")
	tpl.Funcs(template.FuncMap{
		"t": printer.Printf,
		"elapsed": func(_ string, _ time.Time) string {
			return "in the future"
		},
	})

	entry := &model.Entry{ID: 77}
	entry.WebhookDelivery = &model.WebhookDeliveryView{
		Status:        status,
		LastError:     lastError,
		NextAttemptAt: nextAttemptAt,
	}

	data := map[string]any{
		"language": "en_US",
		"entry":    entry,
		"user":     map[string]any{"Timezone": "UTC"},
	}

	var b bytes.Buffer
	if err := tpl.ExecuteTemplate(&b, "common/webhook_delivery.html", data); err != nil {
		t.Fatalf("Unable to render partial: %v", err)
	}

	return b.String()
}

func TestWebhookDeliveryPartialRendersAllStatuses(t *testing.T) {
	future := time.Now().Add(2 * time.Hour)

	tests := []struct {
		name        string
		status      string
		wantLabel   string
		wantRetry   bool
		wantNext    bool
		lastError   string
	}{
		{"pending", model.WebhookDeliveryStatusPending, "Pending", false, false, ""},
		{"in_flight", model.WebhookDeliveryStatusInFlight, "Sending…", false, false, ""},
		{"retry_waiting", model.WebhookDeliveryStatusRetryWaiting, "Retry scheduled", false, true, ""},
		{"succeeded", model.WebhookDeliveryStatusSucceeded, "Delivered", false, false, ""},
		{"canceled", model.WebhookDeliveryStatusCanceled, "Delivery canceled", false, false, ""},
		{"failed", model.WebhookDeliveryStatusFailed, "Delivery failed", true, false, "HTTP 404: no such hook"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			html := renderWebhookDeliveryPartial(t, tc.status, tc.lastError, future)

			if !strings.Contains(html, "webhook-delivery-"+tc.status) {
				t.Fatalf("Expected CSS status class for %q in %s", tc.status, html)
			}

			if !strings.Contains(html, tc.wantLabel) {
				t.Fatalf("Expected label %q in %s", tc.wantLabel, html)
			}

			if tc.wantNext && !strings.Contains(html, "Next attempt:") {
				t.Fatalf("Expected next-attempt hint for %s", tc.status)
			}

			if tc.wantRetry {
				if !strings.Contains(html, "data-webhook-retry=\"true\"") {
					t.Fatal("Expected a retry button on failure")
				}

				if !strings.Contains(html, "/entry/save/77/webhook-retry") {
					t.Fatalf("Expected the retry URL to target entry 77, got %s", html)
				}

				if !strings.Contains(html, tc.lastError) {
					t.Fatalf("Expected the failure reason to be rendered, got %s", html)
				}
			} else if strings.Contains(html, "data-webhook-retry=") {
				t.Fatalf("No retry button should be rendered for status %s", tc.status)
			}
		})
	}
}

func TestWebhookDeliveryPartialEmptyWhenNoDelivery(t *testing.T) {
	funcs := (&funcMap{}).Map()
	tpl := template.Must(template.New("").Funcs(funcs).ParseFS(commonTemplateFiles, "templates/common/webhook_delivery.html"))

	printer := locale.NewPrinter("en_US")
	tpl.Funcs(template.FuncMap{"t": printer.Printf})

	data := map[string]any{
		"language": "en_US",
		"entry":    &model.Entry{ID: 1},
		"user":     map[string]any{"Timezone": "UTC"},
	}

	var b bytes.Buffer
	if err := tpl.ExecuteTemplate(&b, "common/webhook_delivery.html", data); err != nil {
		t.Fatalf("Unable to render partial: %v", err)
	}

	if strings.Contains(b.String(), "webhook-delivery") {
		t.Fatalf("No delivery markup should be rendered without a record, got %s", b.String())
	}
}
