package client_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	httpclient "github.com/buildbarn/bb-storage/pkg/http/client"
	configuration "github.com/buildbarn/bb-storage/pkg/proto/configuration/http/client"

	"golang.org/x/oauth2"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/durationpb"
)

func tokenSource(timeout *durationpb.Duration, endpoint string) (oauth2.TokenSource, error) {
	return httpclient.NewOAuth2TokenSourceFromConfiguration(&configuration.OAuth2Configuration{
		Credentials: &configuration.OAuth2Configuration_ClientCredentials{
			ClientCredentials: &configuration.OAuth2ClientCredentials{
				ClientId:            "synthetic-client",
				ClientSecret:        "synthetic-secret",
				TokenEndpointUrl:    endpoint,
				TokenRequestTimeout: timeout,
			},
		},
	})
}

func TestTokenRequestTimeoutValidation(t *testing.T) {
	for _, tc := range []struct {
		name    string
		timeout *durationpb.Duration
		code    codes.Code
	}{
		{name: "unset", code: codes.OK},
		{name: "positive", timeout: durationpb.New(15 * time.Second), code: codes.OK},
		{name: "zero", timeout: &durationpb.Duration{}, code: codes.InvalidArgument},
		{name: "negative", timeout: durationpb.New(-time.Second), code: codes.InvalidArgument},
		{name: "invalid-nanoseconds", timeout: &durationpb.Duration{Nanos: 1000000000}, code: codes.InvalidArgument},
		{name: "invalid-seconds", timeout: &durationpb.Duration{Seconds: 315576000001}, code: codes.InvalidArgument},
	} {
		t.Run(tc.name, func(t *testing.T) {
			source, err := tokenSource(tc.timeout, "http://127.0.0.1:1/token")
			if got := status.Code(err); got != tc.code {
				t.Fatalf("status = %s, want %s: %v", got, tc.code, err)
			}
			if (source != nil) != (tc.code == codes.OK) {
				t.Fatal("invalid configuration returned a token source")
			}
		})
	}
}

func TestTokenRequestTimeoutJSONConfiguration(t *testing.T) {
	var config configuration.OAuth2Configuration
	if err := protojson.Unmarshal([]byte(`{
		"scopes": ["example-scope"],
		"clientCredentials": {
			"clientId": "synthetic-client",
			"clientSecret": "synthetic-secret",
			"tokenEndpointUrl": "http://127.0.0.1:1/token",
			"tokenRequestTimeout": "15s"
		}
	}`), &config); err != nil {
		t.Fatal(err)
	}
	if got := config.GetClientCredentials().GetTokenRequestTimeout().AsDuration(); got != 15*time.Second {
		t.Fatalf("JSON token request timeout = %s, want 15s", got)
	}
	if _, err := httpclient.NewOAuth2TokenSourceFromConfiguration(&config); err != nil {
		t.Fatal(err)
	}
}

func TestTokenRequestTimeoutUnset(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Longer than the configured bound in the stall tests. An omitted
		// setting must not impose that bound on existing configurations.
		time.Sleep(400 * time.Millisecond)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"access_token": "synthetic-unbounded", "token_type": "Bearer", "expires_in": 300,
		})
	}))
	defer server.Close()
	source, err := tokenSource(nil, server.URL)
	if err != nil {
		t.Fatal(err)
	}
	token, err := source.Token()
	if err != nil || token == nil || token.AccessToken != "synthetic-unbounded" {
		t.Fatalf("unset timeout changed existing behavior: %v", err)
	}
}

func TestTokenRequestTimeoutFailsClosedThenRecovers(t *testing.T) {
	for _, mode := range []string{"no-headers", "partial-body", "trickle-body"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			release := make(chan struct{})
			var once sync.Once
			unblock := func() { once.Do(func() { close(release) }) }
			var recovering atomic.Bool
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.ParseForm() != nil || r.Form.Get("grant_type") != "client_credentials" {
					w.WriteHeader(http.StatusBadRequest)
					return
				}
				calls.Add(1)
				w.Header().Set("Content-Type", "application/json")
				if recovering.Load() {
					_ = json.NewEncoder(w).Encode(map[string]any{
						"access_token": "synthetic-recovered", "token_type": "Bearer", "expires_in": 300,
					})
					return
				}
				if mode != "no-headers" {
					_, _ = w.Write([]byte(`{"access_token":"`))
					w.(http.Flusher).Flush()
				}
				ticker := time.NewTicker(20 * time.Millisecond)
				defer ticker.Stop()
				for {
					select {
					case <-release:
						return
					case <-r.Context().Done():
						return
					case <-ticker.C:
						if mode == "trickle-body" {
							if _, err := w.Write([]byte("x")); err != nil {
								return
							}
							w.(http.Flusher).Flush()
						}
					}
				}
			}))
			defer server.Close()
			defer unblock()
			source, err := tokenSource(durationpb.New(200*time.Millisecond), server.URL)
			if err != nil {
				t.Fatal(err)
			}
			type result struct {
				token *oauth2.Token
				err   error
			}
			done := make(chan result, 1)
			go func() {
				token, err := source.Token()
				done <- result{token, err}
			}()
			// Initial auth-style probing can make two HTTP requests. The
			// watchdog only fails the test; it cannot establish a pass.
			select {
			case got := <-done:
				// x/oauth2 formats body-read errors with %v, losing their
				// error chain. Accept only its explicit Client.Timeout
				// body marker when the deadline type is no longer visible.
				timedOut := got.err != nil && (errors.Is(got.err, context.DeadlineExceeded) ||
					strings.Contains(got.err.Error(), "Client.Timeout or context cancellation while reading body"))
				if got.token != nil || !timedOut {
					t.Fatalf("stalled acquisition = %v, %v; want no token and HTTP timeout", got.token, got.err)
				}
			case <-time.After(5 * time.Second):
				unblock()
				<-done
				t.Fatal("token acquisition exceeded the configured HTTP request bounds")
			}
			if got := calls.Load(); got < 1 || got > 2 {
				t.Fatalf("acquisition requests = %d, want one or two", got)
			}
			recovering.Store(true)
			token, err := source.Token()
			if err != nil || token == nil || token.AccessToken != "synthetic-recovered" {
				t.Fatalf("token source did not recover: %v", err)
			}
			before := calls.Load()
			if _, err := source.Token(); err != nil || calls.Load() != before {
				t.Fatal("valid recovered token was not reused")
			}
		})
	}
}
