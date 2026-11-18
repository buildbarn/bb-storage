package grpc

import (
	"bytes"
	"context"
	"encoding/json"
	"net/url"
	"os/exec"
	"strings"
	"sync"
	"time"

	"github.com/buildbarn/bb-storage/pkg/clock"
	configuration "github.com/buildbarn/bb-storage/pkg/proto/configuration/grpc"
	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
)

const (
	credentialHelperDefaultTimeout          = 10 * time.Second
	credentialHelperDefaultExpirationMargin = 30 * time.Second

	// Upper bound on the amount of output we're willing to buffer
	// from a credential helper.
	credentialHelperMaximumOutputSizeBytes = 1 << 20
)

type credentialHelperRequest struct {
	URI string `json:"uri"`
}

type credentialHelperResponse struct {
	Expires *time.Time          `json:"expires"`
	Headers map[string][]string `json:"headers"`
}

type credentialHelperCacheEntry struct {
	lock sync.Mutex

	// Set to true once headers has been populated.
	valid   bool
	headers map[string]string
	// Time after which the entry must be refreshed. The zero value
	// indicates that the entry never expires.
	refreshAt time.Time
}

type credentialHelperCredentials struct {
	path            string
	timeout         time.Duration
	margin          time.Duration
	defaultValidity *time.Duration
	clock           clock.Clock

	lock    sync.Mutex
	entries map[string]*credentialHelperCacheEntry
}

// NewCredentialHelperCredentialsFromConfiguration creates gRPC
// per-RPC credentials that obtain request headers by invoking an
// external program that implements the Credential Helper Protocol:
// https://github.com/EngFlow/credential-helper-spec
//
// The URI provided to the helper is of the form
// "https://${host}/${service}", which is identical to what Bazel uses
// for gRPC calls. This permits a single helper to be shared between
// Bazel and Buildbarn.
func NewCredentialHelperCredentialsFromConfiguration(config *configuration.CredentialHelperConfiguration, clock clock.Clock) (credentials.PerRPCCredentials, error) {
	if config.Path == "" {
		return nil, status.Error(codes.InvalidArgument, "No credential helper path provided")
	}
	c := &credentialHelperCredentials{
		path:    config.Path,
		timeout: credentialHelperDefaultTimeout,
		margin:  credentialHelperDefaultExpirationMargin,
		clock:   clock,
		entries: map[string]*credentialHelperCacheEntry{},
	}
	if d := config.Timeout; d != nil {
		if err := d.CheckValid(); err != nil {
			return nil, util.StatusWrapWithCode(err, codes.InvalidArgument, "Invalid credential helper timeout")
		}
		c.timeout = d.AsDuration()
	}
	if d := config.ExpirationMargin; d != nil {
		if err := d.CheckValid(); err != nil {
			return nil, util.StatusWrapWithCode(err, codes.InvalidArgument, "Invalid credential helper expiration margin")
		}
		c.margin = d.AsDuration()
	}
	if d := config.DefaultValidity; d != nil {
		if err := d.CheckValid(); err != nil {
			return nil, util.StatusWrapWithCode(err, codes.InvalidArgument, "Invalid credential helper default validity")
		}
		v := d.AsDuration()
		c.defaultValidity = &v
	}
	return c, nil
}

// normalizeCredentialHelperURI converts the URI provided by gRPC
// ("https://host:port/service") into the form used by Bazel, which
// omits the default HTTPS port.
func normalizeCredentialHelperURI(uri string) (string, error) {
	u, err := url.Parse(uri)
	if err != nil {
		return "", util.StatusWrapWithCode(err, codes.Internal, "Failed to parse request URI")
	}
	u.Scheme = "https"
	u.Host = strings.TrimSuffix(u.Host, ":443")
	return u.String(), nil
}

func (c *credentialHelperCredentials) GetRequestMetadata(ctx context.Context, uris ...string) (map[string]string, error) {
	if len(uris) != 1 {
		return nil, status.Errorf(codes.Internal, "Expected exactly one request URI, got %d", len(uris))
	}
	uri, err := normalizeCredentialHelperURI(uris[0])
	if err != nil {
		return nil, err
	}

	c.lock.Lock()
	entry, ok := c.entries[uri]
	if !ok {
		entry = &credentialHelperCacheEntry{}
		c.entries[uri] = entry
	}
	c.lock.Unlock()

	// Hold the per-URI lock while refreshing, so that concurrent
	// RPCs share a single invocation of the helper.
	entry.lock.Lock()
	defer entry.lock.Unlock()
	if entry.valid && (entry.refreshAt.IsZero() || c.clock.Now().Before(entry.refreshAt)) {
		return entry.headers, nil
	}

	headers, refreshAt, err := c.invoke(ctx, uri)
	if err != nil {
		return nil, util.StatusWrapf(err, "Credential helper %#v failed for %#v", c.path, uri)
	}
	entry.valid = true
	entry.headers = headers
	entry.refreshAt = refreshAt
	return headers, nil
}

func (c *credentialHelperCredentials) invoke(ctx context.Context, uri string) (map[string]string, time.Time, error) {
	request, err := json.Marshal(credentialHelperRequest{URI: uri})
	if err != nil {
		return nil, time.Time{}, util.StatusWrapWithCode(err, codes.Internal, "Failed to marshal request")
	}

	ctxWithTimeout, cancel := c.clock.NewContextWithTimeout(ctx, c.timeout)
	defer cancel()
	cmd := exec.CommandContext(ctxWithTimeout, c.path, "get")
	cmd.Stdin = bytes.NewReader(request)
	stdout := limitedBuffer{limit: credentialHelperMaximumOutputSizeBytes}
	stderr := limitedBuffer{limit: 4096}
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	// Ensure we don't hang on pipes held open by grandchildren.
	cmd.WaitDelay = time.Second
	if err := cmd.Run(); err != nil {
		code := codes.Unavailable
		if ctxWithTimeout.Err() != nil {
			code = codes.DeadlineExceeded
		}
		return nil, time.Time{}, status.Errorf(code, "%s: %s", err, strings.TrimSpace(stderr.buf.String()))
	}
	if stdout.exceeded {
		return nil, time.Time{}, status.Error(codes.Unavailable, "Response is too large")
	}

	var response credentialHelperResponse
	if err := json.Unmarshal(stdout.buf.Bytes(), &response); err != nil {
		return nil, time.Time{}, util.StatusWrapWithCode(err, codes.Unavailable, "Failed to parse response")
	}
	headers := make(map[string]string, len(response.Headers))
	for name, values := range response.Headers {
		// gRPC metadata keys must be lower case, and
		// pseudo-headers must not be overridden.
		name = strings.ToLower(name)
		if name == "" || strings.HasPrefix(name, ":") {
			return nil, time.Time{}, status.Errorf(codes.Unavailable, "Response contains invalid header name %#v", name)
		}
		if len(values) > 0 {
			headers[name] = strings.Join(values, ", ")
		}
	}

	var refreshAt time.Time
	now := c.clock.Now()
	switch {
	case response.Expires != nil:
		refreshAt = response.Expires.Add(-c.margin)
	case c.defaultValidity != nil:
		refreshAt = now.Add(*c.defaultValidity - c.margin)
	}
	if !refreshAt.IsZero() && !refreshAt.After(now) {
		// Already (almost) expired. Use it for this request, but
		// make sure we don't cache it. The zero value means
		// "never expires", so use a time in the past instead.
		refreshAt = now
	}
	return headers, refreshAt, nil
}

// The headers contain secrets, so never send them over plaintext
// connections.
func (*credentialHelperCredentials) RequireTransportSecurity() bool {
	return true
}

// limitedBuffer is a bytes.Buffer that discards data past a limit.
type limitedBuffer struct {
	buf      bytes.Buffer
	limit    int
	exceeded bool
}

func (b *limitedBuffer) Write(p []byte) (int, error) {
	if remaining := b.limit - b.buf.Len(); len(p) > remaining {
		b.exceeded = true
		b.buf.Write(p[:remaining])
	} else {
		b.buf.Write(p)
	}
	return len(p), nil
}
