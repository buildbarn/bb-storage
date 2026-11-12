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
	credentialHelperDefaultTimeout = 10 * time.Second

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
	// Time at which the entry expires and must be refreshed. The
	// zero value indicates that the entry never expires.
	expiresAt time.Time
}

type credentialHelperCredentials struct {
	path          string
	timeout       time.Duration
	cacheDuration *time.Duration
	clock         clock.Clock

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
		clock:   clock,
		entries: map[string]*credentialHelperCacheEntry{},
	}
	if d := config.Timeout; d != nil {
		if err := d.CheckValid(); err != nil {
			return nil, util.StatusWrapWithCode(err, codes.InvalidArgument, "Invalid credential helper timeout")
		}
		c.timeout = d.AsDuration()
	}
	if d := config.CacheDuration; d != nil {
		if err := d.CheckValid(); err != nil {
			return nil, util.StatusWrapWithCode(err, codes.InvalidArgument, "Invalid credential helper cache duration")
		}
		v := d.AsDuration()
		c.cacheDuration = &v
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
	if entry.valid && (entry.expiresAt.IsZero() || c.clock.Now().Before(entry.expiresAt)) {
		return entry.headers, nil
	}

	headers, expiresAt, err := c.invoke(ctx, uri)
	if err != nil {
		return nil, util.StatusWrapf(err, "Credential helper %#v failed for %#v", c.path, uri)
	}
	entry.valid = true
	entry.headers = headers
	entry.expiresAt = expiresAt
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
		// PerRPCCredentials can only provide a single value
		// per header. Joining values with commas is only
		// equivalent to sending them separately for headers
		// that are defined as lists, which isn't the case for
		// headers like Authorization.
		switch len(values) {
		case 0:
		case 1:
			headers[name] = values[0]
		default:
			return nil, time.Time{}, status.Errorf(codes.Unavailable, "Response contains multiple values for header %#v", name)
		}
	}

	// Like Bazel, use the expiration time exactly as provided,
	// without applying any tolerance.
	var expiresAt time.Time
	now := c.clock.Now()
	switch {
	case response.Expires != nil:
		expiresAt = *response.Expires
	case c.cacheDuration != nil:
		expiresAt = now.Add(*c.cacheDuration)
	}
	if !expiresAt.IsZero() && !expiresAt.After(now) {
		// Already expired. Use it for this request, but make
		// sure we don't cache it. The zero value means "never
		// expires", so use the current time instead.
		expiresAt = now
	}
	return headers, expiresAt, nil
}

// Whether TLS is used is fully controlled by the client configuration,
// so there is no need to forbid the use of plaintext connections.
func (*credentialHelperCredentials) RequireTransportSecurity() bool {
	return false
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
