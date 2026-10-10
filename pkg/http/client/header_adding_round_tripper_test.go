package client_test

import (
	"bytes"
	"io"
	"net/http"
	"testing"

	"github.com/buildbarn/bb-storage/internal/mock"
	http_client "github.com/buildbarn/bb-storage/pkg/http/client"
	pb "github.com/buildbarn/bb-storage/pkg/proto/configuration/http/client"
	"github.com/stretchr/testify/require"

	"go.uber.org/mock/gomock"
)

const testCredential = "Basic bXktdXNlcjpteS1wYXNzd29yZA=="

// hop is one expected call to the RoundTripper below the decorator. A non-empty
// location answers 302 instead of 200.
type hop struct {
	url      string
	location string
	// http.Transport always sets Response.Request. This models one that does not.
	omitResponseRequest bool
}

// runChain drives a real http.Client, so net/http's own redirect handling runs
// between hops, and reports headerName on each hop. Empty means absent.
func runChain(t *testing.T, headerValues []*pb.Configuration_HeaderValues, headerName string, hops []hop) []string {
	ctrl := gomock.NewController(t)
	base := mock.NewMockRoundTripper(ctrl)

	observed := make([]string, 0, len(hops))
	calls := make([]any, 0, len(hops))
	for _, h := range hops {
		calls = append(calls, base.EXPECT().
			RoundTrip(gomock.Any()).
			DoAndReturn(func(req *http.Request) (*http.Response, error) {
				require.Equal(t, h.url, req.URL.String())
				observed = append(observed, req.Header.Get(headerName))
				response := &http.Response{
					StatusCode: http.StatusOK,
					Header:     http.Header{},
					Body:       io.NopCloser(bytes.NewReader(nil)),
					// The decorator walks this back to the original.
					Request: req,
				}
				if h.omitResponseRequest {
					response.Request = nil
				}
				if h.location != "" {
					response.StatusCode = http.StatusFound
					response.Header.Set("Location", h.location)
				}
				return response, nil
			}))
	}
	gomock.InOrder(calls...)

	client := http.Client{
		Transport: http_client.NewHeaderAddingRoundTripper(base, headerValues),
	}
	response, err := client.Get(hops[0].url)
	require.NoError(t, err)
	require.NoError(t, response.Body.Close())
	return observed
}

func TestHeaderAddingRoundTripper(t *testing.T) {
	authorization := []*pb.Configuration_HeaderValues{{
		Header: "Authorization",
		Values: []string{testCredential},
	}}

	t.Run("NoRedirect", func(t *testing.T) {
		require.Equal(t, []string{testCredential}, runChain(t, authorization, "Authorization", []hop{
			{url: "https://origin.example.com/package.tar"},
		}))
	})

	t.Run("SameHostRedirect", func(t *testing.T) {
		require.Equal(t, []string{testCredential, testCredential}, runChain(t, authorization, "Authorization", []hop{
			{url: "https://origin.example.com/package.tar", location: "https://origin.example.com/v2/package.tar"},
			{url: "https://origin.example.com/v2/package.tar"},
		}))
	})

	// net/http lets credentials follow a redirect into a subdomain.
	t.Run("SubdomainRedirect", func(t *testing.T) {
		require.Equal(t, []string{testCredential, testCredential}, runChain(t, authorization, "Authorization", []hop{
			{url: "https://example.com/package.tar", location: "https://cdn.example.com/blob"},
			{url: "https://cdn.example.com/blob"},
		}))
	})

	t.Run("CrossHostRedirect", func(t *testing.T) {
		require.Equal(t, []string{testCredential, ""}, runChain(t, authorization, "Authorization", []hop{
			{url: "https://origin.example.com/package.tar", location: "https://objects.example.net/blob"},
			{url: "https://objects.example.net/blob"},
		}))
	})

	t.Run("CrossHostRedirectKeepsOtherHeaders", func(t *testing.T) {
		trace := []*pb.Configuration_HeaderValues{{
			Header: "X-Trace-Id",
			Values: []string{"a3f0"},
		}}
		require.Equal(t, []string{"a3f0", "a3f0"}, runChain(t, trace, "X-Trace-Id", []hop{
			{url: "https://origin.example.com/package.tar", location: "https://objects.example.net/blob"},
			{url: "https://objects.example.net/blob"},
		}))
	})

	// net/http decides once and stays decided, so returning to the origin
	// host does not bring the credential back.
	t.Run("ReturnToOriginAfterCrossHostRedirect", func(t *testing.T) {
		require.Equal(t, []string{testCredential, "", ""}, runChain(t, authorization, "Authorization", []hop{
			{url: "https://origin.example.com/package.tar", location: "https://objects.example.net/blob"},
			{url: "https://objects.example.net/blob", location: "https://origin.example.com/v2/package.tar"},
			{url: "https://origin.example.com/v2/package.tar"},
		}))
	})

	t.Run("NonCanonicalHeaderName", func(t *testing.T) {
		lowercase := []*pb.Configuration_HeaderValues{{
			Header: "authorization",
			Values: []string{testCredential},
		}}
		require.Equal(t, []string{testCredential, ""}, runChain(t, lowercase, "Authorization", []hop{
			{url: "https://origin.example.com/package.tar", location: "https://objects.example.net/blob"},
			{url: "https://objects.example.net/blob"},
		}))
	})

	t.Run("CookieIsSensitiveToo", func(t *testing.T) {
		cookie := []*pb.Configuration_HeaderValues{{
			Header: "Cookie",
			Values: []string{"session=opaque"},
		}}
		require.Equal(t, []string{"session=opaque", ""}, runChain(t, cookie, "Cookie", []hop{
			{url: "https://origin.example.com/package.tar", location: "https://objects.example.net/blob"},
			{url: "https://objects.example.net/blob"},
		}))
	})

	// Three hops because the origin and the later hop are lowered separately.
	t.Run("MixedCaseHostRedirect", func(t *testing.T) {
		require.Equal(t, []string{testCredential, testCredential, testCredential}, runChain(t, authorization, "Authorization", []hop{
			{url: "https://ORIGIN.example.com/package.tar", location: "https://origin.example.com/v2/package.tar"},
			{url: "https://origin.example.com/v2/package.tar", location: "https://Origin.Example.COM/v3/package.tar"},
			{url: "https://Origin.Example.COM/v3/package.tar"},
		}))
	})

	t.Run("BrokenResponseChain", func(t *testing.T) {
		require.Equal(t, []string{testCredential, ""}, runChain(t, authorization, "Authorization", []hop{
			{url: "https://origin.example.com/package.tar", location: "https://origin.example.com/v2/package.tar", omitResponseRequest: true},
			{url: "https://origin.example.com/v2/package.tar"},
		}))
	})
}
