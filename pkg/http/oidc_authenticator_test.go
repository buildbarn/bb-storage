package http_test

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/buildbarn/bb-storage/internal/mock"
	bb_http "github.com/buildbarn/bb-storage/pkg/http"
	"github.com/buildbarn/bb-storage/pkg/proto/auth"
	"github.com/buildbarn/bb-storage/pkg/proto/http/oidc"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/jmespath/go-jmespath"
	"github.com/stretchr/testify/require"

	"golang.org/x/oauth2"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"go.uber.org/mock/gomock"
)

func protoMustMarshal(m proto.Message) []byte {
	data, err := proto.MarshalOptions{Deterministic: true}.Marshal(m)
	if err != nil {
		panic(err)
	}
	return data
}

func expectRead(r *mock.MockThreadSafeGenerator, dataToReturn []byte) {
	r.EXPECT().
		Read(gomock.Len(len(dataToReturn))).
		DoAndReturn(func(p []byte) (int, error) { return copy(p, dataToReturn), nil })
}

func TestOIDCAuthenticator(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	// The OAuth2 library only allows injecting a clock by
	// overriding a global variable.
	clock := mock.NewMockClock(ctrl)
	oauth2.TimeNow = clock.Now
	defer func() { oauth2.TimeNow = time.Now }()

	roundTripper := mock.NewMockRoundTripper(ctrl)
	randomNumberGenerator := mock.NewMockThreadSafeGenerator(ctrl)
	cookieAEAD := mock.NewMockAEAD(ctrl)
	oauth2Config := &oauth2.Config{
		ClientID:     "MyClientID",
		ClientSecret: "MyClientSecret",
		Endpoint: oauth2.Endpoint{
			AuthURL:   "https://login.com/authorize",
			TokenURL:  "https://login.com/token",
			AuthStyle: oauth2.AuthStyleInParams,
		},
		RedirectURL: "https://myserver.com/callback",
		Scopes:      []string{"openid", "email"},
	}

	t.Run("UserInfo", func(t *testing.T) {
		cookieAEAD.EXPECT().NonceSize().Return(4)
		claimsFetcher := bb_http.NewUserInfoOIDCClaimsFetcher(
			oauth2Config,
			"https://login.com/userinfo",
		)
		authenticator, err := bb_http.NewOIDCAuthenticator(
			oauth2Config,
			claimsFetcher,
			jmespath.MustCompile("{\"public\": @}"),
			&http.Client{Transport: roundTripper},
			randomNumberGenerator,
			"CookieName",
			cookieAEAD,
			clock,
		)
		require.NoError(t, err)

		t.Run("RegularRequestWithoutCookie", func(t *testing.T) {
			// If a request is performed against an arbitrary URL
			// without any cookie being set, we should receive a
			// redirect that points to the authorization endpoint,
			// containing information such as the client ID,
			// redirect URI, and scopes.
			stateVerifier := []byte{0x75, 0xe6, 0x5d, 0xc1, 0x2c, 0x0e, 0x35, 0x16, 0x8a, 0xbd, 0xc7, 0xc7, 0x39, 0xa4, 0xd0, 0xe0}
			expectRead(randomNumberGenerator, stateVerifier)
			nonce := []byte{0x5b, 0xd1, 0x9b, 0x39}
			expectRead(randomNumberGenerator, nonce)
			cookieAEAD.EXPECT().Seal(
				gomock.Any(),
				nonce,
				protoMustMarshal(&oidc.CookieValue{
					SessionState: &oidc.CookieValue_Authenticating_{
						Authenticating: &oidc.CookieValue_Authenticating{
							StateVerifier:      stateVerifier,
							OriginalRequestUri: "/index.html?foo=bar&baz=qux",
						},
					},
				}),
				nil,
			).DoAndReturn(func(dst, nonce, plaintext, additionalData []byte) []byte {
				return append(dst, 0x22, 0xa2, 0x42, 0xab, 0xbd, 0x9f, 0xf5, 0x12)
			})

			w := httptest.NewRecorder()
			r, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://myserver.com/index.html?foo=bar&baz=qux", nil)
			require.NoError(t, err)
			metadata, err := authenticator.Authenticate(w, r)
			require.NoError(t, err)
			require.Nil(t, metadata)

			require.Equal(t, http.StatusSeeOther, w.Code)
			require.Equal(t, http.Header{
				"Content-Type": []string{"text/html; charset=utf-8"},
				"Location":     []string{"https://login.com/authorize?client_id=MyClientID&redirect_uri=https%3A%2F%2Fmyserver.com%2Fcallback&response_type=code&scope=openid+email&state=deZdwSwONRaKvcfHOaTQ4A"},
				"Set-Cookie":   []string{"CookieName=W9GbOSKiQqu9n_US; Path=/; HttpOnly; Secure; SameSite=Lax"},
			}, w.HeaderMap)
		})

		t.Run("RegularPageRequestWithoutCookie", func(t *testing.T) {
			// If a page request is performed against an arbitrary URL
			// without any cookie being set, we should receive a
			// redirect. A page request is indicated by the
			// Sec-Fetch-Dest=document header.
			stateVerifier := []byte{0x75, 0xe6, 0x5d, 0xc1, 0x2c, 0x0e, 0x35, 0x16, 0x8a, 0xbd, 0xc7, 0xc7, 0x39, 0xa4, 0xd0, 0xe0}
			expectRead(randomNumberGenerator, stateVerifier)
			nonce := []byte{0x5b, 0xd1, 0x9b, 0x39}
			expectRead(randomNumberGenerator, nonce)
			cookieAEAD.EXPECT().Seal(
				gomock.Any(),
				nonce,
				protoMustMarshal(&oidc.CookieValue{
					SessionState: &oidc.CookieValue_Authenticating_{
						Authenticating: &oidc.CookieValue_Authenticating{
							StateVerifier:      stateVerifier,
							OriginalRequestUri: "/index.html?foo=bar&baz=qux",
						},
					},
				}),
				nil,
			).DoAndReturn(func(dst, nonce, plaintext, additionalData []byte) []byte {
				return append(dst, 0x22, 0xa2, 0x42, 0xab, 0xbd, 0x9f, 0xf5, 0x12)
			})

			w := httptest.NewRecorder()
			r, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://myserver.com/index.html?foo=bar&baz=qux", nil)
			require.NoError(t, err)
			r.Header.Set("Sec-Fetch-Dest", "document")
			metadata, err := authenticator.Authenticate(w, r)
			require.NoError(t, err)
			require.Nil(t, metadata)

			require.Equal(t, http.StatusSeeOther, w.Code)
			require.Equal(t, http.Header{
				"Content-Type": []string{"text/html; charset=utf-8"},
				"Location":     []string{"https://login.com/authorize?client_id=MyClientID&redirect_uri=https%3A%2F%2Fmyserver.com%2Fcallback&response_type=code&scope=openid+email&state=deZdwSwONRaKvcfHOaTQ4A"},
				"Set-Cookie":   []string{"CookieName=W9GbOSKiQqu9n_US; Path=/; HttpOnly; Secure; SameSite=Lax"},
			}, w.HeaderMap)
		})

		t.Run("RegularXhrRequestWithoutCookie", func(t *testing.T) {
			// If a XHR-request is performed against an arbitrary URL without
			// any cookie being set, we should receive a 401 Unauthorized.
			// A XHR request is indicated by the Sec-Fetch-Dest=empty header.
			w := httptest.NewRecorder()
			r, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://myserver.com/index.html?foo=bar&baz=qux", nil)
			require.NoError(t, err)
			r.Header.Set("Sec-Fetch-Dest", "empty")
			_, err = authenticator.Authenticate(w, r)
			testutil.RequireEqualStatus(t, status.Error(codes.Unauthenticated, "No valid OIDC session state cookie found"), err)
		})

		t.Run("RegularRequestExpiredAccessTokenWithoutRefreshToken", func(t *testing.T) {
			// If the access token has expired and no refresh token
			// is present, attempting to open an arbitrary URL
			// should redirect to the authorization endpoint again.
			cookieAEAD.EXPECT().Open(
				gomock.Any(),
				[]byte{0x67, 0xa9, 0xa4, 0x6f},
				[]byte{0x63, 0xd4, 0xee, 0x6a, 0xfd, 0x97, 0x9d, 0x52},
				nil,
			).DoAndReturn(func(dst, nonce, ciphertext, additionalData []byte) ([]byte, error) {
				return append(dst, protoMustMarshal(&oidc.CookieValue{
					SessionState: &oidc.CookieValue_Authenticated_{
						Authenticated: &oidc.CookieValue_Authenticated{
							AuthenticationMetadata: &auth.AuthenticationMetadata{},
							Expiration:             &timestamppb.Timestamp{Seconds: 1693145873},
							DefaultExpiration:      &durationpb.Duration{Seconds: 60},
						},
					},
				})...), nil
			})
			clock.EXPECT().Now().Return(time.Unix(1693146029, 0))
			stateVerifier := []byte{0x25, 0xd8, 0xc0, 0xb6, 0x57, 0xf1, 0xf1, 0x3e, 0xaf, 0x78, 0x01, 0x40, 0x3c, 0xa9, 0x4b, 0xdf}
			expectRead(randomNumberGenerator, stateVerifier)
			nonce := []byte{0xa1, 0x6d, 0x1f, 0xf2}
			expectRead(randomNumberGenerator, nonce)
			cookieAEAD.EXPECT().Seal(
				gomock.Any(),
				nonce,
				protoMustMarshal(&oidc.CookieValue{
					SessionState: &oidc.CookieValue_Authenticating_{
						Authenticating: &oidc.CookieValue_Authenticating{
							StateVerifier:      stateVerifier,
							OriginalRequestUri: "/favicon.ico",
						},
					},
				}),
				nil,
			).DoAndReturn(func(dst, nonce, plaintext, additionalData []byte) []byte {
				return append(dst, 0x45, 0xc5, 0x0e, 0xbb, 0xe6, 0x0e, 0x2c, 0x2f)
			})

			w := httptest.NewRecorder()
			r, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://myserver.com/favicon.ico", nil)
			r.AddCookie(&http.Cookie{
				Name:  "CookieName",
				Value: "Z6mkb2PU7mr9l51S",
			})
			require.NoError(t, err)
			metadata, err := authenticator.Authenticate(w, r)
			require.NoError(t, err)
			require.Nil(t, metadata)

			require.Equal(t, http.StatusSeeOther, w.Code)
			require.Equal(t, http.Header{
				"Content-Type": []string{"text/html; charset=utf-8"},
				"Location":     []string{"https://login.com/authorize?client_id=MyClientID&redirect_uri=https%3A%2F%2Fmyserver.com%2Fcallback&response_type=code&scope=openid+email&state=JdjAtlfx8T6veAFAPKlL3w"},
				"Set-Cookie":   []string{"CookieName=oW0f8kXFDrvmDiwv; Path=/; HttpOnly; Secure; SameSite=Lax"},
			}, w.HeaderMap)
		})

		t.Run("RegularRequestExpiredAccessTokenWithRefreshTokenWithExpiresIn", func(t *testing.T) {
			// If the access token is expired and a refresh token is
			// available, we may be able to continue the session
			// without sending the user through the authorization
			// endpoint. We use the occasion to update the claims.
			// Upon success, the cookie should be updated.
			cookieAEAD.EXPECT().Open(
				gomock.Any(),
				[]byte{0x29, 0xc3, 0x4d, 0xf7},
				[]byte{0x8a, 0x16, 0x13, 0x13, 0x7c, 0xc5, 0x7b, 0x5a},
				nil,
			).DoAndReturn(func(dst, nonce, ciphertext, additionalData []byte) ([]byte, error) {
				return append(dst, protoMustMarshal(&oidc.CookieValue{
					SessionState: &oidc.CookieValue_Authenticated_{
						Authenticated: &oidc.CookieValue_Authenticated{
							AuthenticationMetadata: &auth.AuthenticationMetadata{},
							Expiration:             &timestamppb.Timestamp{Seconds: 1693147158},
							RefreshToken:           "RefreshToken1",
							DefaultExpiration:      &durationpb.Duration{Seconds: 60},
						},
					},
				})...), nil
			})
			clock.EXPECT().Now().Return(time.Unix(1693147212, 0))
			roundTripper.EXPECT().RoundTrip(gomock.Any()).DoAndReturn(func(r *http.Request) (*http.Response, error) {
				require.Equal(t, "https://login.com/token", r.URL.String())
				r.ParseForm()
				require.Equal(t, url.Values{
					"client_id":     []string{"MyClientID"},
					"client_secret": []string{"MyClientSecret"},
					"grant_type":    []string{"refresh_token"},
					"refresh_token": []string{"RefreshToken1"},
				}, r.Form)
				return &http.Response{
					Status:     "200 OK",
					StatusCode: 200,
					Body: io.NopCloser(bytes.NewBufferString(`{
						"access_token": "AccessToken2",
						"expires_in": 3600,
						"refresh_token": "RefreshToken2",
						"token_type": "Bearer"
					}`)),
				}, nil
			})
			clock.EXPECT().Now().Return(time.Unix(1693147213, 0)).Times(2)
			roundTripper.EXPECT().RoundTrip(gomock.Any()).DoAndReturn(func(r *http.Request) (*http.Response, error) {
				require.Equal(t, "https://login.com/userinfo", r.URL.String())
				require.Equal(t, http.Header{
					"Authorization": []string{"Bearer AccessToken2"},
				}, r.Header)
				return &http.Response{
					Status:     "200 OK",
					StatusCode: 200,
					Body: io.NopCloser(bytes.NewBufferString(`{
						"email": "john@myserver.com",
						"name": "John Doe"
					}`)),
				}, nil
			})
			nonce := []byte{0xcf, 0xcc, 0x43, 0xbd}
			expectRead(randomNumberGenerator, nonce)
			expectedAuthenticationMetadata := &auth.AuthenticationMetadata{
				Public: structpb.NewStructValue(&structpb.Struct{
					Fields: map[string]*structpb.Value{
						"email": structpb.NewStringValue("john@myserver.com"),
						"name":  structpb.NewStringValue("John Doe"),
					},
				}),
			}
			cookieAEAD.EXPECT().Seal(
				gomock.Any(),
				nonce,
				protoMustMarshal(&oidc.CookieValue{
					SessionState: &oidc.CookieValue_Authenticated_{
						Authenticated: &oidc.CookieValue_Authenticated{
							AuthenticationMetadata: expectedAuthenticationMetadata,
							Expiration:             &timestamppb.Timestamp{Seconds: 1693150813},
							RefreshToken:           "RefreshToken2",
							DefaultExpiration:      &durationpb.Duration{Seconds: 60},
						},
					},
				}),
				nil,
			).DoAndReturn(func(dst, nonce, plaintext, additionalData []byte) []byte {
				return append(dst, 0xf4, 0xf3, 0xe9, 0xd7, 0x19, 0x29, 0x23, 0x83)
			})

			w := httptest.NewRecorder()
			r, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://myserver.com/hello.png", nil)
			r.AddCookie(&http.Cookie{
				Name:  "CookieName",
				Value: "KcNN94oWExN8xXta",
			})
			require.NoError(t, err)
			metadata, err := authenticator.Authenticate(w, r)
			require.NoError(t, err)
			testutil.RequireEqualProto(t, expectedAuthenticationMetadata, metadata.GetFullProto())

			require.Equal(t, http.Header{
				"Set-Cookie": []string{"CookieName=z8xDvfTz6dcZKSOD; Path=/; HttpOnly; Secure; SameSite=Lax"},
			}, w.HeaderMap)
		})

		t.Run("RegularRequestExpiredAccessTokenWithRefreshTokenWithoutExpiresIn", func(t *testing.T) {
			// It is only recommended that the access token response
			// contains an 'expires_in' value. If it does not, let's
			// pick an exponentially growing expiration time.
			//
			// More details: RFC 6749, section 4.2.2.
			cookieAEAD.EXPECT().Open(
				gomock.Any(),
				[]byte{0x84, 0x4b, 0x47, 0xdd},
				[]byte{0xac, 0x4f, 0xc2, 0x0d, 0x5b, 0x01, 0x78, 0x67},
				nil,
			).DoAndReturn(func(dst, nonce, ciphertext, additionalData []byte) ([]byte, error) {
				return append(dst, protoMustMarshal(&oidc.CookieValue{
					SessionState: &oidc.CookieValue_Authenticated_{
						Authenticated: &oidc.CookieValue_Authenticated{
							AuthenticationMetadata: &auth.AuthenticationMetadata{},
							Expiration:             &timestamppb.Timestamp{Seconds: 1693631281},
							RefreshToken:           "RefreshToken1",
							DefaultExpiration:      &durationpb.Duration{Seconds: 60},
						},
					},
				})...), nil
			})
			clock.EXPECT().Now().Return(time.Unix(1693631288, 0))
			roundTripper.EXPECT().RoundTrip(gomock.Any()).DoAndReturn(func(r *http.Request) (*http.Response, error) {
				require.Equal(t, "https://login.com/token", r.URL.String())
				r.ParseForm()
				require.Equal(t, url.Values{
					"client_id":     []string{"MyClientID"},
					"client_secret": []string{"MyClientSecret"},
					"grant_type":    []string{"refresh_token"},
					"refresh_token": []string{"RefreshToken1"},
				}, r.Form)
				return &http.Response{
					Status:     "200 OK",
					StatusCode: 200,
					Body: io.NopCloser(bytes.NewBufferString(`{
						"access_token": "AccessToken2",
						"refresh_token": "RefreshToken2",
						"token_type": "Bearer"
					}`)),
				}, nil
			})
			clock.EXPECT().Now().Return(time.Unix(1693631289, 0))
			roundTripper.EXPECT().RoundTrip(gomock.Any()).DoAndReturn(func(r *http.Request) (*http.Response, error) {
				require.Equal(t, "https://login.com/userinfo", r.URL.String())
				require.Equal(t, http.Header{
					"Authorization": []string{"Bearer AccessToken2"},
				}, r.Header)
				return &http.Response{
					Status:     "200 OK",
					StatusCode: 200,
					Body: io.NopCloser(bytes.NewBufferString(`{
						"email": "john@myserver.com",
						"name": "John Doe"
					}`)),
				}, nil
			})
			nonce := []byte{0xa9, 0xe3, 0x9f, 0xc5}
			expectRead(randomNumberGenerator, nonce)
			expectedAuthenticationMetadata := &auth.AuthenticationMetadata{
				Public: structpb.NewStructValue(&structpb.Struct{
					Fields: map[string]*structpb.Value{
						"email": structpb.NewStringValue("john@myserver.com"),
						"name":  structpb.NewStringValue("John Doe"),
					},
				}),
			}
			cookieAEAD.EXPECT().Seal(
				gomock.Any(),
				nonce,
				protoMustMarshal(&oidc.CookieValue{
					SessionState: &oidc.CookieValue_Authenticated_{
						Authenticated: &oidc.CookieValue_Authenticated{
							AuthenticationMetadata: expectedAuthenticationMetadata,
							Expiration:             &timestamppb.Timestamp{Seconds: 1693631349},
							RefreshToken:           "RefreshToken2",
							DefaultExpiration:      &durationpb.Duration{Seconds: 120},
						},
					},
				}),
				nil,
			).DoAndReturn(func(dst, nonce, plaintext, additionalData []byte) []byte {
				return append(dst, 0xfc, 0x39, 0x91, 0xcd, 0x66, 0xd1, 0xbb, 0x95)
			})

			w := httptest.NewRecorder()
			r, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://myserver.com/hello.png", nil)
			r.AddCookie(&http.Cookie{
				Name:  "CookieName",
				Value: "hEtH3axPwg1bAXhn",
			})
			require.NoError(t, err)
			metadata, err := authenticator.Authenticate(w, r)
			require.NoError(t, err)
			testutil.RequireEqualProto(t, expectedAuthenticationMetadata, metadata.GetFullProto())

			require.Equal(t, http.Header{
				"Set-Cookie": []string{"CookieName=qeOfxfw5kc1m0buV; Path=/; HttpOnly; Secure; SameSite=Lax"},
			}, w.HeaderMap)
		})

		t.Run("CallbackRequestWithoutCookie", func(t *testing.T) {
			// After authorization has been performed, the user is
			// sent to the redirect URL. We can only finalize the
			// process if the cookie we set up front is still
			// present. The cookie contains the original path that
			// the user requested, which we need to redirect back to
			// the right location.
			w := httptest.NewRecorder()
			r, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://myserver.com/callback?code=MyCode&state=f5QVXNK7njK_Kn1OdEGQJA", nil)
			require.NoError(t, err)
			_, err = authenticator.Authenticate(w, r)
			testutil.RequireEqualStatus(t, status.Error(codes.InvalidArgument, "No valid OIDC session state cookie found"), err)
		})

		t.Run("CallbackRequestMismatchingStateVerifier", func(t *testing.T) {
			// The state verifier that is provided to the callback
			// must match with the one that was provided to the
			// authorization endpoint URL. The purpose for this is
			// twofold:
			//
			// - It prevents cross-site request forgery, as
			//   discussed in RFC 6749, section 10.12.
			// - It ensures that we don't accidentally redirect the
			//   user back to the wrong page.
			cookieAEAD.EXPECT().Open(
				gomock.Any(),
				[]byte{0x82, 0xf9, 0x85, 0xf9},
				[]byte{0xe8, 0xa7, 0x6f, 0x31, 0x58, 0x7e, 0xf0, 0x47},
				nil,
			).DoAndReturn(func(dst, nonce, ciphertext, additionalData []byte) ([]byte, error) {
				return append(dst, protoMustMarshal(&oidc.CookieValue{
					SessionState: &oidc.CookieValue_Authenticating_{
						Authenticating: &oidc.CookieValue_Authenticating{
							StateVerifier: []byte{0xf1, 0x57, 0x0d, 0xad, 0x3e, 0x38, 0xd8, 0x3d, 0xa4, 0x71, 0x09, 0x65, 0x9f, 0x85, 0xe5, 0x13},
						},
					},
				})...), nil
			})

			w := httptest.NewRecorder()
			r, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://myserver.com/callback?code=MyCode&state=kSJyNDUwF3i0IFZLp0ljjw", nil)
			r.AddCookie(&http.Cookie{
				Name:  "CookieName",
				Value: "gvmF-einbzFYfvBH",
			})
			require.NoError(t, err)
			_, err = authenticator.Authenticate(w, r)
			testutil.RequireEqualStatus(t, status.Error(codes.InvalidArgument, "State verifier does not match with OIDC session state cookie"), err)
		})

		t.Run("CallbackRequestTokenRequestFailure", func(t *testing.T) {
			// Successfully managed to obtain an authorization code,
			// but failed to exchange it for an access token and
			// refresh token.
			cookieAEAD.EXPECT().Open(
				gomock.Any(),
				[]byte{0x82, 0xf9, 0x85, 0xf9},
				[]byte{0xe8, 0xa7, 0x6f, 0x31, 0x58, 0x7e, 0xf0, 0x47},
				nil,
			).DoAndReturn(func(dst, nonce, ciphertext, additionalData []byte) ([]byte, error) {
				return append(dst, protoMustMarshal(&oidc.CookieValue{
					SessionState: &oidc.CookieValue_Authenticating_{
						Authenticating: &oidc.CookieValue_Authenticating{
							StateVerifier: []byte{0x7a, 0xbe, 0x5c, 0x8c, 0xe4, 0xdf, 0x0e, 0xb4, 0x09, 0xd9, 0x23, 0xe4, 0x79, 0x9a, 0x45, 0x7d},
						},
					},
				})...), nil
			})

			roundTripper.EXPECT().RoundTrip(gomock.Any()).DoAndReturn(func(r *http.Request) (*http.Response, error) {
				require.Equal(t, "https://login.com/token", r.URL.String())
				r.ParseForm()
				require.Equal(t, url.Values{
					"client_id":     []string{"MyClientID"},
					"client_secret": []string{"MyClientSecret"},
					"code":          []string{"MyCode"},
					"grant_type":    []string{"authorization_code"},
					"redirect_uri":  []string{"https://myserver.com/callback"},
				}, r.Form)
				return nil, status.Error(codes.Unavailable, "Connection reset by peer")
			})

			r := httptest.NewRecorder()
			w, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://myserver.com/callback?code=MyCode&state=er5cjOTfDrQJ2SPkeZpFfQ", nil)
			w.AddCookie(&http.Cookie{
				Name:  "CookieName",
				Value: "gvmF-einbzFYfvBH",
			})
			require.NoError(t, err)
			_, err = authenticator.Authenticate(r, w)
			testutil.RequireEqualStatus(t, status.Error(codes.Unavailable, "Failed to obtain token: Connection reset by peer"), err)
		})
	})

	t.Run("IDToken", func(t *testing.T) {
		cookieAEAD.EXPECT().NonceSize().Return(4)
		claimsFetcher := bb_http.IDTokenOIDCClaimsFetcher
		authenticator, err := bb_http.NewOIDCAuthenticator(
			oauth2Config,
			claimsFetcher,
			jmespath.MustCompile("{\"public\": @}"),
			&http.Client{Transport: roundTripper},
			randomNumberGenerator,
			"CookieName",
			cookieAEAD,
			clock,
		)
		require.NoError(t, err)

		t.Run("RegularRequestExpiredAccessTokenWithRefreshTokenWithExpiresInWithIDTokenClaims", func(t *testing.T) {
			// If the access token is expired and a refresh token is
			// available, we may be able to continue the session
			// without sending the user through the authorization
			// endpoint. We use the occasion to update the claims.
			// Upon success, the cookie should be updated. Fetches
			// claims from the ID token instead of the user info
			// endpoint.
			cookieAEAD.EXPECT().Open(
				gomock.Any(),
				[]byte{0x29, 0xc3, 0x4d, 0xf7},
				[]byte{0x8a, 0x16, 0x13, 0x13, 0x7c, 0xc5, 0x7b, 0x5a},
				nil,
			).DoAndReturn(func(dst, nonce, ciphertext, additionalData []byte) ([]byte, error) {
				return append(dst, protoMustMarshal(&oidc.CookieValue{
					SessionState: &oidc.CookieValue_Authenticated_{
						Authenticated: &oidc.CookieValue_Authenticated{
							AuthenticationMetadata: &auth.AuthenticationMetadata{},
							Expiration:             &timestamppb.Timestamp{Seconds: 1693147158},
							RefreshToken:           "RefreshToken1",
							DefaultExpiration:      &durationpb.Duration{Seconds: 60},
						},
					},
				})...), nil
			})
			clock.EXPECT().Now().Return(time.Unix(1693147212, 0))
			roundTripper.EXPECT().RoundTrip(gomock.Any()).DoAndReturn(func(r *http.Request) (*http.Response, error) {
				require.Equal(t, "https://login.com/token", r.URL.String())
				r.ParseForm()
				require.Equal(t, url.Values{
					"client_id":     []string{"MyClientID"},
					"client_secret": []string{"MyClientSecret"},
					"grant_type":    []string{"refresh_token"},
					"refresh_token": []string{"RefreshToken1"},
				}, r.Form)
				return &http.Response{
					Status:     "200 OK",
					StatusCode: 200,
					Body: io.NopCloser(bytes.NewBufferString(`{
						"access_token": "AccessToken2",
						"expires_in": 3600,
						"refresh_token": "RefreshToken2",
						"token_type": "Bearer",
						"id_token": "something.eyJlbWFpbCI6ImpvaG5AbXlzZXJ2ZXIuY29tIiwibmFtZSI6IkpvaG4gRG9lIn0.something"
					}`)),
				}, nil
			})
			clock.EXPECT().Now().Return(time.Unix(1693147213, 0)).Times(1)
			nonce := []byte{0xcf, 0xcc, 0x43, 0xbd}
			expectRead(randomNumberGenerator, nonce)
			expectedAuthenticationMetadata := &auth.AuthenticationMetadata{
				Public: structpb.NewStructValue(&structpb.Struct{
					Fields: map[string]*structpb.Value{
						"email": structpb.NewStringValue("john@myserver.com"),
						"name":  structpb.NewStringValue("John Doe"),
					},
				}),
			}
			cookieAEAD.EXPECT().Seal(
				gomock.Any(),
				nonce,
				protoMustMarshal(&oidc.CookieValue{
					SessionState: &oidc.CookieValue_Authenticated_{
						Authenticated: &oidc.CookieValue_Authenticated{
							AuthenticationMetadata: expectedAuthenticationMetadata,
							Expiration:             &timestamppb.Timestamp{Seconds: 1693150813},
							RefreshToken:           "RefreshToken2",
							DefaultExpiration:      &durationpb.Duration{Seconds: 60},
						},
					},
				}),
				nil,
			).DoAndReturn(func(dst, nonce, plaintext, additionalData []byte) []byte {
				return append(dst, 0xf4, 0xf3, 0xe9, 0xd7, 0x19, 0x29, 0x23, 0x83)
			})

			w := httptest.NewRecorder()
			r, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://myserver.com/hello.png", nil)
			r.AddCookie(&http.Cookie{
				Name:  "CookieName",
				Value: "KcNN94oWExN8xXta",
			})
			require.NoError(t, err)
			metadata, err := authenticator.Authenticate(w, r)
			require.NoError(t, err)
			testutil.RequireEqualProto(t, expectedAuthenticationMetadata, metadata.GetFullProto())

			require.Equal(t, http.Header{
				"Set-Cookie": []string{"CookieName=z8xDvfTz6dcZKSOD; Path=/; HttpOnly; Secure; SameSite=Lax"},
			}, w.HeaderMap)
		})
	})
}
