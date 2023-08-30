package http

import (
	"bytes"
	"context"
	"crypto/cipher"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"time"

	"github.com/buildbarn/bb-storage/pkg/auth"
	"github.com/buildbarn/bb-storage/pkg/clock"
	"github.com/buildbarn/bb-storage/pkg/proto/http/oidc"
	"github.com/buildbarn/bb-storage/pkg/random"
	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/jmespath/go-jmespath"

	"golang.org/x/oauth2"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type oidcAuthenticator struct {
	oauth2Config          *oauth2.Config
	redirectURLPath       string
	userInfoURL           string
	metadataExtractor     *jmespath.JMESPath
	httpClient            *http.Client
	randomNumberGenerator random.ThreadSafeGenerator
	cookieName            string
	cookieAEAD            cipher.AEAD
	cookieNonceSize       int
	clock                 clock.Clock
}

// NewOIDCAuthenticator creates an Authenticator that enforces that all
// requests are authorized by an OAuth2 server. Authentication metadata
// is constructed by obtaining claims through the OpenID Connect user
// info endpoint, and transforming it using a JMESPath expression.
func NewOIDCAuthenticator(
	oauth2Config *oauth2.Config,
	userInfoURL string,
	metadataExtractor *jmespath.JMESPath,
	httpClient *http.Client,
	randomNumberGenerator random.ThreadSafeGenerator,
	cookieName string,
	cookieAEAD cipher.AEAD,
	clock clock.Clock,
) (Authenticator, error) {
	// Extract the path in the redirect URL of the OAuth2
	// configuration, as we need to match it in incoming HTTP
	// requests.
	redirectURL, err := url.Parse(oauth2Config.RedirectURL)
	if err != nil {
		return nil, util.StatusWrapWithCode(err, codes.InvalidArgument, "Invalid redirect URL")
	}
	return &oidcAuthenticator{
		oauth2Config:          oauth2Config,
		redirectURLPath:       redirectURL.Path,
		userInfoURL:           userInfoURL,
		metadataExtractor:     metadataExtractor,
		httpClient:            httpClient,
		randomNumberGenerator: randomNumberGenerator,
		cookieName:            cookieName,
		cookieAEAD:            cookieAEAD,
		cookieNonceSize:       cookieAEAD.NonceSize(),
		clock:                 clock,
	}, nil
}

func (a *oidcAuthenticator) getCookieValue(r *http.Request) *oidc.CookieValue {
	cookie, err := r.Cookie(a.cookieName)
	if err != nil {
		return nil
	}
	nonceAndCiphertext, err := base64.RawURLEncoding.DecodeString(cookie.Value)
	if err != nil {
		return nil
	}

	// Authenticate and decrypt the cookie.
	if len(nonceAndCiphertext) < a.cookieNonceSize {
		return nil
	}
	plaintext, err := a.cookieAEAD.Open(nil, nonceAndCiphertext[:a.cookieNonceSize], nonceAndCiphertext[a.cookieNonceSize:], nil)
	if err != nil {
		return nil
	}

	var value oidc.CookieValue
	if proto.Unmarshal(plaintext, &value) != nil {
		return nil
	}
	return &value
}

func (a *oidcAuthenticator) setCookieValue(w http.ResponseWriter, cookieValue *oidc.CookieValue) error {
	plaintext, err := proto.MarshalOptions{Deterministic: true}.Marshal(cookieValue)
	if err != nil {
		return err
	}

	// Encrypt the cookie.
	nonce := make([]byte, a.cookieNonceSize)
	if _, err := a.randomNumberGenerator.Read(nonce); err != nil {
		return err
	}
	http.SetCookie(w, &http.Cookie{
		Name:     a.cookieName,
		Value:    base64.RawURLEncoding.EncodeToString(a.cookieAEAD.Seal(nonce, nonce, plaintext, nil)),
		Path:     "/",
		Secure:   true,
		HttpOnly: true,
		SameSite: http.SameSiteStrictMode,
	})
	return nil
}

func (a *oidcAuthenticator) getClaimsAndSetCookie(ctx context.Context, token *oauth2.Token, w http.ResponseWriter) (*auth.AuthenticationMetadata, error) {
	// Obtain claims from the user info endpoint.
	claimsRequest, err := http.NewRequestWithContext(ctx, http.MethodGet, a.userInfoURL, nil)
	if err != nil {
		return nil, util.StatusWrap(err, "Failed to create user info request")
	}
	claimsResponse, err := a.oauth2Config.Client(ctx, token).Do(claimsRequest)
	if err != nil {
		// Strip the user info endpoint URL from the error
		// message, as it's not for the user to see.
		var urlErr *url.Error
		if errors.As(err, &urlErr) {
			err = urlErr.Unwrap()
		}
		return nil, util.StatusWrap(err, "Failed to request claims")
	}
	defer claimsResponse.Body.Close()
	if claimsResponse.StatusCode != 200 {
		return nil, status.Errorf(codes.Unavailable, "Requesting claims failed with HTTP status %#v", claimsResponse.Status)
	}
	var claims interface{}
	err = json.NewDecoder(claimsResponse.Body).Decode(&claims)
	if err != nil {
		return nil, util.StatusWrap(err, "Failed to unmarshal claims")
	}

	// Convert claims to authentication metadata.
	metadataRaw, err := a.metadataExtractor.Search(claims)
	if err != nil {
		return nil, util.StatusWrap(err, "Failed to convert claims to authentication metadata")
	}
	authenticationMetadata, err := auth.NewAuthenticationMetadataFromRaw(metadataRaw)
	if err != nil {
		return nil, util.StatusWrap(err, "Failed to create authentication metadata")
	}

	// Redirect to the originally requested path, with the
	// authentication metadata stored in a cookie.
	if err := a.setCookieValue(w, &oidc.CookieValue{
		SessionState: &oidc.CookieValue_Authenticated_{
			Authenticated: &oidc.CookieValue_Authenticated{
				AuthenticationMetadata: authenticationMetadata.GetFullProto(),
				Expiration:             timestamppb.New(token.Expiry),
				RefreshToken:           token.RefreshToken,
			},
		},
	}); err != nil {
		return nil, util.StatusWrap(err, "Failed to set OIDC session state cookie")
	}
	return authenticationMetadata, nil
}

func (a *oidcAuthenticator) Authenticate(w http.ResponseWriter, r *http.Request) (*auth.AuthenticationMetadata, error) {
	ctx := context.WithValue(r.Context(), oauth2.HTTPClient, a.httpClient)
	if r.URL.Path == a.redirectURLPath {
		// Check whether we have a valid cookie in place that
		// contains the originally requested path.
		cookieValue := a.getCookieValue(r)
		if cookieValue == nil {
			return nil, status.Error(codes.InvalidArgument, "No valid OIDC session state cookie found")
		}
		authenticating := cookieValue.GetAuthenticating()
		if authenticating == nil {
			return nil, status.Error(codes.InvalidArgument, "OIDC session state cookie cannot be used to validate callback state")
		}
		stateVerifier, err := base64.RawURLEncoding.DecodeString(r.FormValue("state"))
		if err != nil {
			return nil, util.StatusWrapWithCode(err, codes.InvalidArgument, "Failed to decode state verifier")
		}
		if !bytes.Equal(stateVerifier, authenticating.StateVerifier) {
			return nil, status.Error(codes.InvalidArgument, "State verifier does not match with OIDC session state cookie")
		}

		// Obtain an access token and refresh token.
		token, err := a.oauth2Config.Exchange(ctx, r.FormValue("code"))
		if err != nil {
			// Strip the token endpoint URL from the error
			// message, as it's not for the user to see.
			var urlErr *url.Error
			if errors.As(err, &urlErr) {
				err = urlErr.Unwrap()
			}
			return nil, util.StatusWrap(err, "Failed to obtain token")
		}

		// Redirect back to the originally requested page.
		if _, err := a.getClaimsAndSetCookie(ctx, token, w); err != nil {
			return nil, err
		}
		http.Redirect(w, r, authenticating.OriginalRequestUri, http.StatusSeeOther)
		return nil, nil
	}

	if cookieValue := a.getCookieValue(r); cookieValue != nil {
		if authenticated := cookieValue.GetAuthenticated(); authenticated != nil {
			// Return the existing authentication metadata
			// if the access token is still valid.
			if a.clock.Now().Before(authenticated.Expiration.AsTime()) {
				if authenticationMetadata, err := auth.NewAuthenticationMetadataFromProto(authenticated.AuthenticationMetadata); err == nil {
					return authenticationMetadata, nil
				}
			}

			// If the access token is expired, attempt to
			// use the refresh token to obtain a new access
			// token. Upon success, permit the request,
			// while updating the cookie.
			if refreshedToken, err := a.oauth2Config.TokenSource(
				ctx,
				&oauth2.Token{
					RefreshToken: authenticated.RefreshToken,
					Expiry:       time.Unix(0, 0),
				},
			).Token(); err == nil {
				if authenticationMetadata, err := a.getClaimsAndSetCookie(ctx, refreshedToken, w); err == nil {
					return authenticationMetadata, nil
				}
			}
		}
	}

	// No valid session present. Redirect to the authorization service.
	var stateVerifier [16]byte
	if _, err := a.randomNumberGenerator.Read(stateVerifier[:]); err != nil {
		return nil, err
	}
	if err := a.setCookieValue(w, &oidc.CookieValue{
		SessionState: &oidc.CookieValue_Authenticating_{
			Authenticating: &oidc.CookieValue_Authenticating{
				StateVerifier:      stateVerifier[:],
				OriginalRequestUri: r.URL.RequestURI(),
			},
		},
	}); err != nil {
		return nil, util.StatusWrap(err, "Failed to set OIDC session state cookie")
	}
	authCodeURL := a.oauth2Config.AuthCodeURL(base64.RawURLEncoding.EncodeToString(stateVerifier[:]))
	http.Redirect(w, r, authCodeURL, http.StatusSeeOther)
	return nil, nil
}
