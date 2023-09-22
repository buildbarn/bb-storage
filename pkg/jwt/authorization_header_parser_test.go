package jwt_test

import (
	"testing"
	"time"

	"github.com/buildbarn/bb-storage/internal/mock"
	"github.com/buildbarn/bb-storage/pkg/eviction"
	"github.com/buildbarn/bb-storage/pkg/jwt"
	"github.com/golang/mock/gomock"
	"github.com/jmespath/go-jmespath"
	"github.com/stretchr/testify/require"
)

func TestAuthorizationHeaderParser(t *testing.T) {
	ctrl := gomock.NewController(t)

	clock := mock.NewMockClock(ctrl)
	signatureValidator := mock.NewMockSignatureValidator(ctrl)
	authenticator := jwt.NewAuthorizationHeaderParser(
		clock,
		signatureValidator,
		jmespath.MustCompile("forbiddenField == null"),
		jmespath.MustCompile("{\"private\": @}"),
		1000,
		eviction.NewLRUSet[string]())

	t.Run("NoAuthorizationHeadersProvided", func(t *testing.T) {
		clock.EXPECT().Now().Return(time.Unix(1635747849, 0))

		_, ok := authenticator.ParseAuthorizationHeaders(nil)
		require.False(t, ok)
	})

	t.Run("InvalidSignature", func(t *testing.T) {
		clock.EXPECT().Now().Return(time.Unix(1635747849, 0))
		signatureValidator.EXPECT().ValidateSignature(
			"HS256",
			"",
			"eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiaWF0IjoxNTE2MjM5MDIyfQ",
			[]byte{
				0x49, 0xf9, 0x4a, 0xc7, 0x04, 0x49, 0x48, 0xc7,
				0x8a, 0x28, 0x5d, 0x90, 0x4f, 0x87, 0xf0, 0xa4,
				0xc7, 0x89, 0x7f, 0x7e, 0x8f, 0x3a, 0x4e, 0xb2,
				0x25, 0x5f, 0xda, 0x75, 0x0b, 0x2c, 0xc3, 0x97,
			},
		).Return(false)

		_, ok := authenticator.ParseAuthorizationHeaders([]string{
			"Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiaWF0IjoxNTE2MjM5MDIyfQ.SflKxwRJSMeKKF2QT4fwpMeJf36POk6yJV_adQssw5c",
		})
		require.False(t, ok)
	})

	t.Run("WithoutTimestamps", func(t *testing.T) {
		// Though it is wise to do so, it is not required that
		// tokens contain timestamps. Check that tokens without
		// any timestamp fields in the payload also validate
		// properly.
		clock.EXPECT().Now().Return(time.Unix(1635781700, 0))
		signatureValidator.EXPECT().ValidateSignature(
			"HS256",
			"",
			"eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiaWF0IjoxNTE2MjM5MDIyfQ",
			[]byte{
				0x69, 0xf2, 0xcf, 0x62, 0xca, 0x9a, 0xa4, 0x3c,
				0x6f, 0xc1, 0xe7, 0x61, 0x35, 0x39, 0xd8, 0xaa,
				0x99, 0x37, 0x62, 0x65, 0xe8, 0xf6, 0xb4, 0x8e,
				0xdb, 0x85, 0x03, 0xc8, 0x2a, 0x24, 0x97, 0xd3,
			},
		).Return(true)

		metadata, ok := authenticator.ParseAuthorizationHeaders([]string{
			"Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiaWF0IjoxNTE2MjM5MDIyfQ.afLPYsqapDxvwedhNTnYqpk3YmXo9rSO24UDyCokl9M",
		})
		require.True(t, ok)
		require.Equal(t, map[string]any{
			"private": map[string]any{
				"sub":  "1234567890",
				"name": "John Doe",
				"iat":  1516239022.0,
			},
		}, metadata.GetRaw())

		// Successive calls for the same token should have cache hits.
		clock.EXPECT().Now().Return(time.Unix(1635781701, 0))

		metadata, ok = authenticator.ParseAuthorizationHeaders([]string{
			"Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiaWF0IjoxNTE2MjM5MDIyfQ.afLPYsqapDxvwedhNTnYqpk3YmXo9rSO24UDyCokl9M",
		})
		require.True(t, ok)
		require.Equal(t, map[string]any{
			"private": map[string]any{
				"sub":  "1234567890",
				"name": "John Doe",
				"iat":  1516239022.0,
			},
		}, metadata.GetRaw())
	})

	t.Run("WithTimestamps", func(t *testing.T) {
		// Provide a token where the "nbf" (Not Before) claim is
		// in the future. Validation should fail.
		clock.EXPECT().Now().Return(time.Unix(1635781778, 0))
		signatureValidator.EXPECT().ValidateSignature(
			"HS256",
			"",
			"eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwibmJmIjoxNjM1NzgxNzgwLCJleHAiOjE2MzU3ODE3OTJ9",
			[]byte{
				0x9a, 0xf0, 0xa6, 0x11, 0xb2, 0x62, 0xcb, 0xec,
				0x48, 0x43, 0x7c, 0xec, 0x21, 0x3a, 0x6a, 0x6e,
				0xd8, 0x57, 0xad, 0x24, 0xe3, 0xb6, 0xea, 0x61,
				0xd5, 0x27, 0x76, 0x28, 0x6b, 0xcc, 0x5e, 0x16,
			},
		).Return(true)

		_, ok := authenticator.ParseAuthorizationHeaders([]string{
			"Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwibmJmIjoxNjM1NzgxNzgwLCJleHAiOjE2MzU3ODE3OTJ9.mvCmEbJiy-xIQ3zsITpqbthXrSTjtuph1Sd2KGvMXhY",
		})
		require.False(t, ok)

		// Successive calls for the same token should be cached.
		clock.EXPECT().Now().Return(time.Unix(1635781779, 0))

		_, ok = authenticator.ParseAuthorizationHeaders([]string{
			"Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwibmJmIjoxNjM1NzgxNzgwLCJleHAiOjE2MzU3ODE3OTJ9.mvCmEbJiy-xIQ3zsITpqbthXrSTjtuph1Sd2KGvMXhY",
		})
		require.False(t, ok)

		// Retry the same request as before, but now let the
		// time be in bounds.
		clock.EXPECT().Now().Return(time.Unix(1635781780, 0))

		metadata, ok := authenticator.ParseAuthorizationHeaders([]string{
			"Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwibmJmIjoxNjM1NzgxNzgwLCJleHAiOjE2MzU3ODE3OTJ9.mvCmEbJiy-xIQ3zsITpqbthXrSTjtuph1Sd2KGvMXhY",
		})
		require.True(t, ok)
		require.Equal(t, map[string]any{
			"private": map[string]any{
				"sub":  "1234567890",
				"name": "John Doe",
				"nbf":  1635781780.0,
				"exp":  1635781792.0,
			},
		}, metadata.GetRaw())

		// Future calls that occur before the expiration time
		// should have cache hits.
		clock.EXPECT().Now().Return(time.Unix(1635781786, 0))

		metadata, ok = authenticator.ParseAuthorizationHeaders([]string{
			"Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwibmJmIjoxNjM1NzgxNzgwLCJleHAiOjE2MzU3ODE3OTJ9.mvCmEbJiy-xIQ3zsITpqbthXrSTjtuph1Sd2KGvMXhY",
		})
		require.True(t, ok)
		require.Equal(t, map[string]any{
			"private": map[string]any{
				"sub":  "1234567890",
				"name": "John Doe",
				"nbf":  1635781780.0,
				"exp":  1635781792.0,
			},
		}, metadata.GetRaw())

		clock.EXPECT().Now().Return(time.Unix(1635781791, 0))

		metadata, ok = authenticator.ParseAuthorizationHeaders([]string{
			"Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwibmJmIjoxNjM1NzgxNzgwLCJleHAiOjE2MzU3ODE3OTJ9.mvCmEbJiy-xIQ3zsITpqbthXrSTjtuph1Sd2KGvMXhY",
		})
		require.True(t, ok)
		require.Equal(t, map[string]any{
			"private": map[string]any{
				"sub":  "1234567890",
				"name": "John Doe",
				"nbf":  1635781780.0,
				"exp":  1635781792.0,
			},
		}, metadata.GetRaw())

		// If the time exceeds the original expiration time, the
		// token should no longer be valid.
		clock.EXPECT().Now().Return(time.Unix(1635781792, 0))

		_, ok = authenticator.ParseAuthorizationHeaders([]string{
			"Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwibmJmIjoxNjM1NzgxNzgwLCJleHAiOjE2MzU3ODE3OTJ9.mvCmEbJiy-xIQ3zsITpqbthXrSTjtuph1Sd2KGvMXhY",
		})
		require.False(t, ok)

		// It is valid to cache the fact that a token has
		// expired, as it can never become valid again.
		clock.EXPECT().Now().Return(time.Unix(1635781793, 0))

		_, ok = authenticator.ParseAuthorizationHeaders([]string{
			"Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwibmJmIjoxNjM1NzgxNzgwLCJleHAiOjE2MzU3ODE3OTJ9.mvCmEbJiy-xIQ3zsITpqbthXrSTjtuph1Sd2KGvMXhY",
		})
		require.False(t, ok)
	})

	t.Run("ClaimValidation", func(t *testing.T) {
		// A token that contains a claim named "forbiddenField"
		// should get rejected, as the JMESPath that was used to
		// construct the AuthorizationHeaderParser rejects it.
		clock.EXPECT().Now().Return(time.Unix(1636144433, 0))
		signatureValidator.EXPECT().ValidateSignature(
			"HS256",
			"",
			"eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJmb3JiaWRkZW5GaWVsZCI6Im9vcHMifQ",
			[]byte{
				0xf1, 0x5c, 0xbc, 0x0c, 0x47, 0x71, 0x2d, 0x88,
				0x42, 0x8a, 0xe3, 0x52, 0x32, 0x77, 0xee, 0xb7,
				0x87, 0x3b, 0x50, 0x99, 0x87, 0x8c, 0x74, 0x16,
				0x7a, 0x77, 0x0d, 0x85, 0xe3, 0xe7, 0x28, 0x7e,
			},
		).Return(true)

		_, ok := authenticator.ParseAuthorizationHeaders([]string{
			"Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJmb3JiaWRkZW5GaWVsZCI6Im9vcHMifQ.8Vy8DEdxLYhCiuNSMnfut4c7UJmHjHQWencNhePnKH4",
		})
		require.False(t, ok)
	})
}
