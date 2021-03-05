package grpc_test

import (
	"context"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"testing"
	"time"

	"github.com/buildbarn/bb-storage/internal/mock"
	bb_grpc "github.com/buildbarn/bb-storage/pkg/grpc"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"gopkg.in/square/go-jose.v2"
	"gopkg.in/square/go-jose.v2/jwt"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const rsaPrivateKey = `
-----BEGIN RSA PRIVATE KEY-----
MIIEogIBAAKCAQEAslWybuiNYR7uOgKuvaBwqVk8saEutKhOAaW+3hWF65gJei+Z
V8QFfYDxs9ZaRZlWAUMtncQPnw7ZQlXO9ogN5cMcN50C6qMOOZzghK7danalhF5l
UETC4Hk3Eisbi/PR3IfVyXaRmqL6X66MKj/JAKyD9NFIDVy52K8A198Jojnrw2+X
XQW72U68fZtvlyl/BTBWQ9Re5JSTpEcVmpCR8FrFc0RPMBm+G5dRs08vvhZNiTT2
JACO5V+J5ZrgP3s5hnGFcQFZgDnXLInDUdoi1MuCjaAU0ta8/08pHMijNix5kFof
dPEB954MiZ9k4kQ5/utt02I9x2ssHqw71ojjvwIDAQABAoIBABrYDYDmXom1BzUS
PE1s/ihvt1QhqA8nmn5i/aUeZkc9XofW7GUqq4zlwPxKEtKRL0IHY7Fw1s0hhhCX
LA0uE7F3OiMg7lR1cOm5NI6kZ83jyCxxrRx1DUSO2nxQotfhPsDMbaDiyS4WxEts
0cp2SYJhdYd/jTH9uDfmt+DGwQN7Jixio1Dj3vwB7krDY+mdre4SFY7Gbk9VxkDg
LgCLMoq52m+wYufP8CTgpKFpMb2/yJrbLhuJxYZrJ3qd/oYo/91k6v7xlBKEOkwD
2veGk9Dqi8YPNxaRktTEjnZb6ybhezat93+VVxq4Oem3wMwou1SfXrSUKtgM/p2H
vfw/76ECgYEA2fNL9tC8u9M0wjA+kvvtDG96qO6O66Hksssy6RWInD+Iqk3MtHQt
LeoCjvX+zERqwOb6SI6empk5pZ9E3/9vJ0dBqkxx3nqn4M/nRWnExGgngJsL959t
f50cdxva8y1RjNhT4kCwTrupX/TP8lAG8SfG1Alo2VFR8iWd8hDQcTECgYEA0Xfj
EgqAsVh4U0s3lFxKjOepEyp0G1Imty5J16SvcOEAD1Mrmz94aSSp0bYhXNVdbf7n
Rk77htWC7SE29fGjOzZRS76wxj/SJHF+rktHB2Zt23k1jBeZ4uLMPMnGLY/BJ099
5DTGo0yU0rrPbyXosx+ukfQLAHFuggX4RNeM5+8CgYB7M1J/hGMLcUpjcs4MXCgV
XXbiw2c6v1r9zmtK4odEe42PZ0cNwpY/XAZyNZAAe7Q0stxL44K4NWEmxC80x7lX
ZKozz96WOpNnO16qGC3IMHAT/JD5Or+04WTT14Ue7UEp8qcIQDTpbJ9DxKk/eglS
jH+SIHeKULOXw7fSu7p4IQKBgBnyVchIUMSnBtCagpn4DKwDjif3nEY+GNmb/D2g
ArNiy5UaYk5qwEmV5ws5GkzbiSU07AUDh5ieHgetk5dHhUayZcOSLWeBRFCLVnvU
i0nZYEZNb1qZGdDG8zGcdNXz9qMd76Qy/WAA/nZT+Zn1AiweAovFxQ8a/etRPf2Z
DbU1AoGAHpCgP7B/4GTBe49H0AQueQHBn4RIkgqMy9xiMeR+U+U0vaY0TlfLhnX+
5PkNfkPXohXlfL7pxwZNYa6FZhCAubzvhKCdUASivkoGaIEk6g1VTVYS/eDVQ4CA
slfl+elXtLq/l1kQ8C14jlHrQzSXx4PQvjDEnAmaHSJNz4mP9Fg=
-----END RSA PRIVATE KEY-----
`

const rsaPublicKey = `
-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAslWybuiNYR7uOgKuvaBw
qVk8saEutKhOAaW+3hWF65gJei+ZV8QFfYDxs9ZaRZlWAUMtncQPnw7ZQlXO9ogN
5cMcN50C6qMOOZzghK7danalhF5lUETC4Hk3Eisbi/PR3IfVyXaRmqL6X66MKj/J
AKyD9NFIDVy52K8A198Jojnrw2+XXQW72U68fZtvlyl/BTBWQ9Re5JSTpEcVmpCR
8FrFc0RPMBm+G5dRs08vvhZNiTT2JACO5V+J5ZrgP3s5hnGFcQFZgDnXLInDUdoi
1MuCjaAU0ta8/08pHMijNix5kFofdPEB954MiZ9k4kQ5/utt02I9x2ssHqw71ojj
vwIDAQAB
-----END PUBLIC KEY-----
`

func getSymmetricKey() []byte {
	return []byte("0123456789ABCDEF")
}

func TestJWTAuthenticator(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)
	defer ctrl.Finish()
	clock := mock.NewMockClock(ctrl)

	symmetricKey := getSymmetricKey()

	jwtKey := bb_grpc.JWTKeyConfig{
		Key: symmetricKey,
	}

	authenticator := bb_grpc.NewJWTAuthenticator(jwtKey, clock)

	t.Run("NoGRPC", func(t *testing.T) {
		// Authenticator is used outside of gRPC, meaning it cannot
		// extract request metadata.
		require.Equal(
			t,
			status.Error(codes.Unauthenticated, "Connection was not established using gRPC"),
			authenticator.Authenticate(ctx))
	})

	t.Run("NoAuthorizationMetadata", func(t *testing.T) {
		// Should deny authentication if no `authorization` header is present.
		md := metadata.MD{}
		require.Equal(
			t,
			status.Error(codes.Unauthenticated, "Missing authorization header"),
			authenticator.Authenticate(metadata.NewIncomingContext(ctx, md)),
		)
	})

	t.Run("HasAuthorizationMetadataKeyButNoValues", func(t *testing.T) {
		// Should deny authentication if `authorization` header is present but has no values.
		md := metadata.MD{
			"authorization": nil,
		}
		require.Equal(
			t,
			status.Error(codes.Unauthenticated, "Missing authorization header"),
			authenticator.Authenticate(metadata.NewIncomingContext(ctx, md)),
		)
	})

	t.Run("ParsesAndValidateValidJWS", func(t *testing.T) {
		// Should parse and validate a valid JWS.
		clock.EXPECT().Now().Return(time.Unix(1600000000, 0))
		token := "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJidWlsZGJhcm4iLCJzdWIiOiJzdWJqZWN0In0.X_Q51pR4gJ-NqVBbDTHUWt7poeCpX4ClFsRWE3sNfXg"
		md := metadata.Pairs("authorization", "Bearer "+token)
		require.NoError(
			t,
			authenticator.Authenticate(metadata.NewIncomingContext(ctx, md)),
		)
	})

	t.Run("RejectsInvalidJWS", func(t *testing.T) {
		// Should reject an invalid JWS.
		invalidPartsSignedToken := `eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJzdWJqZWN0IiwiaXNzIjoiaXNzdWVyIiwic2NvcGVzIjpbInMxIiwiczIiXX0`
		md := metadata.Pairs("authorization", "Bearer "+invalidPartsSignedToken)
		require.Equal(
			t,
			status.Error(codes.Unauthenticated, "Failed to parse signed bearer token: square/go-jose: compact JWS format must have three parts"),
			authenticator.Authenticate(metadata.NewIncomingContext(ctx, md)),
		)
	})

	t.Run("RejectsExpiredJWS", func(t *testing.T) {
		// Should reject an expired JWS.
		clock.EXPECT().Now().Return(time.Unix(1600000000, 0))
		token := "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJleHAiOjE1OTk5OTY0MDAsImlzcyI6ImJ1aWxkYmFybiIsInN1YiI6InN1YmplY3QifQ.tLxc6KRzPI7qL5wCl7eL-iLguinJzKnO11ejBqxsrE4"
		md := metadata.Pairs("authorization", "Bearer "+token)
		require.Equal(
			t,
			status.Error(codes.Unauthenticated, "Authorization required: square/go-jose/jwt: validation failed, token is expired (exp)"),
			authenticator.Authenticate(metadata.NewIncomingContext(ctx, md)),
		)
	})

	t.Run("ParsesAndValidateValidJWSWithPublicKey", func(t *testing.T) {
		input := []byte(rsaPublicKey)
		block, _ := pem.Decode(input)
		if block != nil {
			input = block.Bytes
		}

		rsaPubKey, err := x509.ParsePKIXPublicKey(input)
		require.NoError(t, err)

		jwtKey := bb_grpc.JWTKeyConfig{
			Key: rsaPubKey,
		}

		authenticator := bb_grpc.NewJWTAuthenticator(jwtKey, clock)

		clock.EXPECT().Now().Return(time.Unix(1600000000, 0))
		token := "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJidWlsZGJhcm4iLCJzdWIiOiJzdWJqZWN0In0.JZgfFQW47GWEeN6Esn40gnM4XA4QiWXq8ejAWkPIup5b7K8PGeVjnNfGGZhQsI6xmkKDFO4qbRbnJByhtDCYcPNfNAVz3iA_zXnBXKucY5jFpkhM7mvkr55NVDDkVSkyW5tjz3vTXTUMko5UgPJ5RLlZFrTGRYNRu5h8dxGc5MD5O46bKmQbn9JjV8fT6ngSXjJjC06KMjDLDutYyyTLj2hIeFbeQgIfKClYqmrH47XMSeyOHH1-bCmdWmZVwQhwsjYaGBHy2-SxkwocKOBB9Wo_8lpl301PogX1sqIlIE1bt_aylHZbT27l8gTov51r8qSJzhZ30CSgAWFysH8rHg"

		md := metadata.Pairs("authorization", "Bearer "+token)
		require.NoError(
			t,
			authenticator.Authenticate(metadata.NewIncomingContext(ctx, md)),
		)
	})
}

func decodeRsaPrivateKey(privateKeyData string) *rsa.PrivateKey {
	block, _ := pem.Decode([]byte(privateKeyData))
	if block == nil {
		panic("failed to decode PEM data")
	}
	key, err := x509.ParsePKCS1PrivateKey(block.Bytes)
	if err != nil {
		panic("failed to parse RSA key: " + err.Error())
	}
	return key
}

// Generates the JWT test fixtures used in other tests in this file. Uncomment the
// `t.Skip` call in the test and run the test. It will fail by design. Once it does,
// read the test log and copy the JWT fixtures to the appropriate tests above.
//
// Note: The test is only skipped and not commented out so it will still be compiled
// to avoid bit rot.
func TestGenerateJWTAuthenticatorFixtures(t *testing.T) {
	// Un-skip this test to generate the JWT fixtures.
	t.Skip()

	symmetricKey := getSymmetricKey()

	symmetricKeySigner, err := jose.NewSigner(jose.SigningKey{Algorithm: jose.HS256, Key: symmetricKey}, (&jose.SignerOptions{}).WithType("JWT"))
	require.NoError(t, err)

	// ParsesAndValidateValidJWS
	token, err := jwt.Signed(symmetricKeySigner).
		Claims(&jwt.Claims{
			Issuer:  "buildbarn",
			Subject: "subject",
		}).CompactSerialize()
	require.NoError(t, err, "Error creating JWT.")
	t.Logf("ParsesAndValidateValidJWS: %s\n", token)

	// RejectsExpiredJWS
	token, err = jwt.Signed(symmetricKeySigner).
		Claims(&jwt.Claims{
			Issuer:  "buildbarn",
			Subject: "subject",
			Expiry:  jwt.NewNumericDate(time.Unix(1599996400, 0)),
		}).CompactSerialize()
	require.NoError(t, err, "Error creating JWT.")
	t.Logf("RejectsExpiredJWS: %s\n", token)

	// ParsesAndValidateValidJWSWithPublicKey
	rsaKey := decodeRsaPrivateKey(rsaPrivateKey)
	rsaSigner, err := jose.NewSigner(jose.SigningKey{
		Algorithm: jose.RS256,
		Key:       rsaKey,
	}, (&jose.SignerOptions{}).WithType("JWT"))
	require.NoError(t, err)

	token, err = jwt.Signed(rsaSigner).
		Claims(&jwt.Claims{
			Issuer:  "buildbarn",
			Subject: "subject",
		}).CompactSerialize()
	require.NoError(t, err, "Error creating JWT.")
	t.Logf("ParsesAndValidateValidJWSWithPublicKey: %s\n", token)

	t.Fail()
}
