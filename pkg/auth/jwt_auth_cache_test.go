package auth_test

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"github.com/buildbarn/bb-storage/pkg/auth"
	"github.com/dgrijalva/jwt-go"
	"github.com/grpc-ecosystem/go-grpc-middleware/util/metautils"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func generateContext(secret []byte, exp time.Time) context.Context {
	ctx := context.Background()
	md := metautils.ExtractIncoming(ctx)
	token := jwt.NewWithClaims(
		jwt.SigningMethodHS256,
		jwt.MapClaims{
			"iat": time.Now().UTC().Unix(),
			"exp": exp.UTC().Unix()})
	rawToken, _ := token.SignedString(secret)
	md.Set("authorization", strings.Join([]string{"Bearer", rawToken}, " "))
	return md.ToIncoming(ctx)
}

func TestJWTAuthCacheNoCredentials(t *testing.T) {
	secret := []byte("none")

	authCache := auth.NewJWTAuthCache(secret, 0)
	_, err := authCache.ValidateCredentials(context.Background())
	require.Equal(t, err, status.Error(codes.Unauthenticated, "Request unauthenticated with bearer"))
}

func TestJWTAuthCacheValidCredentials(t *testing.T) {
	exp := time.Date(2970, 1, 1, 1, 1, 0, 0, time.UTC)
	secret := []byte("the-256-bit-secret")
	ctx := generateContext(secret, exp)

	authCache := auth.NewJWTAuthCache(secret, 1)
	_, err := authCache.ValidateCredentials(ctx)
	require.NoError(t, err)
}

func TestJWTAuthCacheInvalidCredentials(t *testing.T) {
	exp := time.Date(2970, 1, 1, 1, 1, 0, 0, time.UTC)
	secret := []byte("the-256-bit-secret")
	ctx := generateContext(bytes.ToUpper(secret), exp)

	authCache := auth.NewJWTAuthCache(secret, 1)
	_, err := authCache.ValidateCredentials(ctx)
	require.Error(t, err)
	_, ok := err.(*jwt.ValidationError)
	require.True(t, ok)
}

func TestJWTAuthCacheExpiredCredentials(t *testing.T) {
	exp := time.Date(1970, 1, 1, 1, 1, 0, 0, time.UTC)
	secret := []byte("the-256-bit-secret")
	ctx := generateContext(secret, exp)

	authCache := auth.NewJWTAuthCache(secret, 1)
	_, err := authCache.ValidateCredentials(ctx)
	require.Error(t, err)
	_, ok := err.(*jwt.ValidationError)
	require.True(t, ok)
}

// TODO: Add a test exercising the caching mechanism:
// https://github.com/buildbarn/bb-storage/issues/7
