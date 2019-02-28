package auth

import (
	"context"
	"sync"
	"time"

	"github.com/dgrijalva/jwt-go"
	"github.com/grpc-ecosystem/go-grpc-middleware/auth"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type jwtAuthCache struct {
	jwtSecret  []byte

	lock       sync.Mutex
	tokenCache map[string]time.Time
	cacheOrder []string
	cacheSize  int
}

// NewJWTAuthCache creates an AuthCache that implements bearer style
// JWT based authorization validation.
func NewJWTAuthCache(secret []byte, maxCacheSize int) *jwtAuthCache {
	return &jwtAuthCache{
		jwtSecret:    secret,
		tokenCache:   make(map[string]time.Time),
		cacheSize:    maxCacheSize,
	}
}

func (jac *jwtAuthCache) ValidateCredentials(ctx context.Context) (context.Context, error) {
	rawToken, err := grpc_auth.AuthFromMD(ctx, "bearer")
	if err != nil {
		return nil, err
	}
	// Fast-path: valid cached and non expired token
	jac.lock.Lock()
	exp, ok := jac.tokenCache[rawToken]
	jac.lock.Unlock()
	if ok && exp.After(time.Now().UTC()) {
		return ctx, nil
	}
	claims := &jwt.StandardClaims{}
	token, err := jwt.ParseWithClaims(rawToken, claims, func(token *jwt.Token) (interface{}, error) {
		return jac.jwtSecret, nil
	})
	if err != nil {
		return nil, err
	} else if !token.Valid {
		return nil, status.Errorf(codes.PermissionDenied, "Invalid authorization token")
	}
	// Update the token cache before returning
	jac.lock.Lock()
	if len(jac.cacheOrder) >= jac.cacheSize {
		oldestToken := jac.cacheOrder[0]
		jac.cacheOrder = jac.cacheOrder[1:]
		delete(jac.tokenCache, oldestToken)
	}
	jac.tokenCache[rawToken] = time.Unix(claims.ExpiresAt, 0)
	jac.cacheOrder = append(jac.cacheOrder, rawToken)
	jac.lock.Unlock()
	return ctx, nil
}
