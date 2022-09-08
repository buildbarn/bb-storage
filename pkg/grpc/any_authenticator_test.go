package grpc_test

import (
	"context"
	"testing"

	"github.com/buildbarn/bb-storage/internal/mock"
	"github.com/buildbarn/bb-storage/pkg/auth"
	bb_grpc "github.com/buildbarn/bb-storage/pkg/grpc"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestAnyAuthenticatorZero(t *testing.T) {
	a := bb_grpc.NewAnyAuthenticator(nil)

	_, err := a.Authenticate(context.Background())
	testutil.RequireEqualStatus(
		t,
		status.Error(codes.Unauthenticated, "No authenticators configured"),
		err)
}

func TestAnyAuthenticatorMultiple(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	m0 := mock.NewMockAuthenticator(ctrl)
	m1 := mock.NewMockAuthenticator(ctrl)
	m2 := mock.NewMockAuthenticator(ctrl)
	a := bb_grpc.NewAnyAuthenticator([]bb_grpc.Authenticator{m0, m1, m2})

	t.Run("Success", func(t *testing.T) {
		// There is no need to check the third authentication
		// backend if the second already returns success.
		m0.EXPECT().Authenticate(ctx).Return(nil, status.Error(codes.Unauthenticated, "No token present"))
		m1.EXPECT().Authenticate(ctx).Return(auth.MustNewAuthenticationMetadata("You're totally who you say you are"), nil)

		metadata, err := a.Authenticate(ctx)
		require.NoError(t, err)
		require.Equal(t, "You're totally who you say you are", metadata.GetRaw())
	})

	t.Run("AllUnauthenticated", func(t *testing.T) {
		// A user is unauthenticated if all backends consider it
		// being unauthenticated.
		m0.EXPECT().Authenticate(ctx).Return(nil, status.Error(codes.Unauthenticated, "No TLS used"))
		m1.EXPECT().Authenticate(ctx).Return(nil, status.Error(codes.Unauthenticated, "No token present"))
		m2.EXPECT().Authenticate(ctx).Return(nil, status.Error(codes.Unauthenticated, "Not an internal IP range"))

		_, err := a.Authenticate(ctx)
		testutil.RequireEqualStatus(
			t,
			status.Error(codes.Unauthenticated, "No TLS used, No token present, Not an internal IP range"),
			err)
	})

	t.Run("AllUnauthenticatedIdentical", func(t *testing.T) {
		// Identical error message should be filtered out, so
		// that error messages aren't too spammy.
		m0.EXPECT().Authenticate(ctx).Return(nil, status.Error(codes.Unauthenticated, "No TLS used"))
		m1.EXPECT().Authenticate(ctx).Return(nil, status.Error(codes.Unauthenticated, "No TLS used"))
		m2.EXPECT().Authenticate(ctx).Return(nil, status.Error(codes.Unauthenticated, "No TLS used"))

		_, err := a.Authenticate(ctx)
		testutil.RequireEqualStatus(t, status.Error(codes.Unauthenticated, "No TLS used"), err)
	})

	t.Run("InternalError", func(t *testing.T) {
		// If an internal error occurs, we should return it, as
		// that may be the reason the user cannot be
		// authenticated.
		m0.EXPECT().Authenticate(ctx).Return(nil, status.Error(codes.Unauthenticated, "No TLS used"))
		m1.EXPECT().Authenticate(ctx).Return(nil, status.Error(codes.Internal, "Failed to contact OAuth2 server"))
		m2.EXPECT().Authenticate(ctx).Return(nil, status.Error(codes.Unauthenticated, "Not an internal IP range"))

		_, err := a.Authenticate(ctx)
		testutil.RequireEqualStatus(
			t,
			status.Error(codes.Internal, "Failed to contact OAuth2 server"),
			err)
	})

	t.Run("InternalErrorIgnoredUponSuccess", func(t *testing.T) {
		// An internal error in one backend should not cause
		// requests to be dropped that can be validated through
		// some other backend. This prevents the service from
		// going down entirely.
		m0.EXPECT().Authenticate(ctx).Return(nil, status.Error(codes.Unauthenticated, "No TLS used"))
		m1.EXPECT().Authenticate(ctx).Return(nil, status.Error(codes.Internal, "Failed to contact OAuth2 server"))
		m2.EXPECT().Authenticate(ctx).Return(auth.MustNewAuthenticationMetadata("You're totally who you say you are"), nil)

		metadata, err := a.Authenticate(ctx)
		require.NoError(t, err)
		require.Equal(t, "You're totally who you say you are", metadata.GetRaw())
	})
}
