package auth_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/buildbarn/bb-storage/internal/mock"
	"github.com/buildbarn/bb-storage/pkg/auth"
	"github.com/buildbarn/bb-storage/pkg/eviction"
	auth_pb "github.com/buildbarn/bb-storage/pkg/proto/auth"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestRemoteRequestHeadersAuthenticatorFailure(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	t.Run("BackendFailure", func(t *testing.T) {
		client := mock.NewMockClientConnInterface(ctrl)
		clock := mock.NewMockClock(ctrl)

		client.EXPECT().Invoke(
			ctx, "/buildbarn.auth.Authentication/Authenticate", gomock.Any(), gomock.Any(), gomock.Any(),
		).Return(status.Error(codes.Unavailable, "Server offline"))
		clock.EXPECT().Now().Return(time.Unix(1000, 0))

		authenticator := auth.NewRemoteRequestHeadersAuthenticator(
			client,
			structpb.NewStringValue("auth-scope"),
			clock,
			eviction.NewLRUSet[auth.RemoteRequestHeadersAuthenticatorCacheKey](),
			100,
		)
		// authMetadata, err := authenticator.Authenticate(ctx)
		_, err := authenticator.Authenticate(ctx, map[string][]string{"Authorization": {"token", "token2"}})
		testutil.RequireEqualStatus(
			t,
			status.Error(codes.Unauthenticated, "Remote authentication failed: Server offline"),
			err)
	})

	t.Run("InvalidVerdict", func(t *testing.T) {
		client := mock.NewMockClientConnInterface(ctrl)
		clock := mock.NewMockClock(ctrl)

		client.EXPECT().Invoke(
			ctx, "/buildbarn.auth.Authentication/Authenticate", gomock.Any(), gomock.Any(), gomock.Any(),
		).DoAndReturn(func(ctx context.Context, method string, args, reply interface{}, opts ...grpc.CallOption) error {
			proto.Merge(reply.(proto.Message), &auth_pb.AuthenticateResponse{})
			return nil
		})
		clock.EXPECT().Now().Return(time.Unix(1000, 0))

		authenticator := auth.NewRemoteRequestHeadersAuthenticator(
			client,
			structpb.NewStringValue("auth-scope"),
			clock,
			eviction.NewLRUSet[auth.RemoteRequestHeadersAuthenticatorCacheKey](),
			100,
		)
		// authMetadata, err := authenticator.Authenticate(ctx)
		_, err := authenticator.Authenticate(ctx, map[string][]string{"Authorization": {"token", "token2"}})
		testutil.RequireEqualStatus(
			t,
			status.Error(codes.Unauthenticated, "Invalid authentication verdict"),
			err)
	})
}

func TestRemoteRequestHeadersAuthenticatorSuccess(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	remoteService := func(ctx context.Context, method string, args, reply interface{}, opts ...grpc.CallOption) error {
		token := args.(*auth_pb.AuthenticateRequest).RequestMetadata["Authorization"].Value[0]
		if strings.HasPrefix(token, "allow") {
			proto.Merge(reply.(proto.Message), &auth_pb.AuthenticateResponse{
				Verdict: &auth_pb.AuthenticateResponse_Allow{
					Allow: &auth_pb.AuthenticationMetadata{
						Public: structpb.NewStringValue("You're totally who you say you are: " + token),
					},
				},
				CacheExpirationTime: timestamppb.New(time.Unix(1002, 0)),
			})
		} else if strings.HasPrefix(token, "deny") {
			proto.Merge(reply.(proto.Message), &auth_pb.AuthenticateResponse{
				Verdict: &auth_pb.AuthenticateResponse_Deny{
					Deny: "You are an alien: " + token,
				},
				CacheExpirationTime: timestamppb.New(time.Unix(1002, 0)),
			})
		}
		return nil
	}

	authenticateAllowFunc := func(authenticator auth.RequestHeadersAuthenticator, token string) {
		authMetadata, err := authenticator.Authenticate(ctx, map[string][]string{"Authorization": {token}})
		require.NoError(t, err)
		require.Equal(t, map[string]any{
			"public": "You're totally who you say you are: " + token,
		}, authMetadata.GetRaw())
	}
	authenticateDenyFunc := func(authenticator auth.RequestHeadersAuthenticator, token string) {
		_, err := authenticator.Authenticate(ctx, map[string][]string{"Authorization": {token}})
		testutil.RequireEqualStatus(
			t,
			status.Error(codes.Unauthenticated, "You are an alien: "+token),
			err)
	}

	t.Run("SuccessAllow", func(t *testing.T) {
		client := mock.NewMockClientConnInterface(ctrl)
		clock := mock.NewMockClock(ctrl)

		client.EXPECT().Invoke(
			ctx,
			"/buildbarn.auth.Authentication/Authenticate",
			testutil.EqProto(t, &auth_pb.AuthenticateRequest{
				RequestMetadata: map[string]*auth_pb.AuthenticateRequest_ValueList{
					"Authorization": {
						Value: []string{"allow1", "token2"},
					},
				},
				Scope: structpb.NewStringValue("auth-scope"),
			}),
			gomock.Any(),
			gomock.Any(),
		).DoAndReturn(remoteService)
		clock.EXPECT().Now().Return(time.Unix(1000, 0))

		authenticator := auth.NewRemoteRequestHeadersAuthenticator(
			client,
			structpb.NewStringValue("auth-scope"),
			clock,
			eviction.NewLRUSet[auth.RemoteRequestHeadersAuthenticatorCacheKey](),
			100,
		)
		authMetadata, err := authenticator.Authenticate(ctx, map[string][]string{"Authorization": {"allow1", "token2"}})
		require.NoError(t, err)
		require.Equal(t, map[string]any{
			"public": "You're totally who you say you are: allow1",
		}, authMetadata.GetRaw())
	})

	t.Run("SuccessDeny", func(t *testing.T) {
		client := mock.NewMockClientConnInterface(ctrl)
		clock := mock.NewMockClock(ctrl)

		client.EXPECT().Invoke(
			ctx,
			"/buildbarn.auth.Authentication/Authenticate",
			testutil.EqProto(t, &auth_pb.AuthenticateRequest{
				RequestMetadata: map[string]*auth_pb.AuthenticateRequest_ValueList{
					"Authorization": {
						Value: []string{"deny3", "token4"},
					},
				},
				Scope: structpb.NewStringValue("auth-scope"),
			}),
			gomock.Any(),
			gomock.Any(),
		).DoAndReturn(remoteService)
		clock.EXPECT().Now().Return(time.Unix(1000, 0))

		authenticator := auth.NewRemoteRequestHeadersAuthenticator(
			client,
			structpb.NewStringValue("auth-scope"),
			clock,
			eviction.NewLRUSet[auth.RemoteRequestHeadersAuthenticatorCacheKey](),
			100,
		)
		_, err := authenticator.Authenticate(ctx, map[string][]string{"Authorization": {"deny3", "token4"}})
		testutil.RequireEqualStatus(
			t,
			status.Error(codes.Unauthenticated, "You are an alien: deny3"),
			err)
	})

	t.Run("ExpireResponses", func(t *testing.T) {
		client := mock.NewMockClientConnInterface(ctrl)
		clock := mock.NewMockClock(ctrl)

		authenticator := auth.NewRemoteRequestHeadersAuthenticator(
			client,
			structpb.NewStringValue("auth-scope"),
			clock,
			eviction.NewLRUSet[auth.RemoteRequestHeadersAuthenticatorCacheKey](),
			100,
		)

		// First request should hit the backend.
		// Second request should hit the cache.
		// Third request should hit the backend again, as the cache entry has expired.
		for _, timestamp := range []int64{1000, 1001, 1002} {
			if timestamp != 1001 {
				client.EXPECT().Invoke(
					ctx, "/buildbarn.auth.Authentication/Authenticate", gomock.Any(), gomock.Any(), gomock.Any(),
				).DoAndReturn(remoteService).Times(2) // Times 2 for both allow and deny calls.
			}
			clock.EXPECT().Now().Return(time.Unix(timestamp, 0))
			authenticateAllowFunc(authenticator, "allow")
			clock.EXPECT().Now().Return(time.Unix(timestamp, 0))
			authenticateDenyFunc(authenticator, "deny")
		}
	})

	t.Run("MaxCacheSizeAndLRUEviction", func(t *testing.T) {
		client := mock.NewMockClientConnInterface(ctrl)
		clock := mock.NewMockClock(ctrl)

		clock.EXPECT().Now().Return(time.Unix(1000, 0)).AnyTimes()

		authenticator := auth.NewRemoteRequestHeadersAuthenticator(
			client,
			structpb.NewStringValue("auth-scope"),
			clock,
			eviction.NewLRUSet[auth.RemoteRequestHeadersAuthenticatorCacheKey](),
			2, // Only two spaces in this test.
		)

		// 1 uncached.
		client.EXPECT().Invoke(ctx, "/buildbarn.auth.Authentication/Authenticate", gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(remoteService)
		authenticateAllowFunc(authenticator, "allow1")
		// 2 uncached.
		client.EXPECT().Invoke(ctx, "/buildbarn.auth.Authentication/Authenticate", gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(remoteService)
		authenticateDenyFunc(authenticator, "deny2")
		// 1 should be cached.
		authenticateAllowFunc(authenticator, "allow1")
		// 3 uncached, replacing 2.
		client.EXPECT().Invoke(ctx, "/buildbarn.auth.Authentication/Authenticate", gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(remoteService)
		authenticateAllowFunc(authenticator, "allow3")
		// 1 should still be cached.
		authenticateAllowFunc(authenticator, "allow1")
		// 2 should have been evicted.
		client.EXPECT().Invoke(ctx, "/buildbarn.auth.Authentication/Authenticate", gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(remoteService)
		authenticateDenyFunc(authenticator, "deny2")
	})

	t.Run("DeduplicateCalls", func(t *testing.T) {
		client := mock.NewMockClientConnInterface(ctrl)
		clock := mock.NewMockClock(ctrl)

		authCalled := make(chan struct{})
		authRelease := make(map[string]chan struct{})

		client.EXPECT().Invoke(
			ctx, "/buildbarn.auth.Authentication/Authenticate", gomock.Any(), gomock.Any(), gomock.Any(),
		).DoAndReturn(func(ctx context.Context, method string, args, reply interface{}, opts ...grpc.CallOption) error {
			token := args.(*auth_pb.AuthenticateRequest).RequestMetadata["Authorization"].Value[0]
			proto.Merge(reply.(proto.Message), &auth_pb.AuthenticateResponse{})
			authCalled <- struct{}{}
			<-authRelease[token]
			return nil
		}).Times(2) // token1 and token2

		clock.EXPECT().Now().Return(time.Unix(1000, 0)).AnyTimes()

		authenticator := auth.NewRemoteRequestHeadersAuthenticator(
			client,
			structpb.NewStringValue("auth-scope"),
			clock,
			eviction.NewLRUSet[auth.RemoteRequestHeadersAuthenticatorCacheKey](),
			100,
		)
		doAuth := func(token string, done chan<- struct{}) {
			_, err := authenticator.Authenticate(ctx, map[string][]string{"Authorization": {token}})
			testutil.RequireEqualStatus(
				t,
				status.Error(codes.Unauthenticated, "Invalid authentication verdict"),
				err)
			close(done)
		}
		authRelease["token1"] = make(chan struct{})
		authRelease["token2"] = make(chan struct{})
		done1a := make(chan struct{})
		done1b := make(chan struct{})
		done1c := make(chan struct{})
		done2 := make(chan struct{})
		go doAuth("token1", done1a)
		<-authCalled
		go doAuth("token2", done2)
		<-authCalled
		go doAuth("token1", done1b)
		ctx1c, cancel1c := context.WithCancel(ctx)
		go func() {
			_, err := authenticator.Authenticate(ctx1c, map[string][]string{"Authorization": {"token1"}})
			testutil.RequireEqualStatus(
				t,
				status.Error(codes.Canceled, "context canceled"),
				err)
			close(done1c)
		}()
		// Nothing done yet.
		time.Sleep(100 * time.Millisecond)
		select {
		case <-done1a:
			t.Error("done1a too early")
		case <-done1b:
			t.Error("done1b too early")
		case <-done1c:
			t.Error("done1c too early")
		case <-done2:
			t.Error("done2 too early")
		default:
			// Noop.
		}
		cancel1c()
		<-done1c
		close(authRelease["token2"])
		// token1 still blocked.
		time.Sleep(100 * time.Millisecond)
		select {
		case <-done1a:
			t.Error("done1a too early")
		case <-done1b:
			t.Error("done1b too early")
		case <-done2:
			// Noop.
		}
		close(authRelease["token1"])
		<-done1a
		<-done1b
	})

	t.Run("SkipDeduplicateErrors", func(t *testing.T) {
		client := mock.NewMockClientConnInterface(ctrl)
		clock := mock.NewMockClock(ctrl)

		authCalled := make(chan struct{})
		authRelease := make(map[string]chan struct{})

		client.EXPECT().Invoke(
			ctx, "/buildbarn.auth.Authentication/Authenticate", gomock.Any(), gomock.Any(), gomock.Any(),
		).DoAndReturn(func(ctx context.Context, method string, args, reply interface{}, opts ...grpc.CallOption) error {
			token := args.(*auth_pb.AuthenticateRequest).RequestMetadata["Authorization"].Value[0]
			proto.Merge(reply.(proto.Message), &auth_pb.AuthenticateResponse{})
			authCalled <- struct{}{}
			<-authRelease[token]
			return status.Error(codes.DataLoss, "Data loss")
		})
		client.EXPECT().Invoke(
			ctx, "/buildbarn.auth.Authentication/Authenticate", gomock.Any(), gomock.Any(), gomock.Any(),
		).DoAndReturn(func(ctx context.Context, method string, args, reply interface{}, opts ...grpc.CallOption) error {
			token := args.(*auth_pb.AuthenticateRequest).RequestMetadata["Authorization"].Value[0]
			proto.Merge(reply.(proto.Message), &auth_pb.AuthenticateResponse{})
			authCalled <- struct{}{}
			<-authRelease[token]
			return nil
		})

		clock.EXPECT().Now().Return(time.Unix(1000, 0)).AnyTimes()

		authenticator := auth.NewRemoteRequestHeadersAuthenticator(
			client,
			structpb.NewStringValue("auth-scope"),
			clock,
			eviction.NewLRUSet[auth.RemoteRequestHeadersAuthenticatorCacheKey](),
			100,
		)
		doAuth := func(token string, done chan<- struct{}, verdict string) {
			_, err := authenticator.Authenticate(ctx, map[string][]string{"Authorization": {token}})
			defer close(done)
			testutil.RequireEqualStatus(
				t,
				status.Error(codes.Unauthenticated, verdict),
				err)
		}

		authRelease["token1"] = make(chan struct{})
		done1a := make(chan struct{})
		done1b := make(chan struct{})
		done1c := make(chan struct{})
		go doAuth("token1", done1a, "Remote authentication failed: Data loss")
		<-authCalled // token1a
		go doAuth("token1", done1b, "Invalid authentication verdict")
		go doAuth("token1", done1c, "Invalid authentication verdict")
		// Nothing done yet.
		time.Sleep(100 * time.Millisecond)
		select {
		case <-done1a:
			t.Error("done1a too early")
		case <-done1b:
			t.Error("done1b too early")
		case <-done1c:
			t.Error("done1c too early")
		case <-authCalled:
			t.Error("authCalled second time too early")
		default:
			// Noop.
		}
		close(authRelease["token1"])
		// token1 still blocked.
		time.Sleep(100 * time.Millisecond)
		select {
		case <-done1b:
			t.Error("done1b too early")
		case <-done1c:
			t.Error("done1c too early")
		case <-authCalled:
			// token1b released.
			// Noop.
		}
		<-done1b
		<-done1c
	})
}
