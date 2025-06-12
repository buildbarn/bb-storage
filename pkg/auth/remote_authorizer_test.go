package auth_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/buildbarn/bb-storage/internal/mock"
	"github.com/buildbarn/bb-storage/pkg/auth"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/eviction"
	auth_pb "github.com/buildbarn/bb-storage/pkg/proto/auth"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestRemoteAuthorizerFailure(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	client := mock.NewMockClientConnInterface(ctrl)
	clock := mock.NewMockClock(ctrl)

	t.Run("BackendFailure", func(t *testing.T) {
		client.EXPECT().Invoke(
			ctx, "/buildbarn.auth.Authorizer/Authorize", gomock.Any(), gomock.Any(), gomock.Any(),
		).Return(status.Error(codes.Unavailable, "Server offline"))
		clock.EXPECT().Now().Return(time.Unix(1000, 0))

		authorizer := auth.NewRemoteAuthorizer(
			client,
			structpb.NewStringValue("auth-scope"),
			clock,
			eviction.NewLRUSet[auth.RemoteAuthorizerCacheKey](),
			100,
		)
		errs := authorizer.Authorize(ctx, []digest.InstanceName{util.Must(digest.NewInstanceName("allowed"))})
		require.Len(t, errs, 1)
		testutil.RequireEqualStatus(
			t,
			status.Error(codes.PermissionDenied, "Remote authorization failed: Server offline"),
			errs[0])
	})

	t.Run("InvalidVerdict", func(t *testing.T) {
		client.EXPECT().Invoke(
			ctx, "/buildbarn.auth.Authorizer/Authorize", gomock.Any(), gomock.Any(), gomock.Any(),
		).DoAndReturn(func(ctx context.Context, method string, args, reply interface{}, opts ...grpc.CallOption) error {
			proto.Merge(reply.(proto.Message), &auth_pb.AuthorizeResponse{})
			return nil
		})
		clock.EXPECT().Now().Return(time.Unix(1000, 0))

		authorizer := auth.NewRemoteAuthorizer(
			client,
			structpb.NewStringValue("auth-scope"),
			clock,
			eviction.NewLRUSet[auth.RemoteAuthorizerCacheKey](),
			100,
		)
		errs := authorizer.Authorize(ctx, []digest.InstanceName{util.Must(digest.NewInstanceName("allowed"))})
		require.Len(t, errs, 1)
		testutil.RequireEqualStatus(
			t,
			status.Error(codes.PermissionDenied, "Invalid authorize verdict"),
			errs[0])
	})
}

func TestRemoteAuthorizerSuccess(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)
	authCtx := auth.NewContextWithAuthenticationMetadata(
		ctx,
		util.Must(auth.NewAuthenticationMetadataFromProto(&auth_pb.AuthenticationMetadata{
			Public: structpb.NewStringValue("I'm here"),
		})),
	)

	remoteService := func(ctx context.Context, method string, args, reply interface{}, opts ...grpc.CallOption) error {
		instanceName := args.(*auth_pb.AuthorizeRequest).InstanceName
		if strings.HasPrefix(instanceName, "allow") {
			proto.Merge(reply.(proto.Message), &auth_pb.AuthorizeResponse{
				Verdict: &auth_pb.AuthorizeResponse_Allow{
					Allow: &emptypb.Empty{},
				},
				CacheExpirationTime: timestamppb.New(time.Unix(1002, 0)),
			})
		} else if strings.HasPrefix(instanceName, "deny") {
			proto.Merge(reply.(proto.Message), &auth_pb.AuthorizeResponse{
				Verdict: &auth_pb.AuthorizeResponse_Deny{
					Deny: instanceName,
				},
				CacheExpirationTime: timestamppb.New(time.Unix(1002, 0)),
			})
		}
		return nil
	}

	testAuthorizeAllow := func(authorizer auth.Authorizer, instanceName string) {
		errs := authorizer.Authorize(ctx, []digest.InstanceName{util.Must(digest.NewInstanceName(instanceName))})
		require.Len(t, errs, 1)
		require.NoError(t, errs[0])
	}
	testAuthorizeDeny := func(authorizer auth.Authorizer, instanceName string) {
		errs := authorizer.Authorize(ctx, []digest.InstanceName{util.Must(digest.NewInstanceName(instanceName))})
		require.Len(t, errs, 1)
		testutil.RequireEqualStatus(
			t,
			status.Error(codes.PermissionDenied, instanceName),
			errs[0])
	}

	t.Run("Success", func(t *testing.T) {
		client := mock.NewMockClientConnInterface(ctrl)
		clock := mock.NewMockClock(ctrl)

		client.EXPECT().Invoke(
			gomock.Any(),
			"/buildbarn.auth.Authorizer/Authorize",
			testutil.EqProto(t, &auth_pb.AuthorizeRequest{
				AuthenticationMetadata: &auth_pb.AuthenticationMetadata{
					Public: structpb.NewStringValue("I'm here"),
				},
				Scope:        structpb.NewStringValue("auth-scope"),
				InstanceName: "deny-success",
			}),
			gomock.Any(),
			gomock.Any(),
		).DoAndReturn(remoteService)
		client.EXPECT().Invoke(
			gomock.Any(),
			"/buildbarn.auth.Authorizer/Authorize",
			testutil.EqProto(t, &auth_pb.AuthorizeRequest{
				AuthenticationMetadata: &auth_pb.AuthenticationMetadata{
					Public: structpb.NewStringValue("I'm here"),
				},
				Scope:        structpb.NewStringValue("auth-scope"),
				InstanceName: "allow-success",
			}),
			gomock.Any(),
			gomock.Any(),
		).DoAndReturn(remoteService)
		clock.EXPECT().Now().Return(time.Unix(1000, 0)).Times(2)

		authorizer := auth.NewRemoteAuthorizer(
			client,
			structpb.NewStringValue("auth-scope"),
			clock,
			eviction.NewLRUSet[auth.RemoteAuthorizerCacheKey](),
			100,
		)
		errs := authorizer.Authorize(authCtx, []digest.InstanceName{
			util.Must(digest.NewInstanceName("deny-success")),
			util.Must(digest.NewInstanceName("allow-success")),
		})
		// The returned errors should be in the same order as the request instance names.
		require.Len(t, errs, 2)
		testutil.RequireEqualStatus(
			t,
			status.Error(codes.PermissionDenied, "deny-success"),
			errs[0])
		require.NoError(t, errs[1])
	})

	t.Run("ExpireResponses", func(t *testing.T) {
		client := mock.NewMockClientConnInterface(ctrl)
		clock := mock.NewMockClock(ctrl)

		authorizer := auth.NewRemoteAuthorizer(
			client,
			structpb.NewStringValue("auth-scope"),
			clock,
			eviction.NewLRUSet[auth.RemoteAuthorizerCacheKey](),
			100,
		)

		// First request should hit the backend.
		// Second request should hit the cache.
		// Third request should hit the backend again, as the cache entry has expired.
		for _, timestamp := range []int64{1000, 1001, 1002} {
			if timestamp != 1001 {
				client.EXPECT().Invoke(
					ctx, "/buildbarn.auth.Authorizer/Authorize", gomock.Any(), gomock.Any(), gomock.Any(),
				).DoAndReturn(remoteService).Times(2) // Times 2 for both allow and deny calls.
			}
			clock.EXPECT().Now().Return(time.Unix(timestamp, 0))
			testAuthorizeAllow(authorizer, "allow")
			clock.EXPECT().Now().Return(time.Unix(timestamp, 0))
			testAuthorizeDeny(authorizer, "deny")
		}
	})

	t.Run("CacheKeyContent", func(t *testing.T) {
		client := mock.NewMockClientConnInterface(ctrl)
		clock := mock.NewMockClock(ctrl)

		clock.EXPECT().Now().Return(time.Unix(1000, 0)).AnyTimes()

		authorizer := auth.NewRemoteAuthorizer(
			client,
			structpb.NewStringValue("auth-scope"),
			clock,
			eviction.NewLRUSet[auth.RemoteAuthorizerCacheKey](),
			100,
		)

		// First call uncached.
		client.EXPECT().Invoke(gomock.Any(), "/buildbarn.auth.Authorizer/Authorize", gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(remoteService)
		errs := authorizer.Authorize(ctx, []digest.InstanceName{util.Must(digest.NewInstanceName("allow"))})
		require.Len(t, errs, 1)
		require.NoError(t, errs[0])
		// Different instanceName, not cached.
		client.EXPECT().Invoke(gomock.Any(), "/buildbarn.auth.Authorizer/Authorize", gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(remoteService)
		errs = authorizer.Authorize(ctx, []digest.InstanceName{util.Must(digest.NewInstanceName("allow2"))})
		require.Len(t, errs, 1)
		require.NoError(t, errs[0])
		// Different authMetadata, not cached.
		client.EXPECT().Invoke(gomock.Any(), "/buildbarn.auth.Authorizer/Authorize", gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(remoteService)
		errs = authorizer.Authorize(authCtx, []digest.InstanceName{util.Must(digest.NewInstanceName("allow"))})
		require.Len(t, errs, 1)
		require.NoError(t, errs[0])
		// Different context instance, should be cached.
		ctxOther := context.WithValue(ctx, "unused-key", "value")
		errs = authorizer.Authorize(ctxOther, []digest.InstanceName{util.Must(digest.NewInstanceName("allow"))})
		require.Len(t, errs, 1)
		require.NoError(t, errs[0])
	})

	t.Run("MaxCacheSizeAndLRUEviction", func(t *testing.T) {
		client := mock.NewMockClientConnInterface(ctrl)
		clock := mock.NewMockClock(ctrl)

		clock.EXPECT().Now().Return(time.Unix(1000, 0)).AnyTimes()

		authorizer := auth.NewRemoteAuthorizer(
			client,
			structpb.NewStringValue("auth-scope"),
			clock,
			eviction.NewLRUSet[auth.RemoteAuthorizerCacheKey](),
			2, // Only two spaces in this test.
		)

		// 1 uncached.
		client.EXPECT().Invoke(gomock.Any(), "/buildbarn.auth.Authorizer/Authorize", gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(remoteService)
		testAuthorizeAllow(authorizer, "allow1")
		// 2 uncached.
		client.EXPECT().Invoke(gomock.Any(), "/buildbarn.auth.Authorizer/Authorize", gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(remoteService)
		testAuthorizeDeny(authorizer, "deny2")
		// 1 should be cached.
		testAuthorizeAllow(authorizer, "allow1")
		// 3 uncached, replacing 2.
		client.EXPECT().Invoke(gomock.Any(), "/buildbarn.auth.Authorizer/Authorize", gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(remoteService)
		testAuthorizeAllow(authorizer, "allow3")
		// 1 should still be cached.
		testAuthorizeAllow(authorizer, "allow1")
		// 2 should have been evicted.
		client.EXPECT().Invoke(gomock.Any(), "/buildbarn.auth.Authorizer/Authorize", gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(remoteService)
		testAuthorizeDeny(authorizer, "deny2")
	})

	t.Run("DeduplicateCalls", func(t *testing.T) {
		client := mock.NewMockClientConnInterface(ctrl)
		clock := mock.NewMockClock(ctrl)

		authCalled := make(chan struct{})
		authRelease := make(map[string]chan struct{})

		client.EXPECT().Invoke(
			ctx, "/buildbarn.auth.Authorizer/Authorize", gomock.Any(), gomock.Any(), gomock.Any(),
		).DoAndReturn(func(ctx context.Context, method string, args, reply interface{}, opts ...grpc.CallOption) error {
			name := args.(*auth_pb.AuthorizeRequest).InstanceName
			proto.Merge(reply.(proto.Message), &auth_pb.AuthorizeResponse{})
			authCalled <- struct{}{}
			<-authRelease[name]
			return nil
		}).Times(2) // name1 and name2

		clock.EXPECT().Now().Return(time.Unix(1000, 0)).AnyTimes()

		authorizer := auth.NewRemoteAuthorizer(
			client,
			structpb.NewStringValue("auth-scope"),
			clock,
			eviction.NewLRUSet[auth.RemoteAuthorizerCacheKey](),
			100,
		)
		doAuth := func(name string, done chan<- struct{}) {
			errs := authorizer.Authorize(ctx, []digest.InstanceName{util.Must(digest.NewInstanceName(name))})
			require.Len(t, errs, 1)
			defer close(done)
			testutil.RequireEqualStatus(
				t,
				status.Error(codes.PermissionDenied, "Invalid authorize verdict"),
				errs[0])
		}
		authRelease["name1"] = make(chan struct{})
		authRelease["name2"] = make(chan struct{})
		done1a := make(chan struct{})
		done1b := make(chan struct{})
		done1c := make(chan struct{})
		done2 := make(chan struct{})
		go doAuth("name1", done1a)
		<-authCalled
		go doAuth("name2", done2)
		<-authCalled
		go doAuth("name1", done1b)
		ctx1c, cancel1c := context.WithCancel(ctx)
		go func() {
			errs := authorizer.Authorize(ctx1c, []digest.InstanceName{util.Must(digest.NewInstanceName("name1"))})
			require.Len(t, errs, 1)
			testutil.RequireEqualStatus(
				t,
				status.Error(codes.Canceled, "context canceled"),
				errs[0])
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
		close(authRelease["name2"])
		// name1 still blocked.
		time.Sleep(100 * time.Millisecond)
		select {
		case <-done1a:
			t.Error("done1a too early")
		case <-done1b:
			t.Error("done1b too early")
		case <-done2:
			// Noop.
		}
		close(authRelease["name1"])
		<-done1a
		<-done1b
	})

	t.Run("SkipDeduplicateErrors", func(t *testing.T) {
		client := mock.NewMockClientConnInterface(ctrl)
		clock := mock.NewMockClock(ctrl)

		authCalled := make(chan struct{})
		authRelease := make(map[string]chan struct{})

		client.EXPECT().Invoke(
			ctx, "/buildbarn.auth.Authorizer/Authorize", gomock.Any(), gomock.Any(), gomock.Any(),
		).DoAndReturn(func(ctx context.Context, method string, args, reply interface{}, opts ...grpc.CallOption) error {
			name := args.(*auth_pb.AuthorizeRequest).InstanceName
			proto.Merge(reply.(proto.Message), &auth_pb.AuthorizeResponse{})
			authCalled <- struct{}{}
			<-authRelease[name]
			return status.Error(codes.DataLoss, "Data loss")
		})

		client.EXPECT().Invoke(
			ctx, "/buildbarn.auth.Authorizer/Authorize", gomock.Any(), gomock.Any(), gomock.Any(),
		).DoAndReturn(func(ctx context.Context, method string, args, reply interface{}, opts ...grpc.CallOption) error {
			name := args.(*auth_pb.AuthorizeRequest).InstanceName
			proto.Merge(reply.(proto.Message), &auth_pb.AuthorizeResponse{})
			authCalled <- struct{}{}
			<-authRelease[name]
			return nil
		})

		clock.EXPECT().Now().Return(time.Unix(1000, 0)).AnyTimes()

		authorizer := auth.NewRemoteAuthorizer(
			client,
			structpb.NewStringValue("auth-scope"),
			clock,
			eviction.NewLRUSet[auth.RemoteAuthorizerCacheKey](),
			100,
		)
		doAuth := func(name string, done chan<- struct{}, verdict string) {
			errs := authorizer.Authorize(ctx, []digest.InstanceName{util.Must(digest.NewInstanceName(name))})
			require.Len(t, errs, 1)
			defer close(done)
			testutil.RequireEqualStatus(
				t,
				status.Error(codes.PermissionDenied, verdict),
				errs[0])
		}

		authRelease["token1"] = make(chan struct{})
		done1a := make(chan struct{})
		done1b := make(chan struct{})
		done1c := make(chan struct{})
		go doAuth("token1", done1a, "Remote authorization failed: Data loss")
		<-authCalled // token1a
		go doAuth("token1", done1b, "Invalid authorize verdict")
		go doAuth("token1", done1c, "Invalid authorize verdict")
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
