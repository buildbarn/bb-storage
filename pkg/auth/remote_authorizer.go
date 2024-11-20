package auth

import (
	"context"
	"crypto/sha256"
	"sync"
	"time"

	"github.com/buildbarn/bb-storage/pkg/clock"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/eviction"
	auth_pb "github.com/buildbarn/bb-storage/pkg/proto/auth"
	"github.com/buildbarn/bb-storage/pkg/util"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
)

type remoteAuthorizer struct {
	remoteAuthClient auth_pb.AuthorizerClient
	scope            *structpb.Value

	clock            clock.Clock
	maximumCacheSize int

	lock sync.Mutex
	// The channel is closed when the response is ready. If the response is
	// missing from cachedResponses, the request failed and should be retried.
	pendingRequests map[RemoteAuthorizerCacheKey]<-chan struct{}
	cachedResponses map[RemoteAuthorizerCacheKey]remoteAuthorizerResponse
	evictionSet     eviction.Set[RemoteAuthorizerCacheKey]
}

// RemoteAuthorizerCacheKey is the key type for the cache inside
// remoteAuthorizer.
type RemoteAuthorizerCacheKey [sha256.Size]byte

type remoteAuthorizerResponse struct {
	expirationTime time.Time
	err            error
}

// NewRemoteAuthorizer creates a new Authorizer which asks a remote gRPC
// service for authorize response. The result from the remote service is
// cached.
func NewRemoteAuthorizer(
	client grpc.ClientConnInterface,
	scope *structpb.Value,
	clock clock.Clock,
	evictionSet eviction.Set[RemoteAuthorizerCacheKey],
	maximumCacheSize int,
) Authorizer {
	return &remoteAuthorizer{
		remoteAuthClient: auth_pb.NewAuthorizerClient(client),
		scope:            scope,

		clock:            clock,
		maximumCacheSize: maximumCacheSize,

		pendingRequests: make(map[RemoteAuthorizerCacheKey]<-chan struct{}),
		cachedResponses: make(map[RemoteAuthorizerCacheKey]remoteAuthorizerResponse),
		evictionSet:     evictionSet,
	}
}

func (a *remoteAuthorizer) Authorize(ctx context.Context, instanceNames []digest.InstanceName) []error {
	errs := make([]error, 0, len(instanceNames))
	for _, instanceName := range instanceNames {
		errs = append(errs, a.authorizeSingle(ctx, instanceName))
	}
	return errs
}

func (a *remoteAuthorizer) authorizeSingle(ctx context.Context, instanceName digest.InstanceName) error {
	authenticationMetadata := AuthenticationMetadataFromContext(ctx)
	request := &auth_pb.AuthorizeRequest{
		AuthenticationMetadata: authenticationMetadata.GetFullProto(),
		Scope:                  a.scope,
		InstanceName:           instanceName.String(),
	}
	if a.maximumCacheSize == 0 {
		// Cache disabled, skip request deduplication.
		response, err := a.authorizeRemotely(ctx, request)
		if err != nil {
			return err
		}
		return response.err
	}

	requestBytes, err := proto.Marshal(request)
	if err != nil {
		return util.StatusWrapWithCode(err, codes.PermissionDenied, "Failed to marshal authorize request")
	}
	// Hash the request to use as a cache key to save memory.
	requestKey := sha256.Sum256(requestBytes)

	for {
		now := a.clock.Now()
		a.lock.Lock()
		if response, ok := a.getAndTouchCacheEntry(requestKey); ok && response.expirationTime.After(now) {
			a.lock.Unlock()
			return response.err
		}
		// No valid cache entry available. Deduplicate requests.
		responseReadyChan, ok := a.pendingRequests[requestKey]
		if !ok {
			// No pending request. Create one.
			responseReadyChan := make(chan struct{})
			a.pendingRequests[requestKey] = responseReadyChan
			a.lock.Unlock()
			// Perform the remote authentication request.
			response, err := a.authorizeRemotely(ctx, request)
			a.lock.Lock()
			delete(a.pendingRequests, requestKey)
			close(responseReadyChan)
			if err != nil {
				a.lock.Unlock()
				return err
			}
			a.insertCacheEntry(requestKey, response)
			a.lock.Unlock()
			return response.err
		}
		// A remote request is pending, wait for it to finish.
		a.lock.Unlock()
		select {
		case <-ctx.Done():
			return util.StatusFromContext(ctx)
		case <-responseReadyChan:
		}
		// Check whether the remote authentication call succeeded.
		a.lock.Lock()
		response, ok := a.cachedResponses[requestKey]
		a.lock.Unlock()
		if ok {
			// Note that the expiration time is not checked, as the
			// response is as fresh as it can be.
			return response.err
		}
		// The remote authentication call failed. Retry.
	}
}

func (a *remoteAuthorizer) getAndTouchCacheEntry(requestKey RemoteAuthorizerCacheKey) (remoteAuthorizerResponse, bool) {
	if entry, ok := a.cachedResponses[requestKey]; ok {
		// Cache contains a matching entry.
		a.evictionSet.Touch(requestKey)
		return entry, true
	}
	return remoteAuthorizerResponse{}, false
}

func (a *remoteAuthorizer) insertCacheEntry(requestKey RemoteAuthorizerCacheKey, response remoteAuthorizerResponse) {
	if _, ok := a.cachedResponses[requestKey]; ok {
		a.evictionSet.Touch(requestKey)
	} else {
		// Cache contains no matching entry. Free up space, so that the
		// caller may insert a new entry.
		for len(a.cachedResponses) > 0 && len(a.cachedResponses) >= a.maximumCacheSize {
			delete(a.cachedResponses, a.evictionSet.Peek())
			a.evictionSet.Remove()
		}
		a.evictionSet.Insert(requestKey)
	}
	a.cachedResponses[requestKey] = response
}

func (a *remoteAuthorizer) authorizeRemotely(ctx context.Context, request *auth_pb.AuthorizeRequest) (remoteAuthorizerResponse, error) {
	ret := remoteAuthorizerResponse{
		// The default expirationTime has already passed.
		expirationTime: time.Time{},
	}

	response, err := a.remoteAuthClient.Authorize(ctx, request)
	if err != nil {
		return ret, util.StatusWrapWithCode(err, codes.PermissionDenied, "Remote authorization failed")
	}

	// An invalid expiration time indicates that the response should not be cached.
	if response.CacheExpirationTime != nil {
		if err := response.CacheExpirationTime.CheckValid(); err != nil {
			return ret, util.StatusWrapWithCode(err, codes.PermissionDenied, "Invalid authorization expiration time")
		}
		// Note that the expiration time might still be valid for non-allow verdicts.
		ret.expirationTime = response.GetCacheExpirationTime().AsTime()
	}

	switch verdict := response.Verdict.(type) {
	case *auth_pb.AuthorizeResponse_Allow:
		// noop
	case *auth_pb.AuthorizeResponse_Deny:
		ret.err = status.Error(codes.PermissionDenied, verdict.Deny)
	default:
		ret.err = status.Error(codes.PermissionDenied, "Invalid authorize verdict")
	}
	return ret, nil
}
