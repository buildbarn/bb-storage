package auth

import (
	"context"
	"crypto/sha256"
	"sync"
	"time"

	"github.com/buildbarn/bb-storage/pkg/clock"
	"github.com/buildbarn/bb-storage/pkg/eviction"
	auth_pb "github.com/buildbarn/bb-storage/pkg/proto/auth"
	"github.com/buildbarn/bb-storage/pkg/util"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
)

type remoteRequestHeadersAuthenticator struct {
	remoteAuthClient auth_pb.AuthenticationClient
	scope            *structpb.Value

	clock            clock.Clock
	maximumCacheSize int

	lock sync.Mutex
	// The channel is closed when the response is ready. If the response is
	// missing from cachedResponses, the request failed and should be retried.
	pendingRequests map[RemoteRequestHeadersAuthenticatorCacheKey]<-chan struct{}
	cachedResponses map[RemoteRequestHeadersAuthenticatorCacheKey]remoteAuthResponse
	evictionSet     eviction.Set[RemoteRequestHeadersAuthenticatorCacheKey]
}

// RemoteRequestHeadersAuthenticatorCacheKey is the key type for the cache
// inside remoteRequestHeadersAuthenticator.
type RemoteRequestHeadersAuthenticatorCacheKey [sha256.Size]byte

type remoteAuthResponse struct {
	expirationTime time.Time
	authMetadata   *AuthenticationMetadata
	err            error
}

// NewRemoteRequestHeadersAuthenticator creates a new
// RequestHeadersAuthenticator for incoming requests that forwards headers to a
// remote service for authentication. The result from the remote service is
// cached.
func NewRemoteRequestHeadersAuthenticator(
	client grpc.ClientConnInterface,
	scope *structpb.Value,
	clock clock.Clock,
	evictionSet eviction.Set[RemoteRequestHeadersAuthenticatorCacheKey],
	maximumCacheSize int,
) RequestHeadersAuthenticator {
	return &remoteRequestHeadersAuthenticator{
		remoteAuthClient: auth_pb.NewAuthenticationClient(client),
		scope:            scope,

		clock:            clock,
		maximumCacheSize: maximumCacheSize,

		pendingRequests: make(map[RemoteRequestHeadersAuthenticatorCacheKey]<-chan struct{}),
		cachedResponses: make(map[RemoteRequestHeadersAuthenticatorCacheKey]remoteAuthResponse),
		evictionSet:     evictionSet,
	}
}

func (a *remoteRequestHeadersAuthenticator) Authenticate(ctx context.Context, headers map[string][]string) (*AuthenticationMetadata, error) {
	request := &auth_pb.AuthenticateRequest{
		RequestMetadata: make(map[string]*auth_pb.AuthenticateRequest_ValueList, len(headers)),
		Scope:           a.scope,
	}
	for headerKey, headerValues := range headers {
		request.RequestMetadata[headerKey] = &auth_pb.AuthenticateRequest_ValueList{
			Value: headerValues,
		}
	}
	if a.maximumCacheSize == 0 {
		// Cache disabled, skip request deduplication.
		response, err := a.authenticateRemotely(ctx, request)
		if err != nil {
			return nil, err
		}
		return response.authMetadata, response.err
	}

	requestBytes, err := proto.Marshal(request)
	if err != nil {
		return nil, util.StatusWrapWithCode(err, codes.Unauthenticated, "Failed to marshal authenticate request")
	}
	// Hash the request to use as a cache key to both save memory and avoid
	// keeping credentials in the memory.
	requestKey := sha256.Sum256(requestBytes)

	for {
		now := a.clock.Now()
		a.lock.Lock()
		if response, ok := a.getAndTouchCacheEntry(requestKey); ok && response.expirationTime.After(now) {
			a.lock.Unlock()
			return response.authMetadata, response.err
		}
		// No valid cache entry available. Deduplicate requests.
		responseReadyChan, ok := a.pendingRequests[requestKey]
		if !ok {
			// No pending request. Create one.
			responseReadyChan := make(chan struct{})
			a.pendingRequests[requestKey] = responseReadyChan
			a.lock.Unlock()
			// Perform the remote authentication request.
			response, err := a.authenticateRemotely(ctx, request)
			a.lock.Lock()
			delete(a.pendingRequests, requestKey)
			close(responseReadyChan)
			if err != nil {
				a.lock.Unlock()
				return nil, err
			}
			a.insertCacheEntry(requestKey, response)
			a.lock.Unlock()
			return response.authMetadata, response.err
		}
		// A remote request is pending, wait for it to finish.
		a.lock.Unlock()
		select {
		case <-ctx.Done():
			return nil, util.StatusFromContext(ctx)
		case <-responseReadyChan:
		}
		// Check whether the remote authentication call succeeded.
		a.lock.Lock()
		response, ok := a.cachedResponses[requestKey]
		a.lock.Unlock()
		if ok {
			// Note that the expiration time is not checked, as the
			// response is as fresh as it can be.
			return response.authMetadata, response.err
		}
		// The remote authentication call failed. Retry.
	}
}

func (a *remoteRequestHeadersAuthenticator) getAndTouchCacheEntry(requestKey RemoteRequestHeadersAuthenticatorCacheKey) (remoteAuthResponse, bool) {
	if entry, ok := a.cachedResponses[requestKey]; ok {
		// Cache contains a matching entry.
		a.evictionSet.Touch(requestKey)
		return entry, true
	}
	return remoteAuthResponse{}, false
}

func (a *remoteRequestHeadersAuthenticator) insertCacheEntry(requestKey RemoteRequestHeadersAuthenticatorCacheKey, response remoteAuthResponse) {
	if _, ok := a.cachedResponses[requestKey]; ok {
		a.evictionSet.Touch(requestKey)
	} else {
		// Cache contains no matching entry. Free up space, so that the
		// caller may insert a new entry.
		for len(a.cachedResponses) >= a.maximumCacheSize {
			delete(a.cachedResponses, a.evictionSet.Peek())
			a.evictionSet.Remove()
		}
		a.evictionSet.Insert(requestKey)
	}
	a.cachedResponses[requestKey] = response
}

func (a *remoteRequestHeadersAuthenticator) authenticateRemotely(ctx context.Context, request *auth_pb.AuthenticateRequest) (remoteAuthResponse, error) {
	ret := remoteAuthResponse{
		// The default expirationTime has already passed.
		expirationTime: time.Time{},
	}

	response, err := a.remoteAuthClient.Authenticate(ctx, request)
	if err != nil {
		return ret, util.StatusWrapWithCode(err, codes.Unauthenticated, "Remote authentication failed")
	}

	// An invalid expiration time indicates that the response should not be cached.
	if response.CacheExpirationTime != nil {
		if err := response.CacheExpirationTime.CheckValid(); err != nil {
			return ret, util.StatusWrapWithCode(err, codes.Unauthenticated, "Invalid authentication expiration time")
		}
		// Note that the expiration time might still be valid for non-allow verdicts.
		ret.expirationTime = response.CacheExpirationTime.AsTime()
	}

	switch verdict := response.Verdict.(type) {
	case *auth_pb.AuthenticateResponse_Allow:
		ret.authMetadata, err = NewAuthenticationMetadataFromProto(verdict.Allow)
		if err != nil {
			ret.err = util.StatusWrapWithCode(err, codes.Unauthenticated, "Bad authentication response")
		}
	case *auth_pb.AuthenticateResponse_Deny:
		ret.err = status.Error(codes.Unauthenticated, verdict.Deny)
	default:
		ret.err = status.Error(codes.Unauthenticated, "Invalid authentication verdict")
	}
	return ret, nil
}
