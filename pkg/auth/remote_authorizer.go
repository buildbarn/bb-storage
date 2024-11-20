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

	lock            sync.Mutex
	cachedResponses map[RemoteAuthorizerCacheKey]*remoteAuthorizerCacheEntry
	evictionSet     eviction.Set[RemoteAuthorizerCacheKey]
}

// RemoteAuthorizerCacheKey is the key type for the cache inside
// remoteAuthorizer.
type RemoteAuthorizerCacheKey [sha256.Size]byte

type remoteAuthorizerCacheEntry struct {
	ready          <-chan struct{}
	valid          bool
	expirationTime time.Time
	err            error
}

func (ce *remoteAuthorizerCacheEntry) IsReady() bool {
	select {
	case <-ce.ready:
		return true
	default:
		return false
	}
}

// IsValid returns false if a new remote request should be made.
func (ce *remoteAuthorizerCacheEntry) IsValid(now time.Time) bool {
	if !ce.valid {
		// Error response on the remote request, make a new request.
		return false
	}
	return now.Before(ce.expirationTime)
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

		cachedResponses: make(map[RemoteAuthorizerCacheKey]*remoteAuthorizerCacheEntry),
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
	requestBytes, err := proto.Marshal(request)
	if err != nil {
		return util.StatusWrapWithCode(err, codes.PermissionDenied, "Failed to marshal authorize request")
	}
	// Hash the request to use as a cache key to save memory.
	requestKey := sha256.Sum256(requestBytes)

	for {
		a.lock.Lock()
		now := a.clock.Now()
		entry := a.getAndTouchCacheEntry(requestKey)
		if entry == nil || (entry.IsReady() && !entry.IsValid(now)) {
			// No valid cache entry available. Deduplicate requests by creating a
			// pending cached response.
			responseReady := make(chan struct{})
			entry = &remoteAuthorizerCacheEntry{
				ready: responseReady,
			}
			a.cachedResponses[requestKey] = entry
			a.lock.Unlock()

			// Perform the remote authentication request.
			expirationTime, err := a.authorizeRemotely(ctx, request)
			if expirationTime == nil {
				// The response should not be cached.
				entry.valid = false
				close(responseReady)
				return err
			}
			entry.valid = true
			entry.expirationTime = *expirationTime
			entry.err = err
			close(responseReady)
			return entry.err
		}
		a.lock.Unlock()

		// Wait for the remote request to finish.
		select {
		case <-ctx.Done():
			return util.StatusFromContext(ctx)
		case <-entry.ready:
			// Check whether the remote authentication call succeeded, otherwise
			// retry with our own ctx.
			if entry.valid {
				// Note that the expiration time is not checked, as the response
				// is as fresh as it can be.
				return entry.err
			}
		}
	}
}

func (a *remoteAuthorizer) getAndTouchCacheEntry(requestKey RemoteAuthorizerCacheKey) *remoteAuthorizerCacheEntry {
	if entry, ok := a.cachedResponses[requestKey]; ok {
		// Cache contains a matching entry.
		a.evictionSet.Touch(requestKey)
		return entry
	}

	// Cache contains no matching entry. Free up space, so that the
	// caller may insert a new entry.
	for len(a.cachedResponses) >= a.maximumCacheSize {
		delete(a.cachedResponses, a.evictionSet.Peek())
		a.evictionSet.Remove()
	}
	a.evictionSet.Insert(requestKey)
	return nil
}

func (a *remoteAuthorizer) authorizeRemotely(ctx context.Context, request *auth_pb.AuthorizeRequest) (*time.Time, error) {
	// The default expirationTime has already passed.
	expirationTime := time.Time{}

	response, err := a.remoteAuthClient.Authorize(ctx, request)
	if err != nil {
		return nil, util.StatusWrapWithCode(err, codes.PermissionDenied, "Remote authorization failed")
	}

	// An invalid expiration time indicates that the response should not be cached.
	if response.GetCacheExpirationTime().IsValid() {
		// Note that the expiration time might still be valid for non-allow verdicts.
		expirationTime = response.GetCacheExpirationTime().AsTime()
	}

	switch verdict := response.GetVerdict().(type) {
	case *auth_pb.AuthorizeResponse_Allow:
		return &expirationTime, nil
	case *auth_pb.AuthorizeResponse_Deny:
		return &expirationTime, status.Error(codes.PermissionDenied, verdict.Deny)
	default:
		return &expirationTime, status.Error(codes.PermissionDenied, "Invalid authorize verdict")
	}
}
