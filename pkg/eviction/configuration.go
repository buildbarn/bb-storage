package eviction

import (
	pb "github.com/buildbarn/bb-storage/pkg/proto/configuration/eviction"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// NewSetFromConfiguration creates a new cache replacement set using an
// algorithm specified in a Protobuf enumeration value.
func NewSetFromConfiguration[T comparable](cacheReplacementPolicy pb.CacheReplacementPolicy) (Set[T], error) {
	switch cacheReplacementPolicy {
	case pb.CacheReplacementPolicy_FIRST_IN_FIRST_OUT:
		return NewFIFOSet[T](), nil
	case pb.CacheReplacementPolicy_LEAST_RECENTLY_USED:
		return NewLRUSet[T](), nil
	case pb.CacheReplacementPolicy_RANDOM_REPLACEMENT:
		return NewRRSet[T](), nil
	default:
		return nil, status.Errorf(codes.InvalidArgument, "Unknown cache replacement policy")
	}
}
