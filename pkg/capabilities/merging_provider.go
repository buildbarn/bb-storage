package capabilities

import (
	"context"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/bazelbuild/remote-apis/build/bazel/semver"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/util"

	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type mergingProvider struct {
	providers []Provider
}


// NewMergingProvider creates a capabilities provider that merges the
// capabilities reported by multiple backends. It can, for example, be
// used by frontend processes to merge the capabilities reported by
// separate storage cluster and scheduler.
//
// This implementation assumes that backends report non-overlapping
// capabilities.
func NewMergingProvider(providers []Provider) Provider {
	// Only use an actual instance of mergingProvider if multiple
	// backends are provided. This keeps the implementation simple.
	switch len(providers) {
	case 0:
		return emptyProvider{}
	case 1:
		return providers[0]
	default:
		return &mergingProvider{
			providers: providers,
		}
	}
}

func (p *mergingProvider) GetCapabilities(ctx context.Context, instanceName digest.InstanceName) (*remoteexecution.ServerCapabilities, error) {
	// Query all underlying providers in parallel.
	type providerResult struct {
		capabilities *remoteexecution.ServerCapabilities
		err          error
	}

	results := make([]providerResult, len(p.providers))
	group, groupCtx := errgroup.WithContext(ctx)
	for iIter, providerIter := range p.providers {
		i, provider := iIter, providerIter
		group.Go(func() error {
			capabilities, err := provider.GetCapabilities(groupCtx, instanceName)
			switch status.Code(err) {
			case codes.OK:
				// Underlying provider returned
				// CacheCapabilities,
				// ExecutionCapabilities or both.
				results[i] = providerResult{
					capabilities: capabilities,
				}
				return nil
			case codes.InvalidArgument, codes.NotFound, codes.PermissionDenied:
				// Don't terminate if we see these
				// errors, as other subsystems may still
				// report other capabilities.
				results[i] = providerResult{
					capabilities: &remoteexecution.ServerCapabilities{},
					err:          err,
				}
				return nil
			default:
				return err
			}
		})
	}
	if err := group.Wait(); err != nil {
		return nil, err
	}

	// Collect valid capabilities for merging
	validCapabilities := make([]*remoteexecution.ServerCapabilities, 0, len(results))
	for _, result := range results {
		if result.capabilities != nil {
			validCapabilities = append(validCapabilities, result.capabilities)
		}
	}

	// Merge results from all providers into a single
	// ServerCapabilities message with proper API version intersection.
	capabilities := p.mergeCapabilities(validCapabilities)
	if capabilities.CacheCapabilities != nil || capabilities.ExecutionCapabilities != nil {
		return capabilities, nil
	}

	// None of the providers yielded any capabilities. Combine all
	// observed errors.
	errs := make([]error, 0, len(results))
	for _, result := range results {
		if result.err != nil {
			errs = append(errs, result.err)
		}
	}
	return nil, util.StatusFromMultiple(errs)
}

// mergeCapabilities merges capabilities from multiple providers
// with proper API version intersection logic.
func (p *mergingProvider) mergeCapabilities(capabilities []*remoteexecution.ServerCapabilities) *remoteexecution.ServerCapabilities {
	var merged remoteexecution.ServerCapabilities
	var maxLowApiVersion, minHighApiVersion, maxDeprecatedApiVersion *semver.SemVer

	for _, capability := range capabilities {
		if capability == nil {
			continue
		}

		maxLowApiVersion = maxSemanticVersions(maxLowApiVersion, capability.LowApiVersion)
		minHighApiVersion = minSemanticVersions(minHighApiVersion, capability.HighApiVersion)
		maxDeprecatedApiVersion = maxSemanticVersions(maxDeprecatedApiVersion, capability.DeprecatedApiVersion)

		// Null out the API version fields, so we don't pollute the proto merge
		capability.LowApiVersion = nil
		capability.HighApiVersion = nil
		capability.DeprecatedApiVersion = nil

		proto.Merge(&merged, capability)
	}

	// Set final API versions on merged capabilities
	if maxLowApiVersion != nil && minHighApiVersion != nil {
		// Check for valid intersection
		if compareSemanticVersions(maxLowApiVersion, minHighApiVersion) <= 0 {
			// Valid intersection exists
			merged.LowApiVersion = maxLowApiVersion
			merged.HighApiVersion = minHighApiVersion
		}
		// If no valid intersection, leave API versions nil (server.go will set defaults)
	}

	merged.DeprecatedApiVersion = maxDeprecatedApiVersion

	return &merged
}

// minSemanticVersions returns the minimum of two semantic versions.
// Treats nil as plus infinity (so non-nil always wins as minimum).
// Returns nil if both are nil.
func minSemanticVersions(a, b *semver.SemVer) *semver.SemVer {
	if a == nil {
		return b
	}
	if b == nil {
		return a
	}
	if compareSemanticVersions(a, b) <= 0 {
		return a
	}
	return b
}

// maxSemanticVersions returns the maximum of two semantic versions.
// Treats nil as minus infinity (so non-nil always wins as maximum).
// Returns nil if both are nil.
func maxSemanticVersions(a, b *semver.SemVer) *semver.SemVer {
	if a == nil {
		return b
	}
	if b == nil {
		return a
	}
	if compareSemanticVersions(a, b) >= 0 {
		return a
	}
	return b
}

// compareSemanticVersions compares two semantic versions
// Returns: -1 if a < b, 0 if a == b, 1 if a > b
func compareSemanticVersions(a, b *semver.SemVer) int {
	if a.Major != b.Major {
		if a.Major < b.Major {
			return -1
		}
		return 1
	}
	if a.Minor != b.Minor {
		if a.Minor < b.Minor {
			return -1
		}
		return 1
	}
	if a.Patch != b.Patch {
		if a.Patch < b.Patch {
			return -1
		}
		return 1
	}
	return 0
}

type emptyProvider struct{}

func (emptyProvider) GetCapabilities(ctx context.Context, instanceName digest.InstanceName) (*remoteexecution.ServerCapabilities, error) {
	return nil, status.Error(codes.NotFound, "No capabilities providers registered")
}
