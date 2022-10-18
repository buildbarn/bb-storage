package capabilities

import (
	"context"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
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
	type result struct {
		capabilities *remoteexecution.ServerCapabilities
		err          error
	}
	results := make([]result, len(p.providers))
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
				results[i] = result{
					capabilities: capabilities,
				}
				return nil
			case codes.InvalidArgument, codes.NotFound, codes.PermissionDenied:
				// Don't terminate if we see these
				// errors, as other subsystems may still
				// report other capabilities.
				results[i] = result{
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

	// Merge results from all providers into a single
	// ServerCapabilities message.
	var capabilities remoteexecution.ServerCapabilities
	for _, result := range results {
		proto.Merge(&capabilities, result.capabilities)
	}
	if capabilities.CacheCapabilities != nil || capabilities.ExecutionCapabilities != nil {
		return &capabilities, nil
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

type emptyProvider struct{}

func (emptyProvider) GetCapabilities(ctx context.Context, instanceName digest.InstanceName) (*remoteexecution.ServerCapabilities, error) {
	return nil, status.Error(codes.NotFound, "No capabilities providers registered")
}
