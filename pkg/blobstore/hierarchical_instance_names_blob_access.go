package blobstore

import (
	"context"
	"slices"

	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type hierarchicalInstanceNamesBlobAccess[T any] struct {
	BlobAccess[T]
}

// NewHierarchicalInstanceNamesBlobAccess creates a decorator for
// BlobAccess that falls back to reading objects from parent instance
// names. This can be used to let non-empty instance names inherit their
// contents from parent instance names.
// This BlobAccess reads blobs in descending order of specificity, which is
// useful for the AC because it respects potential overriding, but should not
// be used for the CAS because with the CAS ascending-specificity checks
// are preferred to maximise sharing.
func NewHierarchicalInstanceNamesBlobAccess[T any](base BlobAccess[T]) BlobAccess[T] {
	return &hierarchicalInstanceNamesBlobAccess[T]{
		BlobAccess: base,
	}
}

func (ba *hierarchicalInstanceNamesBlobAccess[T]) Get(ctx context.Context, digest digest.Digest) (T, error) {
	digests := digest.GetDigestsWithParentInstanceNames()
	var zero, ret T
	var err error
	for _, d := range slices.Backward(digests) {
		ret, err = ba.BlobAccess.Get(ctx, d)
		if err == nil {
			return ret, nil
		}
		if status.Code(err) != codes.NotFound {
			// Serious error. Prepend the instance name, so that errors
			// can be disambiguated.
			return zero, util.StatusWrapf(err, "Instance name %#v", d.GetInstanceName().String())
		}
	}
	// The object was found in none of the instance names. There is no
	// need to prepend the instance name.
	return zero, err
}

func (ba *hierarchicalInstanceNamesBlobAccess[T]) FindMissing(ctx context.Context, digests digest.Set) (digest.Set, error) {
	// Skip as much work as possible during the initial phase by
	// just requesting the original set of digests. This keeps the
	// overhead of workloads that don't actually use instance names
	// hierarchically fast.
	initiallyMissing, err := ba.BlobAccess.FindMissing(ctx, digests)
	if err != nil {
		return digest.EmptySet, err
	}

	// Place all initially missing objects in a list, together with
	// all of the parent digests. These are the ones that need to be
	// checked using successive FindMissing() calls.
	initiallyMissingItems := initiallyMissing.Items()
	type digestWithParents struct {
		originalDigest digest.Digest
		parentDigests  []digest.Digest
	}
	digestsWithParents := make([]digestWithParents, 0, len(initiallyMissingItems))
	finallyMissing := digest.NewSetBuilder(0)
	for _, originalDigest := range initiallyMissingItems {
		if parentDigests := originalDigest.GetDigestsWithParentInstanceNames(); len(parentDigests) > 1 {
			digestsWithParents = append(digestsWithParents, digestWithParents{
				originalDigest: originalDigest,
				parentDigests:  parentDigests[:len(parentDigests)-1],
			})
		} else {
			finallyMissing.Add(originalDigest)
		}
	}

	for len(digestsWithParents) > 0 {
		// Call FindMissing() on the set of all parents of
		// digests checked during the previous iteration.
		// Convert the results to a set, so that we can
		// efficiently check membership.
		directParentDigests := digest.NewSetBuilder(len(digestsWithParents))
		for _, digestWithParents := range digestsWithParents {
			directParentDigests.Add(digestWithParents.parentDigests[len(digestWithParents.parentDigests)-1])
		}
		missing, err := ba.BlobAccess.FindMissing(ctx, directParentDigests.Build())
		if err != nil {
			return digest.EmptySet, err
		}
		missingItems := missing.Items()
		missingSet := make(map[digest.Digest]struct{}, len(missingItems))
		for _, digest := range missingItems {
			missingSet[digest] = struct{}{}
		}

		// Scan through the list of digests that still need to
		// be checked, pruning objects that were present or
		// exhausted.
		for i := 0; i < len(digestsWithParents); {
			digestWithParents := &digestsWithParents[i]
			if _, ok := missingSet[digestWithParents.parentDigests[len(digestWithParents.parentDigests)-1]]; !ok {
				// Object was found. We can stop
				// searching for this specific object.
				*digestWithParents = digestsWithParents[len(digestsWithParents)-1]
				digestsWithParents = digestsWithParents[:len(digestsWithParents)-1]
			} else if parentDigests := &digestWithParents.parentDigests; len(*parentDigests) > 1 {
				// Object was not found, but there are
				// more parent digests for us to consider.
				*parentDigests = (*parentDigests)[:len(*parentDigests)-1]
				i++
			} else {
				// Object was not found, but no parent
				// digests remain. It is truly missing.
				finallyMissing.Add(digestWithParents.originalDigest)
				*digestWithParents = digestsWithParents[len(digestsWithParents)-1]
				digestsWithParents = digestsWithParents[:len(digestsWithParents)-1]
			}
		}
	}
	return finallyMissing.Build(), nil
}
