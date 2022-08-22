package blobstore

import (
	"context"

	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/blobstore/slicing"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type hierarchicalInstanceNamesBlobAccess struct {
	BlobAccess
}

// NewHierarchicalInstanceNamesBlobAccess creates a decorator for
// BlobAccess that falls back to reading objects from parent instance
// names. This can be used to let non-empty instance names inherit their
// contents from parent instance names.
// This BlobAccess reads blobs in descending order of specificity, which is
// useful for the AC because it respects potential overriding, but should not
// be used for the CAS because with the CAS ascending-specificity checks
// are preferred to maximise sharing.
func NewHierarchicalInstanceNamesBlobAccess(base BlobAccess) BlobAccess {
	return &hierarchicalInstanceNamesBlobAccess{
		BlobAccess: base,
	}
}

func (ba *hierarchicalInstanceNamesBlobAccess) Get(ctx context.Context, digest digest.Digest) buffer.Buffer {
	digests := digest.GetDigestsWithParentInstanceNames()
	return buffer.WithErrorHandler(
		ba.BlobAccess.Get(ctx, digests[len(digests)-1]),
		&hierarchicalInstanceNamesGetErrorHandler{
			blobAccess: ba.BlobAccess,
			context:    ctx,
			digests:    digests,
		})
}

func (ba *hierarchicalInstanceNamesBlobAccess) GetFromComposite(ctx context.Context, parentDigest, childDigest digest.Digest, slicer slicing.BlobSlicer) buffer.Buffer {
	parentDigests := parentDigest.GetDigestsWithParentInstanceNames()
	childDigests := childDigest.GetDigestsWithParentInstanceNames()
	return buffer.WithErrorHandler(
		ba.BlobAccess.GetFromComposite(ctx, parentDigests[len(parentDigests)-1], childDigests[len(childDigests)-1], slicer),
		&hierarchicalInstanceNamesGetFromCompositeErrorHandler{
			blobAccess:    ba.BlobAccess,
			context:       ctx,
			parentDigests: parentDigests,
			childDigests:  childDigests,
			slicer:        slicer,
		})
}

func (ba *hierarchicalInstanceNamesBlobAccess) FindMissing(ctx context.Context, digests digest.Set) (digest.Set, error) {
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
	finallyMissing := digest.NewSetBuilder()
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
		directParentDigests := digest.NewSetBuilder()
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

type hierarchicalInstanceNamesGetErrorHandler struct {
	blobAccess BlobAccess
	context    context.Context
	digests    []digest.Digest
}

func (eh *hierarchicalInstanceNamesGetErrorHandler) OnError(err error) (buffer.Buffer, error) {
	if status.Code(err) != codes.NotFound {
		// Serious error. Prepend the instance name, so that
		// errors can be disambiguated.
		return nil, util.StatusWrapf(err, "Instance name %#v", eh.digests[len(eh.digests)-1].GetInstanceName().String())
	}
	if len(eh.digests) == 1 {
		// The object was found in none of the instance names.
		// There is no need to prepend the instance name.
		return nil, err
	}
	eh.digests = eh.digests[:len(eh.digests)-1]
	return eh.blobAccess.Get(eh.context, eh.digests[len(eh.digests)-1]), nil
}

func (eh *hierarchicalInstanceNamesGetErrorHandler) Done() {}

type hierarchicalInstanceNamesGetFromCompositeErrorHandler struct {
	blobAccess    BlobAccess
	context       context.Context
	parentDigests []digest.Digest
	childDigests  []digest.Digest
	slicer        slicing.BlobSlicer
}

func (eh *hierarchicalInstanceNamesGetFromCompositeErrorHandler) OnError(err error) (buffer.Buffer, error) {
	if status.Code(err) != codes.NotFound {
		// Serious error. Prepend the instance name, so that
		// errors can be disambiguated.
		return nil, util.StatusWrapf(err, "Instance name %#v", eh.parentDigests[len(eh.parentDigests)-1].GetInstanceName().String())
	}
	if len(eh.parentDigests) == 1 {
		// The object was found in none of the instance names.
		// There is no need to prepend the instance name.
		return nil, err
	}
	eh.parentDigests = eh.parentDigests[:len(eh.parentDigests)-1]
	eh.childDigests = eh.childDigests[:len(eh.childDigests)-1]
	return eh.blobAccess.GetFromComposite(
		eh.context,
		eh.parentDigests[len(eh.parentDigests)-1],
		eh.childDigests[len(eh.childDigests)-1],
		eh.slicer,
	), nil
}

func (eh *hierarchicalInstanceNamesGetFromCompositeErrorHandler) Done() {}
