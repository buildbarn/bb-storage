package chunklistvalidating

import (
	"context"
	"io"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/util"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type chunkListValidatingBlobAccess struct {
	blobstore.BlobAccess
	contentAddressableStorage blobstore.BlobAccess
	maximumMessageSizeBytes   int
}

// NewChunkListValidatingBlobAccess creates a wrapper around a Chunk
// List Storage (CLS) that ensures only valid chunk lists are stored in
// the CLS. A valid chunk list is a chunk list which follows the
// chunking parameters, has all the chunks present in the Content
// Addressable Storage (CAS) and where the chunks concatenate into the
// appropriate digest.
//
// This validation is fairly expensive and validation should only be
// done at a single layer as close as possible to the CAS where the full
// view of the CAS is available.
func NewChunkListValidatingBlobAccess(chunkListStorage, contentAddressableStorage blobstore.BlobAccess, maximumMessageSizeBytes int) blobstore.BlobAccess {
	return &chunkListValidatingBlobAccess{
		BlobAccess:                chunkListStorage,
		contentAddressableStorage: contentAddressableStorage,
		maximumMessageSizeBytes:   maximumMessageSizeBytes,
	}
}

// Fetch the chunking parameters from the GetCapabilities
// implementation.
func (ba *chunkListValidatingBlobAccess) getValidChunkingParameters(ctx context.Context, instanceName digest.InstanceName) (*remoteexecution.RepMaxCdcParams, error) {
	capabilities, err := ba.BlobAccess.GetCapabilities(ctx, instanceName)
	if err != nil {
		return nil, util.StatusWrap(err, "Unable to GetCapabilities to determine chunking parameters")
	}

	params := capabilities.CacheCapabilities.GetRepMaxCdcParams()
	if params == nil {
		return nil, status.Error(codes.Unimplemented, "This backend only supports upstream servers with RepMaxCDC support")
	}
	if params.MinChunkSizeBytes < 64 {
		return nil, status.Errorf(codes.Internal, "RepMaxCDC minimum chunk size was %d bytes but a minimum of 64 bytes is required", params.MinChunkSizeBytes)
	}

	return params, nil
}

// Check the downstream blob access if this particular blob has already
// been split. If that's the case and all the chunks are still there we
// can return early. In case of errors we will return nil and continue
// with the regular code path.
func (ba *chunkListValidatingBlobAccess) checkSplitResult(ctx context.Context, d digest.Digest) buffer.Buffer {
	b1, b2 := ba.BlobAccess.Get(ctx, d).CloneCopy(ba.maximumMessageSizeBytes)
	responseMsg, err := b1.ToProto(&remoteexecution.SplitBlobResponse{}, ba.maximumMessageSizeBytes)
	if err != nil {
		b2.Discard()
		return nil
	}

	splitBlobResponse := responseMsg.(*remoteexecution.SplitBlobResponse)
	digestFunction := d.GetDigestFunction()
	digestSetBuilder := digest.NewSetBuilder(len(splitBlobResponse.ChunkDigests))
	digestSetBuilder.Add(d)

	for _, chunkDigestProto := range splitBlobResponse.ChunkDigests {
		chunkDigest, err := digestFunction.NewDigestFromProto(chunkDigestProto)
		if err != nil {
			b2.Discard()
			return nil
		}
		digestSetBuilder.Add(chunkDigest)
	}

	missing, err := ba.contentAddressableStorage.FindMissing(ctx, digestSetBuilder.Build())
	if err == nil && missing.Empty() {
		return b2
	}
	b2.Discard()
	return nil
}

// Get returns a valid SplitResult for the given digest chunking the
// blob and storing the chunk list if needed.
func (ba *chunkListValidatingBlobAccess) Get(ctx context.Context, d digest.Digest) buffer.Buffer {
	params, err := ba.getValidChunkingParameters(ctx, d.GetInstanceName())
	if err != nil {
		return buffer.NewBufferFromError(err)
	}

	// Check for the trivial case, the blob is small enough that it will
	// always decompose to a single chunk of the same size as the
	// original blob. We verify the existence of the blob in CAS and
	// break out early.
	blobSize := d.GetSizeBytes()
	if uint64(blobSize) < 2*params.MinChunkSizeBytes {
		missing, err := ba.contentAddressableStorage.FindMissing(ctx, d.ToSingletonSet())
		if err != nil {
			return buffer.NewBufferFromError(util.StatusWrap(err, "Failed to verify blob existence"))
		}
		if !missing.Empty() {
			return buffer.NewBufferFromError(status.Error(codes.NotFound, "Blob not found in CAS"))
		}

		response := &remoteexecution.SplitBlobResponse{
			ChunkDigests:     []*remoteexecution.Digest{d.GetProto()},
			ChunkingFunction: remoteexecution.ChunkingFunction_REP_MAX_CDC,
		}

		return buffer.NewProtoBufferFromProto(response, buffer.UserProvided)
	}

	// Check if we have already computed the result for this blob.
	if result := ba.checkSplitResult(ctx, d); result != nil {
		return result
	}

	// Fallthrough case, compute the chunk list, upload the chunks and
	// store the chunk list.
	blobReader := ba.contentAddressableStorage.Get(ctx, d).ToReader()
	defer blobReader.Close()
	chunker := NewReaderChunker(d.GetDigestFunction(), blobReader, int64(params.MinChunkSizeBytes), int64(params.HorizonSizeBytes))

	chunkDigests := make([]*remoteexecution.Digest, 0, uint64(blobSize)/params.MinChunkSizeBytes+1)

	for {
		chunk, err := chunker.NextChunk()
		if err == io.EOF {
			break
		}
		if err != nil {
			return buffer.NewBufferFromError(err)
		}

		missing, err := ba.contentAddressableStorage.FindMissing(ctx, chunk.Digest.ToSingletonSet())
		if err != nil {
			return buffer.NewBufferFromError(err)
		}
		if !missing.Empty() {
			if err := ba.contentAddressableStorage.Put(ctx, chunk.Digest, buffer.NewValidatedBufferFromByteSlice(chunk.Data)); err != nil {
				return buffer.NewBufferFromError(err)
			}
		}

		chunkDigests = append(chunkDigests, chunk.Digest.GetProto())
	}

	response := &remoteexecution.SplitBlobResponse{
		ChunkDigests:     chunkDigests,
		ChunkingFunction: remoteexecution.ChunkingFunction_REP_MAX_CDC,
	}

	b1, b2 := buffer.NewProtoBufferFromProto(response, buffer.UserProvided).CloneCopy(ba.maximumMessageSizeBytes)

	if err := ba.BlobAccess.Put(ctx, d, b1); err != nil {
		b2.Discard()
		return buffer.NewBufferFromError(util.StatusWrap(err, "Failed to store the split blob response"))
	}

	return b2
}

func (ba *chunkListValidatingBlobAccess) matchesStoredChunkList(ctx context.Context, d digest.Digest, userResponse *remoteexecution.SplitBlobResponse) bool {
	existingMsg, err := ba.BlobAccess.Get(ctx, d).ToProto(&remoteexecution.SplitBlobResponse{}, ba.maximumMessageSizeBytes)
	if err != nil {
		return false
	}

	cachedResponse := existingMsg.(*remoteexecution.SplitBlobResponse)
	if len(cachedResponse.ChunkDigests) == len(userResponse.ChunkDigests) {
		for i, c := range cachedResponse.ChunkDigests {
			u := userResponse.ChunkDigests[i]
			if u.Hash != c.Hash || u.SizeBytes != c.SizeBytes {
				return false
			}
		}
	}

	return true
}

func (ba *chunkListValidatingBlobAccess) Put(ctx context.Context, d digest.Digest, b buffer.Buffer) error {
	// Parse the buffer as a SplitBlobResponse
	msg, err := b.ToProto(&remoteexecution.SplitBlobResponse{}, ba.maximumMessageSizeBytes)
	if err != nil {
		return util.StatusWrap(err, "Failed to parse input as SplitBlobResponse")
	}
	userResponse := msg.(*remoteexecution.SplitBlobResponse)

	digestFunction := d.GetDigestFunction()
	var userChunks []digest.Digest
	digestSetBuilder := digest.NewSetBuilder(len(userResponse.ChunkDigests))
	for _, chunkDigestProto := range userResponse.ChunkDigests {
		chunkDigest, err := digestFunction.NewDigestFromProto(chunkDigestProto)
		if err != nil {
			return status.Errorf(codes.InvalidArgument, "Invalid chunk digest: %v", err)
		}
		digestSetBuilder.Add(chunkDigest)
		userChunks = append(userChunks, chunkDigest)
	}

	// Check that all referenced chunks are present in storage.
	missing, err := ba.contentAddressableStorage.FindMissing(ctx, digestSetBuilder.Build())
	if err != nil {
		return util.StatusWrap(err, "Failed to check existence of chunks")
	}
	if !missing.Empty() {
		return status.Error(codes.NotFound, "At least one chunk in the chunk list was not found")
	}

	// Check the trivial cases without hitting the downstream blob
	// stores.

	// No chunks given, blob must be the empty blob.
	if len(userChunks) == 0 {
		if d.GetSizeBytes() != 0 {
			return status.Error(codes.InvalidArgument, "Chunk list does not compose to blob")
		}
		if d.GetDigestFunction().NewGenerator(0).Sum() != d {
			return status.Error(codes.InvalidArgument, "Chunk list does not compose to blob")
		}
		return nil
	}
	// Single chunk given, the blob must be equal to the chunk. At this
	// point we have already verified the presence of the chunk so we do
	// not have to verify the presence of the blob.
	if len(userChunks) == 1 {
		if d != userChunks[0] {
			return status.Error(codes.InvalidArgument, "Chunk list does not compose to blob")
		}
		return nil
	}

	chunksMatchesStoredLists := ba.matchesStoredChunkList(ctx, d, userResponse)
	missing, err = ba.contentAddressableStorage.FindMissing(ctx, d.ToSingletonSet())
	if err != nil {
		return util.StatusWrap(err, "Failed to check existence of blob")
	}
	blobExistsInCAS := missing.Empty()

	// The request is identical to an already existing chunk list with
	// content we have verified exists in CAS.
	if blobExistsInCAS && chunksMatchesStoredLists {
		return nil
	}

	// No more shortcuts available go through the heavy path of
	// concatenating/verifying and chunking the blobs.
	params, err := ba.getValidChunkingParameters(ctx, d.GetInstanceName())
	if err != nil {
		return err
	}

	reader := &chunkConcatenatingReader{
		ctx:                       ctx,
		contentAddressableStorage: ba.contentAddressableStorage,
		chunkDigests:              userChunks,
	}

	blobBuffer := buffer.NewCASBufferFromReader(d, reader, buffer.UserProvided)
	b1, b2 := blobBuffer.CloneStream()

	// Stream 1: Uploads the blob to CAS.
	group, gCtx := errgroup.WithContext(ctx)
	group.Go(func() error {
		if blobExistsInCAS {
			// Upload unnecessary, blob already exists in CAS.
			b1.Discard()
			return nil
		}
		return ba.contentAddressableStorage.Put(gCtx, d, b1)
	})

	// Stream 2: Chunk the stream to compute the digest and cache the
	// canonical chunks.
	var canonicalChunkDigests []*remoteexecution.Digest
	group.Go(func() error {
		b2Reader := b2.ToReader()
		defer b2Reader.Close()
		chunker := NewReaderChunker(d.GetDigestFunction(), b2Reader, int64(params.MinChunkSizeBytes), int64(params.HorizonSizeBytes))
		for {
			chunk, err := chunker.NextChunk()
			if err == io.EOF {
				return nil
			}
			if err != nil {
				return err
			}

			missing, err := ba.contentAddressableStorage.FindMissing(gCtx, chunk.Digest.ToSingletonSet())
			if err != nil {
				return err
			}
			if !missing.Empty() {
				if err := ba.contentAddressableStorage.Put(gCtx, chunk.Digest, buffer.NewValidatedBufferFromByteSlice(chunk.Data)); err != nil {
					return util.StatusWrap(err, "Failed to save chunk")
				}
			}
			canonicalChunkDigests = append(canonicalChunkDigests, chunk.Digest.GetProto())
		}
	})

	// Wait for the full blob validation and upload to complete.
	if err := group.Wait(); err != nil {
		return util.StatusWrap(err, "Failed to splice the blob")
	}

	// Store the canonical response.
	canonicalResponse := &remoteexecution.SplitBlobResponse{
		ChunkDigests:     canonicalChunkDigests,
		ChunkingFunction: remoteexecution.ChunkingFunction_REP_MAX_CDC,
	}
	canonicalBuffer := buffer.NewProtoBufferFromProto(canonicalResponse, buffer.UserProvided)
	if err := ba.BlobAccess.Put(ctx, d, canonicalBuffer); err != nil {
		return util.StatusWrap(err, "Failed to save canonical chunk list")
	}
	return nil
}

func (ba *chunkListValidatingBlobAccess) findMissingChunks(ctx context.Context, d digest.Digest) (digest.Set, error) {
	splitBlobResponseProto, err := ba.BlobAccess.Get(ctx, d).ToProto(&remoteexecution.SplitBlobResponse{}, ba.maximumMessageSizeBytes)
	if err != nil {
		return digest.EmptySet, err
	}
	splitBlobResponse := splitBlobResponseProto.(*remoteexecution.SplitBlobResponse)
	digestFunction := d.GetDigestFunction()
	builder := digest.NewSetBuilder(len(splitBlobResponse.ChunkDigests))
	for _, chunkDigestProto := range splitBlobResponse.ChunkDigests {
		chunkDigest, err := digestFunction.NewDigestFromProto(chunkDigestProto)
		if err != nil {
			return digest.EmptySet, util.StatusWrapf(err, "Invalid chunk digest %#v", chunkDigestProto)
		}
		builder.Add(chunkDigest)
	}
	return ba.contentAddressableStorage.FindMissing(ctx, builder.Build())
}

func (ba *chunkListValidatingBlobAccess) FindMissing(ctx context.Context, digests digest.Set) (digest.Set, error) {
	missingBlobs, err := ba.BlobAccess.FindMissing(ctx, digests)
	if err != nil {
		return digest.EmptySet, err
	}
	nonMissingBlobs, _, _ := digest.GetDifferenceAndIntersection(digests, missingBlobs)
	missings := make([]digest.Set, 1, 1+nonMissingBlobs.Length())
	missings[0] = missingBlobs
	for _, d := range nonMissingBlobs.Items() {
		missingChunks, err := ba.findMissingChunks(ctx, d)
		if err != nil {
			return digest.EmptySet, err
		}
		if !missingChunks.Empty() {
			missings = append(missings, d.ToSingletonSet())
		}
	}
	return digest.GetUnion(missings), nil
}
