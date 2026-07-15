package cdc

import (
	"context"
	"io"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/blobstore/slicing"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/util"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type casChunking struct {
	chunkStorage            blobstore.BlobAccess
	chunkGetter             buffer.ChunkGetter
	chunkListStorage        blobstore.BlobAccess
	maximumMessageSizeBytes int
}

// NewCasChunkingBlobAccess creates a cas blob access configuration that
// constructs large case objects from the chunks described in the chunk
// list.
func NewCasChunkingBlobAccess(chunkStorage, chunkListStorage blobstore.BlobAccess, maximumMessageSizeBytes int) blobstore.BlobAccess {
	return &casChunking{
		chunkStorage:            chunkStorage,
		chunkListStorage:        chunkListStorage,
		maximumMessageSizeBytes: maximumMessageSizeBytes,
		chunkGetter:             chunkStorage.Get,
	}
}

func (bc *casChunking) FindMissing(ctx context.Context, digests digest.Set) (digest.Set, error) {
	digestSets := digests.PartitionByInstanceName()
	missings := make([]digest.Set, 0, len(digestSets))
	for _, digestSet := range digestSets {
		// PartitionByInstanceNames guarantees non empty sets.
		missing, err := bc.findMissingFromInstance(ctx, digestSet.Items()[0].GetInstanceName(), digestSet)
		if err != nil {
			return digest.EmptySet, err
		}
		missings = append(missings, missing)
	}
	return digest.GetUnion(missings), nil
}

func (bc *casChunking) Get(ctx context.Context, d digest.Digest) buffer.Buffer {
	params, err := GetCDCParameters(ctx, bc.chunkListStorage, d.GetInstanceName())
	if err != nil {
		return buffer.NewBufferFromError(err)
	}

	if d.GetSizeBytes() < int64(2*params.MinChunkSizeBytes) {
		return bc.chunkStorage.Get(ctx, d)
	}

	chunkDigests, err := bc.chunksOfBlob(ctx, d, params)
	if err != nil {
		return buffer.NewBufferFromError(err)
	}
	return buffer.NewValidatedCASChunkConcatenatingBuffer(ctx, d, chunkDigests, bc.chunkStorage.Get, buffer.UserProvided)
}

func (casChunking) GetCapabilities(ctx context.Context, instanceName digest.InstanceName) (*remoteexecution.ServerCapabilities, error) {
	return nil, status.Error(codes.Unimplemented, "CasChunkingBlobAccess does not implement GetCapabilities")
}

func (casChunking) GetFromComposite(ctx context.Context, parentDigest, childDigest digest.Digest, slicer slicing.BlobSlicer) buffer.Buffer {
	return buffer.NewBufferFromError(status.Error(codes.Unimplemented, "CasChunkingBlobAccess does not implement GetFromComposite"))
}

func (bc *casChunking) Put(ctx context.Context, digest digest.Digest, in buffer.Buffer) error {
	params, err := GetCDCParameters(ctx, bc.chunkListStorage, digest.GetInstanceName())
	if err != nil {
		return err
	}

	// Check for trivial case where we can simply put the value directly
	// to the underlying storage and not involve chunk lists at all.
	if digest.GetSizeBytes() < 2*params.MinChunkSizeBytes {
		return bc.chunkStorage.Put(
			ctx,
			digest,
			in,
		)
	}

	// The blob is big so we chunk it, store all missing chunks and save
	// the corresponding chunk list.
	reader := in.ToReader()
	defer reader.Close()
	chunker := NewReaderChunker(
		digest.GetDigestFunction(),
		reader,
		int64(params.MinChunkSizeBytes),
		int64(params.HorizonSizeBytes),
	)
	chunkDigests := make([]*remoteexecution.Digest, 0, digest.GetSizeBytes()/int64(params.MinChunkSizeBytes))
	for {
		chunk, err := chunker.NextChunk()
		if err == io.EOF {
			break
		}
		if err != nil {
			return util.StatusWrap(err, "Failed to chunk write stream")
		}

		missing, err := bc.chunkStorage.FindMissing(ctx, chunk.Digest.ToSingletonSet())
		if err != nil {
			return err
		}
		if !missing.Empty() {
			if err := bc.chunkStorage.Put(ctx, chunk.Digest, buffer.NewValidatedBufferFromByteSlice(chunk.Data)); err != nil {
				return util.StatusWrap(err, "Failed to save chunk")
			}
		}

		chunkDigests = append(chunkDigests, chunk.Digest.GetProto())
	}

	// All data chunks have been uploaded but before we can return a
	// succesful response we must save the result to our chunk list
	// storage. As we have validated the chunk list here ourselves we
	// can bypass the validation of the blob.
	ctx = NewContextWithChunkListValidationBypass(ctx)
	chunkListProto := &remoteexecution.SplitBlobResponse{
		ChunkDigests:     chunkDigests,
		ChunkingFunction: remoteexecution.ChunkingFunction_REP_MAX_CDC,
	}
	b := buffer.NewProtoBufferFromProto(chunkListProto, buffer.UserProvided)
	if err := bc.chunkListStorage.Put(ctx, digest, b); err != nil {
		return util.StatusWrap(err, "Could not save chunk list for blob")
	}

	return nil
}

func (bc *casChunking) chunksOfBlob(ctx context.Context, d digest.Digest, params Parameters) ([]digest.Digest, error) {
	b := bc.chunkListStorage.Get(ctx, d)
	responseProtoBuf, err := b.ToProto(&remoteexecution.SplitBlobResponse{}, bc.maximumMessageSizeBytes)
	if err != nil {
		return nil, err
	}
	response := responseProtoBuf.(*remoteexecution.SplitBlobResponse)
	digestFunction := d.GetDigestFunction()
	chunkDigestsProto := response.ChunkDigests
	chunkDigests := make([]digest.Digest, len(chunkDigestsProto))
	for i, cdp := range chunkDigestsProto {
		chunkDigests[i], err = digestFunction.NewDigestFromProto(cdp)
		if err != nil {
			return nil, util.StatusWrap(err, "Failed to parse digest from proto")
		}
	}
	return chunkDigests, nil
}

func (bc *casChunking) findMissingFromInstance(ctx context.Context, instanceName digest.InstanceName, digests digest.Set) (digest.Set, error) {
	params, err := GetCDCParameters(ctx, bc.chunkListStorage, instanceName)
	if err != nil {
		return digest.EmptySet, err
	}
	smallDigests := digest.NewSetBuilder(digests.Length())
	largeDigests := digest.NewSetBuilder(digests.Length())
	for _, d := range digests.Items() {
		if d.GetSizeBytes() < 2*params.MinChunkSizeBytes {
			smallDigests.Add(d)
		} else {
			largeDigests.Add(d)
		}
	}
	smallMissing, err := bc.chunkStorage.FindMissing(ctx, smallDigests.Build())
	if err != nil {
		return digest.EmptySet, err
	}
	largeMissing, err := bc.chunkListStorage.FindMissing(ctx, largeDigests.Build())
	if err != nil {
		return digest.EmptySet, err
	}
	return digest.GetUnion([]digest.Set{smallMissing, largeMissing}), nil
}
