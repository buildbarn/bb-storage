package fallback

import (
	"context"
	"time"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/blobstore/slicing"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func isPrimaryUnavailable(err error) bool {
	return status.Code(err) == codes.Unavailable
}

type fallbackBlobAccess struct {
	primary   blobstore.BlobAccess
	secondary blobstore.BlobAccess
}

// NewFallbackBlobAccess creates a BlobAccess that tries to use a primary
// backend by default and falls back to a secondary backend if the primary is
// not available. Successful writes to the primary are async, best-effort
// replicated to the secondary. Writes to the secondary during fallback are not
// replicated back to the primary.
func NewFallbackBlobAccess(primary, secondary blobstore.BlobAccess) blobstore.BlobAccess {
	return &fallbackBlobAccess{
		primary:   primary,
		secondary: secondary,
	}
}

func (ba *fallbackBlobAccess) Get(ctx context.Context, d digest.Digest) buffer.Buffer {
	return buffer.WithErrorHandler(
		ba.primary.Get(ctx, d),
		&getErrorHandler{
			context:   ctx,
			digest:    d,
			secondary: ba.secondary,
		})
}

type getErrorHandler struct {
	context   context.Context
	digest    digest.Digest
	secondary blobstore.BlobAccess
}

func (eh *getErrorHandler) OnError(err error) (buffer.Buffer, error) {
	// If secondary is nil this is an error from a call to the secondary
	// backend and there's nothing more to fallback on, return the error.
	if eh.secondary == nil {
		return nil, util.StatusWrap(err, "Secondary")
	}
	if !isPrimaryUnavailable(err) {
		return nil, util.StatusWrap(err, "Primary")
	}

	// Primary is unvailable, try the secondary backend. Set secondary to nil to
	// mark that we are on the fallback path, since WithErrorHandler will invoke
	// this OnError() again if there is an error on the secondary backend.
	secondary := eh.secondary
	eh.secondary = nil
	return secondary.Get(eh.context, eh.digest), nil
}

func (getErrorHandler) Done() {}

func (ba *fallbackBlobAccess) GetFromComposite(ctx context.Context, parentDigest, childDigest digest.Digest, slicer slicing.BlobSlicer) buffer.Buffer {
	return buffer.WithErrorHandler(
		ba.primary.GetFromComposite(ctx, parentDigest, childDigest, slicer),
		&getFromCompositeErrorHandler{
			context:      ctx,
			parentDigest: parentDigest,
			childDigest:  childDigest,
			slicer:       slicer,
			secondary:    ba.secondary,
		})
}

type getFromCompositeErrorHandler struct {
	context      context.Context
	parentDigest digest.Digest
	childDigest  digest.Digest
	slicer       slicing.BlobSlicer
	secondary    blobstore.BlobAccess
}

func (eh *getFromCompositeErrorHandler) OnError(err error) (buffer.Buffer, error) {
	// If secondary is nil this is an error from a call to the secondary
	// backend and there's nothing more to fallback on, return the error.
	if eh.secondary == nil {
		return nil, util.StatusWrap(err, "Secondary")
	}
	if !isPrimaryUnavailable(err) {
		return nil, util.StatusWrap(err, "Primary")
	}

	// Primary is unvailable, try the secondary backend. Set secondary to nil to
	// mark that we are on the fallback path, since WithErrorHandler will invoke
	// this OnError() again if there is an error on the secondary backend.
	secondary := eh.secondary
	eh.secondary = nil
	return secondary.GetFromComposite(eh.context, eh.parentDigest, eh.childDigest, eh.slicer), nil
}

func (getFromCompositeErrorHandler) Done() {}

func (ba *fallbackBlobAccess) Put(ctx context.Context, digest digest.Digest, buf buffer.Buffer) error {
	sizeBytes, err := buf.GetSizeBytes()
	if err != nil {
		buf.Discard()
		return err
	}
	primaryBuf, secondaryBuf := buf.CloneCopy(int(sizeBytes))

	err = ba.primary.Put(ctx, digest, primaryBuf)

	// Write to primary succeeded, so async, best-effort replicate to secondary.
	if err == nil {
		go func() {
			replicateCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
			defer cancel()
			_ = ba.secondary.Put(replicateCtx, digest, secondaryBuf)
		}()
		return nil
	}

	// Primary is unavailable, try the secondary backend.
	if isPrimaryUnavailable(err) {
		primaryBuf.Discard()
		if err := ba.secondary.Put(ctx, digest, secondaryBuf); err != nil {
			return util.StatusWrap(err, "Secondary")
		}
		return nil
	}

	secondaryBuf.Discard()
	return util.StatusWrap(err, "Primary")
}

func (ba *fallbackBlobAccess) FindMissing(ctx context.Context, digests digest.Set) (digest.Set, error) {
	missing, err := ba.primary.FindMissing(ctx, digests)

	// Primary succeeded, return its results.
	if err == nil {
		return missing, nil
	}

	// Primary is unavailable, try the secondary backend.
	if isPrimaryUnavailable(err) {
		if missing, err = ba.secondary.FindMissing(ctx, digests); err != nil {
			return digest.EmptySet, util.StatusWrap(err, "Secondary")
		}
		return missing, nil
	}

	return digest.EmptySet, util.StatusWrap(err, "Primary")
}

func (ba *fallbackBlobAccess) GetCapabilities(ctx context.Context, instanceName digest.InstanceName) (*remoteexecution.ServerCapabilities, error) {
	capabilities, err := ba.primary.GetCapabilities(ctx, instanceName)

	// Primary succeeded, return its results.
	if err == nil {
		return capabilities, nil
	}

	// Primary is unavailable, try the secondary backend.
	if isPrimaryUnavailable(err) {
		if capabilities, err = ba.secondary.GetCapabilities(ctx, instanceName); err != nil {
			return nil, util.StatusWrap(err, "Secondary")
		}
		return capabilities, nil
	}

	return nil, util.StatusWrap(err, "Primary")
}
