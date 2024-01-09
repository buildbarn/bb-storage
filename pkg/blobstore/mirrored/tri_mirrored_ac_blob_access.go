package mirrored

// TODO(ragost): are connections re-established automatically after a failure?

import (
	"context"
	"fmt"
	"log"
	"strings"
	"sync"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/blobstore/slicing"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type triMirroredACBlobAccess struct {
	backends [N_MIRROR]blobstore.BlobAccess
}

//
// Read a blob from the Action Cache.
//
func (ba *triMirroredACBlobAccess) Get(ctx context.Context, digest digest.Digest) buffer.Buffer {
	var copyBufs [N_MIRROR]*buffer.Buffer
	cbr := &concurrentBlobRead{
		digest: digest,
		srcIdx: -1,
		done:   sync.NewCond(&sync.Mutex{}),
	}
	for i := 0; i < N_MIRROR; i++ {
		go func(idx int) {
			b := ba.backends[idx].Get(ctx, digest)
			cbr.done.L.Lock()
			cbr.bufs[idx] = &b
			cbr.bufCount++
			cbr.done.L.Unlock()
			cbr.done.Broadcast()
		}(i)
	}

	// Kick off a go routine to handle replication, if needed.
	go func() {
		cbr.done.L.Lock()
		for cbr.bufCount != N_MIRROR+1 { // +1 for the context servicing the user request
			cbr.done.Wait()
		}
		cbr.done.L.Unlock()

		// At this point the main caller context has returned.
		// If cbr.errCount == 0, there is no replication needed;
		// If cbr.errCount == 2, we can't trust the one successful copy, so we don't do replication
		// if cbr.errCount == 3, there is no replication possible.
		if cbr.errCount == 0 || cbr.errCount >= N_MIRROR-1 {
			if cbr.errCount == 0 {
				(*cbr.bufs[cbr.srcIdx]).Discard()
			}
			return
		}

		// There will be at most one mirror we need to update.  We need
		// to check the nature of the error to determine if it makes sense
		// to try to write the blob to the backend.
		copyDest := make([]*blobstore.BlobAccess, 0, N_MIRROR-1)
		for i := range ba.backends {
			if shouldCopy(cbr.errors[i]) {
				copyDest = append(copyDest, &ba.backends[i])
				log.Printf("Replicating AC blob %s to %s", digest, backendName(i))
				triMirroredMissingBlobReplicationCount.Inc()
			}
		}
		if cbr.srcIdx == -1 {
			panic("no buffer to replicate")
		}
		if len(copyDest) == 0 {
			// Nothing to do.
			(*cbr.bufs[cbr.srcIdx]).Discard()
			return
		}
		b1 := *cbr.bufs[cbr.srcIdx]
		go func() {
			err := (*copyDest[0]).Put(context.Background(), cbr.digest, b1)
			if err != nil {
				log.Printf("Failed to replicate blob digest %s to backend: %v", digest, err)
				triMirroredMissingBlobReplicationErrorCount.Inc()
			}
		}()
	}()

	// Clone the buffers so that we can ensure we have two matching entries.
	cbr.done.L.Lock()
	for cbr.bufCount != N_MIRROR {
		cbr.done.Wait()
	}
	for i := range cbr.bufs {
		if cbr.bufs[i] == nil {
			panic("no buffer from backend")
		} else {
			sz, err := (*cbr.bufs[i]).GetSizeBytes()
			cbr.errors[i] = err
			if err == nil {
				// Copy the buffer for inspection.
				b1, b2 := (*cbr.bufs[i]).CloneCopy(int(sz))
				cbr.bufs[i] = &b1
				copyBufs[i] = &b2
				if cbr.srcIdx == -1 {
					cbr.srcIdx = i
				}
			} else {
				cbr.errCount++
			}
		}
	}

	// Signal the goroutine that handles replication after we return
	defer func() {
		cbr.bufCount++ // Yeah, it's not pretty, but it's only used to satisfy the wait condition
		cbr.done.L.Unlock()
		cbr.done.Signal()
	}()

	if cbr.errCount == N_MIRROR {
		// Return the first buffer to the user.  Because all the mirrors returned an error, this
		// will be an error buffer.  No need to call Discard on the other buffers; for error
		// buffers, it's a no-op.
		return *cbr.bufs[A_IDX]
	}

	if cbr.errCount == N_MIRROR-1 {
		// Only one buffer was read successfully.  Sadly, we can't trust it, so invalidate it.
		log.Printf("Invalidating AC blob %s in %s", digest, backendName(cbr.srcIdx))
		err := invalidate(ctx, digest, *cbr.bufs[cbr.srcIdx], ba.backends[cbr.srcIdx])
		if err != nil {
			log.Printf("Invalidation failed for digest %s: %v", digest, err)
		}
		b, err := invalidateBuf(*copyBufs[cbr.srcIdx])
		if err != nil {
			return buffer.NewBufferFromError(status.Error(codes.NotFound, "Object not found"))
		}
		return *b
	}

	// TODO(ragost): remember to clean up buffers (copied and otherwise)

	// Check to make sure we have at least two matching buffers.
	goodBuf, mismatchIdx := quorumMatch(copyBufs)
	if goodBuf == nil {
		// Nothing matched.  Invalidate the blobs.
		for i := range cbr.bufs {
			if cbr.errors[i] != nil {
				continue
			}
			log.Printf("Invalidating AC blob %s in %s", digest, backendName(i))
			err := invalidate(ctx, digest, *cbr.bufs[i], ba.backends[i])
			if err != nil {
				log.Printf("Invalidation failed for digest %s: %v", digest, err)
			}
		}
		cbr.errCount = N_MIRROR // disable the replication logic

		// If nothing matches, we don't know which invalidated entry to return to the caller.
		// Instead, make believe the object doesn't exist.
		return buffer.NewBufferFromError(status.Error(codes.NotFound, "Object not found"))
	}
	for i, b := range copyBufs {
		if b == goodBuf {
			cbr.srcIdx = i
			break
		}
	}
	for i := range copyBufs {
		if i != cbr.srcIdx {
			(*cbr.bufs[i]).Discard()
			if cbr.errors[i] == nil && i == mismatchIdx {
				// If this was a buffer with different contents than the quorum,
				// arrange to have it overwritten by the good blob.
				cbr.errors[i] = status.Error(codes.NotFound, "Object not found")
				cbr.errCount++
			}
		}
	}

	// Convert the buffer cbr_bufs[cbr_srcIdx] into two clones.  The replication logic will use
	// b1 to update the replica, and we return b2 to the caller.  If replication isn't needed,
	// the goroutine will release b1.
	b1, b2 := (*cbr.bufs[cbr.srcIdx]).CloneStream()
	cbr.bufs[cbr.srcIdx] = &b1
	return b2
}

//
// Return a buffer that matches at least one other mirror, or nil if there is no match.
// Note: if only two mirrors are up, one of the buffer argument array entries  will be nil.
//
func quorumMatch(b [N_MIRROR]*buffer.Buffer) (*buffer.Buffer, int) {
	var sz [N_MIRROR]int
	var pr [N_MIRROR]proto.Message

	// First look for buffers with matching sizes.  If they aren't the same size, then
	// they can't have the same contents.
	noMatch := -1
	numGood := 0
	for i := range b {
		if b[i] != nil {
			nb, err := (*b[i]).GetSizeBytes()
			if err != nil {
				log.Printf("Can't get buffer size to determine quorum: %v", err)
				sz[i] = noMatch
				noMatch--
			} else {
				sz[i] = int(nb)
				numGood++
			}
		} else {
			sz[i] = noMatch
			noMatch--
		}
	}
	if numGood < 2 {
		return nil, -1
	}
	first := N_MIRROR
	for i := range sz {
		if sz[i] >= 0 {
			first = i
			break
		}
	}
	szMatch := 0
	for first < N_MIRROR {
		szMatch = 0
		for i := first + 1; i < N_MIRROR; i++ {
			if sz[i] == sz[first] {
				szMatch++
			}
		}
		if szMatch == 0 {
			first++
		} else {
			break
		}
	}
	if szMatch == 0 {
		return nil, -1
	}

	// Now get the buffer contents
	numGood = 0
	msgSz := sz[first]
	for i := range b {
		if sz[i] == msgSz {
			var err error
			// TODO(ragost): should we compare byte slices instead?
			pr[i], err = (*b[i]).ToProto(&remoteexecution.ActionResult{}, sz[i])
			if err != nil {
				log.Printf("Failed converting buffer to proto: %v", err)
				sz[i] = noMatch
				noMatch--
			} else {
				numGood++
			}
		}
	}
	if numGood <= 1 {
		return nil, -1
	}
	first = N_MIRROR
	for i := range b {
		if sz[i] == msgSz {
			first = i
			break
		}
	}
	// Here we know first != -1, because we ensured numGood > 1 above
	mismatchIdx := -1
	numGood = 0
	for numGood == 0 && first < N_MIRROR-1 {
		numGood = 1 // assume first is "good"
		for i := first + 1; i < N_MIRROR; i++ {
			if sz[i] == msgSz && proto.Equal(pr[i], pr[first]) {
				numGood++
			} else {
				mismatchIdx = i
			}
		}
		if numGood < 2 {
			// This second loop handles the case where all three buffers are the
			// same size, but the first one didn't match either the second or third.
			// Here, we check if the second and third match each other.
			numGood = 0
			mismatchIdx = first
			for i := first + 1; i < N_MIRROR; i++ {
				if sz[i] == msgSz {
					first = i
					break
				}
			}
		}
	}

	if numGood < 2 {
		return nil, -1
	}
	return b[first], mismatchIdx
}

//
// Store a blob in the Action Cache.
//
func (ba *triMirroredACBlobAccess) Put(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
	// Store the object in all three storage backends.
	sz, err := b.GetSizeBytes()
	if err != nil {
		return err
	}
	bc1, bc2 := b.CloneCopy(int(sz))
	bc3, _ := bc2.CloneCopy(int(sz)) // For verifying the write
	b1, b2 := bc1.CloneStream()
	b3, _ := b2.CloneStream() // consumersRemaining is 3 at this point (see cas_cloned_buffer.go)
	bufs := [N_MIRROR]buffer.Buffer{b1, b2, b3}
	var errChans [N_MIRROR]chan error
	var errors [N_MIRROR]error
	for i := range bufs {
		errChans[i] = make(chan error, 1)
	}
	didRetry := false
retry:
	for i := range bufs {
		go func(idx int) {
			errChans[idx] <- ba.backends[idx].Put(ctx, digest, bufs[idx])
		}(i)
	}
	numErr := 0
	for i := 0; i < N_MIRROR; i++ {
		select {
		case errors[0] = <-errChans[0]:
			if errors[0] != nil {
				numErr++
			}
		case errors[1] = <-errChans[1]:
			if errors[1] != nil {
				numErr++
			}
		case errors[2] = <-errChans[2]:
			if errors[2] != nil {
				numErr++
			}
		}
	}

	// Return success if at least two backend writes succeed.  We only support failure of one mirror at a time.
	if numErr > MAX_ERR {
		if numErr == N_MIRROR-1 {
			// Only one write succeeded.  We need to invalidate it -- we made a copy of the original buffer in bc2.
			// Identify the backend whose write succeeded.
			undoIdx := -1
			for i := range errors {
				if errors[i] == nil {
					undoIdx = i
					break
				}
			}
			if undoIdx == -1 {
				panic("error calculating undoIdx")
			}
			log.Printf("Invalidating AC blob %s in %s", digest, backendName(undoIdx))
			err = invalidate(ctx, digest, bc2, ba.backends[undoIdx])
			if err != nil {
				log.Printf("Can't invalidate lone action cache entry: %v", err)
			}
		} else {
			bc2.Discard()
		}
		var sb strings.Builder
		fmt.Fprintf(&sb, "Too many failures: ")
		comma := ""
		for i := range errors {
			if errors[i] != nil {
				fmt.Fprintf(&sb, "%s%s: %s", comma, backendName(i), errors[i].Error())
				comma = ", "
			}
		}
		log.Printf("Can't write blob %s to AC: %s", digest, sb.String())
		bc3.Discard()
		return fmt.Errorf(sb.String())
	} else {
		// Race detection: validate that we can read back what we just wrote.  If we can't, then try writing
		// again.  If we fail again because of races, invalidate the blobs and return an error to the user.
		cbr := &concurrentBlobRead{
			digest: digest,
			srcIdx: -1,
			done:   sync.NewCond(&sync.Mutex{}),
		}
		nread := 0
		for i := 0; i < N_MIRROR; i++ {
			if errors[i] == nil {
				nread++
				go func(idx int) {
					b := ba.backends[idx].Get(ctx, digest)
					cbr.done.L.Lock()
					cbr.bufs[idx] = &b
					cbr.bufCount++
					cbr.done.L.Unlock()
					cbr.done.Signal()
				}(i)
			} else {
				// Treat a previously failed write as a read error here so the quorum counts are correct.
				cbr.errCount++
			}
		}
		cbr.done.L.Lock()
		for cbr.bufCount != nread {
			cbr.done.Wait()
		}
		cbr.done.L.Unlock()
		for i := range cbr.bufs {
			if cbr.bufs[i] != nil {
				_, err := (*cbr.bufs[i]).GetSizeBytes()
				cbr.errors[i] = err
				if err != nil {
					(*cbr.bufs[i]).Discard()
					cbr.bufs[i] = nil
					cbr.errCount++
				}
			}
		}
		if cbr.errCount <= MAX_ERR {
			goodBuf, _ := quorumMatch(cbr.bufs)
			for _, b := range cbr.bufs {
				if b != nil {
					(*b).Discard()
				}
			}
			if goodBuf != nil {
				// We have a quorum.  It might be our write, it might be a write that raced with us, but
				// it's a quorum at this point.
				bc2.Discard()
				bc3.Discard()
				return nil
			}
		}

		// Use bc2 for writes and bc3 for read verifications
		if didRetry {
			// We already retried once, and the writes still didn't stick.  Return an error to the caller.
			triMirroredBlobRaceRetryFailedCount.Inc()
			bc3.Discard()
			log.Printf("Can't write blob %s to AC: Race recovery failed", digest)
			return status.Error(codes.Aborted, "Race recovery failed")
		} else {
			triMirroredBlobRaceCount.Inc()
			b1, b2 = bc2.CloneStream()
			b3, _ = b2.CloneStream()
			bufs = [N_MIRROR]buffer.Buffer{b1, b2, b3}
			for i := range errors {
				errors[i] = nil
			}
			didRetry = true
			goto retry
		}
	}
	return nil
}

//
// This isn't used, but we still need to satisfy the BlobAccess interface.
//
func (ba *triMirroredACBlobAccess) FindMissing(ctx context.Context, digests digest.Set) (digest.Set, error) {
	return digest.EmptySet, status.Error(codes.Unimplemented, "Bazel action cache does not support bulk existence checking")
}

func (ba *triMirroredACBlobAccess) GetCapabilities(ctx context.Context, instanceName digest.InstanceName) (*remoteexecution.ServerCapabilities, error) {
	// Return the capabilities for the first one that succeeds.
	var err error
	var capabilities *remoteexecution.ServerCapabilities
	for i := range ba.backends {
		capabilities, err = ba.backends[i].GetCapabilities(ctx, instanceName)
		if err == nil {
			return capabilities, nil
		}
	}
	return nil, util.StatusWrap(err, backendName(0))
}

func (ba *triMirroredACBlobAccess) GetFromComposite(ctx context.Context, parentDigest, childDigest digest.Digest, slicer slicing.BlobSlicer) buffer.Buffer {
	// TODO
	return buffer.NewBufferFromError(status.Error(codes.Unimplemented, "GetFromComposite not supported yet"))
}
