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
)

type triMirroredCASBlobAccess struct {
	backends [N_MIRROR]blobstore.BlobAccess
}

//
// Get reads the blob specified by digest from the Content Addressable Store (CAS).
//
func (ba *triMirroredCASBlobAccess) Get(ctx context.Context, digest digest.Digest) buffer.Buffer {
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
		// if cbr.errCount == 3, there is no replication possible.
		if cbr.errCount == 0 || cbr.errCount == N_MIRROR {
			return
		}

		// We might have to copy a buf to at least one mirror.  We need
		// to check the nature of the error to determine if it makes sense
		// to try to write the blob to the backend.
		copyDest := make([]*blobstore.BlobAccess, 0, N_MIRROR-1)
		for i := range ba.backends {
			if shouldCopy(cbr.errors[i]) {
				copyDest = append(copyDest, &ba.backends[i])
				log.Printf("Replicating CAS blob %s to %s", digest, backendName(i))
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
		if len(copyDest) == 2 {
			b2, _ := b1.CloneStream()
			go func() {
				err := (*copyDest[1]).Put(context.Background(), cbr.digest, b2)
				if err != nil {
					log.Printf("Failed to replicate blob digest %s to second backend: %v", digest, err)
					triMirroredMissingBlobReplicationErrorCount.Inc()
				}
			}()
		}
		go func() {
			err := (*copyDest[0]).Put(context.Background(), cbr.digest, b1)
			if err != nil {
				log.Printf("Failed to replicate blob digest %s to first backend: %v", digest, err)
				triMirroredMissingBlobReplicationErrorCount.Inc()
			}
		}()
	}()

	// Return the first good buffer we have to the caller.
	cbr.done.L.Lock()
	for cbr.bufCount != N_MIRROR {
		cbr.done.Wait()
	}
	for i := range cbr.bufs {
		if cbr.bufs[i] == nil {
			panic("no buffer from backend")
		} else {
			_, err := (*cbr.bufs[i]).GetSizeBytes()
			cbr.errors[i] = err
			if err == nil {
				if cbr.srcIdx == -1 {
					cbr.srcIdx = i
				}
			} else {
				cbr.errCount++
			}
		}
	}

	defer func() {
		cbr.bufCount++ // Yeah, it's not pretty, but it's only used to satisfy the wait condition
		cbr.done.L.Unlock()
		cbr.done.Signal()
	}()
	if cbr.errCount == N_MIRROR {
		// All the reads failed.  Return any buffer to the caller.
		(*cbr.bufs[B_IDX]).Discard()
		(*cbr.bufs[C_IDX]).Discard()
		return *cbr.bufs[A_IDX]
	}
	retBufs := cbr.bufs
	retBufs[cbr.srcIdx] = nil
	if cbr.errCount == 0 {
		eh := triMirroredErrorHandler{
			bufs:     retBufs,
			savedErr: nil,
			digest:   digest,
			ctx:      ctx,
		}
		for i := range ba.backends {
			eh.backends[i] = &ba.backends[i]
		}
		return buffer.WithErrorHandler(*cbr.bufs[cbr.srcIdx], &eh)
	}

	// We might need to replicate the blob.
	b1, b2 := (*cbr.bufs[cbr.srcIdx]).CloneStream()
	cbr.bufs[cbr.srcIdx] = &b1
	eh := triMirroredErrorHandler{
		bufs:     retBufs,
		savedErr: nil,
		digest:   digest,
		ctx:      ctx,
	}
	for i := range ba.backends {
		eh.backends[i] = &ba.backends[i]
	}
	return buffer.WithErrorHandler(b2, &eh)
}

//
// Store a blob in the CAS.
//
func (ba *triMirroredCASBlobAccess) Put(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
	// Store the object in all three storage backends.
	b1, b2 := b.CloneStream()
	b3, _ := b2.CloneStream() // consumersRemaining is 3 at this point (see cas_cloned_buffer.go)
	bufs := [N_MIRROR]buffer.Buffer{b1, b2, b3}
	var errChans [N_MIRROR]chan error
	for i := range bufs {
		errChans[i] = make(chan error, 1)
		go func(idx int) {
			errChans[idx] <- ba.backends[idx].Put(ctx, digest, bufs[idx])
		}(i)
	}
	var errors [N_MIRROR]error
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
		var sb strings.Builder
		fmt.Fprintf(&sb, "Too many failures: ")
		comma := ""
		for i := range errors {
			if errors[i] != nil {
				fmt.Fprintf(&sb, "%s%s: %s", comma, backendName(i), errors[i].Error())
				comma = ", "
			}
		}
		log.Printf("Can't write blob %s to CAS: %s", digest, sb.String())
		return fmt.Errorf(sb.String())
	}
	log.Printf("Wrote blob %s to CAS (%v %v %v)", digest, errors[0], errors[1], errors[2])
	return nil
}

type triMirrorFindMissingResults struct {
	missing digest.Set
	err     error
}

func triMirrorCallFindMissing(ctx context.Context, blobAccess blobstore.BlobAccess, digests digest.Set) triMirrorFindMissingResults {
	missing, err := blobAccess.FindMissing(ctx, digests)
	return triMirrorFindMissingResults{missing: missing, err: err}
}

//
// Return blob residency for the CAS.
//
func (ba *triMirroredCASBlobAccess) FindMissing(ctx context.Context, digests digest.Set) (digest.Set, error) {
	// Call FindMissing() on all backends.
	var resChans [N_MIRROR]chan triMirrorFindMissingResults
	var results [N_MIRROR]triMirrorFindMissingResults
	for i := 0; i < N_MIRROR; i++ {
		resChans[i] = make(chan triMirrorFindMissingResults, 1)
		go func(idx int) {
			resChans[idx] <- triMirrorCallFindMissing(ctx, ba.backends[idx], digests)
		}(i)
	}
	numErr := 0
	for i := 0; i < N_MIRROR; i++ {
		select {
		case results[0] = <-resChans[0]:
			if results[0].err != nil {
				numErr++
			}
		case results[1] = <-resChans[1]:
			if results[1].err != nil {
				numErr++
			}
		case results[2] = <-resChans[2]:
			if results[2].err != nil {
				numErr++
			}
		}
	}

	if numErr > MAX_ERR {
		var sb strings.Builder
		fmt.Fprintf(&sb, "Too many failures: ")
		comma := ""
		for i := 0; i < N_MIRROR; i++ {
			if results[i].err != nil {
				fmt.Fprintf(&sb, "%s%s: %s", comma, backendName(i), results[i].err.Error())
				comma = ", "
			}
		}
		return digest.EmptySet, fmt.Errorf(sb.String())
	}

	// We have at least two backends online.  Calculate the differences between them and replicate the blobs
	// to make them equivalent.  Then, if all three backends are online, do the same with either of the first
	// two backends and the third.  Since the first two should now have the same blobs, we don't need to
	// compare them both to the third.  The only difference is that if the third backend has blobs that are
	// missing from both of the first two backends, then we need to replicate the blobs from the third backend
	// to both of the other backends.
	//
	// Replication failures can be ignored as long as we end up with a quorum (i.e., at least 2 backends have
	// copies of each blob).  That way, if one of those two backend fails, we can still read the blobs from the
	// surviving backend that contained the blob.
	var missingOnlyFromBE [N_MIRROR]digest.Set
	var missingFromBoth, missingFromAll digest.Set
	var backendIdx []int = make([]int, 0, N_MIRROR)
	for i := 0; i < N_MIRROR; i++ {
		if results[i].err == nil {
			backendIdx = append(backendIdx, i)
		}
	}

	refCnt := make(map[string]int) // counts number of backend references to each digest
	savedErr := make(map[string]error)

	// We know we have at least two entries because of the check of numErr above.
	missingOnlyFromBE[backendIdx[0]], missingFromBoth, missingOnlyFromBE[backendIdx[1]] = digest.GetDifferenceAndIntersection(results[backendIdx[0]].missing, results[backendIdx[1]].missing)

	// Copy blobs missing in BE[backendIdx[1]] from BE[backendIdx[0]].
	for _, blobDigest := range missingOnlyFromBE[backendIdx[1]].Items() {
		triMirroredMissingBlobReplicationCount.Inc()
		b := ba.backends[backendIdx[0]].Get(ctx, blobDigest)
		if _, err := b.GetSizeBytes(); err != nil {
			// can't read the block, so no ref
			triMirroredMissingBlobReplicationErrorCount.Inc()
			log.Printf("Failed to replicate blob digest %s to %s: %v", blobDigest, backendName(backendIdx[1]), err)
			savedErr[blobDigest.String()] = fmt.Errorf("Can't replicate block %s to %s: %v", blobDigest, backendName(backendIdx[1]), err)
			continue
		}
		refCnt[blobDigest.String()]++
		if err := ba.backends[backendIdx[1]].Put(ctx, blobDigest, b); err != nil {
			triMirroredMissingBlobReplicationErrorCount.Inc()
			log.Printf("Failed to replicate blob digest %s to %s: %v", blobDigest, backendName(backendIdx[1]), err)
			savedErr[blobDigest.String()] = fmt.Errorf("Can't replicate block %s to %s: %v", blobDigest, backendName(backendIdx[1]), err)
		} else {
			refCnt[blobDigest.String()]++
		}
	}

	// Copy blobs missing in BE[backendIdx[0]] from BE[backendIdx[1]].
	for _, blobDigest := range missingOnlyFromBE[backendIdx[0]].Items() {
		triMirroredMissingBlobReplicationCount.Inc()
		b := ba.backends[backendIdx[1]].Get(ctx, blobDigest)
		if _, err := b.GetSizeBytes(); err != nil {
			// can't read the block, so no ref
			triMirroredMissingBlobReplicationErrorCount.Inc()
			log.Printf("Failed to replicate blob digest %s to %s: %v", blobDigest, backendName(backendIdx[0]), err)
			savedErr[blobDigest.String()] = fmt.Errorf("Can't replicate block %s to %s: %v", blobDigest, backendName(backendIdx[0]), err)
			continue
		}
		refCnt[blobDigest.String()]++
		if err := ba.backends[backendIdx[0]].Put(ctx, blobDigest, b); err != nil {
			triMirroredMissingBlobReplicationErrorCount.Inc()
			log.Printf("Failed to replicate blob digest %s to %s: %v", blobDigest, backendName(backendIdx[0]), err)
			savedErr[blobDigest.String()] = fmt.Errorf("Can't replicate block %s to %s: %v", blobDigest, backendName(backendIdx[0]), err)
		} else {
			refCnt[blobDigest.String()]++
		}
	}

	if len(backendIdx) == N_MIRROR-1 {
		// Debuging only
		//fmt.Printf("2-mirror refcnts: %#v\n", refCnt)

		// Make sure all blobs have reference counts equal to 2, otherwise return an error.
		// (See the comment at the end of this function for a complete explantation why.)
		for blobDigestString, refs := range refCnt {
			if refs != 2 {
				return digest.EmptySet, savedErr[blobDigestString]
			}
		}
		return missingFromBoth, nil
	}

	// Now handle the third backend
	missingFromBoth, missingFromAll, missingOnlyFromBE[backendIdx[2]] = digest.GetDifferenceAndIntersection(missingFromBoth, results[backendIdx[2]].missing)
	// Copy blobs missing in BE[backendIdx[2]] from BE[backendIdx[0]] or BE[backendIdx[1]]
	for _, blobDigest := range missingOnlyFromBE[backendIdx[2]].Items() {
		triMirroredMissingBlobReplicationCount.Inc()
		b := ba.backends[backendIdx[0]].Get(ctx, blobDigest)
		if _, err := b.GetSizeBytes(); err != nil {
			// can't read block, try backendIdx[1]
			b = ba.backends[backendIdx[1]].Get(ctx, blobDigest)
			if _, err := b.GetSizeBytes(); err != nil {
				// can't read the block, so no ref
				triMirroredMissingBlobReplicationErrorCount.Inc()
				log.Printf("Failed to replicate blob digest %s to %s: %v", blobDigest, backendName(backendIdx[2]), err)
				savedErr[blobDigest.String()] = fmt.Errorf("Can't replicate block %s to %s: %v", blobDigest, backendName(backendIdx[2]), err)
				continue
			}
		}
		if _, ok := refCnt[blobDigest.String()]; !ok {
			// Don't want to over count a blob that was already referenced
			refCnt[blobDigest.String()]++
		}
		if err := ba.backends[backendIdx[2]].Put(ctx, blobDigest, b); err != nil {
			triMirroredMissingBlobReplicationErrorCount.Inc()
			log.Printf("Failed to replicate blob digest %s to %s: %v", blobDigest, backendName(backendIdx[2]), err)
			savedErr[blobDigest.String()] = fmt.Errorf("Can't replicate block %s to %s: %v", blobDigest, backendName(backendIdx[2]), err)
		} else {
			refCnt[blobDigest.String()]++
		}
	}

	// Copy blobs missing in BE[backendIdx[0]] and BE[backendIdx[1]] from BE[backendIdx[2]]
	for _, blobDigest := range missingFromBoth.Items() {
		b := ba.backends[backendIdx[2]].Get(ctx, blobDigest)
		if _, err := b.GetSizeBytes(); err != nil {
			// can't read the block, so no ref
			triMirroredMissingBlobReplicationErrorCount.Inc()
			log.Printf("Failed to replicate blob digest %s to %s: %v", blobDigest, backendName(backendIdx[0]), err)
			savedErr[blobDigest.String()] = fmt.Errorf("Can't replicate block %s to %s: %v", blobDigest, backendName(backendIdx[0]), err)
			continue
		}
		refCnt[blobDigest.String()]++
		var bufs [2]buffer.Buffer
		bufs[0], bufs[1] = b.CloneStream()
		var errChans [2]chan error
		for i := 0; i < 2; i++ {
			errChans[i] = make(chan error, 1)
			go func(idx int) {
				triMirroredMissingBlobReplicationCount.Inc()
				errChans[idx] <- ba.backends[backendIdx[idx]].Put(ctx, blobDigest, bufs[idx])
			}(i)
		}
		for i := 0; i < 2; i++ {
			select {
			case err := <-errChans[0]:
				if err != nil {
					triMirroredMissingBlobReplicationErrorCount.Inc()
					log.Printf("Failed to replicate blob digest %s to %s: %v", blobDigest, backendName(backendIdx[0]), err)
					if _, ok := savedErr[blobDigest.String()]; !ok {
						savedErr[blobDigest.String()] = fmt.Errorf("Can't replicate block %s to %s: %v", blobDigest, backendName(backendIdx[0]), err)
					} else {
						savedErr[blobDigest.String()] = fmt.Errorf("Can't replicate block %s to %s: %v, %v", blobDigest, backendName(backendIdx[0]), err, savedErr[blobDigest.String()])
					}
				} else {
					refCnt[blobDigest.String()]++
				}
			case err := <-errChans[1]:
				if err != nil {
					triMirroredMissingBlobReplicationErrorCount.Inc()
					log.Printf("Failed to replicate blob digest %s to %s: %v", blobDigest, backendName(backendIdx[1]), err)
					if _, ok := savedErr[blobDigest.String()]; !ok {
						savedErr[blobDigest.String()] = fmt.Errorf("Can't replicate block %s to %s: %v", blobDigest, backendName(backendIdx[1]), err)
					} else {
						savedErr[blobDigest.String()] = fmt.Errorf("%v, Can't replicate block %s to %s: %v", savedErr[blobDigest.String()], blobDigest, backendName(backendIdx[1]), err)
					}
				} else {
					refCnt[blobDigest.String()]++
				}
			}
		}
	}

	// Debuging only
	//fmt.Printf("3-mirror refcnts: %#v\n", refCnt)

	// Make sure all blobs have reference counts equal to or greater than 2, otherwise return an error.
	// We have to be careful to be as acccurate as we can to satisfy the requirements described in
	// https://github.com/buildbarn/bb-adrs/blob/master/0002-storage.md#storage-requirements-of-bazel-and-buildbarn.
	// Even if a blob is present in a single, we can't make believe it doesn't exist, because a Bazel client
	// using one of the "build without the bytes" options won't have intermediate build results, so will fail.
	// We also can't act as if the blob exists if it is available on a single replica, because that replica
	// could fail between the time that FindMissing returns and the Bazel client issues a request to execute
	// an action that uses it.  So we fail fast, instead.
	for blobDigestString, refs := range refCnt {
		if refs < 2 {
			return digest.EmptySet, savedErr[blobDigestString]
		}
	}
	return missingFromAll, nil
}

type triMirroredErrorHandler struct {
	bufs     [N_MIRROR]*buffer.Buffer
	backends [N_MIRROR]*blobstore.BlobAccess
	savedErr error
	digest   digest.Digest
	ctx      context.Context
	retries  int
}

func (eh *triMirroredErrorHandler) OnError(err error) (buffer.Buffer, error) {
	if eh.savedErr == nil {
		eh.savedErr = err
	}

	// First try to use the buffers obtained in Get()
	for i := range eh.bufs {
		if eh.bufs[i] != nil {
			if _, err = (*eh.bufs[i]).GetSizeBytes(); err != nil {
				(*eh.bufs[i]).Discard()
				eh.bufs[i] = nil
			} else {
				b := eh.bufs[i]
				eh.bufs[i] = nil
				eh.retries++
				log.Printf("OnError returning alternate saved buffer for digest %s try %d", eh.digest, eh.retries)
				triMirroredBlobReadHandleErrorRetryCount.Inc()
				return buffer.WithErrorHandler(*b, eh), nil
			}
		}
	}

	// Now try to read directly from the backends
	for i := range eh.backends {
		be := eh.backends[i]
		if be != nil {
			eh.backends[i] = nil
			eh.retries++
			log.Printf("OnError returning alternate read buffer for digest %s try %d", eh.digest, eh.retries)
			triMirroredBlobReadHandleErrorRetryCount.Inc()
			return buffer.WithErrorHandler(
				(*be).Get(eh.ctx, eh.digest),
				eh,
			), nil
		}
	}
	log.Printf("OnError giving up after %d retries, digest = %s, err = %v", eh.retries, eh.digest, eh.savedErr)
	triMirroredBlobReadHandleErrorFailedCount.Inc()
	return nil, eh.savedErr
}

func (eh *triMirroredErrorHandler) Done() {
	for i := range eh.bufs {
		if eh.bufs[i] != nil {
			(*eh.bufs[i]).Discard()
			eh.bufs[i] = nil
		}
	}
	if eh.savedErr == nil {
		log.Printf("EH read %d done after %d retries", eh.digest, eh.retries)
	}
}

func (ba *triMirroredCASBlobAccess) GetCapabilities(ctx context.Context, instanceName digest.InstanceName) (*remoteexecution.ServerCapabilities, error) {
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

func (ba *triMirroredCASBlobAccess) GetFromComposite(ctx context.Context, parentDigest, childDigest digest.Digest, slicer slicing.BlobSlicer) buffer.Buffer {
	// TODO
	return buffer.NewBufferFromError(status.Error(codes.Unimplemented, "GetFromComposite not supported yet"))
}
