package local

import (
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// errClosedForWriting is an error code to indicate that the BlockList does not
// accept any more write operations and ongoing write operations will fail.
var errClosedForWriting = status.Error(codes.Unavailable, "Closed for writing")

// ClosableBlockList is an implementation of BlockList that can be blocked for
// writing. This is useful when shutting down to know that no more data is
// written after the last sync.
type ClosableBlockList struct {
	BlockList

	// Writes to closedForWriting must be synchronized with all write methods.
	// This requirement is pushed down on the user of this struct.
	closedForWriting bool
}

// NewClosableBlockList provides an implementation of BlockList whose
// state can be persisted. This makes it possible to preserve the
// contents of a KeyBlobMap across restarts.
func NewClosableBlockList(blockList BlockList) *ClosableBlockList {
	return &ClosableBlockList{
		BlockList: blockList,
	}
}

// CloseForWriting makes this BlockList return errClosedForWriting for all
// future calls to PushBack and Put. This also applies to any ongoing calls
// that that have not finished when CloseForWriting returns.
//
// Calls to CloseForWriting must be synchronized with PushBack, Put etc.,
// (e.g., under a write lock). Read more in the BlockList interface
// specification.
func (bl *ClosableBlockList) CloseForWriting() {
	bl.closedForWriting = true
}

// PushBack appends a new block to the BlockList. The block is obtained
// by calling into the underlying BlockAllocator.
func (bl *ClosableBlockList) PushBack() error {
	if bl.closedForWriting {
		return errClosedForWriting
	}
	return bl.BlockList.PushBack()
}

// Put data into a block managed by the BlockList.
//
// Even if the ClosableBlockList is closed for writing, the returned
// BlockListPutWriter might forward calls to the backend. The final
// BlockListPutFinalizer will still return errClosedForWriting.
func (bl *ClosableBlockList) Put(index int, sizeBytes int64) BlockListPutWriter {
	var actualWriter BlockListPutWriter
	if !bl.closedForWriting {
		actualWriter = bl.BlockList.Put(index, sizeBytes)
	}
	return func(b buffer.Buffer) BlockListPutFinalizer {
		if bl.closedForWriting {
			b.Discard()
			return func() (int64, error) {
				return 0, errClosedForWriting
			}
		}
		// Copy data into the block without holding any locks.
		actualFinalizer := actualWriter(b)
		return func() (int64, error) {
			offset, err := actualFinalizer()
			if bl.closedForWriting {
				return 0, errClosedForWriting
			}
			return offset, err
		}
	}
}
