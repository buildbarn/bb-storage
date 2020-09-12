package blockdevice

import (
	"io"
	"os"
)

// BlockDevice is an interface for interacting with a block device like
// storage medium. Block devices support random access reads and writes.
// They differ from plain files, in that their size is fixed.
//
// Storage media tend to store data in sectors. These sectors cannot be
// read from and written to partially. Though the ReadAt() and WriteAt()
// methods provided by this interface do not require I/O to be sector
// aligned, not doing so may impact performance, particularly when
// writing.
//
// For writes, it is suggested that data is padded with zero bytes to
// form full sectors. This prevents the kernel from performing reads
// that are needed to compute the new sector contents.
//
// Because of caching, writes may not be applied against the underlying
// storage medium immediately. This can be problematic in case the order
// of writes matters. The Sync() function can be used to block execution
// until all previous writes are persisted.
type BlockDevice interface {
	io.ReaderAt
	io.WriterAt

	Sync() error
}

var _ BlockDevice = (*os.File)(nil)
