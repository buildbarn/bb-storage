package circular

import (
	"io"
)

type fileDataStore struct {
	file ReadWriterAt
	size uint64
}

// NewFileDataStore creates a new file-based store for blob contents.
// All data is stored in a single file, where all blobs are concatenated
// directly. As the file pointer wraps around at a configured size, old
// data is automatically overwritten by new data.
func NewFileDataStore(file ReadWriterAt, size uint64) DataStore {
	return &fileDataStore{
		file: file,
		size: size,
	}
}

func (ds *fileDataStore) Put(r io.Reader, offset uint64) error {
	for {
		// Read data. If at the end of the storage file, limit
		// the size to ensure proper wrap-around.
		writeOffset := offset % ds.size
		var b [65536]byte
		copyLength := uint64(len(b))
		if copyLength > ds.size-writeOffset {
			copyLength = ds.size - writeOffset
		}
		n, err := r.Read(b[:])
		if err == io.EOF {
			return nil
		} else if err != nil {
			return err
		}

		// Write data to storage.
		if _, err := ds.file.WriteAt(b[:n], int64(writeOffset)); err != nil {
			return err
		}
		offset += uint64(n)
	}
}

func (ds *fileDataStore) Get(offset uint64, size int64) io.ReadCloser {
	return &fileDataStoreReader{
		ds:     ds,
		offset: offset,
		size:   uint64(size),
	}
}

type fileDataStoreReader struct {
	ds     *fileDataStore
	offset uint64
	size   uint64
}

func (f *fileDataStoreReader) Read(b []byte) (n int, err error) {
	// No data left.
	if f.size == 0 {
		return 0, io.EOF
	}

	// Determine which amount of data may be read. Perform a short
	// read at the end of the storage file, so that a successive
	// read will start at the beginning of the file.
	readOffset := f.offset % f.ds.size
	readLength := f.size
	if bLength := uint64(len(b)); readLength > bLength {
		readLength = bLength
	}
	if readLength > f.ds.size-readOffset {
		readLength = f.ds.size - readOffset
	}

	// Perform the read.
	if _, err := f.ds.file.ReadAt(b[:readLength], int64(readOffset)); err != nil {
		return 0, err
	}
	f.offset += readLength
	f.size -= readLength
	return int(readLength), nil
}

func (f *fileDataStoreReader) Close() error {
	return nil
}
