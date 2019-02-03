package circular

import (
	"encoding/binary"
	"io"
	"log"
)

type fileStateStore struct {
	file     ReadWriterAt
	dataSize uint64
	cursors  Cursors
}

// NewFileStateStore creates a new storage for global metadata of a
// circular storage backend. Right now only a set of read/write cursors
// are stored.
func NewFileStateStore(file ReadWriterAt, dataSize uint64) (StateStore, error) {
	var cursors Cursors
	var data [16]byte
	if _, err := file.ReadAt(data[:], 0); err == nil {
		readCursor := binary.LittleEndian.Uint64(data[:])
		writeCursor := binary.LittleEndian.Uint64(data[8:])
		if readCursor <= writeCursor {
			cursors.Read = readCursor
			cursors.Write = writeCursor
		}
	} else if err != io.EOF {
		return nil, err
	}
	return &fileStateStore{
		file:     file,
		dataSize: dataSize,
		cursors:  cursors,
	}, nil
}

func (ss *fileStateStore) GetCursors() Cursors {
	return ss.cursors
}

func (ss *fileStateStore) put(cursors Cursors) error {
	// Store cursors.
	if cursors.Read > cursors.Write {
		log.Fatalf("Attempted to write cursors %d > %d", cursors.Read, cursors.Write)
	}
	var data [16]byte
	binary.LittleEndian.PutUint64(data[:], cursors.Read)
	binary.LittleEndian.PutUint64(data[8:], cursors.Write)
	if _, err := ss.file.WriteAt(data[:], 0); err != nil {
		return err
	}

	// Cache cursors for future GetCursors() calls.
	ss.cursors = cursors
	return nil
}

func (ss *fileStateStore) Allocate(sizeBytes int64) (uint64, error) {
	cursors := ss.cursors
	offset := cursors.Write

	// Adjust cursor to new offset.
	cursors.Write += uint64(sizeBytes)
	if cursors.Read > cursors.Write {
		// Overflow of the write counter. Reset.
		cursors.Read = cursors.Write
	} else if cursors.Read+ss.dataSize < cursors.Write {
		// Invalidate data that is about to be overwritten.
		cursors.Read = cursors.Write - ss.dataSize
	}
	return offset, ss.put(cursors)
}

func (ss *fileStateStore) Invalidate(offset uint64, sizeBytes int64) error {
	cursors := ss.cursors
	cursors.Read = offset + uint64(sizeBytes)
	if cursors.Write < cursors.Read {
		// Overflow of the read counter. Reset.
		cursors.Write = cursors.Read
	}
	return ss.put(cursors)
}
