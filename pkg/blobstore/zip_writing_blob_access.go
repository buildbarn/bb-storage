package blobstore

import (
	"bufio"
	"context"
	"encoding/binary"
	"hash/crc32"
	"io"
	"sync"

	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/capabilities"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// zippedFileAccessInfo stores the metadata that's needed by Get() to
// reobtain the contents of an object stored through Put().
type zippedFileAccessInfo struct {
	dataOffsetBytes int64
	dataSizeBytes   int64
}

// zippedFileFinalizeInfo stores the metadata that's needed by
// Finalize() to emit a central directory that's appended to the end of
// the ZIP archive.
type zippedFileFinalizeInfo struct {
	key               string
	headerOffsetBytes uint64
	dataSizeBytes     uint64
	crc32             uint32
}

// ReadWriterAt is a file that can be randomly read and written.
type ReadWriterAt interface {
	io.ReaderAt
	io.WriterAt
}

// ZIPWritingBlobAccess is an implementation of BlobAccess that stores
// all objects in a ZIP archive. The resulting ZIP archives can be read
// using NewZIPReadingBlobAccess().
type ZIPWritingBlobAccess struct {
	capabilities.Provider
	readBufferFactory ReadBufferFactory
	digestKeyFormat   digest.KeyFormat
	rw                ReadWriterAt

	lock             sync.Mutex
	filesAccess      map[string]zippedFileAccessInfo
	filesFinalize    []zippedFileFinalizeInfo
	writeOffsetBytes int64
	finalized        bool
}

var _ BlobAccess = &ZIPWritingBlobAccess{}

// NewZIPWritingBlobAccess creates a new BlobAccess that stores all
// objects in a ZIP archive. In its initial state, the resulting ZIP
// file will be empty.
func NewZIPWritingBlobAccess(capabilitiesProvider capabilities.Provider, readBufferFactory ReadBufferFactory, digestKeyFormat digest.KeyFormat, rw ReadWriterAt) *ZIPWritingBlobAccess {
	return &ZIPWritingBlobAccess{
		Provider:          capabilitiesProvider,
		readBufferFactory: readBufferFactory,
		digestKeyFormat:   digestKeyFormat,
		rw:                rw,

		filesAccess: map[string]zippedFileAccessInfo{},
	}
}

// Get the contents of an object that was successfully stored in the ZIP
// archive through a previous call to Put().
func (ba *ZIPWritingBlobAccess) Get(ctx context.Context, blobDigest digest.Digest) buffer.Buffer {
	key := blobDigest.GetKey(ba.digestKeyFormat)
	ba.lock.Lock()
	file, ok := ba.filesAccess[key]
	ba.lock.Unlock()
	if !ok {
		return buffer.NewBufferFromError(status.Errorf(codes.NotFound, "File %#v not found in ZIP archive", key))
	}

	return ba.readBufferFactory.NewBufferFromReaderAt(
		blobDigest,
		nopAtCloser{ReaderAt: io.NewSectionReader(ba.rw, file.dataOffsetBytes, file.dataSizeBytes)},
		file.dataSizeBytes,
		buffer.Irreparable(blobDigest))
}

// Put a new object in the ZIP archive.
func (ba *ZIPWritingBlobAccess) Put(ctx context.Context, blobDigest digest.Digest, b buffer.Buffer) error {
	key := blobDigest.GetKey(ba.digestKeyFormat)
	dataSizeBytes, err := b.GetSizeBytes()
	if err != nil {
		b.Discard()
		return err
	}

	// Construct the full header to place before the file contents.
	localZIP64ExtraField := [...]byte{
		// Tag for this "extra" block type.
		0x01, 0x00,
		// Size of this "extra" block: 16 bytes.
		16, 0,
		// Uncompressed file size. Filled in below.
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		// Compressed file size. Filled in below.
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
	}
	binary.LittleEndian.PutUint64(localZIP64ExtraField[4:], uint64(dataSizeBytes))
	binary.LittleEndian.PutUint64(localZIP64ExtraField[12:], uint64(dataSizeBytes))
	localFileHeader := [...]byte{
		// Local file header signature.
		0x50, 0x4b, 0x03, 0x04,
		// Version needed to extract: v4.5 or later, which is needed
		// for ZIP64 support.
		45, 0,
		// General purpose bit flags:
		// - Bit 11: use UTF-8 filenames.
		0x00, 0x08,
		// Compression method: STORE (uncompressed), as this allows
		// fast random access.
		0x00, 0x00,
		// Last file modification time.
		0x00, 0x00,
		// Last file modification date.
		0x00, 0x00,
		// CRC-32. The actual value is filled in later.
		0x00, 0x00, 0x00, 0x00,
		// Compressed and uncompressed file size. Set to
		// 0xffffffff, as the actual size is stored in the ZIP64
		// extended information extra field.
		0xff, 0xff, 0xff, 0xff,
		0xff, 0xff, 0xff, 0xff,
		// Filename length. Filled in below.
		0x00, 0x00,
		// Extra field length.
		byte(len(localZIP64ExtraField)), 0x00,
	}
	binary.LittleEndian.PutUint16(localFileHeader[26:], uint16(len(key)))
	fullHeader := append(append(localFileHeader[:], key...), localZIP64ExtraField[:]...)

	// Allocate space.
	ba.lock.Lock()
	if ba.finalized {
		ba.lock.Unlock()
		b.Discard()
		return status.Error(codes.Unavailable, "ZIP archive has already been finalized")
	}
	headerOffsetBytes := ba.writeOffsetBytes
	dataOffsetBytes := headerOffsetBytes + int64(len(fullHeader))
	ba.writeOffsetBytes = dataOffsetBytes + dataSizeBytes
	ba.lock.Unlock()

	// Ingest data, while at the same time computing a CRC32.
	hasher := crc32.NewIEEE()
	if err := b.IntoWriter(io.MultiWriter(&sectionWriter{
		w:           ba.rw,
		offsetBytes: dataOffsetBytes,
	}, hasher)); err != nil {
		return err
	}

	// Write the local file header that needs to go before the data.
	crc32 := hasher.Sum32()
	binary.LittleEndian.PutUint32(fullHeader[14:], crc32)
	if _, err := ba.rw.WriteAt(fullHeader, headerOffsetBytes); err != nil {
		return util.StatusWrap(err, "Failed to write ZIP local file header")
	}

	ba.lock.Lock()
	defer ba.lock.Unlock()

	if ba.finalized {
		return status.Error(codes.Unavailable, "ZIP archive has already been finalized")
	}
	ba.filesAccess[key] = zippedFileAccessInfo{
		dataOffsetBytes: dataOffsetBytes,
		dataSizeBytes:   dataSizeBytes,
	}
	ba.filesFinalize = append(ba.filesFinalize, zippedFileFinalizeInfo{
		key:               key,
		headerOffsetBytes: uint64(headerOffsetBytes),
		dataSizeBytes:     uint64(dataSizeBytes),
		crc32:             crc32,
	})
	return nil
}

// FindMissing reports which objects are absent from a ZIP archive,
// given a set of digests.
func (ba *ZIPWritingBlobAccess) FindMissing(ctx context.Context, digests digest.Set) (digest.Set, error) {
	ba.lock.Lock()
	defer ba.lock.Unlock()

	missing := digest.NewSetBuilder()
	for _, fileDigest := range digests.Items() {
		if _, ok := ba.filesAccess[fileDigest.GetKey(ba.digestKeyFormat)]; !ok {
			missing.Add(fileDigest)
		}
	}
	return missing.Build(), nil
}

// Finalize the ZIP archive by appending a central directory to the
// underlying file. Once called, it is no longer possible to call Put().
func (ba *ZIPWritingBlobAccess) Finalize() error {
	ba.lock.Lock()
	ba.finalized = true
	ba.lock.Unlock()

	bufferedWriter := bufio.NewWriter(&sectionWriter{
		w:           ba.rw,
		offsetBytes: ba.writeOffsetBytes,
	})

	// Write central directory headers.
	countingWriter := countingWriter{w: bufferedWriter}
	for _, file := range ba.filesFinalize {
		centralZIP64ExtraField := [...]byte{
			// Tag for this "extra" block type.
			0x01, 0x00,
			// Size of this "extra" block: 16 bytes.
			24, 0,
			// Uncompressed file size. Filled in below.
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
			// Compressed file size. Filled in below.
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
			// Offset of the local file header. Filled in below.
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		}
		binary.LittleEndian.PutUint64(centralZIP64ExtraField[4:], file.dataSizeBytes)
		binary.LittleEndian.PutUint64(centralZIP64ExtraField[12:], file.dataSizeBytes)
		binary.LittleEndian.PutUint64(centralZIP64ExtraField[20:], file.headerOffsetBytes)
		centralDirectoryHeader := [...]byte{
			// Central file header signature.
			0x50, 0x4b, 0x01, 0x02,
			// Creator version, and version needed to
			// extract: v4.5 or later, which is needed for
			// ZIP64 support.
			45, 0,
			45, 0,
			// General purpose bit flags:
			// - Bit 11: use UTF-8 filenames.
			0x00, 0x08,
			// Compression method: STORE (uncompressed), as
			// this allows fast random access.
			0x00, 0x00,
			// Last file modification time.
			0x00, 0x00,
			// Last file modification date.
			0x00, 0x00,
			// CRC-32. The actual value is filled in later.
			0x00, 0x00, 0x00, 0x00,
			// Compressed and uncompressed file size. Set to
			// 0xffffffff, as the actual size is stored in
			// the ZIP64 extended information extra field.
			0xff, 0xff, 0xff, 0xff,
			0xff, 0xff, 0xff, 0xff,
			// Filename length. Filled in below.
			0x00, 0x00,
			// Extra field length.
			byte(len(centralZIP64ExtraField)), 0x00,
			// File comment length.
			0x00, 0x00,
			// Disk number start.
			0x00, 0x00,
			// Internal file attributes.
			0x00, 0x00,
			// External file attributes.
			0x00, 0x00, 0x00, 0x00,
			// Relative offset of local file header.
			0xff, 0xff, 0xff, 0xff,
		}
		binary.LittleEndian.PutUint32(centralDirectoryHeader[16:], file.crc32)
		binary.LittleEndian.PutUint16(centralDirectoryHeader[28:], uint16(len(file.key)))
		if _, err := countingWriter.Write(centralDirectoryHeader[:]); err != nil {
			return err
		}
		if _, err := countingWriter.WriteString(file.key); err != nil {
			return err
		}
		if _, err := countingWriter.Write(centralZIP64ExtraField[:]); err != nil {
			return err
		}
	}

	// Write ZIP64 end of central directory record, ZIP64
	// end of central directory locator, and end of
	// central directory record.
	end := [...]byte{
		// ZIP64 end of central directory signature.
		0x50, 0x4b, 0x06, 0x06,
		// Size of ZIP64 end of central directory record.
		44, 0, 0, 0, 0, 0, 0, 0,
		// Creator version, and version needed to extract.
		45, 0,
		45, 0,
		// Number of this disk.
		0x00, 0x00, 0x00, 0x00,
		// Number of the disk with the start of the central
		// directory.
		0x00, 0x00, 0x00, 0x00,
		// Total number of entries in the central directory on
		// this disk. Filled in below.
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		// Total number of entries in the central directory.
		// Filled in below.
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		// Size of the central directory. Filled in below.
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		// Offset of the start of the central directory. Filled
		// in below.
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,

		// ZIP64 end of central directory locator signature.
		0x50, 0x4b, 0x06, 0x07,
		// Number of the disk with the start of the ZIP64 end of
		// central directory.
		0x00, 0x00, 0x00, 0x00,
		// End of central directory record. Filled in below.
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		// Total number of disks.
		1, 0, 0, 0,

		// End of central directory record signature.
		0x50, 0x4b, 0x05, 0x06,
		// Number of this disk.
		0x00, 0x00,
		// Number of the disk with the start of the central
		// directory.
		0x00, 0x00,
		// Number of entries in the central directory on this
		// disk.
		0xff, 0xff,
		// Number of entries in the central directory.
		0xff, 0xff,
		// Size of the central directory.
		0xff, 0xff, 0xff, 0xff,
		// Offset of the start of the central directory.
		0xff, 0xff, 0xff, 0xff,
		// ZIP file comment length.
		0x00, 0x00,
	}
	binary.LittleEndian.PutUint64(end[24:], uint64(len(ba.filesFinalize)))
	binary.LittleEndian.PutUint64(end[32:], uint64(len(ba.filesFinalize)))
	binary.LittleEndian.PutUint64(end[40:], countingWriter.sizeBytes)
	binary.LittleEndian.PutUint64(end[48:], uint64(ba.writeOffsetBytes))
	binary.LittleEndian.PutUint64(end[64:], uint64(ba.writeOffsetBytes)+countingWriter.sizeBytes)
	if _, err := bufferedWriter.Write(end[:]); err != nil {
		return err
	}
	return bufferedWriter.Flush()
}

// sectionWriter is an implementation of io.Writer on top of an
// io.WriterAt. Writes are performed using a write cursor.
type sectionWriter struct {
	w           io.WriterAt
	offsetBytes int64
}

func (w *sectionWriter) Write(p []byte) (int, error) {
	n, err := w.w.WriteAt(p, w.offsetBytes)
	w.offsetBytes += int64(n)
	return n, err
}

// countingWriter is an implementation of io.Writer that that counts the
// amount of data written.
type countingWriter struct {
	w         *bufio.Writer
	sizeBytes uint64
}

func (w *countingWriter) Write(p []byte) (int, error) {
	n, err := w.w.Write(p)
	w.sizeBytes += uint64(n)
	return n, err
}

func (w *countingWriter) WriteString(s string) (int, error) {
	n, err := w.w.WriteString(s)
	w.sizeBytes += uint64(n)
	return n, err
}
