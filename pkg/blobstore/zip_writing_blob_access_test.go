package blobstore_test

import (
	"archive/zip"
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/buildbarn/bb-storage/internal/mock"
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestZIPWritingBlobAccess(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	capabilitiesProvider := mock.NewMockCapabilitiesProvider(ctrl)
	readBufferFactory := mock.NewMockReadBufferFactory(ctrl)
	capturingReadWriter := mock.NewMockFileReadWriter(ctrl)
	blobAccess := blobstore.NewZIPWritingBlobAccess(
		capabilitiesProvider,
		readBufferFactory,
		digest.KeyWithoutInstance,
		capturingReadWriter)

	// Multiplex all WriteAt() calls below into a buffer, so that we
	// can also do tests against the fully generated ZIP file.
	var contents []byte
	readWriter := mock.NewMockReadWriterAt(ctrl)
	capturingReadWriter.EXPECT().ReadAt(gomock.Any(), gomock.Any()).
		DoAndReturn(readWriter.ReadAt).
		AnyTimes()
	capturingReadWriter.EXPECT().WriteAt(gomock.Any(), gomock.Any()).
		DoAndReturn(func(p []byte, offsetBytes int64) (int, error) {
			newLength := int(offsetBytes) + len(p)
			if len(contents) < newLength {
				contents = append(contents, make([]byte, newLength-len(contents))...)
			}
			copy(contents[offsetBytes:], p)
			return readWriter.WriteAt(p, offsetBytes)
		}).
		AnyTimes()

	t.Run("PutBeforeFinalize", func(t *testing.T) {
		t.Run("Initial", func(t *testing.T) {
			// The initial file should be written at the
			// very start of the file.
			readWriter.EXPECT().WriteAt([]byte{
				// Local file header signature.
				0x50, 0x4b, 0x03, 0x04,
				// Version needed to extract: v4.5.
				0x2d, 0x00,
				// General purpose bit flags: use UTF-8 filenames.
				0x00, 0x08,
				// Compression method: STORE (uncompressed).
				0x00, 0x00,
				// Last file modification time.
				0x00, 0x00,
				// Last file modification date.
				0x00, 0x00,
				// CRC-32.
				0x82, 0x89, 0xd1, 0xf7,
				// 32-bit compressed and uncompressed file size.
				0xff, 0xff, 0xff, 0xff,
				0xff, 0xff, 0xff, 0xff,
				// Filename length.
				0x42, 0x00,
				// Extra field length.
				0x14, 0x00,
				// Filename.
				0x31, 0x38, 0x35, 0x66, 0x38, 0x64, 0x62, 0x33,
				0x32, 0x32, 0x37, 0x31, 0x66, 0x65, 0x32, 0x35,
				0x66, 0x35, 0x36, 0x31, 0x61, 0x36, 0x66, 0x63,
				0x39, 0x33, 0x38, 0x62, 0x32, 0x65, 0x32, 0x36,
				0x34, 0x33, 0x30, 0x36, 0x65, 0x63, 0x33, 0x30,
				0x34, 0x65, 0x64, 0x61, 0x35, 0x31, 0x38, 0x30,
				0x30, 0x37, 0x64, 0x31, 0x37, 0x36, 0x34, 0x38,
				0x32, 0x36, 0x33, 0x38, 0x31, 0x39, 0x36, 0x39,
				0x2d, 0x35,
				// Tag for the ZIP64 extended information extra field.
				0x01, 0x00,
				// Size of the ZIP64 extended information field.
				0x10, 0x00,
				// 64-bit uncompressed and compressed file size.
				0x05, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
				0x05, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
			}, int64(0)).Return(116, nil)
			readWriter.EXPECT().WriteAt([]byte("Hello"), int64(116)).Return(5, nil)

			require.NoError(
				t,
				blobAccess.Put(
					ctx,
					digest.MustNewDigest("example", "185f8db32271fe25f561a6fc938b2e264306ec304eda518007d1764826381969", 5),
					buffer.NewValidatedBufferFromByteSlice([]byte("Hello"))))
		})

		t.Run("Successive", func(t *testing.T) {
			// Space for successive files should be
			// allocated right after the previous one.
			readWriter.EXPECT().WriteAt([]byte{
				// Local file header signature.
				0x50, 0x4b, 0x03, 0x04,
				// Version needed to extract: v4.5.
				0x2d, 0x00,
				// General purpose bit flags: use UTF-8 filenames.
				0x00, 0x08,
				// Compression method: STORE (uncompressed).
				0x00, 0x00,
				// Last file modification time.
				0x00, 0x00,
				// Last file modification date.
				0x00, 0x00,
				// CRC-32.
				0xf5, 0x7e, 0x4e, 0x90,
				// 32-bit compressed and uncompressed file size.
				0xff, 0xff, 0xff, 0xff,
				0xff, 0xff, 0xff, 0xff,
				// Filename length.
				0x22, 0x00,
				// Extra field length.
				0x14, 0x00,
				// Filename.
				0x65, 0x62, 0x62, 0x62, 0x62, 0x30, 0x39, 0x39,
				0x65, 0x39, 0x64, 0x32, 0x66, 0x37, 0x38, 0x39,
				0x32, 0x64, 0x39, 0x37, 0x61, 0x62, 0x33, 0x36,
				0x34, 0x30, 0x61, 0x65, 0x38, 0x32, 0x38, 0x33,
				0x2d, 0x39,
				// Tag for the ZIP64 extended information extra field.
				0x01, 0x00,
				// Size of the ZIP64 extended information field.
				0x10, 0x00,
				// 64-bit uncompressed and compressed file size.
				0x09, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
				0x09, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
			}, int64(121)).Return(84, nil)
			readWriter.EXPECT().WriteAt([]byte("Buildbarn"), int64(205)).Return(9, nil)

			require.NoError(
				t,
				blobAccess.Put(
					ctx,
					digest.MustNewDigest("example", "ebbbb099e9d2f7892d97ab3640ae8283", 9),
					buffer.NewValidatedBufferFromByteSlice([]byte("Buildbarn"))))
		})
	})

	t.Run("Get", func(t *testing.T) {
		t.Run("NotFound", func(t *testing.T) {
			// Attempt to access a file that wasn't stored.
			_, err := blobAccess.
				Get(ctx, digest.MustNewDigest("example", "cac4074a2a428b50fe9588583544ac5412c61b34", 42)).
				ToByteSlice(1000)
			testutil.RequireEqualStatus(t, status.Error(codes.NotFound, "File \"cac4074a2a428b50fe9588583544ac5412c61b34-42\" not found in ZIP archive"), err)
		})

		t.Run("Success", func(t *testing.T) {
			// Attempt to access a file that was written
			// using Put() previously.
			fileDigest := digest.MustNewDigest("example", "185f8db32271fe25f561a6fc938b2e264306ec304eda518007d1764826381969", 5)
			readBufferFactory.EXPECT().NewBufferFromReaderAt(fileDigest, gomock.Any(), int64(5), gomock.Any()).
				DoAndReturn(blobstore.CASReadBufferFactory.NewBufferFromReaderAt)
			readWriter.EXPECT().ReadAt(gomock.Len(5), int64(116)).
				DoAndReturn(func(p []byte, offset int64) (int, error) {
					return copy(p, "Hello"), nil
				})

			data, err := blobAccess.Get(ctx, fileDigest).ToByteSlice(1000)
			require.NoError(t, err)
			require.Equal(t, []byte("Hello"), data)
		})
	})

	t.Run("FindMissing", func(t *testing.T) {
		missing, err := blobAccess.FindMissing(
			ctx,
			digest.NewSetBuilder().
				Add(digest.MustNewDigest("example", "185f8db32271fe25f561a6fc938b2e264306ec304eda518007d1764826381969", 5)).
				Add(digest.MustNewDigest("example", "ebbbb099e9d2f7892d97ab3640ae8283", 9)).
				Add(digest.MustNewDigest("example", "5479092d62cdb7c8e8eedef4a5eb2164", 720)).
				Build())
		require.NoError(t, err)
		require.Equal(t, digest.MustNewDigest("example", "5479092d62cdb7c8e8eedef4a5eb2164", 720).ToSingletonSet(), missing)
	})

	t.Run("Finalize", func(t *testing.T) {
		readWriter.EXPECT().WriteAt([]byte{
			// Central directory header signature.
			0x50, 0x4b, 0x01, 0x02,
			// Creator version, and version needed to extract.
			0x2d, 0x00,
			0x2d, 0x00,
			// General purpose bit flags:
			// - Bit 11: use UTF-8 filenames.
			0x00, 0x08,
			// Compression method: STORE (uncompressed).
			0x00, 0x00,
			// Last file modification time.
			0x00, 0x00,
			// Last file modification date.
			0x00, 0x00,
			// CRC-32.
			0x82, 0x89, 0xd1, 0xf7,
			// 32-bit compressed and uncompressed file size.
			0xff, 0xff, 0xff, 0xff,
			0xff, 0xff, 0xff, 0xff,
			// Filename length.
			0x42, 0x00,
			// Extra length.
			0x1c, 0x00,
			// File comment length.
			0x00, 0x00,
			// Disk number start.
			0x00, 0x00,
			// Internal file attributes.
			0x00, 0x00,
			// External file attributes.
			0x00, 0x00, 0x00, 0x00,
			// 32-bit offset of local file header.
			0xff, 0xff, 0xff, 0xff,
			// Filename.
			0x31, 0x38, 0x35, 0x66, 0x38, 0x64, 0x62, 0x33, 0x32,
			0x32, 0x37, 0x31, 0x66, 0x65, 0x32, 0x35, 0x66, 0x35,
			0x36, 0x31, 0x61, 0x36, 0x66, 0x63, 0x39, 0x33, 0x38,
			0x62, 0x32, 0x65, 0x32, 0x36, 0x34, 0x33, 0x30, 0x36,
			0x65, 0x63, 0x33, 0x30, 0x34, 0x65, 0x64, 0x61, 0x35,
			0x31, 0x38, 0x30, 0x30, 0x37, 0x64, 0x31, 0x37, 0x36,
			0x34, 0x38, 0x32, 0x36, 0x33, 0x38, 0x31, 0x39, 0x36,
			0x39, 0x2d, 0x35,
			// Tag for the ZIP64 extended information extra field.
			0x01, 0x00,
			// Size of the ZIP64 extended information field.
			0x18, 0x00,
			// 64-bit uncompressed and compressed file size.
			0x05, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
			0x05, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
			// 64-bit offset of local file header.
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,

			// Central directory header signature.
			0x50, 0x4b, 0x01, 0x02,
			// Creator version, and version needed to extract.
			0x2d, 0x00,
			0x2d, 0x00,
			// General purpose bit flags:
			// - Bit 11: use UTF-8 filenames.
			0x00, 0x08,
			// Compression method: STORE (uncompressed).
			0x00, 0x00,
			// Last file modification time.
			0x00, 0x00,
			// Last file modification date.
			0x00, 0x00,
			// CRC-32.
			0xf5, 0x7e, 0x4e, 0x90,
			// 32-bit compressed and uncompressed file size.
			0xff, 0xff, 0xff, 0xff,
			0xff, 0xff, 0xff, 0xff,
			// Filename length.
			0x22, 0x00,
			// Extra length.
			0x1c, 0x00,
			// File comment length.
			0x00, 0x00,
			// Disk number start.
			0x00, 0x00,
			// Internal file attributes.
			0x00, 0x00,
			// External file attributes.
			0x00, 0x00, 0x00, 0x00,
			// 32-bit offset of local file header.
			0xff, 0xff, 0xff, 0xff,
			// Filename.
			0x65, 0x62, 0x62, 0x62, 0x62, 0x30, 0x39, 0x39, 0x65,
			0x39, 0x64, 0x32, 0x66, 0x37, 0x38, 0x39, 0x32, 0x64,
			0x39, 0x37, 0x61, 0x62, 0x33, 0x36, 0x34, 0x30, 0x61,
			0x65, 0x38, 0x32, 0x38, 0x33, 0x2d, 0x39,
			// Tag for the ZIP64 extended information extra field.
			0x01, 0x00,
			// Size of the ZIP64 extended information field.
			0x18, 0x00,
			// 64-bit uncompressed and compressed file size.
			0x09, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
			0x09, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
			// 64-bit offset of local file header.
			0x79, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,

			// ZIP64 end of central directory record signature.
			0x50, 0x4b, 0x06, 0x06,
			// Size of ZIP64 end of central directory record.
			0x2c, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
			// Creator version, and version needed to extract.
			0x2d, 0x00,
			0x2d, 0x00,
			// Number of this disk.
			0x00, 0x00, 0x00, 0x00,
			// Number of the disk with the start of the
			// central directory.
			0x00, 0x00, 0x00, 0x00,
			// 64-bit number of entries in the central
			// directory on this disk.
			0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
			// 64-bit number  of entries in the central
			// directory.
			0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
			// Size of the central directory.
			0xf8, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
			// Offset of the start of the central directory.
			0xd6, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,

			// ZIP64 end of central directory locator signature.
			0x50, 0x4b, 0x06, 0x07,
			// Number of the disk with the start of the
			// ZIP64 end of central directory.
			0x00, 0x00, 0x00, 0x00,
			// 64-bit offset of the end of the central directory.
			0xce, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
			// Total number of disks.
			0x01, 0x00, 0x00, 0x00,

			// End of central directory record signature.
			0x50, 0x4b, 0x05, 0x06,
			// Number of this disk.
			0x00, 0x00,
			// Number of the disk with the start of the
			// central directory.
			0x00, 0x00,
			// 16-bit number of entries in the central
			// directory on this disk.
			0xff, 0xff,
			// 16-bit number of entries in the central directory.
			0xff, 0xff,
			// 32-bit size of the central directory.
			0xff, 0xff, 0xff, 0xff,
			// 32-bit offset of the end of the central directory.
			0xff, 0xff, 0xff, 0xff,
			// ZIP file comment length.
			0x00, 0x00,
		}, int64(214)).Return(346, nil)

		require.NoError(t, blobAccess.Finalize())

		// Validate that the ZIP file can be parsed properly
		// using Go's "archive/zip" package.
		zipReader, err := zip.NewReader(bytes.NewReader(contents), int64(len(contents)))
		require.NoError(t, err)

		files := zipReader.File
		require.Len(t, files, 2)

		require.Equal(t, "185f8db32271fe25f561a6fc938b2e264306ec304eda518007d1764826381969-5", files[0].Name)
		require.False(t, files[0].NonUTF8)
		require.Equal(t, zip.Store, files[0].Method)
		require.Equal(t, uint32(0xf7d18982), files[0].CRC32)
		require.Equal(t, uint64(5), files[0].CompressedSize64)
		require.Equal(t, uint64(5), files[0].UncompressedSize64)
		r, err := files[0].Open()
		require.NoError(t, err)
		data, err := io.ReadAll(r)
		require.NoError(t, err)
		require.Equal(t, []byte("Hello"), data)
		require.NoError(t, r.Close())

		require.Equal(t, "ebbbb099e9d2f7892d97ab3640ae8283-9", files[1].Name)
		require.False(t, files[1].NonUTF8)
		require.Equal(t, zip.Store, files[1].Method)
		require.Equal(t, uint32(0x904e7ef5), files[1].CRC32)
		require.Equal(t, uint64(9), files[1].CompressedSize64)
		require.Equal(t, uint64(9), files[1].UncompressedSize64)
		r, err = files[1].Open()
		require.NoError(t, err)
		data, err = io.ReadAll(r)
		require.NoError(t, err)
		require.Equal(t, []byte("Buildbarn"), data)
		require.NoError(t, r.Close())
	})

	t.Run("PutAfterFinalize", func(t *testing.T) {
		reader := mock.NewMockReadAtCloser(ctrl)
		reader.EXPECT().Close()

		testutil.RequireEqualStatus(
			t,
			status.Error(codes.Unavailable, "ZIP archive has already been finalized"),
			blobAccess.Put(
				ctx,
				digest.MustNewDigest("example", "848c3b8d79097873c69534c194bd666a", 3000),
				buffer.NewValidatedBufferFromReaderAt(reader, 3000)))
	})
}
