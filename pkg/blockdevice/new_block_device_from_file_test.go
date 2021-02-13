package blockdevice_test

import (
	"os"
	"path/filepath"
	"runtime/debug"
	"testing"

	"github.com/buildbarn/bb-storage/pkg/blockdevice"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestNewBlockDeviceFromFile(t *testing.T) {
	blockDevicePath := filepath.Join(t.TempDir(), "blockdevice")
	blockDevice, sectorSizeBytes, sectorCount, err := blockdevice.NewBlockDeviceFromFile(blockDevicePath, 123456, true)
	require.NoError(t, err)

	// The sector size should be a power of two, and the number of
	// sectors should be sufficient to hold the required space.
	require.LessOrEqual(t, 512, sectorSizeBytes)
	require.Equal(t, 0, sectorSizeBytes&(sectorSizeBytes-1))
	require.Equal(t, int64((123456+sectorSizeBytes-1)/sectorSizeBytes), sectorCount)

	// The file on disk should have a size that corresponds to the
	// sector size and count.
	fileInfo, err := os.Stat(blockDevicePath)
	require.NoError(t, err)
	require.Equal(t, int64(sectorSizeBytes)*sectorCount, fileInfo.Size())

	// Test read, write and sync operations.
	n, err := blockDevice.WriteAt([]byte("Hello"), 12345)
	require.Equal(t, 5, n)
	require.NoError(t, err)

	var b [16]byte
	n, err = blockDevice.ReadAt(b[:], 12340)
	require.Equal(t, 16, n)
	require.NoError(t, err)
	require.Equal(t, []byte("\x00\x00\x00\x00\x00Hello\x00\x00\x00\x00\x00\x00"), b[:])

	require.NoError(t, blockDevice.Sync())

	// Truncating the file will cause future read access to the
	// memory map underneath the BlockDevice to raise SIGBUS. This
	// may also occur in case of actual I/O errors. These page
	// faults should be caught properly.
	//
	// To be able to implement this, ReadAt() temporary enables the
	// debug.SetPanicOnFault() option. Test that the original value
	// of this option is restored upon completion.
	require.NoError(t, os.Truncate(blockDevicePath, 0))

	debug.SetPanicOnFault(false)
	n, err = blockDevice.ReadAt(b[:], 12340)
	require.False(t, debug.SetPanicOnFault(false))
	require.Equal(t, 0, n)
	require.Equal(t, status.Error(codes.Internal, "Page fault occurred while reading from memory map"), err)

	debug.SetPanicOnFault(true)
	n, err = blockDevice.ReadAt(b[:], 12340)
	require.True(t, debug.SetPanicOnFault(false))
	require.Equal(t, 0, n)
	require.Equal(t, status.Error(codes.Internal, "Page fault occurred while reading from memory map"), err)
}

func TestNewBlockDeviceFromFileNoZeroInitialize(t *testing.T) {
	// Initialize the file that backs the block device with some
	// contents. These contents should be accessible when the block
	// device is created without the zeroInitialize option set.
	blockDevicePath := filepath.Join(t.TempDir(), "blockdevice")
	f, err := os.OpenFile(blockDevicePath, os.O_CREATE|os.O_WRONLY, 0o666)
	require.NoError(t, err)
	_, err = f.Write([]byte("Hello"))
	require.NoError(t, err)
	require.NoError(t, f.Close())

	blockDevice, _, _, err := blockdevice.NewBlockDeviceFromFile(blockDevicePath, 5, false)
	require.NoError(t, err)

	var b [5]byte
	n, err := blockDevice.ReadAt(b[:], 0)
	require.Equal(t, 5, n)
	require.NoError(t, err)
	require.Equal(t, []byte("Hello"), b[:])
}

func TestNewBlockDeviceFromFileZeroInitialize(t *testing.T) {
	// This test is identical to the previous one, except that the
	// block device is opened with the zeroInitialize option set.
	// This means that the original contents of the file that backs
	// the block device are gone.
	blockDevicePath := filepath.Join(t.TempDir(), "blockdevice")
	f, err := os.OpenFile(blockDevicePath, os.O_CREATE|os.O_WRONLY, 0o666)
	require.NoError(t, err)
	_, err = f.Write([]byte("Hello"))
	require.NoError(t, err)
	require.NoError(t, f.Close())

	blockDevice, _, _, err := blockdevice.NewBlockDeviceFromFile(blockDevicePath, 5, true)
	require.NoError(t, err)

	var b [5]byte
	n, err := blockDevice.ReadAt(b[:], 0)
	require.Equal(t, 5, n)
	require.NoError(t, err)
	require.Equal(t, []byte("\x00\x00\x00\x00\x00"), b[:])
}
