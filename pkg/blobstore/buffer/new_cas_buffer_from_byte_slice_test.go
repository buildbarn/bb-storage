package buffer_test

import (
	"testing"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/internal/mock"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// These tests only cover aspects of NewCASBufferFromByteSlice() itself.
// Testing coverage for the actual behavior of the Buffer object is
// provided by TestNewValidatedBufferFromByteSlice*() and
// TestNewBufferFromError*().

func TestNewCASBufferFromByteSliceSuccess(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	identity := make([]byte, 4194305)
	for i := 0; i < len(identity); i++ {
		identity[i] = byte(i)
	}
	for hash, body := range map[string][]byte{
		// MD5:
		"8b1a9953c4611296a827abf8c47804d7": []byte("Hello"),
		// SHA-1:
		"a54d88e06612d820bc3be72877c74f257b561b19": []byte("This is a test"),
		// SHA-256:
		"1d1f71aecd9b2d8127e5a91fc871833fffe58c5c63aceed9f6fd0b71fe732504": []byte("And another test"),
		// SHA-384:
		"8eb24e0851260f9ee83e88a47a0ae76871c8c8a8befdfc39931b42a334cd0fcd595e8e6766ef471e5f2d50b74e041e8d": []byte("Even longer checksums"),
		// SHA-512:
		"b1d33bb21db304209f584b55e1a86db38c7c44c466c680c38805db07a92d43260d0e82ffd0a48c337d40372a4ac5b9be1ff24beef2c990e6ea3f2079d067b0e0": []byte("Ridiculously long checksums"),
		// VSO hashes as observed in the BuildXL source tree:
		"c8de9915376dbac9f79ad7888d3c9448be0f17a0511004f3d4a470f9e94b9f2e00": []byte("hi"),
		"e87891e21cd24671b953bab6a2d6f9c91049c67f9ea0e5015620c3dbc766edc500": []byte("Hello World"),
		"1d6240b6c13ac7b412f81ef6bf26a529c8d9b6bf3ec6d3f9e5305eb922f050f700": []byte("Hello Galaxy"),
		"1e57cf2792a900d06c1cdfb3c453f35bc86f72788aa9724c96c929d1cc6b456a00": identity[:0],
		"3da32150b5e69b54e7ad1765d9573bc5e6e05d3b6529556c1b4a436a76a511f400": identity[:1],
		"4ae1ad6462d75d117a5dafcf98167981371a4b21e1cee49d0b982de2ce01032300": identity[:65535],
		"85840e1cb7cbfd78b464921c54c96f68c19066f20860efa8cce671b40ba5162300": identity[:65536],
		"d92a37c547f9d5b6b7b791a24f587da8189cca14ebc8511d2482e7448763e2bd00": identity[:65537],
		"1c3c73f7e829e84a5ba05631195105fb49e033fa23bda6d379b3e46b5d73ef3700": identity[:2097151],
		"6dae3ed3e623aed293297c289c3d20a53083529138b7631e99920ef0d93af3cd00": identity[:2097152],
		"1f9f3c008ea37ecb65bc5fb14a420cebb3ca72a9601ec056709a6b431f91807100": identity[:2097153],
		"df0e0db15e866592dbfa9bca74e6d547d67789f7eb088839fc1a5cefa862353700": identity[:4194303],
		"5e3a80b2acb2284cd21a08979c49cbb80874e1377940699b07a8abee9175113200": identity[:4194304],
		"b9a44a420593fa18453b3be7b63922df43c93ff52d88f2cab26fe1fadba7003100": identity[:4194305],
	} {
		digest := util.MustNewDigest("fedora29", &remoteexecution.Digest{
			Hash:      hash,
			SizeBytes: int64(len(body)),
		})
		repairFunc := mock.NewMockRepairFunc(ctrl)

		data, err := buffer.NewCASBufferFromByteSlice(
			digest,
			body,
			buffer.Reparable(digest, repairFunc.Call)).ToByteSlice(len(body))
		require.NoError(t, err)
		require.Equal(t, body, data)
	}
}

func TestNewCASBufferFromByteSliceSizeMismatch(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	digest := util.MustNewDigest("ubuntu1804", &remoteexecution.Digest{
		Hash:      "8b1a9953c4611296a827abf8c47804d7",
		SizeBytes: 6,
	})
	repairFunc := mock.NewMockRepairFunc(ctrl)
	repairFunc.EXPECT().Call()

	_, err := buffer.NewCASBufferFromByteSlice(
		digest,
		[]byte("Hello"),
		buffer.Reparable(digest, repairFunc.Call)).ToByteSlice(5)
	require.Equal(t, status.Error(codes.Internal, "Buffer is 5 bytes in size, while 6 bytes were expected"), err)
}

func TestNewCASBufferFromByteSliceHashMismatch(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	digest := util.MustNewDigest("ubuntu1804", &remoteexecution.Digest{
		Hash:      "d41d8cd98f00b204e9800998ecf8427e",
		SizeBytes: 5,
	})
	repairFunc := mock.NewMockRepairFunc(ctrl)
	repairFunc.EXPECT().Call()

	_, err := buffer.NewCASBufferFromByteSlice(
		digest,
		[]byte("Hello"),
		buffer.Reparable(digest, repairFunc.Call)).ToByteSlice(5)
	require.Equal(t, status.Error(codes.Internal, "Buffer has checksum 8b1a9953c4611296a827abf8c47804d7, while d41d8cd98f00b204e9800998ecf8427e was expected"), err)
}
