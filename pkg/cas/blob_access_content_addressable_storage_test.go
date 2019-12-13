package cas_test

import (
	"context"
	"io"
	"testing"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/internal/mock"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/cas"
	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func TestBlobAccessContentAddressableStoragePutFileSuccess(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)
	defer ctrl.Finish()

	directory := mock.NewMockDirectory(ctrl)
	file := mock.NewMockFileReader(ctrl)
	directory.EXPECT().OpenRead("hello").Return(file, nil)

	// Operations that should appear on the file that is being
	// uploaded. A first pass is used to compute the file's digest.
	// A second pass is used to upload the file's contents. The file
	// may have grown in the meantime, but the second pass should
	// not read beyond the part that was used for digest computation.
	gomock.InOrder(
		file.EXPECT().ReadAt(gomock.Any(), int64(0)).DoAndReturn(
			func(p []byte, off int64) (int, error) {
				require.Greater(t, len(p), 11)
				copy(p, "Hello world")
				return 11, io.EOF
			}),
		file.EXPECT().ReadAt(gomock.Any(), int64(0)).DoAndReturn(
			func(p []byte, off int64) (int, error) {
				require.Len(t, p, 11)
				copy(p, "Hello world")
				return 11, nil
			}),
		file.EXPECT().Close().Return(nil),
	)

	// Operations that should appear against the BlobAccess. Read
	// all the data to ensure all file operations are triggered.
	blobAccess := mock.NewMockBlobAccess(ctrl)
	helloWorldDigest := util.MustNewDigest(
		"default-scheduler",
		&remoteexecution.Digest{
			Hash:      "3e25960a79dbc69b674cd4ec67a72c62",
			SizeBytes: 11,
		})
	blobAccess.EXPECT().Put(ctx, helloWorldDigest, gomock.Any()).DoAndReturn(
		func(ctx context.Context, digest *util.Digest, b buffer.Buffer) error {
			data, err := b.ToByteSlice(100)
			require.NoError(t, err)
			require.Equal(t, []byte("Hello world"), data)
			return nil
		})

	contentAddressableStorage := cas.NewBlobAccessContentAddressableStorage(blobAccess, 1000)
	digest, err := contentAddressableStorage.PutFile(ctx, directory, "hello", util.MustNewDigest(
		"default-scheduler",
		&remoteexecution.Digest{
			Hash:      "d41d8cd98f00b204e9800998ecf8427e",
			SizeBytes: 123,
		}))
	require.NoError(t, err)
	require.Equal(t, digest, helloWorldDigest)
}
