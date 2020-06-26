package buffer_test

import (
	"bytes"
	"io"
	"testing"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/internal/mock"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/golang/mock/gomock"
	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestNewValidatedBufferFromFileReaderGetSizeBytes(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	reader := mock.NewMockFileReader(ctrl)
	reader.EXPECT().Close()

	b := buffer.NewValidatedBufferFromFileReader(reader, 123)
	n, err := b.GetSizeBytes()
	require.NoError(t, err)
	require.Equal(t, int64(123), n)
	b.Discard()
}

func TestNewValidatedBufferFromFileReaderIntoWriter(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	t.Run("Success", func(t *testing.T) {
		reader := mock.NewMockFileReader(ctrl)
		gomock.InOrder(
			reader.EXPECT().ReadAt(gomock.Any(), int64(0)).DoAndReturn(func(p []byte, off int64) (int, error) {
				return copy(p, []byte("Hello")), nil
			}),
			reader.EXPECT().Close(),
		)
		writer := bytes.NewBuffer(nil)

		err := buffer.NewValidatedBufferFromFileReader(reader, 5).IntoWriter(writer)
		require.NoError(t, err)
		require.Equal(t, []byte("Hello"), writer.Bytes())
	})

	t.Run("IOError", func(t *testing.T) {
		reader := mock.NewMockFileReader(ctrl)
		gomock.InOrder(
			reader.EXPECT().ReadAt(gomock.Any(), int64(0)).Return(0, status.Error(codes.Internal, "Storage backend on fire")),
			reader.EXPECT().Close(),
		)
		writer := bytes.NewBuffer(nil)

		err := buffer.NewValidatedBufferFromFileReader(reader, 10).IntoWriter(writer)
		require.Equal(t, status.Error(codes.Internal, "Storage backend on fire"), err)
	})
}

func TestNewValidatedBufferFromFileReaderReadAt(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	t.Run("Success", func(t *testing.T) {
		reader := mock.NewMockFileReader(ctrl)
		gomock.InOrder(
			reader.EXPECT().ReadAt(gomock.Any(), int64(0)).DoAndReturn(func(p []byte, off int64) (int, error) {
				return copy(p, []byte("Hello")), nil
			}),
			reader.EXPECT().Close(),
		)

		var p [5]byte
		n, err := buffer.NewValidatedBufferFromFileReader(reader, 5).ReadAt(p[:], 0)
		require.Equal(t, 5, n)
		require.NoError(t, err)
		require.Equal(t, []byte("Hello"), p[:])
	})

	t.Run("IOFailure", func(t *testing.T) {
		reader := mock.NewMockFileReader(ctrl)
		reader.EXPECT().ReadAt(gomock.Any(), int64(0)).Return(0, status.Error(codes.Internal, "Server on fire"))
		reader.EXPECT().Close()

		var p [5]byte
		n, err := buffer.NewValidatedBufferFromFileReader(reader, 5).ReadAt(p[:], 0)
		require.Equal(t, 0, n)
		require.Equal(t, status.Error(codes.Internal, "Server on fire"), err)
	})
}

func TestNewValidatedBufferFromFileReaderToProto(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	t.Run("Success", func(t *testing.T) {
		reader := mock.NewMockFileReader(ctrl)
		gomock.InOrder(
			reader.EXPECT().ReadAt(gomock.Any(), int64(0)).DoAndReturn(func(p []byte, off int64) (int, error) {
				return copy(p, exampleActionResultBytes), nil
			}),
			reader.EXPECT().Close(),
		)

		actionResult, err := buffer.NewValidatedBufferFromFileReader(reader, int64(len(exampleActionResultBytes))).
			ToProto(&remoteexecution.ActionResult{}, len(exampleActionResultBytes))
		require.NoError(t, err)
		require.True(t, proto.Equal(&exampleActionResultMessage, actionResult))
	})

	t.Run("InvalidProtobuf", func(t *testing.T) {
		reader := mock.NewMockFileReader(ctrl)
		gomock.InOrder(
			reader.EXPECT().ReadAt(gomock.Any(), int64(0)).DoAndReturn(func(p []byte, off int64) (int, error) {
				return copy(p, []byte("Hello")), nil
			}),
			reader.EXPECT().Close(),
		)

		_, err := buffer.NewValidatedBufferFromFileReader(reader, 5).
			ToProto(&remoteexecution.ActionResult{}, len(exampleActionResultBytes))
		require.Equal(t, status.Error(codes.InvalidArgument, "Failed to unmarshal message: proto: can't skip unknown wire type 4"), err)
	})

	t.Run("IOFailure", func(t *testing.T) {
		reader := mock.NewMockFileReader(ctrl)
		gomock.InOrder(
			reader.EXPECT().ReadAt(gomock.Any(), int64(0)).Return(0, status.Error(codes.Internal, "Storage backend on fire")),
			reader.EXPECT().Close(),
		)

		_, err := buffer.NewValidatedBufferFromFileReader(reader, int64(len(exampleActionResultBytes))).
			ToProto(&remoteexecution.ActionResult{}, len(exampleActionResultBytes))
		require.Equal(t, status.Error(codes.Internal, "Storage backend on fire"), err)
	})
}

func TestNewValidatedBufferFromFileReaderToByteSlice(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	// Only test the successful case, as other aspects are already
	// covered by TestNewValidatedBufferFromFileReaderToProto.
	t.Run("Success", func(t *testing.T) {
		reader := mock.NewMockFileReader(ctrl)
		gomock.InOrder(
			reader.EXPECT().ReadAt(gomock.Any(), int64(0)).DoAndReturn(func(p []byte, off int64) (int, error) {
				return copy(p, []byte("Hello")), nil
			}),
			reader.EXPECT().Close(),
		)

		data, err := buffer.NewValidatedBufferFromFileReader(reader, 5).ToByteSlice(10)
		require.NoError(t, err)
		require.Equal(t, []byte("Hello"), data)
	})

	t.Run("Empty", func(t *testing.T) {
		reader := mock.NewMockFileReader(ctrl)
		reader.EXPECT().Close()

		data, err := buffer.NewValidatedBufferFromFileReader(reader, 0).ToByteSlice(10)
		require.NoError(t, err)
		require.Empty(t, data)
	})
}

func TestNewValidatedBufferFromFileReaderToChunkReader(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	t.Run("Success", func(t *testing.T) {
		reader := mock.NewMockFileReader(ctrl)
		gomock.InOrder(
			reader.EXPECT().ReadAt(gomock.Any(), int64(3)).DoAndReturn(func(p []byte, off int64) (int, error) {
				return copy(p, []byte("lo")), nil
			}),
			reader.EXPECT().ReadAt(gomock.Any(), int64(5)).DoAndReturn(func(p []byte, off int64) (int, error) {
				return copy(p, []byte(" w")), nil
			}),
			reader.EXPECT().ReadAt(gomock.Any(), int64(7)).DoAndReturn(func(p []byte, off int64) (int, error) {
				return copy(p, []byte("or")), nil
			}),
			reader.EXPECT().ReadAt(gomock.Any(), int64(9)).DoAndReturn(func(p []byte, off int64) (int, error) {
				return copy(p, []byte("ld")), nil
			}),
			reader.EXPECT().Close(),
		)

		// The ChunkReader returned by ToChunkReader() should
		// omit empty chunks and split up chunks that are too
		// large.
		r := buffer.NewValidatedBufferFromFileReader(reader, 11).ToChunkReader(
			/* offset = */ 3,
			/* chunk size = */ 2)
		chunk, err := r.Read()
		require.NoError(t, err)
		require.Equal(t, []byte("lo"), chunk)
		chunk, err = r.Read()
		require.NoError(t, err)
		require.Equal(t, []byte(" w"), chunk)
		chunk, err = r.Read()
		require.NoError(t, err)
		require.Equal(t, []byte("or"), chunk)
		chunk, err = r.Read()
		require.NoError(t, err)
		require.Equal(t, []byte("ld"), chunk)
		_, err = r.Read()
		require.Equal(t, io.EOF, err)
		_, err = r.Read()
		require.Equal(t, io.EOF, err)
		r.Close()
	})

	t.Run("AtTheEnd", func(t *testing.T) {
		reader := mock.NewMockFileReader(ctrl)
		reader.EXPECT().Close()

		// Reading at the very end is permitted, but should
		// return an end-of-file immediately.
		r := buffer.NewValidatedBufferFromFileReader(reader, 11).ToChunkReader(
			/* offset = */ 11,
			/* chunk size = */ 2)
		_, err := r.Read()
		require.Equal(t, io.EOF, err)
		r.Close()
	})

	t.Run("NegativeOffset", func(t *testing.T) {
		reader := mock.NewMockFileReader(ctrl)
		reader.EXPECT().Close()

		r := buffer.NewValidatedBufferFromFileReader(reader, 11).ToChunkReader(
			/* offset = */ -1,
			/* chunk size = */ 2)
		_, err := r.Read()
		require.Equal(t, status.Error(codes.InvalidArgument, "Negative read offset: -1"), err)
		r.Close()
	})

	t.Run("TooFar", func(t *testing.T) {
		reader := mock.NewMockFileReader(ctrl)
		reader.EXPECT().Close()

		r := buffer.NewValidatedBufferFromFileReader(reader, 11).ToChunkReader(
			/* offset = */ 12,
			/* chunk size = */ 2)
		_, err := r.Read()
		require.Equal(t, status.Error(codes.InvalidArgument, "Buffer is 11 bytes in size, while a read at offset 12 was requested"), err)
		r.Close()
	})

	t.Run("IOFailure", func(t *testing.T) {
		reader := mock.NewMockFileReader(ctrl)
		gomock.InOrder(
			reader.EXPECT().ReadAt(gomock.Any(), int64(3)).Return(0, status.Error(codes.Internal, "Storage backend on fire")),
			reader.EXPECT().Close(),
		)

		r := buffer.NewValidatedBufferFromFileReader(reader, 11).ToChunkReader(
			/* offset = */ 3,
			/* chunk size = */ 2)
		_, err := r.Read()
		require.Equal(t, status.Error(codes.Internal, "Storage backend on fire"), err)
		r.Close()
	})
}

func TestNewValidatedBufferFromFileReaderToReader(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	t.Run("Success", func(t *testing.T) {
		reader := mock.NewMockFileReader(ctrl)
		gomock.InOrder(
			reader.EXPECT().ReadAt(gomock.Any(), int64(0)).DoAndReturn(func(p []byte, off int64) (int, error) {
				return copy(p, []byte("Hel")), nil
			}),
			reader.EXPECT().ReadAt(gomock.Any(), int64(3)).DoAndReturn(func(p []byte, off int64) (int, error) {
				return copy(p, []byte("lo ")), nil
			}),
			reader.EXPECT().ReadAt(gomock.Any(), int64(6)).DoAndReturn(func(p []byte, off int64) (int, error) {
				return copy(p, []byte("wor")), nil
			}),
			reader.EXPECT().ReadAt(gomock.Any(), int64(9)).DoAndReturn(func(p []byte, off int64) (int, error) {
				return copy(p, []byte("ld")), io.EOF
			}),
			reader.EXPECT().Close(),
		)

		r := buffer.NewValidatedBufferFromFileReader(reader, 11).ToReader()
		var p [3]byte
		n, err := r.Read(p[:])
		require.Equal(t, 3, n)
		require.NoError(t, err)
		require.Equal(t, []byte("Hel"), p[:])
		n, err = r.Read(p[:])
		require.Equal(t, 3, n)
		require.NoError(t, err)
		require.Equal(t, []byte("lo "), p[:])
		n, err = r.Read(p[:])
		require.Equal(t, 3, n)
		require.NoError(t, err)
		require.Equal(t, []byte("wor"), p[:])
		n, err = r.Read(p[:])
		require.Equal(t, 2, n)
		require.Equal(t, io.EOF, err)
		require.Equal(t, []byte("ld"), p[:2])
		n, err = r.Read(p[:])
		require.Equal(t, 0, n)
		require.Equal(t, io.EOF, err)
		require.Nil(t, r.Close())
	})

	t.Run("IOFailure", func(t *testing.T) {
		reader := mock.NewMockFileReader(ctrl)
		gomock.InOrder(
			reader.EXPECT().ReadAt(gomock.Any(), int64(0)).Return(0, status.Error(codes.Internal, "Storage backend on fire")),
			reader.EXPECT().Close(),
		)

		r := buffer.NewValidatedBufferFromFileReader(reader, 11).ToReader()
		var p [3]byte
		n, err := r.Read(p[:])
		require.Equal(t, 0, n)
		require.Equal(t, status.Error(codes.Internal, "Storage backend on fire"), err)
		require.Nil(t, r.Close())
	})
}

func TestNewValidatedBufferFromFileReaderCloneCopy(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	// Because NewValidatedBufferFromFileReader() returns a buffer
	// that supports random access, cloned versions of the buffer
	// can do very little to merge read operations. Both the
	// ToByteSlice() calls should trigger a read.
	reader := mock.NewMockFileReader(ctrl)
	gomock.InOrder(
		reader.EXPECT().ReadAt(gomock.Any(), int64(0)).DoAndReturn(func(p []byte, off int64) (int, error) {
			return copy(p, []byte("Hello")), nil
		}).Times(2),
		reader.EXPECT().Close(),
	)

	b1, b2 := buffer.NewValidatedBufferFromFileReader(reader, 5).CloneCopy(10)

	data1, err := b1.ToByteSlice(10)
	require.NoError(t, err)
	require.Equal(t, []byte("Hello"), data1)

	data2, err := b2.ToByteSlice(10)
	require.NoError(t, err)
	require.Equal(t, []byte("Hello"), data2)
}

func TestNewValidatedBufferFromFileReaderCloneStream(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	reader := mock.NewMockFileReader(ctrl)
	gomock.InOrder(
		reader.EXPECT().ReadAt(gomock.Any(), int64(0)).DoAndReturn(func(p []byte, off int64) (int, error) {
			return copy(p, []byte("Hello")), nil
		}).Times(2),
		reader.EXPECT().Close(),
	)

	b1, b2 := buffer.NewValidatedBufferFromFileReader(reader, 5).CloneCopy(10)
	done := make(chan struct{}, 2)

	go func() {
		data, err := b1.ToByteSlice(10)
		require.NoError(t, err)
		require.Equal(t, []byte("Hello"), data)
		done <- struct{}{}
	}()

	go func() {
		data, err := b2.ToByteSlice(10)
		require.NoError(t, err)
		require.Equal(t, []byte("Hello"), data)
		done <- struct{}{}
	}()

	<-done
	<-done
}

func TestNewValidatedBufferFromFileReaderDiscard(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	reader := mock.NewMockFileReader(ctrl)
	reader.EXPECT().Close()

	buffer.NewValidatedBufferFromFileReader(reader, 11).Discard()
}
