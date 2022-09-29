package util_test

import (
	"bytes"
	"io"
	"testing"

	"github.com/buildbarn/bb-storage/internal/mock"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protowire"
)

func TestVisitProtoBytesFields(t *testing.T) {
	ctrl := gomock.NewController(t)

	t.Run("Empty", func(t *testing.T) {
		visitor := mock.NewMockProtoBytesFieldVisitor(ctrl)

		require.NoError(t, util.VisitProtoBytesFields(bytes.NewBuffer(nil), visitor.Call))
	})

	t.Run("InvalidTag", func(t *testing.T) {
		// The field's tag is stored as a base 128 varint. This
		// means that 0x80 is not a valid tag, as more bytes
		// must follow.
		visitor := mock.NewMockProtoBytesFieldVisitor(ctrl)

		testutil.RequireEqualStatus(
			t,
			status.Error(codes.InvalidArgument, "Field at offset 0 has an invalid tag: unexpected EOF"),
			util.VisitProtoBytesFields(bytes.NewBuffer([]byte{
				// Tag.
				0x80,
			}), visitor.Call))
	})

	t.Run("InvalidType", func(t *testing.T) {
		// Only fields with type 2 (bytes) are supported.
		// Messages containing integer values cannot be visited.
		visitor := mock.NewMockProtoBytesFieldVisitor(ctrl)

		testutil.RequireEqualStatus(
			t,
			status.Error(codes.InvalidArgument, "Field with number 1 at offset 0 has type 0, while 2 was expected"),
			util.VisitProtoBytesFields(bytes.NewBuffer([]byte{
				// Tag == (1 << 3) | 0.
				0x08,
				// Value == 150.
				0x96, 0x01,
			}), visitor.Call))
	})

	t.Run("InvalidSize", func(t *testing.T) {
		// The field's size is also stored as a base 128 varint.
		visitor := mock.NewMockProtoBytesFieldVisitor(ctrl)

		testutil.RequireEqualStatus(
			t,
			status.Error(codes.InvalidArgument, "Field with number 2 at offset 0 has an invalid size: unexpected EOF"),
			util.VisitProtoBytesFields(bytes.NewBuffer([]byte{
				// Tag == (2 << 3) | 2.
				0x12,
				// Size.
				0x80,
			}), visitor.Call))
	})

	t.Run("TruncatedPayloadRead", func(t *testing.T) {
		// Announce a five byte field, but let the contents end
		// after just three bytes. Reading the contents
		// should fail.
		visitor := mock.NewMockProtoBytesFieldVisitor(ctrl)
		visitor.EXPECT().Call(protowire.Number(2), int64(2), int64(5), gomock.Any()).
			DoAndReturn(func(fieldNumber protowire.Number, offsetBytes, sizeBytes int64, fieldReader io.Reader) error {
				_, err := io.ReadAll(fieldReader)
				testutil.RequireEqualStatus(t, status.Error(codes.InvalidArgument, "Field ended prematurely, as 2 more bytes were expected"), err)
				return err
			})

		testutil.RequireEqualStatus(
			t,
			status.Error(codes.InvalidArgument, "Field with number 2 at offset 2 size 5: Field ended prematurely, as 2 more bytes were expected"),
			util.VisitProtoBytesFields(bytes.NewBuffer([]byte{
				// Tag == (2 << 3) | 2.
				0x12,
				// Size.
				0x05,
				// Payload == "Hel".
				0x48, 0x65, 0x6c,
			}), visitor.Call))
	})

	t.Run("TruncatedPayloadDiscard", func(t *testing.T) {
		// Even if the field's contents aren't read, we should
		// report truncated fields, as we can't continue to read
		// the next field afterwards.
		visitor := mock.NewMockProtoBytesFieldVisitor(ctrl)
		visitor.EXPECT().Call(protowire.Number(2), int64(2), int64(5), gomock.Any())

		testutil.RequireEqualStatus(
			t,
			status.Error(codes.InvalidArgument, "Field with number 2 at offset 2 size 5 ended prematurely, as 2 more bytes were expected"),
			util.VisitProtoBytesFields(bytes.NewBuffer([]byte{
				// Tag == (2 << 3) | 2.
				0x12,
				// Size.
				0x05,
				// Payload == "Hel".
				0x48, 0x65, 0x6c,
			}), visitor.Call))
	})

	t.Run("Success", func(t *testing.T) {
		// Successfully process three bytes fields.
		visitor := mock.NewMockProtoBytesFieldVisitor(ctrl)
		visitor.EXPECT().Call(protowire.Number(2), int64(2), int64(5), gomock.Any()).
			DoAndReturn(func(fieldNumber protowire.Number, offsetBytes, sizeBytes int64, fieldReader io.Reader) error {
				data, err := io.ReadAll(fieldReader)
				require.NoError(t, err)
				require.Equal(t, []byte("Hello"), data)
				return nil
			})
		visitor.EXPECT().Call(protowire.Number(3), int64(9), int64(7), gomock.Any()).
			DoAndReturn(func(fieldNumber protowire.Number, offsetBytes, sizeBytes int64, fieldReader io.Reader) error {
				// Only read the contents of this field
				// partially. The remaining contents
				// should be skipped automatically.
				var b [3]byte
				n, err := fieldReader.Read(b[:])
				require.NoError(t, err)
				require.Equal(t, 3, n)
				require.Equal(t, []byte("Par"), b[:])
				return nil
			})
		visitor.EXPECT().Call(protowire.Number(4), int64(18), int64(5), gomock.Any()).
			DoAndReturn(func(fieldNumber protowire.Number, offsetBytes, sizeBytes int64, fieldReader io.Reader) error {
				data, err := io.ReadAll(fieldReader)
				require.NoError(t, err)
				require.Equal(t, []byte("World"), data)
				return nil
			})

		require.NoError(t, util.VisitProtoBytesFields(bytes.NewBuffer([]byte{
			// Tag == (2 << 3) | 2.
			0x12,
			// Size == 5.
			0x05,
			// Payload == "Hello".
			0x48, 0x65, 0x6c, 0x6c, 0x6f,

			// Tag == (3 << 3) | 2.
			0x1a,
			// Size == 5.
			0x07,
			// Payload == "Partial".
			0x50, 0x61, 0x72, 0x74, 0x69, 0x61, 0x6c,

			// Tag == (4 << 3) | 2.
			0x22,
			// Size == 5.
			0x05,
			// Payload == "World".
			0x57, 0x6f, 0x72, 0x6c, 0x64,
		}), visitor.Call))
	})
}
