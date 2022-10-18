package jwt_test

import (
	"testing"

	"github.com/buildbarn/bb-storage/internal/mock"
	"github.com/buildbarn/bb-storage/pkg/jwt"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestGenerateAuthorizationHeader(t *testing.T) {
	ctrl := gomock.NewController(t)

	signatureGenerator := mock.NewMockSignatureGenerator(ctrl)

	t.Run("SigningFailure", func(t *testing.T) {
		signatureGenerator.EXPECT().GetAlgorithm().Return("ES256")
		signatureGenerator.EXPECT().GenerateSignature("eyJhbGciOiJFUzI1NiIsInR5cCI6IkpXVCJ9.eyJuYW1lIjoiSm9obiBEb2UifQ").
			Return(nil, status.Error(codes.Internal, "Cannot read from random number generator"))

		_, err := jwt.GenerateAuthorizationHeader(map[string]string{
			"name": "John Doe",
		}, signatureGenerator)
		testutil.RequireEqualStatus(t, status.Error(codes.Internal, "Failed to generate signature: Cannot read from random number generator"), err)
	})

	t.Run("Success", func(t *testing.T) {
		signatureGenerator.EXPECT().GetAlgorithm().Return("HS256")
		signatureGenerator.EXPECT().GenerateSignature("eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJuYW1lIjoiSm9obiBEb2UifQ").Return([]byte{
			0x49, 0xf9, 0x4a, 0xc7, 0x04, 0x49, 0x48, 0xc7,
			0x8a, 0x28, 0x5d, 0x90, 0x4f, 0x87, 0xf0, 0xa4,
			0xc7, 0x89, 0x7f, 0x7e, 0x8f, 0x3a, 0x4e, 0xb2,
			0x25, 0x5f, 0xda, 0x75, 0x0b, 0x2c, 0xc3, 0x97,
		}, nil)

		header, err := jwt.GenerateAuthorizationHeader(map[string]string{
			"name": "John Doe",
		}, signatureGenerator)
		require.NoError(t, err)
		require.Equal(t, "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJuYW1lIjoiSm9obiBEb2UifQ.SflKxwRJSMeKKF2QT4fwpMeJf36POk6yJV_adQssw5c", header)
	})
}
