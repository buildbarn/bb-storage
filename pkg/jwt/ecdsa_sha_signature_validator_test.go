package jwt_test

import (
	"crypto/ecdsa"
	"crypto/x509"
	"encoding/pem"
	"testing"

	"github.com/buildbarn/bb-storage/pkg/jwt"
	"github.com/stretchr/testify/require"
)

func TestECDSASHASignatureValidator(t *testing.T) {
	t.Run("ES256", func(t *testing.T) {
		block, _ := pem.Decode([]byte(`-----BEGIN PUBLIC KEY-----
MFkwEwYHKoZIzj0CAQYIKoZIzj0DAQcDQgAEEVs/o5+uQbTjL3chynL4wXgUg2R9
q9UU8I5mEovUf86QZ7kOBIjJwqnzD1omageEHWwHdBO6B+dFabmdT9POxg==
-----END PUBLIC KEY-----`))
		require.NotNil(t, block)
		key, err := x509.ParsePKIXPublicKey(block.Bytes)
		require.NoError(t, err)
		signatureValidator, err := jwt.NewECDSASHASignatureValidator(key.(*ecdsa.PublicKey))
		require.NoError(t, err)

		// Algorithm "HS256" uses HMAC; not ECDSA. Validation should fail.
		require.False(t, signatureValidator.ValidateSignature(
			"HS256",
			/* keyID = */ nil,
			"eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiaWF0IjoxNTE2MjM5MDIyfQ",
			[]byte{
				0xb3, 0x57, 0x72, 0xdf, 0xc5, 0xc6, 0x74, 0xba,
				0x79, 0xcc, 0xb6, 0x04, 0x07, 0x65, 0x0e, 0xb7,
				0xd6, 0x65, 0x06, 0x1a, 0x09, 0xed, 0x97, 0xeb,
				0x35, 0x80, 0x06, 0x26, 0xdc, 0x19, 0xec, 0x61,
			}))

		// ECDSA with SHA-256, both with a valid and invalid signature.
		require.True(t, signatureValidator.ValidateSignature(
			"ES256",
			/* keyID = */ nil,
			"eyJhbGciOiJFUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiYWRtaW4iOnRydWUsImlhdCI6MTUxNjIzOTAyMn0",
			[]byte{
				// R.
				0xb7, 0x28, 0x7e, 0x55, 0xfb, 0xb3, 0x23, 0x10,
				0xb2, 0x19, 0x80, 0xe5, 0x90, 0x10, 0x3b, 0x0d,
				0xfc, 0xa3, 0xae, 0xa9, 0x92, 0x1e, 0xee, 0xa9,
				0x43, 0x68, 0x68, 0x66, 0xe1, 0x6a, 0x51, 0x22,
				// S.
				0xcf, 0x35, 0x8d, 0x8d, 0xd2, 0x6a, 0x47, 0x6f,
				0x79, 0xe4, 0xe4, 0xad, 0x7b, 0x1d, 0x63, 0xff,
				0xdd, 0xc6, 0x07, 0x07, 0x0e, 0xc0, 0x84, 0x76,
				0xa5, 0x7b, 0x9c, 0x24, 0xcb, 0xaf, 0xac, 0x54,
			}))
		require.False(t, signatureValidator.ValidateSignature(
			"ES256",
			/* keyID = */ nil,
			"eyJhbGciOiJFUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiYWRtaW4iOnRydWUsImlhdCI6MTUxNjIzOTAyMn0",
			[]byte{
				// R.
				0x4c, 0xbd, 0x22, 0x9a, 0xe3, 0xc4, 0x8a, 0xae,
				0x06, 0x2f, 0x86, 0x42, 0xdb, 0x80, 0xa9, 0xbd,
				0xa6, 0xdb, 0xa0, 0xae, 0xd7, 0xdd, 0xed, 0x86,
				0x65, 0x92, 0xf4, 0x4a, 0xa5, 0x27, 0x12, 0x01,
				// S.
				0x9e, 0x04, 0xda, 0xa1, 0x32, 0xc8, 0xbe, 0x50,
				0x02, 0xc7, 0xea, 0x1f, 0xf8, 0x19, 0x90, 0x9d,
				0xac, 0x8d, 0x87, 0xaa, 0x7d, 0x6e, 0x77, 0xd8,
				0xec, 0x46, 0x8a, 0x0e, 0x29, 0xe2, 0x48, 0xd9,
			}))
	})

	t.Run("ES384", func(t *testing.T) {
		block, _ := pem.Decode([]byte(`-----BEGIN PUBLIC KEY-----
MHYwEAYHKoZIzj0CAQYFK4EEACIDYgAEC1uWSXj2czCDwMTLWV5BFmwxdM6PX9p+
Pk9Yf9rIf374m5XP1U8q79dBhLSIuaojsvOT39UUcPJROSD1FqYLued0rXiooIii
1D3jaW6pmGVJFhodzC31cy5sfOYotrzF
-----END PUBLIC KEY-----`))
		require.NotNil(t, block)
		key, err := x509.ParsePKIXPublicKey(block.Bytes)
		require.NoError(t, err)
		signatureValidator, err := jwt.NewECDSASHASignatureValidator(key.(*ecdsa.PublicKey))
		require.NoError(t, err)

		// A 384-bit ECDSA key cannot be used to validate
		// 256-bit signatures.
		require.False(t, signatureValidator.ValidateSignature(
			"ES256",
			/* keyID = */ nil,
			"eyJhbGciOiJFUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiYWRtaW4iOnRydWUsImlhdCI6MTUxNjIzOTAyMn0",
			[]byte{
				// R.
				0xb7, 0x28, 0x7e, 0x55, 0xfb, 0xb3, 0x23, 0x10,
				0xb2, 0x19, 0x80, 0xe5, 0x90, 0x10, 0x3b, 0x0d,
				0xfc, 0xa3, 0xae, 0xa9, 0x92, 0x1e, 0xee, 0xa9,
				0x43, 0x68, 0x68, 0x66, 0xe1, 0x6a, 0x51, 0x22,
				// S.
				0xcf, 0x35, 0x8d, 0x8d, 0xd2, 0x6a, 0x47, 0x6f,
				0x79, 0xe4, 0xe4, 0xad, 0x7b, 0x1d, 0x63, 0xff,
				0xdd, 0xc6, 0x07, 0x07, 0x0e, 0xc0, 0x84, 0x76,
				0xa5, 0x7b, 0x9c, 0x24, 0xcb, 0xaf, 0xac, 0x54,
			}))

		// ECDSA with SHA-384, both with a valid and invalid signature.
		require.True(t, signatureValidator.ValidateSignature(
			"ES384",
			/* keyID = */ nil,
			"eyJhbGciOiJFUzM4NCIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiYWRtaW4iOnRydWUsImlhdCI6MTUxNjIzOTAyMn0",
			[]byte{
				// R.
				0x55, 0x43, 0xd6, 0x41, 0x9b, 0x82, 0x96, 0x79,
				0x05, 0x6d, 0xa1, 0x0a, 0x0a, 0xc3, 0xf2, 0xec,
				0x26, 0x55, 0x32, 0x1e, 0x70, 0xc5, 0xb0, 0x92,
				0xa5, 0xa0, 0x16, 0x14, 0xba, 0x67, 0x4d, 0xef,
				0x49, 0xd3, 0xef, 0x8f, 0xcc, 0x73, 0x5e, 0x4c,
				0x53, 0x57, 0x0a, 0xb5, 0x47, 0xca, 0xc0, 0x1a,
				// S.
				0xdd, 0xe1, 0x5b, 0xbb, 0x30, 0xfc, 0xfd, 0xb2,
				0xd2, 0xb2, 0x04, 0xca, 0x0b, 0xc4, 0xb3, 0x1f,
				0x14, 0x55, 0x4a, 0x3e, 0x5e, 0x37, 0xce, 0xaf,
				0x04, 0xfa, 0x3f, 0xd5, 0xf3, 0x5f, 0x13, 0xb4,
				0x87, 0x4d, 0x88, 0x56, 0xa3, 0x08, 0xd8, 0xe0,
				0x4f, 0xf0, 0xba, 0x20, 0xb5, 0xdf, 0x2a, 0x23,
			}))
		require.False(t, signatureValidator.ValidateSignature(
			"ES384",
			/* keyID = */ nil,
			"eyJhbGciOiJFUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiYWRtaW4iOnRydWUsImlhdCI6MTUxNjIzOTAyMn0",
			[]byte{
				// R.
				0x84, 0x58, 0xfe, 0x59, 0x31, 0x59, 0x77, 0xc3,
				0xb6, 0xa2, 0x11, 0x3a, 0x15, 0xdd, 0xaa, 0x9c,
				0x46, 0x99, 0x9f, 0x2c, 0x7a, 0x1f, 0xc8, 0xb5,
				0x80, 0xc9, 0xe5, 0x1f, 0x5c, 0xca, 0x73, 0x3e,
				0xfd, 0x0d, 0xda, 0xa2, 0x09, 0x45, 0x77, 0xba,
				0xfc, 0x51, 0xaa, 0x2b, 0xc4, 0x7a, 0xa1, 0x30,
				// S.
				0xe0, 0xad, 0x07, 0xa8, 0x81, 0x58, 0x5d, 0xd0,
				0x64, 0x47, 0x43, 0x38, 0x0c, 0x6e, 0x7e, 0xc5,
				0x54, 0x4c, 0x63, 0x39, 0xdb, 0xc3, 0xc1, 0x66,
				0xff, 0x3a, 0x20, 0xea, 0x36, 0xe1, 0xe4, 0xa9,
				0x8c, 0xde, 0x69, 0xd1, 0x0b, 0x52, 0x9b, 0xb3,
				0x95, 0x8a, 0xdd, 0x89, 0x5b, 0xed, 0x45, 0x18,
			}))
	})

	t.Run("ES512", func(t *testing.T) {
		block, _ := pem.Decode([]byte(`-----BEGIN PUBLIC KEY-----
MIGbMBAGByqGSM49AgEGBSuBBAAjA4GGAAQACVyLsNdjFM6R4IImvTzgRWF0sWjh
ihmzIyMgyPuqu8IuyzMNx4G2jpoCKhRu9qPCQUMGDeCG1x3/n/OgkWNQANsB82x7
7eiIZAl0zcQRH32tcjFILvJ/XCihdoi4MkCnCqlt9/HxjsP590ZtmHfxAeertq5w
9vakvpzjPXhkvoMt/Tk=
-----END PUBLIC KEY-----`))
		require.NotNil(t, block)
		key, err := x509.ParsePKIXPublicKey(block.Bytes)
		require.NoError(t, err)
		signatureValidator, err := jwt.NewECDSASHASignatureValidator(key.(*ecdsa.PublicKey))
		require.NoError(t, err)

		// ECDSA with SHA-512, both with a valid and invalid signature.
		require.True(t, signatureValidator.ValidateSignature(
			"ES512",
			/* keyID = */ nil,
			"eyJ0eXAiOiJKV1QiLCJhbGciOiJFUzUxMiJ9.eyJmb28iOiJiYXIifQ",
			[]byte{
				// R.
				0x00, 0x05, 0x34, 0x4e, 0xf1, 0x90, 0x39, 0xc7,
				0x60, 0xd8, 0xeb, 0xeb, 0xc1, 0x8e, 0xf7, 0x34,
				0x72, 0xa0, 0x7e, 0x4d, 0xba, 0x50, 0x37, 0xa4,
				0x87, 0xd3, 0xeb, 0xcf, 0xe2, 0xff, 0x89, 0x6b,
				0x93, 0x04, 0x80, 0x6a, 0x38, 0x50, 0x96, 0xc3,
				0x02, 0xec, 0x46, 0x21, 0xc1, 0xd4, 0x93, 0x9d,
				0x75, 0xf2, 0x80, 0x96, 0xce, 0x5d, 0xa3, 0x55,
				0xb4, 0x8d, 0x1f, 0xc5, 0xdf, 0x42, 0x69, 0x55,
				0xdf, 0xba,
				// S.
				0x00, 0x97, 0x93, 0x8c, 0x62, 0x80, 0xe0, 0x2d,
				0x40, 0xd4, 0x7e, 0xa0, 0x20, 0xac, 0x11, 0x63,
				0x4a, 0x3f, 0xb4, 0x50, 0xc8, 0xc9, 0xd6, 0x42,
				0x97, 0xf9, 0x4c, 0x04, 0xc7, 0x86, 0xe5, 0x53,
				0x45, 0x02, 0x33, 0xbc, 0xeb, 0xe5, 0x82, 0x48,
				0xac, 0x1c, 0xde, 0x1a, 0x9a, 0x1a, 0x4a, 0xb5,
				0x32, 0xe8, 0x0f, 0x46, 0xaf, 0xea, 0xac, 0x5f,
				0x40, 0x55, 0xbb, 0x84, 0x07, 0x82, 0x4f, 0x2f,
				0x22, 0xf9,
			}))
		require.False(t, signatureValidator.ValidateSignature(
			"ES512",
			/* keyID = */ nil,
			"eyJ0eXAiOiJKV1QiLCJhbGciOiJFUzUxMiJ9.eyJmb28iOiJiYXIifQ",
			[]byte{
				// R.
				0x1f, 0x25, 0x18, 0x94, 0x10, 0x95, 0xff, 0x75,
				0x5a, 0xa6, 0xe1, 0x05, 0xad, 0xbf, 0x50, 0x2a,
				0xa5, 0xef, 0x8e, 0x63, 0xe7, 0x61, 0x41, 0x50,
				0xc2, 0xf7, 0x0d, 0x70, 0xad, 0xf9, 0x59, 0x98,
				0x94, 0xbc, 0xe5, 0xb8, 0xf4, 0xf1, 0xa3, 0xf0,
				0xf9, 0xf2, 0x0a, 0x53, 0x67, 0x56, 0x40, 0xac,
				0xe9, 0x5f, 0xfa, 0x30, 0xb5, 0x28, 0x12, 0xa9,
				0x98, 0xa4, 0x6a, 0x4b, 0x9b, 0xe6, 0xc0, 0x85,
				0x55, 0x61,
				// S.
				0xc0, 0xca, 0x9c, 0x5a, 0x87, 0xbb, 0x9c, 0xdf,
				0xb3, 0xaa, 0x8a, 0x93, 0x0d, 0x59, 0x3f, 0xe9,
				0xb5, 0xe9, 0x35, 0x0a, 0x87, 0x16, 0xa4, 0x03,
				0x1d, 0xe3, 0x53, 0x3e, 0xa9, 0xc7, 0x7e, 0x9e,
				0x62, 0xce, 0x55, 0x15, 0xdd, 0xdc, 0x7f, 0x31,
				0x13, 0x49, 0x76, 0x4e, 0x8a, 0xc5, 0x2a, 0x76,
				0x1d, 0x52, 0xca, 0xa1, 0x6f, 0x88, 0x64, 0x19,
				0x8a, 0x95, 0xd2, 0xb4, 0x47, 0x2e, 0xe6, 0x9b,
				0xfd, 0x0f,
			}))
	})
}
