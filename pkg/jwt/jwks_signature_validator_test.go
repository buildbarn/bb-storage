package jwt_test

import (
	"testing"

	"github.com/buildbarn/bb-storage/pkg/jwt"
	"github.com/stretchr/testify/require"
)

func TestJWKSSignatureValidatorCreation(t *testing.T) {
	key := []byte(`{
		"keys": [
			{
				"kty": "RSA",
				"n": "u1SU1LfVLPHCozMxH2Mo4lgOEePzNm0tRgeLezV6ffAt0gunVTLw7onLRnrq0_IzW7yWR7QkrmBL7jTKEn5u-qKhbwKfBstIs-bMY2Zkp18gnTxKLxoS2tFczGkPLPgizskuemMghRniWaoLcyehkd3qqGElvW_VDL5AaWTg0nLVkjRo9z-40RQzuVaE8AkAFmxZzow3x-VJYKdjykkJ0iT9wCS0DRTXu269V264Vf_3jvredZiKRkgwlL9xNAwxXFg0x_XFw005UWVRIkdgcKWTjpBP2dPwVZ4WWC-9aGVd-Gyn1o0CLelf4rEjGoXbAAEgAqeGUxrcIlbjXfbcmw",
				"e": "AQAB",
				"alg": "RS256",
				"kid": "7c0b6913fe13820a333399ace426e70535a9a0bf",
				"use": "sig"
			}
		]
	}`)

	_, err := jwt.NewJWKSSignatureValidator(key)
	require.NoError(t, err)
}
