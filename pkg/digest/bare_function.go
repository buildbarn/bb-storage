package digest

import (
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/sha512"
	"hash"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/go-sha256tree"
	"github.com/zeebo/blake3"
)

// SupportedDigestFunctions is the list of digest functions supported by
// digest.Digest, using the enumeration values that are part of the
// Remote Execution protocol.
var SupportedDigestFunctions = []remoteexecution.DigestFunction_Value{
	remoteexecution.DigestFunction_BLAKE3,
	remoteexecution.DigestFunction_MD5,
	remoteexecution.DigestFunction_SHA1,
	remoteexecution.DigestFunction_SHA256,
	remoteexecution.DigestFunction_SHA256TREE,
	remoteexecution.DigestFunction_SHA384,
	remoteexecution.DigestFunction_SHA512,
}

// shortestSupportedHashStringSize is the size of the shortest string
// that may be returned by Digest.GetHashString().
const shortestSupportedHashStringSize = md5.Size * 2

// bareFunction is contains all of the properties of a bare REv2 digest
// function that is not bound to an instance name. Exactly one instance
// is declared for each of the digest functions that are supported by
// this implementation.
type bareFunction struct {
	enumValue     remoteexecution.DigestFunction_Value
	hasherFactory func(expectedSizeBytes int64) hash.Hash
	hashBytesSize int
}

var (
	blake3BareFunction = bareFunction{
		enumValue: remoteexecution.DigestFunction_BLAKE3,
		hasherFactory: func(expectedSizeBytes int64) hash.Hash {
			return blake3.New()
		},
		hashBytesSize: 32,
	}
	md5BareFunction = bareFunction{
		enumValue: remoteexecution.DigestFunction_MD5,
		hasherFactory: func(expectedSizeBytes int64) hash.Hash {
			return md5.New()
		},
		hashBytesSize: md5.Size,
	}
	sha1BareFunction = bareFunction{
		enumValue: remoteexecution.DigestFunction_SHA1,
		hasherFactory: func(expectedSizeBytes int64) hash.Hash {
			return sha1.New()
		},
		hashBytesSize: sha1.Size,
	}
	sha256BareFunction = bareFunction{
		enumValue: remoteexecution.DigestFunction_SHA256,
		hasherFactory: func(expectedSizeBytes int64) hash.Hash {
			return sha256.New()
		},
		hashBytesSize: sha256.Size,
	}
	sha256treeBareFunction = bareFunction{
		enumValue:     remoteexecution.DigestFunction_SHA256TREE,
		hasherFactory: sha256tree.New,
		hashBytesSize: sha256tree.Size,
	}
	sha384BareFunction = bareFunction{
		enumValue: remoteexecution.DigestFunction_SHA384,
		hasherFactory: func(expectedSizeBytes int64) hash.Hash {
			return sha512.New384()
		},
		hashBytesSize: sha512.Size384,
	}
	sha512BareFunction = bareFunction{
		enumValue: remoteexecution.DigestFunction_SHA512,
		hasherFactory: func(expectedSizeBytes int64) hash.Hash {
			return sha512.New()
		},
		hashBytesSize: sha512.Size,
	}
)

// getBareFunctionByEnumValue returns the bare digest function that
// corresponds to an REv2 digest function enumeration value.
func getBareFunction(digestFunction remoteexecution.DigestFunction_Value, hashStringSize int) *bareFunction {
	switch digestFunction {
	case remoteexecution.DigestFunction_UNKNOWN:
		// Caller did not provide an explicit digest function.
		// For compatibility, attempt to infer the digest
		// function from the hash length.
		switch hashStringSize {
		case md5.Size * 2:
			return &md5BareFunction
		case sha1.Size * 2:
			return &sha1BareFunction
		case sha256.Size * 2:
			return &sha256BareFunction
		case sha512.Size384 * 2:
			return &sha384BareFunction
		case sha512.Size * 2:
			return &sha512BareFunction
		}
	case remoteexecution.DigestFunction_BLAKE3:
		return &blake3BareFunction
	case remoteexecution.DigestFunction_MD5:
		return &md5BareFunction
	case remoteexecution.DigestFunction_SHA1:
		return &sha1BareFunction
	case remoteexecution.DigestFunction_SHA256:
		return &sha256BareFunction
	case remoteexecution.DigestFunction_SHA256TREE:
		return &sha256treeBareFunction
	case remoteexecution.DigestFunction_SHA384:
		return &sha384BareFunction
	case remoteexecution.DigestFunction_SHA512:
		return &sha512BareFunction
	}
	return nil
}
