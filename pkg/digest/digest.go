package digest

import (
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/hex"
	"fmt"
	"hash"
	"strconv"
	"strings"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Digest holds the identification of an object stored in the Content
// Addressable Storage (CAS) or Action Cache (AC). The use of this
// object is preferred over remoteexecution.Digest for a couple of
// reasons.
//
// - Instances of these objects are guaranteed not to contain any
//   degenerate values. The hash has already been decoded from
//   hexadecimal to binary. The size is non-negative.
// - They keep track of the instance as part of the digest, which allows
//   us to keep function signatures across the codebase simple.
// - They provide utility functions for deriving new digests from them.
//   This ensures that outputs of build actions automatically use the
//   same instance name and hashing algorithm.
//
// Because Digest objects are frequently used as keys (as part of
// caching data structures or to construct sets without duplicate
// values), this implementation immediately constructs a key
// representation upon creation. All functions that extract individual
// components (e.g., GetInstance(), GetHash*() and GetSizeBytes())
// operate directly on the key format.
type Digest struct {
	value string
}

var (
	// BadDigest is a default instance of Digest. It can, for
	// example, be used as a function return value for error cases.
	BadDigest Digest
)

var (
	// SupportedDigestFunctions is the list of digest functions
	// supported by digest.Digest, using the enumeration values that
	// are part of the Remote Execution protocol.
	SupportedDigestFunctions = []remoteexecution.DigestFunction_Value{
		remoteexecution.DigestFunction_MD5,
		remoteexecution.DigestFunction_SHA1,
		remoteexecution.DigestFunction_SHA256,
		remoteexecution.DigestFunction_SHA384,
		remoteexecution.DigestFunction_SHA512,
	}
)

// Unpack the individual hash, size and instance name fields from the
// string representation stored inside the Digest object.
func (d Digest) unpack() (int, int64, int) {
	// Extract the leading hash.
	hashEnd := md5.Size * 2
	for d.value[hashEnd] != '-' {
		hashEnd++
	}

	// Extract the size stored in the middle.
	sizeBytes := int64(0)
	sizeBytesEnd := hashEnd + 1
	for d.value[sizeBytesEnd] != '-' {
		sizeBytes = sizeBytes*10 + int64(d.value[sizeBytesEnd]-'0')
		sizeBytesEnd++
	}

	return hashEnd, sizeBytes, sizeBytesEnd
}

// NewDigest constructs a Digest object from an instance name, hash and
// object size. The instance returned by this function is guaranteed to
// be non-degenerate.
func NewDigest(instance string, hash string, sizeBytes int64) (Digest, error) {
	// TODO(edsch): Validate the instance name. Maybe have a
	// restrictive character set? What about length?

	// Validate the hash.
	if l := len(hash); l != md5.Size*2 && l != sha1.Size*2 &&
		l != sha256.Size*2 && l != sha512.Size384*2 && l != sha512.Size*2 {
		return BadDigest, status.Errorf(codes.InvalidArgument, "Unknown digest hash length: %d characters", l)
	}
	for _, c := range hash {
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return BadDigest, status.Errorf(codes.InvalidArgument, "Non-hexadecimal character in digest hash: %#U", c)
		}
	}

	// Validate the size.
	if sizeBytes < 0 {
		return BadDigest, status.Errorf(codes.InvalidArgument, "Invalid digest size: %d bytes", sizeBytes)
	}

	return newDigestUnchecked(instance, hash, sizeBytes), nil
}

// newDigestUnchecked constructs a Digest object from an instance name,
// hash and object size without validating its contents.
func newDigestUnchecked(instance string, hash string, sizeBytes int64) Digest {
	return Digest{
		value: fmt.Sprintf("%s-%d-%s", hash, sizeBytes, instance),
	}
}

// MustNewDigest constructs a Digest similar to NewDigest, but never
// returns an error. Instead, execution will abort if the resulting
// instance would be degenerate. Useful for unit testing.
func MustNewDigest(instance string, hash string, sizeBytes int64) Digest {
	d, err := NewDigest(instance, hash, sizeBytes)
	if err != nil {
		panic(err)
	}
	return d
}

// NewDigestFromPartialDigest constructs a Digest object from an
// instance name and a protocol-level digest object. The instance
// returned by this function is guaranteed to be non-degenerate.
func NewDigestFromPartialDigest(instance string, partialDigest *remoteexecution.Digest) (Digest, error) {
	if partialDigest == nil {
		return BadDigest, status.Error(codes.InvalidArgument, "No digest provided")
	}
	return NewDigest(instance, partialDigest.Hash, partialDigest.SizeBytes)
}

// NewDigestFromBytestreamPath creates a Digest from a string having one
// of the following two formats:
//
// - blobs/${hash}/${size}
// - ${instance}/blobs/${hash}/${size}
//
// This notation is used by Bazel to refer to files accessible through a
// gRPC Bytestream service.
func NewDigestFromBytestreamPath(path string) (Digest, error) {
	fields := strings.FieldsFunc(path, func(r rune) bool { return r == '/' })
	l := len(fields)
	if (l < 3) || fields[l-3] != "blobs" {
		return BadDigest, status.Error(codes.InvalidArgument, "Invalid resource naming scheme")
	}
	size, err := strconv.ParseInt(fields[l-1], 10, 64)
	if err != nil {
		return BadDigest, status.Error(codes.InvalidArgument, "Invalid resource naming scheme")
	}
	instance := strings.Join(fields[0:(l-3)], "/")
	return NewDigest(instance, fields[l-2], size)
}

// NewDerivedDigest creates a Digest object that uses the same instance
// name as the one from which it is derived. This can be used to refer
// to inputs (command, directories, files) of an action.
func (d Digest) NewDerivedDigest(partialDigest *remoteexecution.Digest) (Digest, error) {
	// TODO(edsch): Check whether the resulting digest uses the same
	// hashing algorithm?
	return NewDigestFromPartialDigest(d.GetInstance(), partialDigest)
}

// GetPartialDigest encodes the digest into the format used by the remote
// execution protocol, so that it may be stored in messages returned to
// the client.
func (d Digest) GetPartialDigest() *remoteexecution.Digest {
	hashEnd, sizeBytes, _ := d.unpack()
	return &remoteexecution.Digest{
		Hash:      d.value[:hashEnd],
		SizeBytes: sizeBytes,
	}
}

// GetInstance returns the instance name of the object.
func (d Digest) GetInstance() string {
	_, _, sizeBytesEnd := d.unpack()
	return d.value[sizeBytesEnd+1:]
}

// GetHashBytes returns the hash of the object as a slice of bytes.
func (d Digest) GetHashBytes() []byte {
	hash, err := hex.DecodeString(d.GetHashString())
	if err != nil {
		panic("Failed to decode digest hash, even though its contents have already been validated")
	}
	return hash
}

// GetHashString returns the hash of the object as a string.
func (d Digest) GetHashString() string {
	hashEnd, _, _ := d.unpack()
	return d.value[:hashEnd]
}

// GetSizeBytes returns the size of the object, in bytes.
func (d Digest) GetSizeBytes() int64 {
	_, sizeBytes, _ := d.unpack()
	return sizeBytes
}

// KeyFormat is an enumeration type that determines the format of object
// keys returned by Digest.GetKey().
type KeyFormat int

const (
	// KeyWithoutInstance lets Digest.GetKey() return a key that
	// does not include the name of the instance; only the hash and
	// the size.
	KeyWithoutInstance KeyFormat = iota
	// KeyWithInstance lets Digest.GetKey() return a key that
	// includes the hash, size and instance name.
	KeyWithInstance
)

// GetKey generates a string representation of the digest object that
// may be used as keys in hash tables.
func (d Digest) GetKey(format KeyFormat) string {
	switch format {
	case KeyWithoutInstance:
		_, _, sizeBytesEnd := d.unpack()
		return d.value[:sizeBytesEnd]
	case KeyWithInstance:
		return d.value
	default:
		panic("Invalid digest key format")
	}
}

func (d Digest) String() string {
	return d.GetKey(KeyWithInstance)
}

// NewHasher creates a standard hash.Hash object that may be used to
// compute a checksum of data. The hash.Hash object uses the same
// algorithm as the one that was used to create the digest, making it
// possible to validate data against a digest.
func (d Digest) NewHasher() hash.Hash {
	hashEnd, _, _ := d.unpack()
	switch hashEnd {
	case md5.Size * 2:
		return md5.New()
	case sha1.Size * 2:
		return sha1.New()
	case sha256.Size * 2:
		return sha256.New()
	case sha512.Size384 * 2:
		return sha512.New384()
	case sha512.Size * 2:
		return sha512.New()
	default:
		panic("Digest hash is of unknown type")
	}
}

// NewGenerator creates a writer that may be used to compute digests of
// newly created files.
func (d Digest) NewGenerator() *Generator {
	return &Generator{
		instance:    d.GetInstance(),
		partialHash: d.NewHasher(),
	}
}

// Generator is a writer that may be used to compute digests of newly
// created files.
type Generator struct {
	instance    string
	partialHash hash.Hash
	sizeBytes   int64
}

// Write a chunk of data from a newly created file into the state of the
// Generator.
func (dg *Generator) Write(p []byte) (int, error) {
	n, err := dg.partialHash.Write(p)
	dg.sizeBytes += int64(n)
	return n, err
}

// Sum creates a new digest based on the data written into the
// Generator.
func (dg *Generator) Sum() Digest {
	return newDigestUnchecked(
		dg.instance,
		hex.EncodeToString(dg.partialHash.Sum(nil)),
		dg.sizeBytes)
}
