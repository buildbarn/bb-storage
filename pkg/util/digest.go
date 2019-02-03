package util

import (
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"hash"
	"log"

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
type Digest struct {
	instance      string
	partialDigest remoteexecution.Digest
}

// NewDigest constructs a Digest object from an instance name and a
// protocol-level digest object. The instance returned by this function
// is guaranteed to be non-degenerate.
func NewDigest(instance string, partialDigest *remoteexecution.Digest) (*Digest, error) {
	if partialDigest == nil {
		return nil, status.Errorf(codes.InvalidArgument, "No digest provided")
	}

	// TODO(edsch): Validate the instance name. Maybe have a
	// restrictive character set? What about length?

	// Validate the hash.
	if len(partialDigest.Hash) != md5.Size*2 && len(partialDigest.Hash) != sha1.Size*2 && len(partialDigest.Hash) != sha256.Size*2 {
		return nil, status.Errorf(codes.InvalidArgument, "Unknown digest hash length: %d characters", len(partialDigest.Hash))
	}
	for _, c := range partialDigest.Hash {
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return nil, status.Errorf(codes.InvalidArgument, "Non-hexadecimal character in digest hash: %#U", c)
		}
	}

	// Validate the size.
	if partialDigest.SizeBytes < 0 {
		return nil, status.Errorf(codes.InvalidArgument, "Invalid digest size: %d bytes", partialDigest.SizeBytes)
	}

	return &Digest{
		instance:      instance,
		partialDigest: *partialDigest,
	}, nil
}

// MustNewDigest constructs a Digest similar to NewDigest, but never
// returns an error. Instead, execution will abort if the resulting
// instance would be degenerate. Useful for unit testing.
func MustNewDigest(instance string, partialDigest *remoteexecution.Digest) *Digest {
	d, err := NewDigest(instance, partialDigest)
	if err != nil {
		log.Fatal(err)
	}
	return d
}

// NewDerivedDigest creates a Digest object that uses the same instance
// name as the one from which it is derived. This can be used to refer
// to inputs (command, directories, files) of an action.
func (d *Digest) NewDerivedDigest(partialDigest *remoteexecution.Digest) (*Digest, error) {
	// TODO(edsch): Check whether the resulting digest uses the same
	// hashing algorithm?
	return NewDigest(d.instance, partialDigest)
}

// GetPartialDigest encodes the digest into the format used by the remote
// execution protocol, so that it may be stored in messages returned to
// the client.
func (d *Digest) GetPartialDigest() *remoteexecution.Digest {
	return &d.partialDigest
}

// GetInstance returns the instance name of the object.
func (d *Digest) GetInstance() string {
	return d.instance
}

// GetHashBytes returns the hash of the object as a slice of bytes.
func (d *Digest) GetHashBytes() []byte {
	hash, err := hex.DecodeString(d.partialDigest.Hash)
	if err != nil {
		log.Fatal("Failed to decode digest hash, even though its contents have already been validated")
	}
	return hash
}

// GetHashString returns the hash of the object as a string.
func (d *Digest) GetHashString() string {
	return d.partialDigest.Hash
}

// GetSizeBytes returns the size of the object, in bytes.
func (d *Digest) GetSizeBytes() int64 {
	return d.partialDigest.SizeBytes
}

// DigestKeyFormat is an enumeration type that determines the format of
// object keys returned by Digest.GetKey().
type DigestKeyFormat int

const (
	// DigestKeyWithoutInstance lets Digest.GetKey() return a key
	// that does not include the name of the instance; only the hash
	// and the size.
	DigestKeyWithoutInstance DigestKeyFormat = iota
	// DigestKeyWithInstance lets Digest.GetKey() return a key
	// that includes the hash, size and instance name.
	DigestKeyWithInstance
)

// GetKey generates a string representation of the digest object that
// may be used as keys in hash tables.
func (d *Digest) GetKey(format DigestKeyFormat) string {
	switch format {
	case DigestKeyWithoutInstance:
		return fmt.Sprintf("%s-%d", d.partialDigest.Hash, d.partialDigest.SizeBytes)
	case DigestKeyWithInstance:
		return fmt.Sprintf("%s-%d-%s", d.partialDigest.Hash, d.partialDigest.SizeBytes, d.instance)
	default:
		log.Fatal("Invalid digest key format")
		return ""
	}
}

func (d *Digest) String() string {
	return d.GetKey(DigestKeyWithInstance)
}

// NewHasher creates a standard hash.Hash object that may be used to
// compute a checksum of data. The hash.Hash object uses the same
// algorithm as the one that was used to create the digest, making it
// possible to validate data against a digest.
func (d *Digest) NewHasher() hash.Hash {
	switch len(d.partialDigest.Hash) {
	case md5.Size * 2:
		return md5.New()
	case sha1.Size * 2:
		return sha1.New()
	case sha256.Size * 2:
		return sha256.New()
	default:
		log.Fatal("Digest hash is of unknown type")
		return nil
	}
}

// NewDigestGenerator creates a writer that may be used to compute
// digests of newly created files.
func (d *Digest) NewDigestGenerator() *DigestGenerator {
	return &DigestGenerator{
		instance:    d.instance,
		partialHash: d.NewHasher(),
	}
}

// DigestGenerator is a writer that may be used to compute digests of
// newly created files.
type DigestGenerator struct {
	instance    string
	partialHash hash.Hash
	sizeBytes   int64
}

// Write a chunk of data from a newly created file into the state of the
// DigestGenerator.
func (dg *DigestGenerator) Write(p []byte) (int, error) {
	n, err := dg.partialHash.Write(p)
	dg.sizeBytes += int64(n)
	return n, err
}

// Sum creates a new digest based on the data written into the
// DigestGenerator.
func (dg *DigestGenerator) Sum() *Digest {
	return &Digest{
		instance: dg.instance,
		partialDigest: remoteexecution.Digest{
			Hash:      hex.EncodeToString(dg.partialHash.Sum(nil)),
			SizeBytes: dg.sizeBytes,
		},
	}
}
