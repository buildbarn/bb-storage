package digest

import (
	"encoding/hex"
	"fmt"
	"hash"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Function for computing new Digest objects. Function is a tuple of the
// REv2 instance name and hashing algorithm.
type Function struct {
	instanceName InstanceName
	bareFunction *bareFunction
}

// MustNewFunction constructs a Function similar to
// InstanceName.GetDigestFunction(), but never returns an error.
// Instead, execution will abort if the provided options are invalid.
// Useful for unit testing.
func MustNewFunction(instanceName string, digestFunction remoteexecution.DigestFunction_Value) Function {
	in, err := NewInstanceName(instanceName)
	if err != nil {
		panic(err)
	}
	f, err := in.GetDigestFunction(digestFunction, 0)
	if err != nil {
		panic(err)
	}
	return f
}

// GetInstanceName returns the instance name that Digest objects would
// use if they were created from this Function.
func (f Function) GetInstanceName() InstanceName {
	return f.instanceName
}

// GetEnumValue returns the REv2 enumeration value for the digest
// function.
func (f Function) GetEnumValue() remoteexecution.DigestFunction_Value {
	return f.bareFunction.enumValue
}

// NewGenerator creates a writer that may be used to compute digests of
// newly created files.
//
// The expected size MUST correspond to the size of the object that is
// being hashed. Digest functions like GITSHA1 require that the size is
// known up front.
func (f Function) NewGenerator(expectedSizeBytes int64) *Generator {
	return &Generator{
		digestFunction: f,
		partialHash:    f.bareFunction.hasherFactory(expectedSizeBytes),
	}
}

// NewDigest constructs a Digest object from a digest function, hash and
// object size. The object returned by this function is guaranteed to be
// non-degenerate.
func (f Function) NewDigest(hash string, sizeBytes int64) (Digest, error) {
	// Validate the digest function.
	if hashStringSize := 2 * f.bareFunction.hashBytesSize; len(hash) != hashStringSize {
		return BadDigest, status.Errorf(codes.InvalidArgument, "Hash has length %d, while %d characters were expected", len(hash), hashStringSize)
	}

	// Validate that the hash is lowercase hexadecimal.
	for _, c := range hash {
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return BadDigest, status.Errorf(codes.InvalidArgument, "Non-hexadecimal character in digest hash: %#U", c)
		}
	}

	// Validate the size.
	if sizeBytes < 0 {
		return BadDigest, status.Errorf(codes.InvalidArgument, "Invalid digest size: %d bytes", sizeBytes)
	}

	return f.newDigestUnchecked(hash, sizeBytes), nil
}

// newDigestUnchecked constructs a Digest object from a digest function,
// hash and object size without validating its contents.
func (f Function) newDigestUnchecked(hash string, sizeBytes int64) Digest {
	return Digest{
		value: fmt.Sprintf("%d-%s-%d-%s", int(f.bareFunction.enumValue), hash, sizeBytes, f.instanceName.value),
	}
}

// NewDigestFromProto constructs a Digest object from a digest function
// and a protocol-level digest object. The object returned by this
// function is guaranteed to be non-degenerate.
func (f Function) NewDigestFromProto(digest *remoteexecution.Digest) (Digest, error) {
	if digest == nil {
		return BadDigest, status.Error(codes.InvalidArgument, "No digest provided")
	}
	return f.NewDigest(digest.Hash, digest.SizeBytes)
}

// Generator is a writer that may be used to compute digests of newly
// created files.
type Generator struct {
	digestFunction Function
	partialHash    hash.Hash
	sizeBytes      int64
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
	return dg.digestFunction.newDigestUnchecked(
		hex.EncodeToString(dg.partialHash.Sum(nil)),
		dg.sizeBytes)
}
