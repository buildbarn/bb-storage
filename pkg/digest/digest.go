package digest

import (
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/hex"
	"hash"
	"path"
	"strconv"
	"strings"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/google/uuid"

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
// components (e.g., GetInstanceName(), GetHash*() and GetSizeBytes())
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

// MustNewDigest constructs a Digest similar to NewDigest, but never
// returns an error. Instead, execution will abort if the resulting
// instance would be degenerate. Useful for unit testing.
func MustNewDigest(instanceName string, hash string, sizeBytes int64) Digest {
	in, err := NewInstanceName(instanceName)
	if err != nil {
		panic(err)
	}
	d, err := in.NewDigest(hash, sizeBytes)
	if err != nil {
		panic(err)
	}
	return d
}

// NewDigestFromByteStreamReadPath creates a Digest from a string having
// the following format: ${instanceName}/blobs/${hash}/${size}. This
// notation is used to read files through the ByteStream service.
func NewDigestFromByteStreamReadPath(path string) (Digest, error) {
	fields := strings.FieldsFunc(path, func(r rune) bool { return r == '/' })
	if len(fields) < 3 {
		return BadDigest, status.Error(codes.InvalidArgument, "Invalid resource naming scheme")
	}
	split := len(fields) - 3
	return newDigestFromByteStreamPathCommon(fields[:split], fields[split:])
}

// NewDigestFromByteStreamWritePath creates a Digest from a string
// having the following format:
// ${instanceName}/uploads/${uuid}/blobs/${hash}/${size}/${path}. This
// notation is used to write files through the ByteStream service.
func NewDigestFromByteStreamWritePath(path string) (Digest, error) {
	fields := strings.FieldsFunc(path, func(r rune) bool { return r == '/' })
	if len(fields) < 5 {
		return BadDigest, status.Errorf(codes.InvalidArgument, "Invalid resource naming scheme")
	}
	// Determine the end of the instance name. Because both the
	// leading instance name and the trailing path have a variable
	// length, this may be ambiguous. This is why instance names are
	// not permitted to contain "uploads" pathname components.
	split := 0
	for fields[split] != "uploads" {
		split++
		if split > len(fields)-5 {
			return BadDigest, status.Errorf(codes.InvalidArgument, "Invalid resource naming scheme")
		}
	}
	return newDigestFromByteStreamPathCommon(fields[:split], fields[split+2:])
}

func newDigestFromByteStreamPathCommon(header []string, trailer []string) (Digest, error) {
	if trailer[0] != "blobs" {
		return BadDigest, status.Error(codes.InvalidArgument, "Invalid resource naming scheme")
	}
	sizeBytes, err := strconv.ParseInt(trailer[2], 10, 64)
	if err != nil {
		return BadDigest, status.Errorf(codes.InvalidArgument, "Invalid blob size %#v", trailer[2])
	}
	instanceName, err := NewInstanceNameFromComponents(header)
	if err != nil {
		return BadDigest, util.StatusWrapf(err, "Invalid instance name %#v", strings.Join(header, "/"))
	}
	return instanceName.NewDigest(trailer[1], sizeBytes)
}

// GetByteStreamReadPath converts the Digest to a string having
// the following format: ${instanceName}/blobs/${hash}/${size}. This
// notation is used to read files through the ByteStream service.
func (d Digest) GetByteStreamReadPath() string {
	hashEnd, sizeBytes, sizeBytesEnd := d.unpack()
	return path.Join(
		d.value[sizeBytesEnd+1:],
		"blobs",
		d.value[:hashEnd],
		strconv.FormatInt(sizeBytes, 10))
}

// GetByteStreamWritePath converts the Digest to a string having the
// following format:
// ${instanceName}/uploads/${uuid}/blobs/${hash}/${size}/${path}. This
// notation is used to write files through the ByteStream service.
func (d Digest) GetByteStreamWritePath(uuid uuid.UUID) string {
	hashEnd, sizeBytes, sizeBytesEnd := d.unpack()
	return path.Join(
		d.value[sizeBytesEnd+1:],
		"uploads",
		uuid.String(),
		"blobs",
		d.value[:hashEnd],
		strconv.FormatInt(sizeBytes, 10))
}

// GetProto encodes the digest into the format used by the remote
// execution protocol, so that it may be stored in messages returned to
// the client.
func (d Digest) GetProto() *remoteexecution.Digest {
	hashEnd, sizeBytes, _ := d.unpack()
	return &remoteexecution.Digest{
		Hash:      d.value[:hashEnd],
		SizeBytes: sizeBytes,
	}
}

// GetInstanceName returns the instance name of the object.
func (d Digest) GetInstanceName() InstanceName {
	_, _, sizeBytesEnd := d.unpack()
	return InstanceName{
		value: d.value[sizeBytesEnd+1:],
	}
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

// Combine two KeyFormats into one, picking the format that contains the
// most information.
//
// This function is used extensively by NewBlobAccessFromConfiguration()
// to ensure that the right KeyFormat is picked based on the behavior of
// two or more backing BlobAccess instances.
func (kf KeyFormat) Combine(other KeyFormat) KeyFormat {
	if kf == KeyWithInstance {
		return KeyWithInstance
	}
	return other
}

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

// GetHashXAttrName returns the extended file attribute retrievable
// through getxattr() that can be used to store a cached copy of the
// object's hash.
func (d Digest) GetHashXAttrName() string {
	hashEnd, _, _ := d.unpack()
	switch hashEnd {
	case md5.Size * 2:
		return "user.buildbarn.hash.md5"
	case sha1.Size * 2:
		return "user.buildbarn.hash.sha1"
	case sha256.Size * 2:
		return "user.buildbarn.hash.sha256"
	case sha512.Size384 * 2:
		return "user.buildbarn.hash.sha384"
	case sha512.Size * 2:
		return "user.buildbarn.hash.sha512"
	default:
		panic("Digest hash is of unknown type")
	}
}

func (d Digest) String() string {
	return d.GetKey(KeyWithInstance)
}

// ToSingletonSet creates a Set that contains a single element that
// corresponds to the Digest.
func (d Digest) ToSingletonSet() Set {
	return Set{
		digests: []Digest{d},
	}
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
		instanceName: d.GetInstanceName(),
		partialHash:  d.NewHasher(),
	}
}

// Generator is a writer that may be used to compute digests of newly
// created files.
type Generator struct {
	instanceName InstanceName
	partialHash  hash.Hash
	sizeBytes    int64
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
	return dg.instanceName.newDigestUnchecked(
		hex.EncodeToString(dg.partialHash.Sum(nil)),
		dg.sizeBytes)
}
