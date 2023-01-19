package digest

import (
	"encoding/binary"
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

// The offset within the digest string at which the hash starts. We
// currently assume that all supported digest functions have an
// enumeration value below 10, meaning all digests starts with "[0-9]-".
const hashStart = 2

// Automatically register all compression algorithms that are part of
// the protocol.
var (
	compressorEnumToMidfix = map[remoteexecution.Compressor_Value]string{
		remoteexecution.Compressor_IDENTITY: "blobs",
	}
	compressorNameToEnum = map[string]remoteexecution.Compressor_Value{}

	digestFunctionEnumToMidfix       = map[remoteexecution.DigestFunction_Value]string{}
	digestFunctionNameToBareFunction = map[string]*bareFunction{}
)

func init() {
	// Generate Bytestream path midfixes for compressor functions.
	for value, name := range remoteexecution.Compressor_Value_name {
		enum := remoteexecution.Compressor_Value(value)
		if enum != remoteexecution.Compressor_IDENTITY {
			lowerName := strings.ToLower(name)
			compressorEnumToMidfix[enum] = "compressed-blobs/" + lowerName
			compressorNameToEnum[lowerName] = enum
		}
	}

	// Generate Bytestream path midfixes for digest functions. For
	// compatibility, these should only be added for digest
	// functions exceeding enumeration value 7.
	for _, digestFunction := range SupportedDigestFunctions {
		if digestFunction > 7 {
			lowerName := strings.ToLower(digestFunction.String())
			digestFunctionEnumToMidfix[digestFunction] = lowerName
			digestFunctionNameToBareFunction[lowerName] = getBareFunction(digestFunction, 0)
		}
	}
}

// Digest holds the identification of an object stored in the Content
// Addressable Storage (CAS) or Action Cache (AC). The use of this
// object is preferred over remoteexecution.Digest for a couple of
// reasons.
//
//   - Instances of these objects are guaranteed not to contain any
//     degenerate values. The hash has already been decoded from
//     hexadecimal to binary. The size is non-negative.
//   - They keep track of the instance as part of the digest, which allows
//     us to keep function signatures across the codebase simple.
//   - They provide utility functions for deriving new digests from them.
//     This ensures that outputs of build actions automatically use the
//     same instance name and hashing algorithm.
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

// BadDigest is a default instance of Digest. It can, for example, be
// used as a function return value for error cases.
var BadDigest Digest

// RemoveUnsupportedDigestFunctions returns the intersection between a
// list of provided digest functions and ones supported by this
// implementation. Results are guaranteed to be deduplicated and in
// alphabetic order.
func RemoveUnsupportedDigestFunctions(reported []remoteexecution.DigestFunction_Value) []remoteexecution.DigestFunction_Value {
	// Convert provided digest functions to a set.
	reportedSet := make(map[remoteexecution.DigestFunction_Value]struct{}, len(reported))
	for _, digestFunction := range reported {
		reportedSet[digestFunction] = struct{}{}
	}
	// Intersect with the supported set of digests.
	supported := make([]remoteexecution.DigestFunction_Value, 0, len(SupportedDigestFunctions))
	for _, digestFunction := range SupportedDigestFunctions {
		if _, ok := reportedSet[digestFunction]; ok {
			supported = append(supported, digestFunction)
		}
	}
	return supported
}

// Unpack the individual hash, size and instance name fields from the
// string representation stored inside the Digest object.
func (d Digest) unpack() (remoteexecution.DigestFunction_Value, int, int64, int) {
	// Extract the leading hash.
	hashEnd := shortestSupportedHashStringSize
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

	return remoteexecution.DigestFunction_Value(d.value[0] - '0'), hashEnd, sizeBytes, sizeBytesEnd
}

// MustNewDigest constructs a Digest similar to NewDigest, but never
// returns an error. Instead, execution will abort if the resulting
// instance would be degenerate. Useful for unit testing.
func MustNewDigest(instanceName string, digestFunctionEnum remoteexecution.DigestFunction_Value, hash string, sizeBytes int64) Digest {
	digestFunction := MustNewFunction(instanceName, digestFunctionEnum)
	d, err := digestFunction.NewDigest(hash, sizeBytes)
	if err != nil {
		panic(err)
	}
	return d
}

// NewDigestFromByteStreamReadPath creates a Digest from a string having
// one of the following formats:
//
// - ${instanceName}/blobs/${digestFunction}/${hash}/${size}
// - ${instanceName}/compressed-blobs/${compressor}/${digestFunction}/${hash}/${size}
//
// This notation is used to read files through the ByteStream service.
func NewDigestFromByteStreamReadPath(path string) (Digest, remoteexecution.Compressor_Value, error) {
	fields := strings.FieldsFunc(path, func(r rune) bool { return r == '/' })
	if len(fields) < 3 {
		return BadDigest, remoteexecution.Compressor_IDENTITY, status.Error(codes.InvalidArgument, "Invalid resource naming scheme")
	}
	// Determine the end of the instance name.
	split := 0
	for fields[split] != "blobs" && fields[split] != "compressed-blobs" {
		split++
		if split > len(fields)-3 {
			return BadDigest, remoteexecution.Compressor_IDENTITY, status.Error(codes.InvalidArgument, "Invalid resource naming scheme")
		}
	}
	return newDigestFromByteStreamPathCommon(fields[:split], fields[split:])
}

// NewDigestFromByteStreamWritePath creates a Digest from a string
// having one of the following formats:
//
// - ${instanceName}/uploads/${uuid}/blobs/${digestFunction}/${hash}/${size}/${path}
// - ${instanceName}/uploads/${uuid}/compressed-blobs/${compressor}/${digestFunction}/${hash}/${size}/${path}
//
// This notation is used to write files through the ByteStream service.
func NewDigestFromByteStreamWritePath(path string) (Digest, remoteexecution.Compressor_Value, error) {
	fields := strings.FieldsFunc(path, func(r rune) bool { return r == '/' })
	if len(fields) < 5 {
		return BadDigest, remoteexecution.Compressor_IDENTITY, status.Error(codes.InvalidArgument, "Invalid resource naming scheme")
	}
	// Determine the end of the instance name. Because both the
	// leading instance name and the trailing path have a variable
	// length, this may be ambiguous. This is why instance names are
	// not permitted to contain "uploads" pathname components.
	split := 0
	for fields[split] != "uploads" {
		split++
		if split > len(fields)-5 {
			return BadDigest, remoteexecution.Compressor_IDENTITY, status.Error(codes.InvalidArgument, "Invalid resource naming scheme")
		}
	}
	return newDigestFromByteStreamPathCommon(fields[:split], fields[split+2:])
}

func newDigestFromByteStreamPathCommon(header, trailer []string) (Digest, remoteexecution.Compressor_Value, error) {
	instanceName, err := NewInstanceNameFromComponents(header)
	if err != nil {
		return BadDigest, remoteexecution.Compressor_IDENTITY, util.StatusWrapf(err, "Invalid instance name %#v", strings.Join(header, "/"))
	}

	// Remove the leading compression scheme name.
	var compressor remoteexecution.Compressor_Value
	switch trailer[0] {
	case "blobs":
		compressor = remoteexecution.Compressor_IDENTITY
		trailer = trailer[1:]
	case "compressed-blobs":
		var ok bool
		compressor, ok = compressorNameToEnum[trailer[1]]
		if !ok {
			return BadDigest, remoteexecution.Compressor_IDENTITY, status.Errorf(codes.Unimplemented, "Unsupported compression scheme %#v", trailer[1])
		}
		trailer = trailer[2:]
	}

	bareFunction, ok := digestFunctionNameToBareFunction[trailer[0]]
	if ok {
		// Explicit digest function name provided.
		trailer = trailer[1:]
	} else {
		// Infer digest function from the hash length.
		bareFunction = getBareFunction(remoteexecution.DigestFunction_UNKNOWN, len(trailer[0]))
		if bareFunction == nil {
			return BadDigest, remoteexecution.Compressor_IDENTITY, status.Errorf(codes.InvalidArgument, "Unsupported digest function %#v", trailer[0])
		}
	}
	digestFunction := Function{
		instanceName: instanceName,
		bareFunction: bareFunction,
	}

	if len(trailer) < 2 {
		return BadDigest, remoteexecution.Compressor_IDENTITY, status.Error(codes.InvalidArgument, "Invalid resource naming scheme")
	}
	sizeBytes, err := strconv.ParseInt(trailer[1], 10, 64)
	if err != nil {
		return BadDigest, remoteexecution.Compressor_IDENTITY, status.Errorf(codes.InvalidArgument, "Invalid blob size %#v", trailer[1])
	}
	d, err := digestFunction.NewDigest(trailer[0], sizeBytes)
	return d, compressor, err
}

// GetByteStreamReadPath converts the Digest to a string having
// one of the following formats:
//
// - ${instanceName}/blobs/${digestFunction}/${hash}/${size}
// - ${instanceName}/compressed-blobs/${compressor}/${digestFunction}/${hash}/${size}
//
// This notation is used to read files through the ByteStream service.
func (d Digest) GetByteStreamReadPath(compressor remoteexecution.Compressor_Value) string {
	digestFunction, hashEnd, sizeBytes, sizeBytesEnd := d.unpack()
	return path.Join(
		d.value[sizeBytesEnd+1:],
		compressorEnumToMidfix[compressor],
		digestFunctionEnumToMidfix[digestFunction],
		d.value[hashStart:hashEnd],
		strconv.FormatInt(sizeBytes, 10))
}

// GetByteStreamWritePath converts the Digest to a string having one of
// the following formats:
//
// - ${instanceName}/uploads/${uuid}/blobs/${digestFunction}/${hash}/${size}
// - ${instanceName}/uploads/${uuid}/compressed-blobs/${digestFunction}/${compressor}/${hash}/${size}
//
// This notation is used to write files through the ByteStream service.
func (d Digest) GetByteStreamWritePath(uuid uuid.UUID, compressor remoteexecution.Compressor_Value) string {
	digestFunction, hashEnd, sizeBytes, sizeBytesEnd := d.unpack()
	return path.Join(
		d.value[sizeBytesEnd+1:],
		"uploads",
		uuid.String(),
		compressorEnumToMidfix[compressor],
		digestFunctionEnumToMidfix[digestFunction],
		d.value[hashStart:hashEnd],
		strconv.FormatInt(sizeBytes, 10))
}

// GetProto encodes the digest into the format used by the remote
// execution protocol, so that it may be stored in messages returned to
// the client.
func (d Digest) GetProto() *remoteexecution.Digest {
	_, hashEnd, sizeBytes, _ := d.unpack()
	return &remoteexecution.Digest{
		Hash:      d.value[hashStart:hashEnd],
		SizeBytes: sizeBytes,
	}
}

// GetInstanceName returns the instance name of the object.
func (d Digest) GetInstanceName() InstanceName {
	_, _, _, sizeBytesEnd := d.unpack()
	return InstanceName{
		value: d.value[sizeBytesEnd+1:],
	}
}

// GetHashBytes returns the hash of the object as a slice of bytes.
func (d Digest) GetHashBytes() []byte {
	_, hashEnd, _, _ := d.unpack()
	hash, err := hex.DecodeString(d.value[hashStart:hashEnd])
	if err != nil {
		panic("Failed to decode digest hash, even though its contents have already been validated")
	}
	return hash
}

// GetHashString returns the hash of the object as a string.
func (d Digest) GetHashString() string {
	_, hashEnd, _, _ := d.unpack()
	return d.value[hashStart:hashEnd]
}

// GetSizeBytes returns the size of the object, in bytes.
func (d Digest) GetSizeBytes() int64 {
	_, _, sizeBytes, _ := d.unpack()
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
		_, _, _, sizeBytesEnd := d.unpack()
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
//
// The expected size can be used as a hint to create an appropriately
// sized hasher. If the expected size is unknown, provide math.MaxInt64.
func (d Digest) NewHasher(expectedSizeBytes int64) hash.Hash {
	digestFunction, _, _, _ := d.unpack()
	return getBareFunction(digestFunction, 0).hasherFactory(expectedSizeBytes)
}

// GetDigestFunction returns a Function object that can be used to
// generate new Digest objects that use the same instance name and
// hashing algorithm. This method can be used in case new digests need
// to be derived based on an existing instance. For example, to generate
// a digest of an output file of a build action, given an action digest.
func (d Digest) GetDigestFunction() Function {
	digestFunction, _, _, sizeBytesEnd := d.unpack()
	return Function{
		instanceName: InstanceName{
			value: d.value[sizeBytesEnd+1:],
		},
		bareFunction: getBareFunction(digestFunction, 0),
	}
}

// UsesDigestFunction returns true iff a Digest has the same instance
// name and uses the same hashing algorithm as a provided Function
// object.
func (d Digest) UsesDigestFunction(f Function) bool {
	digestFunction, _, _, sizeBytesEnd := d.unpack()
	return digestFunction == f.bareFunction.enumValue && d.value[sizeBytesEnd+1:] == f.instanceName.value
}

// GetDigestsWithParentInstanceNames returns a list of Digest objects
// that contain the same hash and size in bytes, but have the instance
// name truncated to an increasing number of components.
//
// For example, if a digest with instance name
// "this/is/an/instance/name" is provided, this function will return a
// list of six digests, having instance names "", "this", "this/is",
// "this/is/an", "this/is/an/instance" and "this/is/an/instance/name".
func (d Digest) GetDigestsWithParentInstanceNames() []Digest {
	_, _, _, sizeBytesEnd := d.unpack()
	instanceNameStart := sizeBytesEnd + 1
	digestWithoutInstanceName := Digest{
		value: d.value[:instanceNameStart],
	}
	if instanceNameStart == len(d.value) {
		// Corner case: The digest uses the empty instance name.
		// Return a singleton list.
		return []Digest{digestWithoutInstanceName}
	}

	// Count the number of components.
	components := 1
	for i := instanceNameStart + 1; i < len(d.value)-1; i++ {
		if d.value[i] == '/' {
			components++
		}
	}

	// Create all of the digests in reverse order.
	digests := make([]Digest, components+1)
	digests[0] = digestWithoutInstanceName
	for {
		digests[components] = d
		components--
		if components == 0 {
			return digests
		}
		end := len(d.value) - 1
		for d.value[end-1] != '/' {
			end--
		}
		d.value = d.value[:end-1]
	}
}

// GetCompactBinary returns a compact binary representation of the
// Digest, not including the instance name. The representation consists
// of the length of the Digest's hash, the hash in binary form, and the
// length of the object encoded as a variable length integer.
//
// This representation is used by the NFSv4 server, as it needs to
// encode digests in file handles.
func (d Digest) GetCompactBinary() []byte {
	digestFunction, hashEnd, sizeBytes, _ := d.unpack()

	hash, err := hex.DecodeString(d.value[hashStart:hashEnd])
	if err != nil {
		panic("Failed to decode digest hash, even though its contents have already been validated")
	}

	var encodedSize [binary.MaxVarintLen64]byte
	encodedSizeLength := binary.PutVarint(encodedSize[:], sizeBytes)

	return append(append([]byte{byte(digestFunction)}, hash...), encodedSize[:encodedSizeLength]...)
}
