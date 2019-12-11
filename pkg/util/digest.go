package util

import (
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/hex"
	"fmt"
	"hash"
	"log"
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
type Digest struct {
	instance  string
	hash      string
	sizeBytes int64
}

var (
	// SupportedDigestFunctions is the list of digest functions
	// supported by util.Digest, using the enumeration values that
	// are part of the Remote Execution protocol.
	SupportedDigestFunctions = []remoteexecution.DigestFunction_Value{
		remoteexecution.DigestFunction_MD5,
		remoteexecution.DigestFunction_SHA1,
		remoteexecution.DigestFunction_SHA256,
		remoteexecution.DigestFunction_SHA384,
		remoteexecution.DigestFunction_SHA512,
		remoteexecution.DigestFunction_VSO,
	}
)

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
	if l := len(partialDigest.Hash); l != md5.Size*2 && l != sha1.Size*2 &&
		l != sha256.Size*2 && l != sha512.Size384*2 && l != sha512.Size*2 &&
		l != vsoHashSize*2 {
		return nil, status.Errorf(codes.InvalidArgument, "Unknown digest hash length: %d characters", l)
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
		instance:  instance,
		hash:      partialDigest.Hash,
		sizeBytes: partialDigest.SizeBytes,
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

// NewDigestFromBytestreamPath creates a Digest from a string having one
// of the following two formats:
//
// - blobs/${hash}/${size}
// - ${instance}/blobs/${hash}/${size}
//
// This notation is used by Bazel to refer to files accessible through a
// gRPC Bytestream service.
func NewDigestFromBytestreamPath(path string) (*Digest, error) {
	fields := strings.FieldsFunc(path, func(r rune) bool { return r == '/' })
	l := len(fields)
	if (l != 3 && l != 4) || fields[l-3] != "blobs" {
		return nil, status.Errorf(codes.InvalidArgument, "Invalid resource naming scheme")
	}
	size, err := strconv.ParseInt(fields[l-1], 10, 64)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "Invalid resource naming scheme")
	}
	instance := ""
	if l == 4 {
		instance = fields[0]
	}
	return NewDigest(
		instance,
		&remoteexecution.Digest{
			Hash:      fields[l-2],
			SizeBytes: size,
		})
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
	return &remoteexecution.Digest{
		Hash:      d.hash,
		SizeBytes: d.sizeBytes,
	}
}

// GetInstance returns the instance name of the object.
func (d *Digest) GetInstance() string {
	return d.instance
}

// GetHashBytes returns the hash of the object as a slice of bytes.
func (d *Digest) GetHashBytes() []byte {
	hash, err := hex.DecodeString(d.hash)
	if err != nil {
		log.Fatal("Failed to decode digest hash, even though its contents have already been validated")
	}
	return hash
}

// GetHashString returns the hash of the object as a string.
func (d *Digest) GetHashString() string {
	return d.hash
}

// GetSizeBytes returns the size of the object, in bytes.
func (d *Digest) GetSizeBytes() int64 {
	return d.sizeBytes
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
		return fmt.Sprintf("%s-%d", d.hash, d.sizeBytes)
	case DigestKeyWithInstance:
		return fmt.Sprintf("%s-%d-%s", d.hash, d.sizeBytes, d.instance)
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
	switch len(d.hash) {
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
	case vsoHashSize * 2:
		return newVSOHasher()
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
		instance:  dg.instance,
		hash:      hex.EncodeToString(dg.partialHash.Sum(nil)),
		sizeBytes: dg.sizeBytes,
	}
}

const (
	vsoHashSize      = sha256.Size + 1
	vsoBytesPerPage  = 1 << 16
	vsoPagesPerBlock = 32
)

type vsoHasher struct {
	pageHash            hash.Hash
	pageRemainingBytes  int
	blockHash           hash.Hash
	blockRemainingPages int
	summaryHash         hash.Hash
}

// newVSOHasher creates a hasher that computes VSO ('Visual Studio
// Online') hashes. These are used as part of the BuildXL system.
// VSO hashes are effectively three layers of checksumming applied on
// top of each other:
//
// - 64 KiB of data is stored in a page.
// - SHA-256 hashes of 32 pages (2 MiB of data) are stored in a block.
// - SHA-256 hashes of blocks are stored in a summary.
// - SHA-256 hashes of summaries are used to identify objects.
//
// The advantage of VSO hashes over plain SHA-256 is that it can be
// computed in parallel for a single block. They also make it possible
// to validate the integrity and perform deduplication of parts of
// files.
//
// References:
// - https://github.com/microsoft/BuildXL/blob/master/Documentation/Specs/PagedHash.md
// - https://github.com/microsoft/BuildXL/blob/master/Public/Src/Cache/ContentStore/Hashing/VsoHash.cs
// - https://github.com/microsoft/BuildXL/blob/master/Public/Src/Cache/ContentStore/InterfacesTest/Hashing/VsoHashTests.cs
func newVSOHasher() hash.Hash {
	vh := &vsoHasher{}
	vh.Reset()
	return vh
}

func (vh *vsoHasher) Write(p []byte) (int, error) {
	total := len(p)
	for {
		// Store more data within the current page.
		nWrite := len(p)
		if nWrite > vh.pageRemainingBytes {
			nWrite = vh.pageRemainingBytes
		}
		nWritten, _ := vh.pageHash.Write(p[:nWrite])
		p = p[nWritten:]
		vh.pageRemainingBytes -= nWritten
		if len(p) == 0 {
			return total, nil
		}

		// Add a single page to the current block.
		vh.blockHash.Write(vh.pageHash.Sum(nil))
		vh.pageHash.Reset()
		vh.pageRemainingBytes = vsoBytesPerPage
		vh.blockRemainingPages--

		// Add a single block to the summary.
		if vh.blockRemainingPages == 0 {
			vh.summaryHash.Write(vh.blockHash.Sum(nil))
			vh.summaryHash.Write([]byte{0})
			blobID := vh.summaryHash.Sum(nil)
			vh.summaryHash.Reset()
			vh.summaryHash.Write(blobID)
			vh.blockHash.Reset()
			vh.blockRemainingPages = vsoPagesPerBlock
		}
	}
}

func (vh *vsoHasher) Sum(b []byte) []byte {
	if vh.pageRemainingBytes != vsoBytesPerPage {
		vh.blockHash.Write(vh.pageHash.Sum(nil))
	}
	vh.summaryHash.Write(vh.blockHash.Sum(nil))
	vh.summaryHash.Write([]byte{1})
	return append(append(b, vh.summaryHash.Sum(nil)...), 0)
}

func (vh *vsoHasher) Reset() {
	*vh = vsoHasher{
		pageHash:            sha256.New(),
		pageRemainingBytes:  vsoBytesPerPage,
		blockHash:           sha256.New(),
		blockRemainingPages: vsoPagesPerBlock,
		summaryHash:         sha256.New(),
	}
	vh.summaryHash.Write([]byte("VSO Content Identifier Seed"))
}

func (vh *vsoHasher) Size() int {
	return vsoHashSize
}

func (vh *vsoHasher) BlockSize() int {
	return vsoBytesPerPage
}
