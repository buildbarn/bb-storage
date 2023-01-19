package digest

import (
	"encoding/binary"
	"encoding/hex"
	"io"
	"strings"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Keywords that are not permitted to be placed inside instance names by
// the REv2 protocol. Permitting these would make parsing of URLs, such
// as the ones provided to the ByteStream service, ambiguous.
var reservedInstanceNameKeywords = map[string]bool{
	"blobs":            true,
	"uploads":          true,
	"actions":          true,
	"actionResults":    true,
	"operations":       true,
	"capabilities":     true,
	"compressed-blobs": true,
}

// InstanceName is a simple container around REv2 instance name strings.
// Because instance names are embedded in URLs, the REv2 protocol places
// some restrictions on which instance names are valid. This type can
// only be instantiated for values that are valid.
type InstanceName struct {
	value string
}

// EmptyInstanceName corresponds to the instance name "". It is mainly
// declared to be used in places where the instance name doesn't matter
// (e.g., return values of functions in error cases).
var EmptyInstanceName InstanceName

func validateInstanceNameComponents(components []string) error {
	for _, component := range components {
		if component == "" {
			panic("Attempted to create an instance name with an empty component")
		}
		if _, ok := reservedInstanceNameKeywords[component]; ok {
			return status.Errorf(codes.InvalidArgument, "Instance name contains reserved keyword %#v", component)
		}
	}
	return nil
}

// NewInstanceName creates a new InstanceName object that can be used to
// parse digests.
func NewInstanceName(value string) (InstanceName, error) {
	if strings.HasPrefix(value, "/") || strings.HasSuffix(value, "/") || strings.Contains(value, "//") {
		return InstanceName{}, status.Error(codes.InvalidArgument, "Instance name contains redundant slashes")
	}
	components := strings.FieldsFunc(value, func(r rune) bool { return r == '/' })
	if err := validateInstanceNameComponents(components); err != nil {
		return InstanceName{}, err
	}
	return InstanceName{
		value: value,
	}, nil
}

// NewInstanceNameFromComponents is identical to NewInstanceName, except
// that it takes a series of pathname components instead of a single
// string.
func NewInstanceNameFromComponents(components []string) (InstanceName, error) {
	if err := validateInstanceNameComponents(components); err != nil {
		return InstanceName{}, err
	}
	return InstanceName{
		value: strings.Join(components, "/"),
	}, nil
}

// MustNewInstanceName is identical to NewInstanceName, except that it
// panics in case the instance name is invalid. This function can be
// used as part of unit tests.
func MustNewInstanceName(value string) InstanceName {
	instanceName, err := NewInstanceName(value)
	if err != nil {
		panic(err)
	}
	return instanceName
}

// NewDigestFromCompactBinary constructs a Digest object by reading data
// from a ByteReader that contains data that was generated using
// Digest.GetCompactBinary().
func (in InstanceName) NewDigestFromCompactBinary(r io.ByteReader) (Digest, error) {
	digestFunctionEnum, err := r.ReadByte()
	if err != nil {
		return BadDigest, err
	}
	digestFunction, err := in.GetDigestFunction(remoteexecution.DigestFunction_Value(digestFunctionEnum), 0)
	if err != nil {
		return BadDigest, err
	}

	hashBytesSize := digestFunction.bareFunction.hashBytesSize
	hash := make([]byte, 0, hashBytesSize)
	for i := 0; i < hashBytesSize; i++ {
		b, err := r.ReadByte()
		if err != nil {
			return BadDigest, err
		}
		hash = append(hash, b)
	}

	sizeBytes, err := binary.ReadVarint(r)
	if err != nil {
		return BadDigest, err
	}

	return digestFunction.NewDigest(hex.EncodeToString(hash), sizeBytes)
}

func (in InstanceName) String() string {
	return in.value
}

// GetComponents splits the instance name by '/' and returns each of the
// components. It is the inverse of NewInstanceNameFromComponents().
func (in InstanceName) GetComponents() []string {
	return strings.FieldsFunc(in.value, func(r rune) bool { return r == '/' })
}

// GetDigestFunction creates a digest function object that is based on
// an instance name object and an REv2 digest function enumeration
// value.
//
// When generating digests from within a context where a parent digest
// exists (e.g., on a worker that is executing an action), it is
// possible to call Digest.GetDigestFunction(). This function can be
// used when digests need to be generated outside of such contexts
// (e.g., on a client that is uploading actions into the Content
// Addressable Storage).
func (in InstanceName) GetDigestFunction(digestFunction remoteexecution.DigestFunction_Value, fallbackHashLength int) (Function, error) {
	bareFunction := getBareFunction(digestFunction, fallbackHashLength)
	if bareFunction == nil {
		return Function{}, status.Error(codes.InvalidArgument, "Unknown digest function")
	}
	return Function{
		instanceName: in,
		bareFunction: bareFunction,
	}, nil
}
