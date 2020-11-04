package buffer

import (
	"encoding/hex"
	"log"

	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// DataIntegrityCallback is a callback that is invoked by Buffer
// whenever the contents of a Buffer have been checked for data
// integrity. Its boolean parameter indicates whether the contents of
// the buffer were valid.
//
// For the CAS, this indicates that the contents correspond with the
// digest. For the AC and ICAS, this indicates that the contents contain
// a valid Protobuf message.
//
// This callback can be used by a storage backend to either discard
// malformed objects or prevent the need for further data integrity
// checking.
type DataIntegrityCallback func(dataIsValid bool)

// Irreparable indicates that the buffer was obtained from storage, but
// that the storage provides no method for repairing the data. This
// doesn't necessarily have to be harmful. It may well be the case that
// the storage backend also has logic in place to detect inconsistencies
// and that there is no need for us to report those.
func Irreparable(blobDigest digest.Digest) DataIntegrityCallback {
	return func(dataIsValid bool) {
		if !dataIsValid {
			log.Printf("Digest %#v is corrupted, but its storage backend does not support repairing corrupted blobs", blobDigest.String())
		}
	}
}

// Source is passed to most New*Buffer() creation functions to specify
// information where the data contained in the buffer originated.
type Source struct {
	errorCode             codes.Code
	dataIntegrityCallback DataIntegrityCallback
}

func (s Source) notifyDataValid() {
	s.dataIntegrityCallback(true)
}

// notifyProtoMarshalFailure triggers a repair due to a Protobuf message
// failing to be marshaled properly.
func (s Source) notifyProtoMarshalFailure(marshalErr error) error {
	s.dataIntegrityCallback(false)
	return util.StatusWrapWithCode(marshalErr, s.errorCode, "Failed to marshal message")
}

// notifyProtoUnmarshalFailure triggers a repair due to a Protobuf
// message failing to be unmarshaled properly.
func (s Source) notifyProtoUnmarshalFailure(unmarshalErr error) error {
	s.dataIntegrityCallback(false)
	return util.StatusWrapWithCode(unmarshalErr, s.errorCode, "Failed to unmarshal message")
}

// notifyCASTooBig triggers a repair due to a Content Addressable
// Storage object being larger than expected.
func (s Source) notifyCASTooBig(sizeExpected, sizeObserved int64) error {
	s.dataIntegrityCallback(false)
	return status.Errorf(
		s.errorCode,
		"Buffer is at least %d bytes in size, while %d bytes were expected",
		sizeObserved,
		sizeExpected)
}

// notifyCASSizeMismatch triggers a repair due to a Content Addressable
// Storage object having the wrong exact size.
func (s Source) notifyCASSizeMismatch(sizeExpected, sizeObserved int64) error {
	s.dataIntegrityCallback(false)
	return status.Errorf(
		s.errorCode,
		"Buffer is %d bytes in size, while %d bytes were expected",
		sizeObserved,
		sizeExpected)
}

// notifyCASHashMismatch triggers a repair due to a Content Addressable
// Storage object having the wrong cryptographic checksum.
func (s Source) notifyCASHashMismatch(hashExpected, hashObserved []byte) error {
	s.dataIntegrityCallback(false)
	return status.Errorf(
		s.errorCode,
		"Buffer has checksum %s, while %s was expected",
		hex.EncodeToString(hashObserved),
		hex.EncodeToString(hashExpected))
}

// UserProvided indicates that the buffer did not come from storage.
// Instead, it is an artifact that is currently being uploaded by a user
// or automated process. When data consistency errors occur, no data
// needs to be repaired. It is sufficient to return an error to the
// user.
var UserProvided = Source{
	errorCode:             codes.InvalidArgument,
	dataIntegrityCallback: func(dataIsValid bool) {},
}

// BackendProvided indicates that the buffer came from storage. A
// DataIntegrityCallback can be provided to receive notifications on
// data integrity of the buffer.
func BackendProvided(dataIntegrityCallback DataIntegrityCallback) Source {
	return Source{
		errorCode:             codes.Internal,
		dataIntegrityCallback: dataIntegrityCallback,
	}
}
