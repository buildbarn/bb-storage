package buffer

import (
	"encoding/hex"
	"log"

	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// RepairFunc is a callback that may be invoked by buffer objects to
// report that the contents of the buffer are observed to be invalid.
// More concretely, it is invoked when an Action Cache buffer object
// could not be unmarshaled into a Protobuf ActionResult message, or
// when the contents of a Content Addressable Storage buffer don't match
// up with the digest of the object.
//
// This callback allows storage backends to repair the object, for
// example by replicating it from other storage backends or by simply
// deleting it, so that it may be recomputed or re-uploaded.
type RepairFunc func() error

// RepairStrategy is passed to most New*Buffer() creation functions to
// specify a strategy for how to deal with data consistency issues.
type RepairStrategy struct {
	errorCode  codes.Code
	digest     *util.Digest
	repairFunc RepairFunc
}

func (rs RepairStrategy) repair() {
	if rs.repairFunc != nil {
		if err := rs.repairFunc(); err == nil {
			log.Printf("Successfully repaired corrupted blob %s", rs.digest)
		} else {
			log.Printf("Failed to repair corrupted blob %s: %s", rs.digest, err)
		}
	}
}

// repairACMarshalFailure triggers a repair due to an Action Cache
// message failing to be marshaled properly.
func (rs RepairStrategy) repairACMarshalFailure(marshalErr error) error {
	rs.repair()
	return util.StatusWrapWithCode(marshalErr, rs.errorCode, "Failed to marshal message")
}

// repairACUnmarshalFailure triggers a repair due to an Action Cache
// message failing to be unmarshaled properly.
func (rs RepairStrategy) repairACUnmarshalFailure(unmarshalErr error) error {
	rs.repair()
	return util.StatusWrapWithCode(unmarshalErr, rs.errorCode, "Failed to unmarshal message")
}

// repairCASTooBig triggers a repair due to a Content Addressable
// Storage object being larger than expected.
func (rs RepairStrategy) repairCASTooBig(sizeExpected int64, sizeObserved int64) error {
	rs.repair()
	return status.Errorf(
		rs.errorCode,
		"Buffer is at least %d bytes in size, while %d bytes were expected",
		sizeObserved,
		sizeExpected)
}

// repairCASSizeMismatch triggers a repair due to a Content Addressable
// Storage object having the wrong exact size.
func (rs RepairStrategy) repairCASSizeMismatch(sizeExpected int64, sizeObserved int64) error {
	rs.repair()
	return status.Errorf(
		rs.errorCode,
		"Buffer is %d bytes in size, while %d bytes were expected",
		sizeObserved,
		sizeExpected)
}

// repairCASHashMismatch triggers a repair due to a Content Addressable
// Storage object having the wrong cryptographic checksum.
func (rs RepairStrategy) repairCASHashMismatch(hashExpected []byte, hashObserved []byte) error {
	rs.repair()
	return status.Errorf(
		rs.errorCode,
		"Buffer has checksum %s, while %s was expected",
		hex.EncodeToString(hashObserved),
		hex.EncodeToString(hashExpected))
}

var (
	// UserProvided indicates that the buffer did not come from
	// storage. Instead, it is an artifact that is currently being
	// uploaded by a user or automated process. When data
	// consistency errors occur, no data needs to be repaired. It is
	// sufficient to return an error to the user.
	UserProvided = RepairStrategy{
		errorCode: codes.InvalidArgument,
	}
	// Irreparable indicates that the buffer was obtained from
	// storage, but that the storage provides no method for
	// repairing the data. This doesn't necessarily have to be
	// harmful. It may well be the case that the storage backend
	// also has logic in place to detect inconsistencies and that
	// there is no need for us to report those.
	Irreparable = RepairStrategy{
		errorCode: codes.Internal,
		repairFunc: func() error {
			return status.Error(codes.Unimplemented, "Storage backend does not support repairing corrupted blobs")
		},
	}
)

// Reparable indicates that the buffer was obtained from storage and
// that the storage backend provides a method for repairing data
// inconsistencies. A callback is provided to trigger repairs.
func Reparable(digest *util.Digest, repairFunc RepairFunc) RepairStrategy {
	return RepairStrategy{
		errorCode:  codes.Internal,
		digest:     digest,
		repairFunc: repairFunc,
	}
}
