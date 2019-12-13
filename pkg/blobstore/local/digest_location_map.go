package local

import (
	"github.com/buildbarn/bb-storage/pkg/util"
)

// DigestLocationMap is equivalent to a map[*util.Digest]Location. It is
// used by LocalBlobAccess to track where blobs are stored, so that they
// may be accessed. Implementations are permitted to discard entries
// for outdated locations during lookups/insertions using the provided
// validator.
type DigestLocationMap interface {
	Get(digest *util.Digest, validator *LocationValidator) (Location, error)
	Put(digest *util.Digest, validator *LocationValidator, location Location) error
}
