package util

import (
	"github.com/google/uuid"
)

// UUIDGenerator is equal to the signature of the UUID library's UUID
// generation functions. It is used within this codebase to make the
// generator injectable as part of unit tests.
type UUIDGenerator func() (uuid.UUID, error)

var _ UUIDGenerator = uuid.NewDCEGroup
var _ UUIDGenerator = uuid.NewDCEPerson
var _ UUIDGenerator = uuid.NewRandom
var _ UUIDGenerator = uuid.NewUUID
