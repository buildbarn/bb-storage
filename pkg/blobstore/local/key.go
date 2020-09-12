package local

import (
	"crypto/sha256"
)

// Key is the key type for objects managed by KeyLocationMap.
type Key [sha256.Size]byte

// NewKeyFromString creates a new Key based on a string value. Because
// keys are fixed size, this function uses SHA-256 to convert a variable
// size string to a key. This means that keys are irreversible.
func NewKeyFromString(s string) Key {
	return sha256.Sum256([]byte(s))
}
