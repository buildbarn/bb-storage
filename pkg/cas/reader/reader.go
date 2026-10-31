package reader

import (
	"context"

	"github.com/buildbarn/bb-storage/pkg/digest"
)

// Reader can be used to read a value from the Content Addressable
// Storage.
type Reader[T any] interface {
	// Read the value stored under the given digest.
	Read(ctx context.Context, d digest.Digest) (T, error)
}
