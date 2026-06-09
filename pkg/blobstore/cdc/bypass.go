package cdc

import "context"

type chunkListValidationBypassKey struct{}

// NewContextWithChunkListValidationBypass creates a derived context
// that signals downstream storage layers that the chunk list has
// already been validated (or was freshly generated) and does not need
// expensive re-validation.
func NewContextWithChunkListValidationBypass(ctx context.Context) context.Context {
	return context.WithValue(ctx, chunkListValidationBypassKey{}, chunkListValidationBypassKey{})
}

// ChunkListValidationBypassed checks if the provided context contains
// the bypass signal.
func ChunkListValidationBypassed(ctx context.Context) bool {
	if value := ctx.Value(chunkListValidationBypassKey{}); value != nil {
		return true
	}
	return false
}
