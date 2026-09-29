package cas

import (
	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/digest"
)

// IsSingleChunk checks if a digest is represented by a single chunk.
func IsSingleChunk(params *remoteexecution.RepMaxCdcParams, d digest.Digest) bool {
	return d.GetSizeBytes() < 2*int64(params.MinChunkSizeBytes)
}
