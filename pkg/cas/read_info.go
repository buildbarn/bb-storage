package cas

import (
	"github.com/buildbarn/bb-storage/pkg/blobstore/chunk"
	"github.com/buildbarn/bb-storage/pkg/capabilities"
	"github.com/buildbarn/bb-storage/pkg/cas/reader"
)

// ReadInfo packages the components that are required to read blobs from
// the Content Addressable Storage (CAS). It is intended to be injected
// in types that expose objects from the CAS, so that readers can be
// constructed from them directly.
type ReadInfo struct {
	// ChunkBytesReader is an interface that reads the raw underlying
	// bytes of a chunk.
	ChunkBytesReader reader.Reader[[]byte]
	// ChunkMappingFetcher is an interface that gets the Chunk Mapping
	// of a given digest.
	ChunkMappingFetcher chunk.MappingFetcher
	// CDCParametersFetcher is an interface that fetches the CDC
	// parameters of a given instance name.
	CDCParametersFetcher capabilities.CDCParametersFetcher
}
