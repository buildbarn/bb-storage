package blobstore

import (
	"io"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/proto/iscc"

	"google.golang.org/protobuf/proto"
)

type isccReadBufferFactory struct{}

func (f isccReadBufferFactory) NewBufferFromByteSlice(digest digest.Digest, data []byte, dataIntegrityCallback buffer.DataIntegrityCallback) buffer.Buffer {
	return buffer.NewProtoBufferFromByteSlice(&iscc.PreviousExecutionStats{}, data, buffer.BackendProvided(dataIntegrityCallback))
}

func (f isccReadBufferFactory) NewBufferFromReader(digest digest.Digest, r io.ReadCloser, dataIntegrityCallback buffer.DataIntegrityCallback) buffer.Buffer {
	return buffer.NewProtoBufferFromReader(&iscc.PreviousExecutionStats{}, r, buffer.BackendProvided(dataIntegrityCallback))
}

func (f isccReadBufferFactory) NewBufferFromReaderAt(digest digest.Digest, r buffer.ReadAtCloser, sizeBytes int64, dataIntegrityCallback buffer.DataIntegrityCallback) buffer.Buffer {
	return f.NewBufferFromReader(digest, newReaderFromReaderAt(r), dataIntegrityCallback)
}

// ISCCReadBufferFactory is capable of creating identifiers and buffers
// for objects stored in the Initial Size Class Cache (ISCC).
var ISCCReadBufferFactory ReadBufferFactory = isccReadBufferFactory{}

// GetReducedActionDigest computes the digest of an Initial Size Class
// Cache (ISCC), or File System Access Cache (FSAC) object that
// corresponds to a given Action.
//
// By only considering the Action's command digest and the platform
// properties when generating the digest, actions with equal command
// line arguments and environment variables will have the same ISCC/FSAC
// digest, even if their input roots differ. This should be an adequate
// heuristic for grouping actions with similar performance
// characteristics.
func GetReducedActionDigest(digestFunction digest.Function, action *remoteexecution.Action) (digest.Digest, error) {
	data, err := proto.Marshal(&remoteexecution.Action{
		CommandDigest: action.CommandDigest,
		Platform:      action.Platform,
	})
	if err != nil {
		return digest.BadDigest, err
	}

	digestGenerator := digestFunction.NewGenerator(int64(len(data)))
	if _, err := digestGenerator.Write(data); err != nil {
		panic(err)
	}
	return digestGenerator.Sum(), nil
}
