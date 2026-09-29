package blobstore

import (
	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/digest"

	"google.golang.org/protobuf/proto"
)

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
