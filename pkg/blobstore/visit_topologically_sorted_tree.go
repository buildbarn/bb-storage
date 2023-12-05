package blobstore

import (
	"io"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

// TreeDirectoryVisitor is a callback type that is invoked by
// VisitTopologicallySortedTree for each Directory message contained in
// the REv2 Tree object.
type TreeDirectoryVisitor[TArgument any] func(d *remoteexecution.Directory, argument *TArgument, childArguments []*TArgument) error

// VisitTopologicallySortedTree iterates over all Directory messages
// contained in an REv2 Tree object. For each directory, the visitor is
// capable of tracking state, which it can also build up when parent
// directories are visited.
//
// This function expects the REv2 Tree object to be topologically
// sorted, with parents being stored before children. As a result,
// directories are also visited in topological order.
func VisitTopologicallySortedTree[TArgument any](r io.Reader, digestFunction digest.Function, maximumDirectorySizeBytes int, rootArgument *TArgument, visitDirectory TreeDirectoryVisitor[TArgument]) error {
	expectedFieldNumber := TreeRootFieldNumber
	expectedDirectories := map[digest.Digest]*TArgument{}
	if err := util.VisitProtoBytesFields(r, func(fieldNumber protowire.Number, offsetBytes, sizeBytes int64, fieldReader io.Reader) error {
		if fieldNumber != expectedFieldNumber {
			return status.Errorf(codes.InvalidArgument, "Expected field number %d", expectedFieldNumber)
		}
		expectedFieldNumber = TreeChildrenFieldNumber

		var directoryMessage proto.Message
		var errUnmarshal error
		var argument *TArgument
		switch fieldNumber {
		case TreeRootFieldNumber:
			directoryMessage, errUnmarshal = buffer.NewProtoBufferFromReader(
				&remoteexecution.Directory{},
				io.NopCloser(fieldReader),
				buffer.UserProvided,
			).ToProto(&remoteexecution.Directory{}, maximumDirectorySizeBytes)
			argument = rootArgument
		case TreeChildrenFieldNumber:
			b1, b2 := buffer.NewProtoBufferFromReader(
				&remoteexecution.Directory{},
				io.NopCloser(fieldReader),
				buffer.UserProvided,
			).CloneCopy(maximumDirectorySizeBytes)

			digestGenerator := digestFunction.NewGenerator(sizeBytes)
			if err := b1.IntoWriter(digestGenerator); err != nil {
				b2.Discard()
				return err
			}
			directoryDigest := digestGenerator.Sum()

			var ok bool
			argument, ok = expectedDirectories[directoryDigest]
			if !ok {
				b2.Discard()
				return status.Errorf(codes.InvalidArgument, "Directory has digest %#v, which was not expected", directoryDigest.String())
			}
			delete(expectedDirectories, directoryDigest)

			directoryMessage, errUnmarshal = b2.ToProto(&remoteexecution.Directory{}, maximumDirectorySizeBytes)
		}
		if errUnmarshal != nil {
			return errUnmarshal
		}

		directory := directoryMessage.(*remoteexecution.Directory)
		childArguments := make([]*TArgument, 0, len(directory.Directories))
		for _, childDirectory := range directory.Directories {
			digest, err := digestFunction.NewDigestFromProto(childDirectory.Digest)
			if err != nil {
				return util.StatusWrapf(err, "Invalid digest for child directory %#v", childDirectory.Name)
			}
			childArgument, ok := expectedDirectories[digest]
			if !ok {
				childArgument = new(TArgument)
				expectedDirectories[digest] = childArgument
			}
			childArguments = append(childArguments, childArgument)
		}

		return visitDirectory(directory, argument, childArguments)
	}); err != nil {
		return err
	}

	if expectedFieldNumber == TreeRootFieldNumber {
		return status.Error(codes.InvalidArgument, "Tree does not contain any directories")
	}
	if len(expectedDirectories) > 0 {
		return status.Errorf(codes.InvalidArgument, "At least %d more directories were expected", len(expectedDirectories))
	}
	return nil
}
