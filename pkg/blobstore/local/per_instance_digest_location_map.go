package local

import (
	"github.com/buildbarn/bb-storage/pkg/digest"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type perInstanceDigestLocationMap struct {
	maps map[string]DigestLocationMap
}

// NewPerInstanceDigestLocationMap creates a demultiplexer that forwards
// calls to DigestLocationmaps based on the instance name that is stored
// in the blob's digest.
func NewPerInstanceDigestLocationMap(maps map[string]DigestLocationMap) DigestLocationMap {
	return perInstanceDigestLocationMap{
		maps: maps,
	}
}

func (dlm perInstanceDigestLocationMap) getMap(digest digest.Digest) (DigestLocationMap, error) {
	instanceName := digest.GetInstance()
	if m, ok := dlm.maps[instanceName]; ok {
		return m, nil
	}
	return nil, status.Errorf(codes.InvalidArgument, "Invalid instance name: %#v", instanceName)
}

func (dlm perInstanceDigestLocationMap) Get(digest digest.Digest, validator *LocationValidator) (Location, error) {
	m, err := dlm.getMap(digest)
	if err != nil {
		return Location{}, err
	}
	return m.Get(digest, validator)
}

func (dlm perInstanceDigestLocationMap) Put(digest digest.Digest, validator *LocationValidator, location Location) error {
	m, err := dlm.getMap(digest)
	if err != nil {
		return err
	}
	return m.Put(digest, validator, location)
}
