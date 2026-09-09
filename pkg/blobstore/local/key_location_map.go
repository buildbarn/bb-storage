package local

import (
	"github.com/buildbarn/bb-storage/pkg/lossymap"
)

// LocationRecordKey contains a compact, partial binary representation
// of a Key that is used to identify blobs in lossymap.HashMap.
//
// Because lossymap.HashMap uses open addressing, LocationRecords may be
// stored at alternative, less preferred indices. The Attempt field
// contains the probing distance at which the record is stored.
type LocationRecordKey = lossymap.RecordKey[Key]

// LocationRecord is a key-value pair that contains information on where
// a blob may be found.
type LocationRecord = lossymap.Record[Key, Location]

// LocationRecordArray is equivalent to a []LocationRecord. It is used
// as the backing store by lossymap.HashMap. Instead of storing data in
// a slice in memory, an implementation could store this information on
// disk for a persistent data store.
type LocationRecordArray = lossymap.RecordArray[Key, Location, BlockReferenceResolver]

// KeyLocationMap is equivalent to a map[Key]Location. It is used by
// FlatBlobAccess and HierarchicalCASBlobAccess to track where blobs are
// stored, so that they may be accessed. Implementations are permitted
// to discard entries for outdated locations during lookups/insertions
// using the provided validator.
type KeyLocationMap = lossymap.Map[Key, Location, BlockReferenceResolver]
