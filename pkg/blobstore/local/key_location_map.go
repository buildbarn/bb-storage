package local

// KeyLocationMap is equivalent to a map[Key]Location. It is used by
// LocationBasedKeyBlobMap to track where blobs are stored, so that they
// may be accessed. Implementations are permitted to discard entries for
// outdated locations during lookups/insertions using the provided
// validator.
type KeyLocationMap interface {
	Get(key Key) (Location, error)
	Put(key Key, location Location) error
}
