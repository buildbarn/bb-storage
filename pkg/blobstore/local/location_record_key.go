package local

// LocationRecordKey contains a compact, partial binary representation
// of a Key that is used to identify blobs in HashingKeyLocationMap.
//
// Because HashingKeyLocationMap uses open addressing, LocationRecords
// may be stored at alternative, less preferred indices. The Attempt
// field contains the probing distance at which the record is stored.
type LocationRecordKey struct {
	Key     Key
	Attempt uint32
}

// Hash a LocationRecordKey using FNV-1a. Instead of using the
// well-known offset basis of 14695981039346656037, a custom
// initialization may be provided. This permits mirrored instances to
// each use a different offset basis.
//
// In the unlikely event that the collision rate on the hash table
// becomes too high, records may simply get lost. By letting mirrored
// instances use different offset bases, it becomes less likely that
// both instances lose the same record.
//
// For non-persistent setups, it is advised to use a randomly chosen
// offset basis to prevent collision attacks.
func (k *LocationRecordKey) Hash(hashInitialization uint64) uint64 {
	h := hashInitialization
	for _, c := range k.Key {
		h ^= uint64(c)
		h *= 1099511628211
	}
	attempt := k.Attempt
	for i := 0; i < 4; i++ {
		h ^= uint64(attempt & 0xff)
		h *= 1099511628211
		attempt >>= 8
	}
	return h
}
