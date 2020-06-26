package local

// LocationRecordKey contains a compact, partial binary representation
// of digest.Digest that is used to identify blobs in
// HashingDigestLocationMap.
//
// Because HashingDigestLocationMap uses open addressing,
// LocationRecords may be stored at alternative, less preferred indices.
// The Attempt field contains the probing distance at which the record
// is stored.
type LocationRecordKey struct {
	Digest  CompactDigest
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
	for _, c := range k.Digest {
		h ^= uint64(c)
		h *= 1099511628211
	}
	attempt := k.Attempt
	for i := 0; i < 4; i++ {
		h ^= uint64(attempt & 0xff)
		h *= 1099511628211
		attempt >>= 8
	}
	// With FNV-1a, the upper bits tend to have a strong avalanche
	// effect, while the lower bits do not. For example, the lowest
	// bit is equal to the lowest bit of every byte XOR'ed together.
	// Mitigate this by folding the upper bits into the lowest.
	return h ^ (h >> 32)
}
