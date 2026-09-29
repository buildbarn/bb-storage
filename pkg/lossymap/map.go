package lossymap

// Map of keys to values.
//
// This type differs from Go's regular map type in that it may be lossy.
// This makes it a good backing store for fixed size caches of data.
type Map[TKey, TValue, TExpirationData any] interface {
	Get(key TKey, expirationData TExpirationData) (TValue, error)
	Put(key TKey, value TValue, expirationData TExpirationData) error
}
