package util

// Must can be used to wrap the invocation of a function that may return
// an error, and panic if an error occurs.
//
// This function should only be used in situations where a failure to
// create an object can occur if some design constraint is violated.
func Must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}
