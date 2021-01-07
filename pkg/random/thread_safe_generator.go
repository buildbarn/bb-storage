package random

// ThreadSafeGenerator is identical to SingleThreadedGenerator, except
// that it is safe to use from within multiple goroutines without
// additional locking. These generators may be slower than their
// single-threaded counterparts.
type ThreadSafeGenerator interface {
	SingleThreadedGenerator

	IsThreadSafe()
}
