package digest

import (
	"slices"
	"strings"
)

// SetBuilder is a builder for Set objects.
type SetBuilder struct {
	digests map[Digest]struct{}
}

// NewSetBuilder creates a SetBuilder that contains no initial
// elements. The capacity argument pre-sizes the underlying map so that
// adding up to that many elements does not require rehashing. Callers
// should provide a sensible estimate based on the context in which the
// set is built (e.g., the number of digests in an incoming request).
// Pass zero when no estimate is available.
func NewSetBuilder(capacity int) SetBuilder {
	return SetBuilder{
		digests: make(map[Digest]struct{}, capacity),
	}
}

// Add a single element to the Set that is being built by the
// SetBuilder.
func (sb SetBuilder) Add(digest Digest) SetBuilder {
	sb.digests[digest] = struct{}{}
	return sb
}

// Length returns the number of elements that the Set would contain if
// built.
func (sb SetBuilder) Length() int {
	return len(sb.digests)
}

// Build the Set containing the Digests provided to Add().
func (sb SetBuilder) Build() Set {
	// Prevent allocation of an empty slice.
	if len(sb.digests) == 0 {
		return Set{}
	}

	// Store all digests in a list.
	digests := make([]Digest, 0, len(sb.digests))
	for digest := range sb.digests {
		digests = append(digests, digest)
	}

	// Sort the list for determinism and to allow linear time
	// implementations of GetDifferenceAndIntersection() and
	// GetUnion(). slices.SortFunc avoids the per-comparison
	// interface dispatch of sort.Sort.
	slices.SortFunc(digests, func(a, b Digest) int {
		return strings.Compare(a.value, b.value)
	})
	return Set{digests: digests}
}
