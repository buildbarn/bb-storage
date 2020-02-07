package digest

import (
	"sort"
)

// SetBuilder is a builder for Set objects.
type SetBuilder struct {
	digests map[Digest]struct{}
}

// NewSetBuilder creates a SetBuilder that contains no initial elements.
func NewSetBuilder() SetBuilder {
	return SetBuilder{
		digests: map[Digest]struct{}{},
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
	digests := make(digestList, 0, len(sb.digests))
	for digest := range sb.digests {
		digests = append(digests, digest)
	}

	// Sort the list for determinism and to allow linear time
	// implementations of GetDifferenceAndIntersection() and
	// GetUnion().
	sort.Sort(digests)
	return Set{digests: digests}
}

// digestList implements a list of digests that is sortable.
type digestList []Digest

func (l digestList) Len() int {
	return len(l)
}

func (l digestList) Less(i int, j int) bool {
	return l[i].String() < l[j].String()
}

func (l digestList) Swap(i int, j int) {
	l[i], l[j] = l[j], l[i]
}
