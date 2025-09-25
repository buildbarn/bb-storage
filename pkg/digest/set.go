package digest

import (
	"container/heap"
)

// Set of digests. Sets are immutable and can be created using
// SetBuilder.
type Set struct {
	digests []Digest
}

// EmptySet is an instance of Set that contains zero elements.
var EmptySet = Set{}

// Items returns a sorted list of all elements stored within the set.
func (s Set) Items() []Digest {
	return s.digests
}

// Empty returns true if the set contains zero elements.
func (s Set) Empty() bool {
	return len(s.digests) == 0
}

// First returns the first element stored in the set. The boolean
// return value denotes whether the operation was successful (i.e., the
// set is non-empty).
func (s Set) First() (Digest, bool) {
	if len(s.digests) == 0 {
		return BadDigest, false
	}
	return s.digests[0], true
}

// Length returns the number of elements stored in the set.
func (s Set) Length() int {
	return len(s.digests)
}

// RemoveEmptyBlob returns a copy of the set that has all of the entries
// corresponding with the empty blob removed.
func (s Set) RemoveEmptyBlob() Set {
	for start, digest := range s.digests {
		if digest.GetSizeBytes() == 0 {
			// At least one non-empty blob was found. Copy
			// the set up to this point and filter all
			// successive results.
			nonEmptyBlobs := append([]Digest(nil), s.digests[:start]...)
			for _, digest := range s.digests[start+1:] {
				if digest.GetSizeBytes() != 0 {
					nonEmptyBlobs = append(nonEmptyBlobs, digest)
				}
			}
			return Set{digests: nonEmptyBlobs}
		}
	}

	// Return the original set, as no non-empty blobs were found.
	return s
}

// PartitionByInstanceName partitions the elements stored in a set into
// a list of sets, each containing elements of a single REv2 instance
// name.
//
// All resulting sets are guaranteed to be non-empty. The order in which
// they are returned corresponds with the order in which their first
// element was stored in the original set.
func (s Set) PartitionByInstanceName() []Set {
	if len(s.digests) == 0 {
		return nil
	}

	firstInstanceName := s.digests[0].GetInstanceName()
	for i := 1; i < len(s.digests); i++ {
		if secondInstanceName := s.digests[i].GetInstanceName(); firstInstanceName != secondInstanceName {
			// Elements in the set use at least two distinct
			// REv2 instance names.
			//
			// Split up the entries processed thus far into
			// two sets. Set capacities to the length, so
			// that append() operations later on leave the
			// original set intact.
			digestsByInstanceName := []Set{
				{digests: s.digests[:i:i]},
				{digests: s.digests[i : i+1 : i+1]},
			}
			observedInstanceNames := map[InstanceName]int{
				firstInstanceName:  0,
				secondInstanceName: 1,
			}
			for j := i + 1; j < len(s.digests); j++ {
				digest := s.digests[j]
				instanceName := digest.GetInstanceName()
				if index, ok := observedInstanceNames[instanceName]; ok {
					digestsByInstanceName[index].digests = append(digestsByInstanceName[index].digests, digest)
				} else {
					// Elements in the set use three
					// or more REv2 instance names.
					observedInstanceNames[instanceName] = len(digestsByInstanceName)
					digestsByInstanceName = append(digestsByInstanceName, Set{
						digests: s.digests[j : j+1 : j+1],
					})
				}
			}
			return digestsByInstanceName
		}
	}

	// Common case: all elements in the set use the same REv2
	// instance name. Return the original set without copying it.
	return []Set{s}
}

// GetDifferenceAndIntersection partitions the elements stored in sets A
// and B across three resulting sets: one containing the elements
// present only in A, one containing the elements present in both A and
// B, and one containing thelements present only in B.
func GetDifferenceAndIntersection(setA, setB Set) (onlyA, both, onlyB Set) {
	a, b := setA.digests, setB.digests
	for len(a) > 0 && len(b) > 0 {
		if sA, sB := a[0].String(), b[0].String(); sA < sB {
			onlyA.digests = append(onlyA.digests, a[0])
			a = a[1:]
		} else if sA == sB {
			both.digests = append(both.digests, a[0])
			a, b = a[1:], b[1:]
		} else {
			onlyB.digests = append(onlyB.digests, b[0])
			b = b[1:]
		}
	}
	onlyA.digests = append(onlyA.digests, a...)
	onlyB.digests = append(onlyB.digests, b...)
	return onlyA, both, onlyB
}

// GetUnion merges all of the elements stored in a list of sets into a
// single resulting set. This implementation uses a k-way merging
// algorithm.
func GetUnion(sets []Set) Set {
	// Place all non-empty sets in a min-heap, ordered by lowest
	// digest in the set.
	var activeSets setHeap
	for _, s := range sets {
		if len(s.digests) > 0 {
			activeSets = append(activeSets, s)
		}
	}

	// Special cases, as the code below assumes the existence of at
	// least one non-empty set.
	if len(activeSets) == 0 {
		return EmptySet
	} else if len(activeSets) == 1 {
		return activeSets[0]
	}

	// Heapify the min-heap of sets. Initialize the output set with
	// the lowest digest of all sets.
	heap.Init(&activeSets)
	outDigests := []Digest{activeSets[0].digests[0]}

	for {
		// Remove the lowest digest from the set. Remove the set
		// if it were to become empty.
		if l := activeSets[0].digests; len(l) == 1 {
			if len(activeSets) == 1 {
				return Set{digests: outDigests}
			}
			heap.Pop(&activeSets)
		} else {
			activeSets[0].digests = l[1:]
			heap.Fix(&activeSets, 0)
		}

		// Next iteration: copy the next lowest digest of all
		// sets, if and only if it's distinct from the
		// previously added digest.
		if d := activeSets[0].digests[0]; d != outDigests[len(outDigests)-1] {
			outDigests = append(outDigests, d)
		}
	}
}

// setHeap implements a min-heap of sets. The sets are sorted by lowest
// digest value. This permits sorted iteration of digests stored in a
// set of sets.
type setHeap []Set

func (h *setHeap) Len() int {
	return len(*h)
}

func (h *setHeap) Less(i, j int) bool {
	return (*h)[i].digests[0].String() < (*h)[j].digests[0].String()
}

func (h *setHeap) Swap(i, j int) {
	(*h)[i], (*h)[j] = (*h)[j], (*h)[i]
}

func (h *setHeap) Push(x interface{}) {
	*h = append(*h, x.(Set))
}

func (h *setHeap) Pop() interface{} {
	last := (*h)[len(*h)-1]
	*h = (*h)[:len(*h)-1]
	return last
}
