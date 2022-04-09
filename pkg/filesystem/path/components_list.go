package path

// ComponentsList is a sortable list of filenames in a directory.
type ComponentsList []Component

func (l ComponentsList) Len() int {
	return len(l)
}

func (l ComponentsList) Less(i, j int) bool {
	return l[i].String() < l[j].String()
}

func (l ComponentsList) Swap(i, j int) {
	l[i], l[j] = l[j], l[i]
}
