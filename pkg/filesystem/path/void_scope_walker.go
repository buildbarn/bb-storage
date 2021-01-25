package path

type voidScopeWalker struct{}

func (w voidScopeWalker) OnScope(absolute bool) (ComponentWalker, error) {
	return VoidComponentWalker, nil
}

// VoidScopeWalker is an instance of ScopeWalker that accepts both
// relative and absolute paths, and can resolve any filename. By itself
// it is of little use. When used in combination with Builder, it can be
// used to construct arbitrary paths.
var VoidScopeWalker ScopeWalker = voidScopeWalker{}
