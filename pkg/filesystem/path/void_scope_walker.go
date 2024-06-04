package path

type voidScopeWalker struct{}

func (w voidScopeWalker) OnAbsolute() (ComponentWalker, error) {
	return VoidComponentWalker, nil
}

func (w voidScopeWalker) OnDriveLetter(drive rune) (ComponentWalker, error) {
	return VoidComponentWalker, nil
}

func (w voidScopeWalker) OnRelative() (ComponentWalker, error) {
	return VoidComponentWalker, nil
}

// VoidScopeWalker is an instance of ScopeWalker that accepts both
// relative and absolute paths, and can resolve any filename. By itself
// it is of little use. When used in combination with Builder, it can be
// used to construct arbitrary paths.
var VoidScopeWalker ScopeWalker = voidScopeWalker{}
