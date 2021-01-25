package path

type voidComponentWalker struct{}

func (cw voidComponentWalker) OnDirectory(name Component) (GotDirectoryOrSymlink, error) {
	return GotDirectory{
		Child:        VoidComponentWalker,
		IsReversible: false,
	}, nil
}

func (cw voidComponentWalker) OnTerminal(name Component) (*GotSymlink, error) {
	return nil, nil
}

func (cw voidComponentWalker) OnUp() (ComponentWalker, error) {
	return VoidComponentWalker, nil
}

// VoidComponentWalker is an instance of ComponentWalker that can resolve
// any filename. By itself it is of little use. When used in combination
// with Builder, it can be used to construct arbitrary paths.
var VoidComponentWalker ComponentWalker = voidComponentWalker{}
