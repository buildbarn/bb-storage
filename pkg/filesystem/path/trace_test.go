package path_test

import (
	"testing"

	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/stretchr/testify/require"
)

func TestTrace(t *testing.T) {
	var p1 *path.Trace
	require.Equal(t, ".", p1.GetUNIXString())

	p2 := p1.Append(path.MustNewComponent("a"))
	require.Equal(t, "a", p2.GetUNIXString())

	p3 := p2.Append(path.MustNewComponent("b"))
	require.Equal(t, "a/b", p3.GetUNIXString())

	p4 := p3.Append(path.MustNewComponent("c"))
	require.Equal(t, "a/b/c", p4.GetUNIXString())
}
