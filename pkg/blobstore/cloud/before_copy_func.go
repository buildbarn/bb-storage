package cloud

import (
	"gocloud.dev/blob"
)

// BeforeCopyFunc can be used from a
// gocloud.dev/blob.CopyOptions.BeforeCopy to force a mutation of an
// object. This operation isn't supported by gocloud.dev so we need a
// cloud-specific asfunc.
type BeforeCopyFunc func(func(asFunc interface{}) bool) error

// Type assertion to self-document where it came from.
var _ BeforeCopyFunc = blob.CopyOptions{}.BeforeCopy
