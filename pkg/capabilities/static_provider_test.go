package capabilities_test

import (
	"context"
	"testing"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/bazelbuild/remote-apis/build/bazel/semver"
	"github.com/buildbarn/bb-storage/pkg/capabilities"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/stretchr/testify/require"
)

func TestStaticProvider(t *testing.T) {
	provider := capabilities.NewStaticProvider(&remoteexecution.ServerCapabilities{
		ExecutionCapabilities: &remoteexecution.ExecutionCapabilities{
			DigestFunction:  remoteexecution.DigestFunction_SHA256,
			DigestFunctions: digest.SupportedDigestFunctions,
			ExecEnabled:     true,
		},
		DeprecatedApiVersion: &semver.SemVer{Major: 2, Minor: 0},
		LowApiVersion:        &semver.SemVer{Major: 2, Minor: 2},
		HighApiVersion:       &semver.SemVer{Major: 2, Minor: 11},
	})

	serverCapabilities, err := provider.GetCapabilities(context.Background(), util.Must(digest.NewInstanceName("example")))
	require.NoError(t, err)
	testutil.RequireEqualProto(t, &remoteexecution.ServerCapabilities{
		ExecutionCapabilities: &remoteexecution.ExecutionCapabilities{
			DigestFunction:  remoteexecution.DigestFunction_SHA256,
			DigestFunctions: digest.SupportedDigestFunctions,
			ExecEnabled:     true,
		},
		DeprecatedApiVersion: &semver.SemVer{Major: 2, Minor: 0},
		LowApiVersion:        &semver.SemVer{Major: 2, Minor: 2},
		HighApiVersion:       &semver.SemVer{Major: 2, Minor: 11},
	}, serverCapabilities)
}
