package util_test

import (
	"testing"

	"github.com/buildbarn/bb-storage/pkg/proto/configuration/bb_storage"

	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/stretchr/testify/require"
)

func TestJsonnetMarshalConfigurationExample(t *testing.T) {
	var configuration bb_storage.ApplicationConfiguration

	t.Run("Default", func(t *testing.T) {
		_, err := util.MarshalExample(&configuration)
		require.NoError(t, err)
	})

	t.Run("nully", func(t *testing.T) {
		_, err := util.MarshalExample(nil)
		require.Error(t, err)
	})
}
