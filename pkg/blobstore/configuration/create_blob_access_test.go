package configuration

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSettingFromEnv(t *testing.T) {
	os.Setenv("test_config_setting_NAME", "test_config_setting_VALUE")
	expanded := maybeFromEnv("env:test_config_setting_NAME")
	require.Equal(t, expanded, "test_config_setting_VALUE")
	os.Unsetenv("test_config_setting_NAME")
}

func TestSettingLiteral(t *testing.T) {
	expanded := maybeFromEnv("a-direct-value")
	require.Equal(t, expanded, "a-direct-value")
}
