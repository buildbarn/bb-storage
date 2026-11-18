package grpc_test

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/buildbarn/bb-storage/internal/mock"
	bb_grpc "github.com/buildbarn/bb-storage/pkg/grpc"
	configuration "github.com/buildbarn/bb-storage/pkg/proto/configuration/grpc"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"

	"go.uber.org/mock/gomock"
)

const (
	helperModeEnv     = "BB_TEST_CREDENTIAL_HELPER_MODE"
	helperResponseEnv = "BB_TEST_CREDENTIAL_HELPER_RESPONSE"
	helperLogEnv      = "BB_TEST_CREDENTIAL_HELPER_LOG"
)

// TestMain allows the test binary to act as the credential helper, so
// that the tests don't depend on a POSIX shell being available.
func TestMain(m *testing.M) {
	switch os.Getenv(helperModeEnv) {
	case "respond":
		if len(os.Args) != 2 || os.Args[1] != "get" {
			os.Exit(2)
		}
		input, err := io.ReadAll(os.Stdin)
		if err != nil {
			os.Exit(2)
		}
		f, err := os.OpenFile(os.Getenv(helperLogEnv), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
		if err != nil {
			os.Exit(2)
		}
		f.Write(input)
		f.WriteString("\n")
		f.Close()
		os.Stdout.WriteString(os.Getenv(helperResponseEnv))
		os.Exit(0)
	case "fail":
		os.Stderr.WriteString("no luck\n")
		os.Exit(1)
	}
	os.Exit(m.Run())
}

// writeHelper configures the test binary to act as a credential helper
// that appends its standard input to a log file, and then prints the
// provided response.
func writeHelper(t *testing.T, response string) (helperPath, logPath string) {
	helperPath, err := os.Executable()
	require.NoError(t, err)
	logPath = filepath.Join(t.TempDir(), "log")
	t.Setenv(helperModeEnv, "respond")
	t.Setenv(helperResponseEnv, response)
	t.Setenv(helperLogEnv, logPath)
	return helperPath, logPath
}

func TestCredentialHelperCredentials(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)
	now := time.Unix(1000, 0).UTC()

	// Every subtest gets its own clock, so that expectations on
	// Now() don't shadow each other.
	newClock := func() *mock.MockClock {
		clock := mock.NewMockClock(ctrl)
		clock.EXPECT().NewContextWithTimeout(gomock.Any(), 10*time.Second).
			DoAndReturn(func(parent context.Context, d time.Duration) (context.Context, context.CancelFunc) {
				return context.WithTimeout(parent, d)
			}).AnyTimes()
		return clock
	}

	t.Run("NoPath", func(t *testing.T) {
		clock := newClock()
		_, err := bb_grpc.NewCredentialHelperCredentialsFromConfiguration(&configuration.CredentialHelperConfiguration{}, clock)
		testutilRequireCode(t, err, codes.InvalidArgument)
	})

	t.Run("RequiresTransportSecurity", func(t *testing.T) {
		clock := newClock()
		helper, _ := writeHelper(t, `{}`)
		creds, err := bb_grpc.NewCredentialHelperCredentialsFromConfiguration(&configuration.CredentialHelperConfiguration{Path: helper}, clock)
		require.NoError(t, err)
		require.True(t, creds.RequireTransportSecurity())
	})

	t.Run("URIAndHeaders", func(t *testing.T) {
		clock := newClock()
		// The URI should be identical to the one Bazel uses,
		// meaning the port 443 is stripped. Header names are
		// lower-cased, and multiple values are joined.
		helper, log := writeHelper(t, `{"headers":{"Authorization":["Bearer abc"],"X-Multi":["a","b"]}}`)
		creds, err := bb_grpc.NewCredentialHelperCredentialsFromConfiguration(&configuration.CredentialHelperConfiguration{Path: helper}, clock)
		require.NoError(t, err)

		clock.EXPECT().Now().Return(now).AnyTimes()
		md, err := creds.GetRequestMetadata(ctx, "https://myhostname:443/build.bazel.remote.execution.v2.Execution")
		require.NoError(t, err)
		require.Equal(t, map[string]string{"authorization": "Bearer abc", "x-multi": "a, b"}, md)

		logged, err := os.ReadFile(log)
		require.NoError(t, err)
		require.JSONEq(t, `{"uri":"https://myhostname/build.bazel.remote.execution.v2.Execution"}`, strings.TrimSpace(string(logged)))

		// No expiration provided, so the result is cached
		// forever. The default port must not create a separate
		// cache entry.
		_, err = creds.GetRequestMetadata(ctx, "https://myhostname/build.bazel.remote.execution.v2.Execution")
		require.NoError(t, err)
		logged, err = os.ReadFile(log)
		require.NoError(t, err)
		require.Equal(t, 1, strings.Count(string(logged), "uri"))

		// A different service needs its own credentials.
		_, err = creds.GetRequestMetadata(ctx, "https://myhostname:443/google.bytestream.ByteStream")
		require.NoError(t, err)
		logged, err = os.ReadFile(log)
		require.NoError(t, err)
		require.Equal(t, 2, strings.Count(string(logged), "uri"))
	})

	t.Run("Expiration", func(t *testing.T) {
		clock := newClock()
		// Credentials should be refreshed once the expiration
		// time minus the margin has been reached.
		expires := time.Unix(2000, 0).UTC()
		helper, log := writeHelper(t, `{"expires":"`+expires.Format(time.RFC3339)+`","headers":{"a":["b"]}}`)
		creds, err := bb_grpc.NewCredentialHelperCredentialsFromConfiguration(&configuration.CredentialHelperConfiguration{
			Path:             helper,
			ExpirationMargin: durationpb.New(100 * time.Second),
		}, clock)
		require.NoError(t, err)

		current := now
		clock.EXPECT().Now().DoAndReturn(func() time.Time { return current }).AnyTimes()
		count := func() int {
			logged, err := os.ReadFile(log)
			require.NoError(t, err)
			return strings.Count(string(logged), "uri")
		}

		_, err = creds.GetRequestMetadata(ctx, "https://host/svc")
		require.NoError(t, err)
		require.Equal(t, 1, count())

		current = time.Unix(1899, 0).UTC()
		_, err = creds.GetRequestMetadata(ctx, "https://host/svc")
		require.NoError(t, err)
		require.Equal(t, 1, count())

		current = time.Unix(1900, 0).UTC()
		_, err = creds.GetRequestMetadata(ctx, "https://host/svc")
		require.NoError(t, err)
		require.Equal(t, 2, count())
	})

	t.Run("HelperFailure", func(t *testing.T) {
		clock := newClock()
		helper, err := os.Executable()
		require.NoError(t, err)
		t.Setenv(helperModeEnv, "fail")
		creds, err := bb_grpc.NewCredentialHelperCredentialsFromConfiguration(&configuration.CredentialHelperConfiguration{Path: helper}, clock)
		require.NoError(t, err)
		clock.EXPECT().Now().Return(now).AnyTimes()
		_, err = creds.GetRequestMetadata(ctx, "https://host/svc")
		testutilRequireCode(t, err, codes.Unavailable)
		require.Contains(t, err.Error(), "no luck")
	})

	t.Run("InvalidResponse", func(t *testing.T) {
		clock := newClock()
		helper, _ := writeHelper(t, `not json`)
		creds, err := bb_grpc.NewCredentialHelperCredentialsFromConfiguration(&configuration.CredentialHelperConfiguration{Path: helper}, clock)
		require.NoError(t, err)
		clock.EXPECT().Now().Return(now).AnyTimes()
		_, err = creds.GetRequestMetadata(ctx, "https://host/svc")
		testutilRequireCode(t, err, codes.Unavailable)
	})
}

func testutilRequireCode(t *testing.T, err error, code codes.Code) {
	require.Error(t, err)
	require.Equal(t, code, status.Code(err), err.Error())
}
