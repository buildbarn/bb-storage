package jmespath_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/buildbarn/bb-storage/internal/mock"
	"github.com/buildbarn/bb-storage/pkg/clock"
	"github.com/buildbarn/bb-storage/pkg/jmespath"
	"github.com/buildbarn/bb-storage/pkg/program"
	pb "github.com/buildbarn/bb-storage/pkg/proto/configuration/jmespath"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/stretchr/testify/require"

	"go.uber.org/mock/gomock"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
)

func TestNewJMESPathExpression(t *testing.T) {
	t.Run("BasicExpression", func(t *testing.T) {
		expr, err := jmespath.NewExpressionFromConfiguration(
			&pb.Expression{
				Expression: "name",
			},
			nil,
			nil)
		require.NoError(t, err)

		result, err := expr.Search(map[string]any{"name": "test"})
		require.NoError(t, err)
		require.Equal(t, "test", result)
	})

	t.Run("ComplexExpression", func(t *testing.T) {
		expr, err := jmespath.NewExpressionFromConfiguration(
			&pb.Expression{
				Expression: "users[?age > `20`].name",
			},
			nil,
			nil)
		require.NoError(t, err)

		result, err := expr.Search(map[string]any{
			"users": []any{
				map[string]any{"name": "alice", "age": float64(25)},
				map[string]any{"name": "bob", "age": float64(15)},
				map[string]any{"name": "charlie", "age": float64(30)},
			},
		})
		require.NoError(t, err)
		require.Equal(t, []any{"alice", "charlie"}, result)
	})

	t.Run("TestVector", func(t *testing.T) {
		_, err := jmespath.NewExpressionFromConfiguration(
			&pb.Expression{
				Expression: "users[?age > `20`].name",
				TestVectors: []*pb.TestVector{
					{
						Input: util.Must(structpb.NewStruct(map[string]any{
							"users": []any{
								map[string]any{"name": "alice", "age": float64(25)},
								map[string]any{"name": "bob", "age": float64(15)},
								map[string]any{"name": "charlie", "age": float64(30)},
							},
						})),
						ExpectedOutput: util.Must(structpb.NewValue([]any{"alice", "charlie"})),
					},
				},
			},
			nil,
			nil)
		require.NoError(t, err)
	})
}

func TestExpressionWithFiles(t *testing.T) {
	tokenFile := filepath.Join(t.TempDir(), "token.txt")
	require.NoError(t, os.WriteFile(tokenFile, []byte("secret123"), 0o644))

	ctrl := gomock.NewController(t)
	clock := mock.NewMockClock(ctrl)

	t.Run("AccessFileContents", func(t *testing.T) {
		program.RunLocal(context.Background(), func(ctx context.Context, siblingsGroup, dependenciesGroup program.Group) error {
			ticker := mock.NewMockTicker(ctrl)
			clock.EXPECT().NewTicker(60*time.Second).Return(ticker, nil)
			ticker.EXPECT().Stop()

			expr, err := jmespath.NewExpressionFromConfiguration(
				&pb.Expression{
					Expression: "files.token",
					Files: []*pb.File{
						{
							Key:  "token",
							Path: tokenFile,
						},
					},
				},
				dependenciesGroup,
				clock)
			require.NoError(t, err)

			result, err := expr.Search(map[string]any{})
			require.NoError(t, err)
			require.Equal(t, "secret123", result)
			return nil
		})
	})

	t.Run("FileReload", func(t *testing.T) {
		program.RunLocal(context.Background(), func(ctx context.Context, siblingsGroup, dependenciesGroup program.Group) error {
			ticker := mock.NewMockTicker(ctrl)
			timerChan := make(chan time.Time, 1)
			clock.EXPECT().NewTicker(60*time.Second).Return(ticker, timerChan)
			ticker.EXPECT().Stop()

			expr, err := jmespath.NewExpressionFromConfiguration(
				&pb.Expression{
					Expression: "files.token",
					Files: []*pb.File{
						{
							Key:  "token",
							Path: tokenFile,
						},
					},
				},
				dependenciesGroup,
				clock)
			require.NoError(t, err)

			// Check the file's contents are correct initially.
			result, err := expr.Search(map[string]any{})
			require.NoError(t, err)
			require.Equal(t, "secret123", result)

			for i := 0; i < 3; i++ {
				secret := fmt.Sprintf("secret%d", i)

				// Update the secret.
				require.NoError(t, os.WriteFile(tokenFile, []byte(secret), 0o644))
				timerChan <- time.Unix(1030, 0)

				// Check the new token is eventually returned, failing if
				// the token is not updated within 5 seconds.
				startTime := time.Now()
				for time.Since(startTime) < 5*time.Second {
					result, err := expr.Search(map[string]any{})
					require.NoError(t, err)
					if result == secret {
						break
					}
					time.Sleep(100 * time.Millisecond)
				}
			}

			return nil
		})
	})

	t.Run("NonexistentFile", func(t *testing.T) {
		program.RunLocal(context.Background(), func(ctx context.Context, siblingsGroup, dependenciesGroup program.Group) error {
			_, err := jmespath.NewExpressionFromConfiguration(
				&pb.Expression{
					Expression: "files.token",
					Files: []*pb.File{
						{
							Key:  "token",
							Path: "/nonexistent/file.txt",
						},
					},
				},
				dependenciesGroup,
				clock)
			testutil.RequirePrefixedStatus(t, status.Error(codes.Unknown, `Failed to read "/nonexistent/file.txt"`), err)
			return nil
		})
	})
}

func TestExpressionWithTestVectors(t *testing.T) {
	t.Run("FilesMismatch1", func(t *testing.T) {
		_, err := jmespath.NewExpressionFromConfiguration(
			&pb.Expression{
				Expression: "name",
				TestVectors: []*pb.TestVector{
					{
						Input: util.Must(structpb.NewStruct(map[string]any{
							"name": "test",
							"files": map[string]any{
								"token": "secret",
							},
						})),
					},
				},
			},
			nil,
			nil)
		require.Error(t, err)
		require.Contains(t, err.Error(), "contains file contents, but no files were provided")
	})

	t.Run("FilesMismatch2", func(t *testing.T) {
		program.RunLocal(context.Background(), func(ctx context.Context, siblingsGroup, dependenciesGroup program.Group) error {
			tokenFile := filepath.Join(t.TempDir(), "token.txt")
			require.NoError(t, os.WriteFile(tokenFile, []byte("secret123"), 0o644))

			_, err := jmespath.NewExpressionFromConfiguration(
				&pb.Expression{
					Expression: "name",
					Files: []*pb.File{
						{
							Key:  "token",
							Path: tokenFile,
						},
					},
					TestVectors: []*pb.TestVector{
						{
							Input: util.Must(structpb.NewStruct(map[string]any{"name": "test"})),
						},
					},
				},
				dependenciesGroup,
				clock.SystemClock)
			require.Error(t, err)
			require.Contains(t, err.Error(), "Test vector input is missing \"files\" key")
			return nil
		})
	})

	t.Run("SimpleTestVector", func(t *testing.T) {
		program.RunLocal(context.Background(), func(ctx context.Context, siblingsGroup, dependenciesGroup program.Group) error {
			tokenFile := filepath.Join(t.TempDir(), "token.txt")
			require.NoError(t, os.WriteFile(tokenFile, []byte("secret123"), 0o644))

			_, err := jmespath.NewExpressionFromConfiguration(
				&pb.Expression{
					Expression: "join(' ', ['Bearer', files.token, name])",
					Files: []*pb.File{
						{
							Key:  "token",
							Path: tokenFile,
						},
					},
					TestVectors: []*pb.TestVector{
						{
							Input: util.Must(structpb.NewStruct(map[string]any{
								"name":  "test",
								"files": map[string]any{"token": "secret"},
							})),
							ExpectedOutput: util.Must(structpb.NewValue("Bearer secret test")),
						},
					},
				},
				dependenciesGroup,
				clock.SystemClock)
			require.NoError(t, err)
			return nil
		})
	})
}
