package jmespath

import (
	"context"
	"encoding/json"
	"log"
	"maps"
	"os"
	"sync/atomic"
	"time"

	"github.com/buildbarn/bb-storage/pkg/clock"
	"github.com/buildbarn/bb-storage/pkg/program"
	pb "github.com/buildbarn/bb-storage/pkg/proto/configuration/jmespath"
	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/jmespath/go-jmespath"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
)

// Expression represents a parsed JMESPath expression.
type Expression struct {
	expression   *jmespath.JMESPath
	currentFiles atomic.Pointer[map[string]any]
}

// NewExpressionFromConfiguration creates a new JMESPath expression.Expression
// from the provided configuration. This will also evaluate all test vectors
// and return an error if any of them fail.
//
// The group parameter is required when there are files and is used to schedule
// refreshes of the file contents.
func NewExpressionFromConfiguration(config *pb.Expression, group program.Group, clock clock.Clock) (*Expression, error) {
	if config == nil {
		return nil, status.Error(codes.InvalidArgument, "No JMESPath expression configuration provided")
	}

	expression, err := jmespath.Compile(config.Expression)
	if err != nil {
		return nil, err
	}

	expr := &Expression{
		expression: expression,
	}

	if len(config.Files) > 0 {
		err = expr.initialiseFiles(config.Files, group, clock)
		if err != nil {
			return nil, err
		}
	}

	for _, t := range config.TestVectors {
		err := expr.checkTestVector(t)
		if err != nil {
			return nil, util.StatusWrapf(
				err,
				"Failed to validate JMESPath expression %q with test vector %s",
				config.Expression,
				protojson.MarshalOptions{}.Format(t.Input),
			)
		}
	}

	return expr, nil
}

// MustCompile creates a Expression from a string, panicking if
// the expression is invalid.
func MustCompile(expression string) *Expression {
	expr, err := NewExpressionFromConfiguration(&pb.Expression{
		Expression: expression,
	}, nil, nil)
	if err != nil {
		panic(util.StatusWrapf(err, "Failed to compile JMESPath expression %q", expression))
	}
	return expr
}

const filesKey = "files"

// Search evaluates the JMESPath expression against the provided data,
// returning the result as structured data.
func (e *Expression) Search(data map[string]any) (any, error) {
	if files := e.currentFiles.Load(); files != nil {
		// Don't mutate the original map
		copy := make(map[string]any, len(data)+1)
		maps.Copy(copy, data)
		copy[filesKey] = *files
		data = copy
	}
	return e.expression.Search(data)
}

func (e *Expression) checkTestVector(t *pb.TestVector) error {
	input := t.Input.AsMap()

	files, filesPresent := input[filesKey].(map[string]any)
	currentFiles := e.currentFiles.Load()
	if currentFiles == nil {
		if filesPresent {
			return status.Error(codes.InvalidArgument, "Test vector contains file contents, but no files were provided in the JMESPath expression configuration")
		}
	} else {
		if !filesPresent {
			return status.Errorf(codes.InvalidArgument, "Test vector input is missing %q key", filesKey)
		}
		for key := range *currentFiles {
			if _, ok := files[key]; !ok {
				return status.Errorf(codes.InvalidArgument, "Test vector is missing file %q", key)
			}
		}
		if len(files) != len(*currentFiles) {
			return status.Errorf(codes.InvalidArgument, "Test vector contains %d files, but JMESPath expression expects %d files", len(files), len(*currentFiles))
		}
	}

	actual, err := e.expression.Search(input)
	if err != nil {
		return util.StatusWrapf(err, "Failed to evaluate JMESPath expression on test vector input")
	}

	actualPb, err := structpb.NewValue(actual)
	if err != nil {
		return util.StatusWrapf(err, "Failed to convert JMESPath result to protobuf value")
	}

	if !proto.Equal(actualPb, t.ExpectedOutput) {
		expectedJSON, _ := json.Marshal(t.ExpectedOutput)
		actualJSON, _ := json.Marshal(actual)
		return status.Errorf(codes.InvalidArgument, "Test vector failed: expected %s, got %s", string(expectedJSON), string(actualJSON))
	}

	return nil
}

// initialiseFiles initialises any files that are read by the JMESPath
// expression. This reads the initial contents and creates a goroutine
// to periodically refresh the contents.
func (e *Expression) initialiseFiles(files []*pb.File, group program.Group, clock clock.Clock) error {
	initial := make(map[string]any, len(files))
	for _, file := range files {
		contents, err := readFile(file.Path)
		if err != nil {
			return err
		}
		initial[file.Key] = contents
	}
	e.currentFiles.Store(&initial)
	group.Go(func(ctx context.Context, siblingsGroup, dependenciesGroup program.Group) error {
		ticker, t := clock.NewTicker(60 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-t:
				lastContents := *e.currentFiles.Load()
				newContents := make(map[string]any, len(files))
				for _, file := range files {
					contents, err := readFile(file.Path)
					if err != nil {
						log.Printf("Failed to reload %s file: %v", file.Path, err)
						newContents[file.Key] = lastContents[file.Key]
					} else {
						newContents[file.Key] = contents
					}
				}
				e.currentFiles.Store(&newContents)
			case <-ctx.Done():
				return nil
			}
		}
	})
	return nil
}

func readFile(path string) (string, error) {
	content, err := os.ReadFile(path)
	if err != nil {
		return "", util.StatusWrapf(err, "Failed to read %q", path)
	}
	return string(content), nil
}
