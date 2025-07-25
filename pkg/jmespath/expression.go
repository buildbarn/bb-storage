package jmespath

import (
	"context"
	"encoding/json"
	"log"
	"maps"
	"os"
	"reflect"
	"sync/atomic"
	"time"

	"github.com/buildbarn/bb-storage/pkg/clock"
	"github.com/buildbarn/bb-storage/pkg/program"
	pb "github.com/buildbarn/bb-storage/pkg/proto/jmespath"
	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/jmespath/go-jmespath"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
)

// Expression represents a parsed JMESPath expression.
type Expression struct {
	expression *jmespath.JMESPath
	files      *metadataFileProvider
}

// NewExpressionFromConfiguration creates a new JMESPath expression.Expression
// from the provided configuration. This will also evaluate all test vectors
// and return an error if any of them fail.
//
// The group parameter is required when there are files and is used to schedule
// refreshes of the file contents.
func NewExpressionFromConfiguration(config *pb.Expression, group program.Group, clock clock.Clock) (*Expression, error) {
	expression, err := jmespath.Compile(config.Expression)
	if err != nil {
		return nil, err
	}

	var files *metadataFileProvider
	if config.Files != nil {
		files, err = newJMESPathMetadataFileProvider(config.Files, group, clock)
		if err != nil {
			return nil, err
		}
	}

	expr := &Expression{
		expression: expression,
		files:      files,
	}

	for _, t := range config.TestVectors {
		err := expr.checkTestVector(t)
		if err != nil {
			return nil, util.StatusWrapf(err, "Failed to validate JMESPath expression %q with test vector %q", config.Expression, t.Input)
		}
	}

	return expr, nil
}

// MustCompile creates a Expression from a string, panicing if
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
	if e.files != nil {
		// Don't mutate the original map
		copy := make(map[string]any, len(data)+1)
		maps.Copy(copy, data)
		copy[filesKey] = e.files.getMetadata()
		data = copy
	}
	return e.expression.Search(data)
}

func (e *Expression) checkTestVector(t *pb.TestVector) error {
	input := t.Input.AsMap()

	files, filesPresent := input[filesKey].(map[string]any)
	if e.files == nil {
		if filesPresent {
			return status.Error(codes.InvalidArgument, "Test vector contains file contents, but no files were provided in the JMESPath expression configuration")
		}
	} else {
		if !filesPresent {
			return status.Errorf(codes.InvalidArgument, "Test vector input is missing %q key", filesKey)
		}
		for _, key := range e.files.files {
			if _, ok := files[key.Key]; !ok {
				return status.Errorf(codes.InvalidArgument, "Test vector is missing file %q", key.Key)
			}
		}
		if len(files) != len(e.files.files) {
			return status.Errorf(codes.InvalidArgument, "Test vector contains %d files, but JMESPath expression expects %d files", len(files), len(e.files.files))
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

	if !reflect.DeepEqual(t.ExpectedOutput, actualPb) {
		expectedJSON, _ := json.Marshal(t.ExpectedOutput)
		actualJSON, _ := json.Marshal(actual)
		return status.Errorf(codes.InvalidArgument, "Test vector failed: expected %s, got %s", string(expectedJSON), string(actualJSON))
	}

	return nil
}

// metadataFileProvider makes the contents of files available as
// metadata to JMESPath expressions.
type metadataFileProvider struct {
	files           []*pb.File
	currentContents atomic.Pointer[map[string]any]
}

// newJMESPathMetadataFileProvider creates a MetadataFileProvider that
// reads files from the filesystem and makes their contents available
// as metadata. The contents of the files are reloaded periodically.
func newJMESPathMetadataFileProvider(files []*pb.File, group program.Group, clock clock.Clock) (*metadataFileProvider, error) {
	provider := &metadataFileProvider{
		files: files,
	}
	currentContents := make(map[string]any, len(files))
	for _, file := range files {
		contents, err := readFile(file.Path)
		if err != nil {
			return nil, err
		}
		currentContents[file.Key] = contents
	}
	provider.currentContents.Store(&currentContents)
	group.Go(func(ctx context.Context, siblingsGroup, dependenciesGroup program.Group) error {
		_, t := clock.NewTimer(60 * time.Second)
		for {
			select {
			case <-t:
				lastContents := *provider.currentContents.Load()
				newContents := make(map[string]any, len(provider.files))
				for _, file := range provider.files {
					contents, err := readFile(file.Path)
					if err != nil {
						log.Printf("Failed to reload %s file: %v", file.Path, err)
						newContents[file.Key] = lastContents[file.Key]
					} else {
						newContents[file.Key] = contents
					}
				}
				provider.currentContents.Store(&newContents)
			case <-ctx.Done():
				return nil
			}
		}
	})
	return provider, nil
}

func readFile(path string) (string, error) {
	content, err := os.ReadFile(path)
	if err != nil {
		return "", util.StatusWrapf(err, "Failed to read %q", path)
	}
	return string(content), nil
}

func (p *metadataFileProvider) getMetadata() map[string]any {
	return *p.currentContents.Load()
}
