package util

import (
	"io/ioutil"
	"os"
	"strings"

	"github.com/golang/protobuf/jsonpb"
	"github.com/golang/protobuf/proto"
	"github.com/google/go-jsonnet"
	"github.com/google/go-jsonnet/ast"
	"github.com/google/go-jsonnet/astgen"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func init() {
	ast.StdAst = astgen.StdAst
}

// UnmarshalConfigurationFromFile reads a Jsonnet file, evaluates it and
// unmarshals the output into a Protobuf message.
func UnmarshalConfigurationFromFile(path string, configuration proto.Message) error {
	jsonnetInput, err := ioutil.ReadFile(path)
	if err != nil {
		return StatusWrapf(err, "Failed to read file contents")
	}

	// Create a Jsonnet VM where all of the environment variables of
	// the current process are available through std.extVar().
	vm := jsonnet.MakeVM()
	for _, env := range os.Environ() {
		parts := strings.SplitN(env, "=", 2)
		if len(parts) != 2 {
			return status.Errorf(codes.InvalidArgument, "Invalid environment variable: %#v", env)
		}
		vm.ExtVar(parts[0], parts[1])
	}

	jsonnetOutput, err := vm.EvaluateSnippet(path, string(jsonnetInput))
	if err != nil {
		return StatusWrapf(err, "Failed to evaluate configuration")
	}

	if err := jsonpb.UnmarshalString(jsonnetOutput, configuration); err != nil {
		return StatusWrap(err, "Failed to unmarshal configuration")
	}
	return nil
}
