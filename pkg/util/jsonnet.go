package util

import (
	"io"
	"os"
	"strings"

	"github.com/google/go-jsonnet"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// UnmarshalConfigurationFromFile reads a Jsonnet file, evaluates it and
// unmarshals the output into a Protobuf message.
func UnmarshalConfigurationFromFile(path string, configuration proto.Message) error {
	// Read configuration file from disk or from stdin.
	var jsonnetInput []byte
	var err error
	if path == "-" {
		jsonnetInput, err = io.ReadAll(os.Stdin)
	} else {
		jsonnetInput, err = os.ReadFile(path)
	}
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

	if err := protojson.Unmarshal([]byte(jsonnetOutput), configuration); err != nil {
		return StatusWrap(err, "Failed to unmarshal configuration")
	}
	return nil
}
