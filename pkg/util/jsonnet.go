package util

import (
	"io/ioutil"

	"github.com/golang/protobuf/jsonpb"
	"github.com/golang/protobuf/proto"
	jsonnet "github.com/google/go-jsonnet"
	"github.com/google/go-jsonnet/ast"
	"github.com/google/go-jsonnet/astgen"
)

func init() {
	ast.StdAst = astgen.StdAst
}

// UnmarshalConfigurationFromFile reads a jsonnet file, renders it, unmarshal
// the output into a protobuf configuration message.
func UnmarshalConfigurationFromFile(path string, configuration proto.Message) error {
	vm := jsonnet.MakeVM()
	jsonnetInput, err := ioutil.ReadFile(path)
	if err != nil {
		return StatusWrapf(err, "Failed to run Jsonnet on %s", path)
	}
	jsonnetOutput, err := vm.EvaluateSnippet(path, string(jsonnetInput))
	if err := jsonpb.UnmarshalString(jsonnetOutput, configuration); err != nil {
		return StatusWrap(err, "Failed to unmarshal configuration")
	}
	return nil
}
