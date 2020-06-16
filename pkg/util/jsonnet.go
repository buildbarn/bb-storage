package util

import (
	"io/ioutil"
	"os"
	"reflect"
	"strings"

	"github.com/golang/protobuf/jsonpb"
	"github.com/golang/protobuf/proto"
	"github.com/google/go-jsonnet"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// UnmarshalConfigurationFromFile reads a Jsonnet file, evaluates it and
// unmarshals the output into a Protobuf message.
func UnmarshalConfigurationFromFile(path string, configuration proto.Message) error {
	// Read configuration file from disk or from stdin.
	var jsonnetInput []byte
	var err error
	if path == "-" {
		jsonnetInput, err = ioutil.ReadAll(os.Stdin)
	} else {
		jsonnetInput, err = ioutil.ReadFile(path)
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

	if err := jsonpb.UnmarshalString(jsonnetOutput, configuration); err != nil {
		return StatusWrap(err, "Failed to unmarshal configuration")
	}
	return nil
}

// MarshalExample marshals the Protobuf message into a json string
func MarshalExample(msg interface{}) (string, error) {
	if msg == nil {
		return "", status.Errorf(codes.InvalidArgument, "Unable to marshal nil type")
	}

	v := reflect.ValueOf(msg)

	initializeMessage(v.Elem().Type(), v.Elem())

	marshaler := &jsonpb.Marshaler{
		EmitDefaults: true,
		Indent:       "  ",
	}
	jsonString, err := marshaler.MarshalToString(msg.(proto.Message))
	if err != nil {
		return jsonString, StatusWrap(err, "Failed to marshal protobuf message")
	}

	return jsonString, err
}

func initializeMessage(t reflect.Type, v reflect.Value) {
	for i := 0; i < v.NumField(); i++ {
		f := v.Field(i)
		ft := t.Field(i)
		if ft.Type.Kind() == reflect.Ptr {
			fv := reflect.New(ft.Type.Elem())
			initializeMessage(ft.Type.Elem(), fv.Elem())
			f.Set(fv)
		}
	}
}
