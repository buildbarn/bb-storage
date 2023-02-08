package gcp

import (
	gcp_pb "github.com/buildbarn/bb-storage/pkg/proto/configuration/cloud/gcp"

	"google.golang.org/api/option"
)

// NewClientOptionsFromConfiguration creates a list of Google Cloud SDK
// client options based on options specified in a Protobuf configuration
// message. The resulting client options object can be used to access
// GCP services such as GCS.
func NewClientOptionsFromConfiguration(configuration *gcp_pb.ClientOptionsConfiguration, name string) ([]option.ClientOption, error) {
	// TODO: Are there any client options which we want to support?
	// https://pkg.go.dev/google.golang.org/api/option
	return nil, nil
}
