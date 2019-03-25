package configuration

import (
	"fmt"
	"os"

	pb "github.com/buildbarn/bb-storage/pkg/proto/configuration/bb_storage"
	"github.com/golang/protobuf/jsonpb"
)

func GetStorageConfiguration(path string) (*pb.StorageConfiguration, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("Failed to load configuration from %s. Caused by: %v",
			path, err)
	}
	var configuration pb.StorageConfiguration
	if err := jsonpb.Unmarshal(file, &configuration); err != nil {
		return nil, fmt.Errorf("Failed to unmarshall configuration from %s. Caused by: %v",
			path, err)
	}
	setDefaultStorageValues(&configuration)
	return &configuration, nil
}

func SetDefaultJaegerValues(configuration *pb.JaegerConfiguration, serviceName string) {
	if configuration != nil {
		if configuration.AgentEndpoint == "" {
			configuration.AgentEndpoint = "127.0.0.1:6831"
		}
		if configuration.CollectorEndpoint == "" {
			configuration.CollectorEndpoint = "http://127.0.0.1:14268/api/traces"
		}
		if configuration.ServiceName == "" {
			configuration.ServiceName = serviceName
		}
	}
}

func setDefaultStorageValues(configuration *pb.StorageConfiguration) {
	if configuration.MetricsListenAddress == "" {
		configuration.MetricsListenAddress = ":80"
	}
	if configuration.GrpcListenAddress == "" {
		configuration.GrpcListenAddress = ":8980"
	}
	SetDefaultJaegerValues(configuration.Jaeger, "bb_storage")
}
