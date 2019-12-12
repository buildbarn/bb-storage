package configuration

import (
	pb "github.com/buildbarn/bb-storage/pkg/proto/configuration/bb_storage"
	"github.com/buildbarn/bb-storage/pkg/util"
)

// GetStorageConfiguration reads the jsonnet configuration from file, renders
// it, and fills in default values.
func GetStorageConfiguration(path string) (*pb.StorageConfiguration, error) {
	var configuration pb.StorageConfiguration
	if err := util.UnmarshalConfigurationFromFile(path, &configuration); err != nil {
		return nil, util.StatusWrap(err, "Failed to retrieve configuration")
	}
	setDefaultStorageValues(&configuration)
	return &configuration, nil
}

func setDefaultJaegerValues(configuration *pb.JaegerConfiguration, serviceName string) {
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
	setDefaultJaegerValues(configuration.Jaeger, "bb_storage")
}
