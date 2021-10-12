package util

import (
	"io/ioutil"
	"path"
	"testing"

	"github.com/buildbarn/bb-storage/pkg/proto/configuration/auth"
	"github.com/buildbarn/bb-storage/pkg/proto/configuration/bb_storage"
	"google.golang.org/protobuf/proto"
)

const readmeConf = `{
	blobstore: {
	  contentAddressableStorage: {
		'local': {
		  keyLocationMapOnBlockDevice: {
			file: {
			  path: '/storage-cas/key_location_map',
			  sizeBytes: 16 * 1024 * 1024,
			},
		  },
		  keyLocationMapMaximumGetAttempts: 8,
		  keyLocationMapMaximumPutAttempts: 32,
		  oldBlocks: 8,
		  currentBlocks: 24,
		  newBlocks: 3,
		  blocksOnBlockDevice: {
			source: {
			  file: {
				path: '/storage-cas/blocks',
				sizeBytes: 10 * 1024 * 1024 * 1024,
			  },
			},
			spareBlocks: 3,
		  },
		  persistent: {
			stateDirectoryPath: '/storage-cas/persistent_state',
			minimumEpochInterval: '300s',
		  },
		},
	  },
	  actionCache: {
		completenessChecking: {
		  'local': {
			keyLocationMapOnBlockDevice: {
			  file: {
				path: '/storage-ac/key_location_map',
				sizeBytes: 1024 * 1024,
			  },
			},
			keyLocationMapMaximumGetAttempts: 8,
			keyLocationMapMaximumPutAttempts: 32,
			oldBlocks: 8,
			currentBlocks: 24,
			newBlocks: 1,
			blocksOnBlockDevice: {
			  source: {
				file: {
				  path: '/storage-ac/blocks',
				  sizeBytes: 100 * 1024 * 1024,
				},
			  },
			  spareBlocks: 3,
			},
			persistent: {
			  stateDirectoryPath: '/storage-ac/persistent_state',
			  minimumEpochInterval: '300s',
			},
		  },
		},
	  },
	},
	global: { diagnosticsHttpServer: {
	  listenAddress: ':9980',
	  enablePrometheus: true,
	  enablePprof: true,
	} },
	grpcServers: [{
	  listenAddresses: [':8980'],
	  authenticationPolicy: { allow: {} },
	}],
	schedulers: {
	  bar: { endpoint: { address: 'bar-scheduler:8981' } },
	},
	contentAddressableStorageAuthorizers: {
	  get: { allow: {} },
	  put: { allow: {} },
	  findMissing: { allow: {} },
	},
	actionCacheAuthorizers: {
	  get: { allow: {} },
	  put: { instanceNamePrefix: {
		allowedInstanceNamePrefixes: ['foo'],
	  } },
	},
	executeAuthorizer: { allow: {} },
	maximumMessageSizeBytes: 16 * 1024 * 1024,
  }`

const spiffeconf = `{
	spiffe:{
		instanceNameSubjectMap:{
			"foo":{
				allowedSpiffeIds:{"example.com":"workload-1"}
			}
		}
	}
}`

func TestUnmarshalConfigurationFromFile(t *testing.T) {
	type args struct {
		configuration string
		confproto     proto.Message
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "readme",
			args: args{
				configuration: readme,
				confproto:     &bb_storage.ApplicationConfiguration{},
			},
			wantErr: false,
		},
		{
			name: "spiffe configuration",
			args: args{
				configuration: spiffeconf,
				confproto:     &auth.AuthorizerConfiguration{},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			td := t.TempDir()
			confFile := path.Join(td, "bb_storange.jsonnet")
			ioutil.WriteFile(confFile, []byte(tt.args.configuration), 0o644)
			if err := UnmarshalConfigurationFromFile(confFile, tt.args.confproto); (err != nil) != tt.wantErr {
				t.Errorf("UnmarshalConfigurationFromFile() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
