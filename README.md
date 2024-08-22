# The Buildbarn storage daemon [![Build status](https://github.com/buildbarn/bb-storage/workflows/master/badge.svg)](https://github.com/buildbarn/bb-storage/actions) [![PkgGoDev](https://pkg.go.dev/badge/github.com/buildbarn/bb-storage)](https://pkg.go.dev/github.com/buildbarn/bb-storage) [![Go Report Card](https://goreportcard.com/badge/github.com/buildbarn/bb-storage)](https://goreportcard.com/report/github.com/buildbarn/bb-storage)

Translations: [Chinese](https://github.com/buildbarn/bb-storage/blob/master/doc/zh_CN/README.md)

The Buildbarn project provides an implementation of the
[Remote Execution protocol](https://github.com/bazelbuild/remote-apis).
This protocol is used by tools such as [Bazel](https://bazel.build/),
[BuildStream](https://wiki.gnome.org/Projects/BuildStream/) and
[recc](https://gitlab.com/bloomberg/recc) to cache and optionally
execute build actions remotely.

This repository provides Buildbarn's storage daemon. This daemon can be
used to build a scalable build cache. On its own, it cannot be used to
execute build actions remotely. When using only this storage daemon,
build actions will still be executed on the local system. This daemon
does, however, facilitate remote execution by allowing execution
requests to be forwarded to a separate remote execution service.

This storage daemon can be configured to use a whole series of backends.
Examples include a backend that forwards traffic over gRPC, but also a
local on-disk storage backend that writes data to a large file, using a
hash table as an index. This storage backend is self-cleaning; no
garbage collection is needed. The
[schema of the storage configuration file](https://github.com/buildbarn/bb-storage/blob/master/pkg/proto/configuration/blobstore/blobstore.proto)
gives a good overview of which storage backends are available and how
they can be configured.

## Setting up the Buildbarn storage daemon

Run the following command to build the Buildbarn storage daemon from
source, create container image and push it into the Docker daemon
running on the current system:

```
$ bazel run --platforms=@io_bazel_rules_go//go/toolchain:linux_amd64 //cmd/bb_storage:bb_storage_container
...
Tagging ... as bazel/cmd/bb_storage:bb_storage_container
```

This container image can then be launched using Docker as follows:

```
$ cat config/bb_storage.jsonnet
{
  contentAddressableStorage: {
    backend: {
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
    getAuthorizer: { allow: {} },
    putAuthorizer: { allow: {} },
    findMissingAuthorizer: { allow: {} },
  },
  actionCache: {
    backend: {
      completenessChecking: {
        backend: {
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
        maximumTotalTreeSizeBytes: 16 * 1024 * 1024,
      },
    },
    getAuthorizer: { allow: {} },
    putAuthorizer: { instanceNamePrefix: {
      allowedInstanceNamePrefixes: ['foo'],
    } },
  },
  global: { diagnosticsHttpServer: {
    httpServers: [{
      listenAddresses: [':9980'],
      authenticationPolicy: { allow: {} },
    }],
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
  executeAuthorizer: { allow: {} },
  maximumMessageSizeBytes: 16 * 1024 * 1024,
}
$ mkdir -p storage-{ac,cas}/persistent_state
$ docker run \
      -p 8980:8980 \
      -p 9980:9980 \
      -v $(pwd)/config:/config \
      -v $(pwd)/storage-cas:/storage-cas \
      -v $(pwd)/storage-ac:/storage-ac \
      bazel/cmd/bb_storage:bb_storage_container \
      /config/bb_storage.jsonnet
```

In the example above, the daemon is configured to store a single on-disk
CAS. Two ACs are made, corresponding with instance names `foo` and
`bar`. The former is intended just for remote caching, which is why it's
made client-writable through `actionCacheAuthorizers` in the
configuration file. The latter is intended for remote execution, which
is why `schedulers` is used to forward build action execution requests
to a separate scheduler service at address `bar-scheduler:8981`.
Please refer to the [configuration schema](https://github.com/buildbarn/bb-storage/blob/master/pkg/proto/configuration/bb_storage/bb_storage.proto)
for an overview of all available options.

Bazel can be configured to use the remote cache as follows:

```
$ bazel build --remote_cache=grpc://localhost:8980 --remote_instance_name=foo //...
```

Prebuilt binaries of the Buildbarn storage daemon may be obtained by
choosing a build on [the GitHub Actions page](https://github.com/buildbarn/bb-storage/actions?query=event%3Apush+branch%3Amaster+is%3Asuccess+workflow%3Amaster).
Prebuilt container images may be found on [the GitHub Packages page](https://github.com/orgs/buildbarn/packages).
More examples of how the Buildbarn storage daemon may be deployed can be
found in [the Buildbarn deployments repository](https://github.com/buildbarn/bb-deployments).

# Join us on Slack!

There is a [#buildbarn channel on buildteamworld.slack.com](https://bit.ly/2SG1amT)
that you can join to get in touch with other people who use and hack on
Buildbarn.

# Commercial Support & Hosting

Via our [partners](https://github.com/buildbarn#commercial-support) commercial support & hosting can be procured.