*注：本文档是从2021-01-25的英语版本翻译而来的*
# Buildbarn的Storage守护进程

Buildbarn提供了远程执行协议（[Remote Execution protocol](https://github.com/bazelbuild/remote-apis)）的一种实现。远端执行协议被 [Bazel](https://bazel.build/)、[BuildStream](https://wiki.gnome.org/Projects/BuildStream/) 、[recc](https://gitlab.com/bloomberg/recc) 等工具用作远端缓存和远端执行。

本仓库提供了Buildbarn的Storage 守护进程（简称Storage Daemon），可用来搭建一个可伸缩拓展的缓存服务。由Storage Daemon本身不提供远程构建功能，所以当仅部署Storage Daemon时，编译将在本地执行。Storage Daemon通过将Build Action请求转发到一个独立的远程执行服务。

Storage Daemon可以通过配置来使用一系列的后端, 例如Redis和S3。同时也提供一种本地磁盘存储， 数据被写入到一个大文件中，用哈希表来索引，这个存储是自清理的，不需要垃圾回收。通过[Storage的配置文件]((https://github.com/buildbarn/bb-storage/blob/master/pkg/proto/configuration/blobstore/blobstore.proto))可以很好的了解哪些后端存储可用以及如何配置他们。

## 搭建Buildbarn Storage Daemon

运行以下命令来构建Buildbarn Storage Daemon，创建容器镜像：

```
$ bazel run --platforms=@io_bazel_rules_go//go/toolchain:linux_amd64 //cmd/bb_storage:bb_storage_container
...
Tagging ... as bazel/cmd/bb_storage:bb_storage_container
```

通过以下Docker命令可以启动容器镜像：

```
$ cat config/bb_storage.jsonnet
{
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
          minimumEpochInterval: '5m',
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
          newBlocks: 3,
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
            minimumEpochInterval: '5m',
          },
        },
      },
    },
  },
  global: { diagnosticsHttpListenAddress: ':9980' },
  grpcServers: [{
    listenAddresses: [':8980'],
    authenticationPolicy: { allow: {} },
  }],
  schedulers: {
    bar: { endpoint: { address: 'bar-scheduler:8981' } },
  },
  allowAcUpdatesForInstanceNamePrefixes: ['foo'],
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

在上面的例子中，Storage Daemon被配置为储存单个CAS，包含两个AC，分别是名字为`foo`和`bar`的实例。前者是用做远端缓存，这里配置文件中加入了`allowAcUpdatesForInstances`选项来配置其对客户端可写。后者是用作远端执行，这里配置文件中加入了`schedulers`选项来将build action转发到一个独立的调度服务（地址为：`bar-scheduler:8981`）。请到[配置文件](https://github.com/buildbarn/bb-storage/blob/master/pkg/proto/configuration/bb_storage/bb_storage.proto)中查看所有的选项。

Bazel通过如下设置就可以使用远端缓存了：

```
$ bazel build --remote_cache=grpc://localhost:8980 --remote_instance_name=foo //...
```

预编译好的Buildbarn storage daemon二进制文件可以从[这里](https://github.com/buildbarn/bb-storage/actions?query=event%3Apush+branch%3Amaster+is%3Asuccess+workflow%3Amaster)取到。编译好的容器镜像可以到[Docker Hub](https://hub.docker.com/r/buildbarn/bb-storage).获取。在[Buildbarn deployments repository](https://github.com/buildbarn/bb-deployments)中可以找到更多的Buildbarn storage daemon的部署案例。

# 到Slack上一起聊聊吧！

在[buildteamworld.slack.com有一个buildbarn频道](https://bit.ly/2SG1amT)，在这里可以与使用和探究Buildbarn的人相互交流。
