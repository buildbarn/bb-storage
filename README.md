# The Buildbarn storage daemon

The Buildbarn project provides an implementation of the
[Remote Execution protocol](https://github.com/bazelbuild/remote-apis).
This protocol is used by tools such as [Bazel](https://bazel.build/),
[BuildStream](https://wiki.gnome.org/Projects/BuildStream/) and
[recc](https://gitlab.com/bloomberg/recc) to cache and optionally
execute build actions remotely.

This repository provides a copy of Buildbarn's storage daemon. This
daemon can be used to build a scalable build cache. On its own, it
cannot be used to execute build actions remotely. When using only this
storage daemon, build actions will still be executed on the local
system. This daemon does, however, facilitate remote execution by
allowing execution requests to be forwarded to a separate remote
execution service.

This storage daemon can be configured to use a whole series of backends.
Examples include Redis and S3. It also provides a local on-disk storage
backend that writes data to a circular file, using a hash table as an
index. This storage backend is self-cleaning; no garbage collection is
needed. The [schema of the storage configuration file](https://github.com/buildbarn/bb-storage/blob/master/pkg/proto/blobstore/blobstore.proto)
gives a good overview of which storage backends are available and how
they can be configured.

## Setting up the Buildbarn storage daemon

Run the following command to build the Buildbarn storage daemon from
source, create container image and push it into the Docker daemon
running on the current system:

```
$ bazel run //cmd/bb_storage:bb_storage_container
...
Tagging ... as bazel/cmd/bb_storage:bb_storage_container
```

This container image can then be launched using Docker as follows:

```
$ cat config/blobstore.conf
content_addressable_storage {
  circular {
    directory: "/storage-cas"
    offset_file_size_bytes: 16777216           # 16 MiB
    offset_cache_size: 10000
    data_file_size_bytes: 10737418240          # 10 GiB
    data_allocation_chunk_size_bytes: 16777216 # 16 MiB
    # Blobs for all instances are stored in a single CAS.
  }
}
action_cache {
  circular {
    directory: "/storage-ac"
    offset_file_size_bytes: 1048576           # 1 MiB
    offset_cache_size: 1000
    data_file_size_bytes: 104857600           # 100 MiB
    data_allocation_chunk_size_bytes: 1048576 # 1 MiB
    # List of instances for which to create an AC.
    instance: "foo"
    instance: "bar"
  }
}
$ docker run \
      -p 8980:8980 \
      -v $(pwd)/config:/config \
      -v $(pwd)/storage-cas:/storage-cas \
      -v $(pwd)/storage-ac:/storage-ac \
      bazel/cmd/bb_storage:bb_storage_container \
      -allow-ac-updates-for-instance foo \
      -scheduler 'bar|bar-scheduler:8981'
```

In the example above, the daemon is configured to store a single on-disk
CAS. Two ACs are made, corresponding with instance names `foo` and
`bar`. The former is intended just for remote caching, which is why it's
made client-writable by passing in `-allow-ac-updates-for-instance`. The
latter is intended for remote execution, which is why `-scheduler` is
used to forward build action execution requests to a separate scheduler
service at address `bar-scheduler:8981`.

Bazel can be configured to use the remote cache as follows:

```
$ bazel build --remote_cache=localhost:8980 --remote_instance_name=foo //...
```

Prebuilt container images of the Buildbarn storage daemon may be found
on [Docker Hub](https://hub.docker.com/r/buildbarn/bb-storage). More
examples of how the Buildbarn storage daemon may be deployed can be
found in [the Buildbarn deployments repository](https://github.com/buildbarn/bb-deployments).
