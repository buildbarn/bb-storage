# Cassandra

## Required setup

`bb-storage` should not be configured to use the admin user by default in
production. Instead, you should be using a regular user which cannot create
tables or keyspaces.

The tables that are required can be set up using command similar to the
below in a `cqlsh` session:

```shell
CREATE KEYSPACE IF NOT EXISTS buildbarn_storage WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor': 3 };

USE buildbarn_storage;

CREATE TABLE IF NOT EXISTS prod_metadata (
    digest_function TEXT,
    digest_hash BLOB,
    digest_size_bytes BIGINT,
    digest_instance_name TEXT,
    blob_id ASCII,
    last_access TIMESTAMP,
    segment_count INT,
    segment_size INT,
    PRIMARY KEY ((digest_function, digest_hash, digest_size_bytes, digest_instance_name)));

CREATE TABLE IF NOT EXISTS prod_content (
    blob_id ASCII,
    segment INT,
    content BLOB,
    PRIMARY KEY ((blob_id, segment)));

CREATE TABLE IF NOT EXISTS prod_orphaned_content (
    blob_id ASCII,
    digest_instance_name TEXT,
    digest_function TEXT,
    digest_hash BLOB,
    digest_size_bytes BIGINT,
    segment_count INT,
    timestamp DATE,
PRIMARY KEY (blob_id, digest_instance_name))
WITH WITH gc_grace_seconds = 86400;
```

Note that you will need a table prefix (`prod` in this case), which allow
multiple different environments to run in the same keyspace.

The `prod_orphaned_content` table is used to track items that may have been
orphaned. Essentially, as blobs begin to be streamed into cassandra, a note
is taken of the blob and the estimated number of segments it will require.
Users are expected to have their own processes in place for using this data
to "reap" orphan content from the table.

The `prod_orphaned_content` table will contain many Cassandra tombstones
(most rows are deleted soon after they are inserted). For that reason, you
should reduce the `gc_grace_seconds` to 1 day. This reduces the minimum
amount of time tombstones will be kept around. This can cause some rows to
resurrect if a Cassandra node is down for more than a day, but it is not a
problem at the application level: they will just need to be reaped again.

In the long term, this might still create many tombstones, causing non-critical
errors when reaping old orphan rows. Consider setting a scheduled compaction
(e.g. every 2h) to help ensure that tombstones are regularly reviewed for
garbage collection.

### Configuring `bb-storage`

An example snippet of jsonnet config for setting up Cassandra is:

```jsonnet
{
  cassandra: {
    hosts: ["cassandra.mycorp.com:1234"],
    context: "example/bazel",
    keyspace: "buildbarn_storage",
    tablePrefix: "prod",
    segmentSize: 524288,
    username: "cassandra_user",
    password: "hunter2",
  }
}
```

The segment size is the size in bytes before a new segment should be created.
It is a balancing act between being able to store as many items as possible
in a single segment and the maximum row size allowed in Cassandra (1MB). In
this example, 512kb was chosen, which should allow the majority of writes
seen by Buildbarn to be written in a single row.

### Using the nearest DC

To minimise latency, you can set a `preferredDC` in the `cassandra`
configuration. This works on the assumption that physical proximity will
lead to lower latencies, so the preferred DC is the one that is closest to the
particular cluster the Buildbarn is installed in. To find the nearest data
centre, talk to someone familiar with your network topology, and select from
the datacenters listed in your `NetworkTopologyStrategy` (if you're using
one) when creating the Cassandra keyspace.
