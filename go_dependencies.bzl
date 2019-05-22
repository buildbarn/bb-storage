load("@bazel_gazelle//:deps.bzl", "go_repository")

def bb_storage_go_dependencies():
    go_repository(
        name = "com_github_aws_aws_sdk_go",
        importpath = "github.com/aws/aws-sdk-go",
        sha256 = "6ba3169493880a63128b6c6edc9119817df257db0b34b27887cad871767f0525",
        strip_prefix = "aws-sdk-go-1.16.26",
        urls = ["https://github.com/aws/aws-sdk-go/archive/v1.16.26.tar.gz"],
    )

    go_repository(
        name = "com_github_bazelbuild_remote_apis",
        importpath = "github.com/bazelbuild/remote-apis",
        sha256 = "99ab1378f10854504c75bcfa43be2129d36bbba8e80a79a4216a3e3026a0985b",
        strip_prefix = "remote-apis-ed4849810292e5fb3c844992133523f01a4ad420",
        urls = ["https://github.com/bazelbuild/remote-apis/archive/ed4849810292e5fb3c844992133523f01a4ad420.tar.gz"],
    )

    go_repository(
        name = "com_github_beorn7_perks",
        commit = "3a771d992973f24aa725d07868b467d1ddfceafb",
        importpath = "github.com/beorn7/perks",
    )

    go_repository(
        name = "com_github_buildbarn_bb_storage",
        commit = "dfb8c06f0dda1d945284616c75ed4b3706906b8b",
        importpath = "github.com/buildbarn/bb-storage",
    )

    go_repository(
        name = "com_github_golang_mock",
        importpath = "github.com/golang/mock",
        sha256 = "0dc7dbcf6d83b4318e26d9481dfa9405042288d666835f810e0b70ada2f54e11",
        strip_prefix = "mock-aedf487a10d1285646a046e4c9537d7854e820e1",
        urls = ["https://github.com/EdSchouten/mock/archive/aedf487a10d1285646a046e4c9537d7854e820e1.tar.gz"],
    )

    go_repository(
        name = "com_github_google_uuid",
        importpath = "github.com/google/uuid",
        sha256 = "7e330758f7c81d9f489493fb7ae0e67d06f50753429758b64f25ad5fb2727e21",
        strip_prefix = "uuid-1.1.0",
        urls = ["https://github.com/google/uuid/archive/v1.1.0.tar.gz"],
    )

    go_repository(
        name = "com_github_go_redis_redis",
        importpath = "github.com/go-redis/redis",
        sha256 = "c997aca07026a52745e3d7aeab528550b90d3bae65ff2b67991d890bb2a7b1ff",
        strip_prefix = "redis-6.15.1",
        urls = ["https://github.com/go-redis/redis/archive/v6.15.1.tar.gz"],
    )

    go_repository(
        name = "com_github_grpc_ecosystem_go_grpc_prometheus",
        importpath = "github.com/grpc-ecosystem/go-grpc-prometheus",
        sha256 = "eba66530952a126ab869205bdb909af607bfd9eb09f00207b62eb29140258aa9",
        strip_prefix = "go-grpc-prometheus-1.2.0",
        urls = ["https://github.com/grpc-ecosystem/go-grpc-prometheus/archive/v1.2.0.tar.gz"],
    )

    go_repository(
        name = "com_github_lazybeaver_xorshift",
        commit = "ce511d4823dd074d7c37a74225320332d6961abb",
        importpath = "github.com/lazybeaver/xorshift",
    )

    go_repository(
        name = "com_github_matttproud_golang_protobuf_extensions",
        commit = "c12348ce28de40eed0136aa2b644d0ee0650e56c",
        importpath = "github.com/matttproud/golang_protobuf_extensions",
    )

    go_repository(
        name = "com_github_prometheus_client_golang",
        importpath = "github.com/prometheus/client_golang",
        sha256 = "5f6ca8740a08622ae0a19c32b1026b8233bfd943a1f4af34963d326ab5fa94e5",
        strip_prefix = "client_golang-0.9.2",
        urls = ["https://github.com/prometheus/client_golang/archive/v0.9.2.tar.gz"],
    )

    go_repository(
        name = "com_github_prometheus_client_model",
        commit = "5c3871d89910bfb32f5fcab2aa4b9ec68e65a99f",
        importpath = "github.com/prometheus/client_model",
    )

    go_repository(
        name = "com_github_prometheus_common",
        importpath = "github.com/prometheus/common",
        sha256 = "3a02a3c8d881ef0be78fc58d63d14979ce0226f91ab52b2d67a11bc120e054dd",
        strip_prefix = "common-0.2.0",
        urls = ["https://github.com/prometheus/common/archive/v0.2.0.tar.gz"],
    )

    go_repository(
        name = "com_github_prometheus_procfs",
        commit = "ae68e2d4c00fed4943b5f6698d504a5fe083da8a",
        importpath = "github.com/prometheus/procfs",
    )

    go_repository(
        name = "com_github_stretchr_testify",
        importpath = "github.com/stretchr/testify",
        sha256 = "0cd9c199a72b8d80621624b37c0ed5ac724352d458506a31dfa86710551e7fc5",
        strip_prefix = "testify-1.3.0",
        urls = ["https://github.com/stretchr/testify/archive/v1.3.0.tar.gz"],
    )

    go_repository(
        name = "io_opencensus_go_contrib_exporter_prometheus",
        importpath = "contrib.go.opencensus.io/exporter/prometheus",
        commit = "f6cda26f80a388eabda7766388c14e96370440e5",
    )

    go_repository(
        name = "io_opencensus_go_contrib_exporter_jaeger",
        commit = "e8b55949d948652e47aae4378212f933ecee856b",
        importpath = "contrib.go.opencensus.io/exporter/jaeger",
    )

    go_repository(
        name = "dev_gocloud",
        commit = "a68836e8e108ad55d26e8e2d21579028090c8aa5",
        importpath = "gocloud.dev",
    )

    go_repository(
        name = "org_golang_google_api",
        importpath = "google.golang.org/api",
        urls = ["https://github.com/googleapis/google-api-go-client/archive/v0.4.0.tar.gz"],
        sha256 = "fde7b06bc002cc886efa94845ac2ba4f48fd4c321a04a9ee5558026f5fa28c0c",
        strip_prefix = "google-api-go-client-0.4.0",
    )

    go_repository(
        name = "com_github_uber_jaeger_client_go",
        importpath = "github.com/uber/jaeger-client-go",
        urls = ["https://github.com/jaegertracing/jaeger-client-go/archive/v2.16.0.tar.gz"],
        sha256 = "9657eb6603d6aae55c5637957ab63400127bcc395981831366998428cc3f7edb",
        strip_prefix = "jaeger-client-go-2.16.0",
    )

    go_repository(
        name = "org_golang_x_sync",
        importpath = "golang.org/x/sync",
        commit = "112230192c580c3556b8cee6403af37a4fc5f28c",
    )

    go_repository(
        name = "io_opencensus_go",
        importpath = "go.opencensus.io",
        urls = ["https://github.com/census-instrumentation/opencensus-go/archive/v0.21.0.tar.gz"],
        sha256 = "e7129aebb9bcb590f01b4fb773b6cf0b10109211cb38cfbaf1f097d191043251",
        strip_prefix = "opencensus-go-0.21.0",
    )

    go_repository(
        name = "com_google_cloud_go",
        commit = "09ad026a62f0561b7f7e276569eda11a6afc9773",
        importpath = "cloud.google.com/go",
    )

    go_repository(
        name = "org_golang_x_xerrors",
        commit = "385005612d73f6925de56cb1886917aeaf90e3c5",
        importpath = "golang.org/x/xerrors",
    )

    go_repository(
        name = "com_github_hashicorp_golang_lru",
        importpath = "github.com/hashicorp/golang-lru",
        urls = ["https://github.com/hashicorp/golang-lru/archive/v0.5.1.tar.gz"],
        sha256 = "3bf57512af746dc0338651ba1c35c65fe907ff214ccb22d679539f7ea791511e",
        strip_prefix = "golang-lru-0.5.1",
    )

    go_repository(
        name = "com_github_googleapis_gax_go",
        commit = "9e334198cafcf7b281a9673424d7b1c3a02ebd50",
        importpath = "github.com/googleapis/gax-go",
    )

    go_repository(
        name = "org_golang_x_oauth2",
        commit = "9f3314589c9a9136388751d9adae6b0ed400978a",
        importpath = "golang.org/x/oauth2",
    )

    go_repository(
        name = "com_github_google_wire",
        commit = "2183ee4806cf1878e136fea26f06f9abef9375b6",
        importpath = "github.com/google/wire",
    )

    go_repository(
        name = "com_github_azure_azure_pipeline_go",
        commit = "55fedc85a614dcd0e942a66f302ae3efb83d563c",
        importpath = "github.com/Azure/azure-pipeline-go",
    )

    go_repository(
        name = "com_github_azure_azure_storage_blob_go",
        commit = "8a1deeeabe0a24f918d29630ede0da2a1c8f3b2f",
        importpath = "github.com/Azure/azure-storage-blob-go",
    )
