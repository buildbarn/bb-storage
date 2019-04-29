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
      name = "com_google_cloud_go",
      importpath = "cloud.google.com/go",
      vcs = "git",
      remote = "https://github.com/googleapis/google-cloud-go",
      tag = "v0.37.4",
    )
  
    go_repository(
        name = "org_golang_x_oauth2",
        importpath = "golang.org/x/oauth2",
        vcs = "git",
        remote = "https://github.com/golang/oauth2",
        commit = "9f3314589c9a9136388751d9adae6b0ed400978a",
    )

    go_repository(
        name = "org_golang_google_api",
        importpath = "google.golang.org/api",
        strip_prefix = "google-api-go-client-0.4.0",
        sha256 = "b3ccb1714d7f60ecbb42882b68a2da1ee3a721087fa5b22c518156ab8f7e1a12",
        urls = ["https://github.com/googleapis/google-api-go-client/archive/v0.4.0.zip"],
    )

    go_repository(
        name = "com_github_googleapis_gax_go",
        importpath = "github.com/googleapis/gax-go",
        tag = "v2.0.4",
    )

    go_repository(
        name = "io_opencensus_go",
        importpath = "go.opencensus.io",
        sha256 = "839fb798c5034805884c4c6632f1e959b04edaf0c34fc17fd8f617b39af91d79",
        strip_prefix = "opencensus-go-0.21.0",
        urls = ["https://github.com/census-instrumentation/opencensus-go/archive/v0.21.0.zip"],

    )
    
    go_repository(
        name = "com_github_hashicorp_golang_lru",
        importpath = "github.com/hashicorp/golang-lru",
        sha256 = "db5fb365252d34390618905a43f444a58567029a74f16a9ee077c1b37b9fd007",
        strip_prefix = "golang-lru-0.5.1",
        urls = ["https://github.com/hashicorp/golang-lru/archive/v0.5.1.zip"],

    )