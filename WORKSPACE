workspace(name = "com_github_buildbarn_bb_storage")

load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

http_archive(
    name = "bazel_gomock",
    sha256 = "eeed097c09e10238ca7ec06ac17eb5505eb7eb38d6282b284cb55c05e8ffc07f",
    strip_prefix = "bazel_gomock-ff6c20a9b6978c52b88b7a1e2e55b3b86e26685b",
    urls = ["https://github.com/jmhodges/bazel_gomock/archive/ff6c20a9b6978c52b88b7a1e2e55b3b86e26685b.tar.gz"],
)

http_archive(
    name = "bazel_toolchains",
    sha256 = "ee854b5de299138c1f4a2edb5573d22b21d975acfc7aa938f36d30b49ef97498",
    strip_prefix = "bazel-toolchains-37419a124bdb9af2fec5b99a973d359b6b899b61",
    urls = [
        "https://mirror.bazel.build/github.com/bazelbuild/bazel-toolchains/archive/37419a124bdb9af2fec5b99a973d359b6b899b61.tar.gz",
        "https://github.com/bazelbuild/bazel-toolchains/archive/37419a124bdb9af2fec5b99a973d359b6b899b61.tar.gz",
    ],
)

http_archive(
    name = "io_bazel_rules_docker",
    sha256 = "aed1c249d4ec8f703edddf35cbe9dfaca0b5f5ea6e4cd9e83e99f3b0d1136c3d",
    strip_prefix = "rules_docker-0.7.0",
    urls = ["https://github.com/bazelbuild/rules_docker/archive/v0.7.0.tar.gz"],
)

http_archive(
    name = "io_bazel_rules_go",
    sha256 = "ade51a315fa17347e5c31201fdc55aa5ffb913377aa315dceb56ee9725e620ee",
    url = "https://github.com/bazelbuild/rules_go/releases/download/0.16.6/rules_go-0.16.6.tar.gz",
)

http_archive(
    name = "bazel_gazelle",
    sha256 = "7949fc6cc17b5b191103e97481cf8889217263acf52e00b560683413af204fcb",
    urls = ["https://github.com/bazelbuild/bazel-gazelle/releases/download/0.16.0/bazel-gazelle-0.16.0.tar.gz"],
)

load("@io_bazel_rules_docker//repositories:repositories.bzl", container_repositories = "repositories")

container_repositories()

load("@io_bazel_rules_go//go:def.bzl", "go_register_toolchains", "go_rules_dependencies")

go_rules_dependencies()

go_register_toolchains()

load("@bazel_gazelle//:deps.bzl", "gazelle_dependencies", "go_repository")

gazelle_dependencies()

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
    name = "com_github_matttproud_golang_protobuf_extensions",
    commit = "c12348ce28de40eed0136aa2b644d0ee0650e56c",
    importpath = "github.com/matttproud/golang_protobuf_extensions",
)

go_repository(
    name = "com_github_lazybeaver_xorshift",
    commit = "ce511d4823dd074d7c37a74225320332d6961abb",
    importpath = "github.com/lazybeaver/xorshift",
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
