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
    sha256 = "ee67651f3d5bab1c5dd7f071bc4566cf809745f23bb32ff26d0aa38ed878b261",
    url = "https://github.com/bazelbuild/rules_go/archive/bb5873bebd9fe63a1789912c8561bcd5fba6f345.tar.gz",
    strip_prefix = "rules_go-bb5873bebd9fe63a1789912c8561bcd5fba6f345",
)

http_archive(
    name = "bazel_gazelle",
    sha256 = "7949fc6cc17b5b191103e97481cf8889217263acf52e00b560683413af204fcb",
    urls = ["https://github.com/bazelbuild/bazel-gazelle/releases/download/0.16.0/bazel-gazelle-0.16.0.tar.gz"],
)

load("@io_bazel_rules_docker//repositories:repositories.bzl", container_repositories = "repositories")

container_repositories()

load("@io_bazel_rules_go//go:deps.bzl", "go_rules_dependencies", "go_register_toolchains")

go_rules_dependencies()

go_register_toolchains()

load("@bazel_gazelle//:deps.bzl", "gazelle_dependencies")

gazelle_dependencies()

load(":go_dependencies.bzl", "bb_storage_go_dependencies")

bb_storage_go_dependencies()
