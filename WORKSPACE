workspace(name = "com_github_buildbarn_bb_storage")

load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

http_archive(
    name = "bazel_gomock",
    sha256 = "5c4e7cde43a80d7bdef8cd3ff0741e33b24f9e2897ae9759bfe0ff2ba80125db",
    strip_prefix = "bazel_gomock-1.1",
    urls = ["https://github.com/jmhodges/bazel_gomock/archive/v1.1.tar.gz"],
)

http_archive(
    name = "bazel_toolchains",
    sha256 = "28cb3666da80fbc62d4c46814f5468dd5d0b59f9064c0b933eee3140d706d330",
    strip_prefix = "bazel-toolchains-0.27.1",
    urls = [
        "https://mirror.bazel.build/github.com/bazelbuild/bazel-toolchains/archive/0.27.1.tar.gz",
        "https://github.com/bazelbuild/bazel-toolchains/archive/0.27.1.tar.gz",
    ],
)

http_archive(
    name = "io_bazel_rules_docker",
    sha256 = "dc97fccceacd4c6be14e800b2a00693d5e8d07f69ee187babfd04a80a9f8e250",
    strip_prefix = "rules_docker-0.14.1",
    urls = ["https://github.com/bazelbuild/rules_docker/releases/download/v0.14.1/rules_docker-v0.14.1.tar.gz"],
)

http_archive(
    name = "io_bazel_rules_go",
    sha256 = "e88471aea3a3a4f19ec1310a55ba94772d087e9ce46e41ae38ecebe17935de7b",
    urls = [
        "https://storage.googleapis.com/bazel-mirror/github.com/bazelbuild/rules_go/releases/download/v0.20.3/rules_go-v0.20.3.tar.gz",
        "https://github.com/bazelbuild/rules_go/releases/download/v0.20.3/rules_go-v0.20.3.tar.gz",
    ],
)

http_archive(
    name = "bazel_gazelle",
    sha256 = "86c6d481b3f7aedc1d60c1c211c6f76da282ae197c3b3160f54bd3a8f847896f",
    urls = [
        "https://storage.googleapis.com/bazel-mirror/github.com/bazelbuild/bazel-gazelle/releases/download/v0.19.1/bazel-gazelle-v0.19.1.tar.gz",
        "https://github.com/bazelbuild/bazel-gazelle/releases/download/v0.19.1/bazel-gazelle-v0.19.1.tar.gz",
    ],
)

load("@io_bazel_rules_docker//repositories:repositories.bzl", container_repositories = "repositories")

container_repositories()

load("@io_bazel_rules_go//go:deps.bzl", "go_register_toolchains", "go_rules_dependencies")

go_rules_dependencies()

go_register_toolchains()

load("@bazel_gazelle//:deps.bzl", "gazelle_dependencies")

gazelle_dependencies()

load("@io_bazel_rules_docker//go:image.bzl", _go_image_repos = "repositories")

_go_image_repos()

http_archive(
    name = "com_github_bazelbuild_buildtools",
    sha256 = "5474cdb16fe9e1db22006b5f48d534a91b68236d588223439135c43215e93fba",
    strip_prefix = "buildtools-0.25.0",
    url = "https://github.com/bazelbuild/buildtools/archive/0.25.0.zip",
)

load("@com_github_bazelbuild_buildtools//buildifier:deps.bzl", "buildifier_dependencies")

buildifier_dependencies()

load(":go_dependencies.bzl", "bb_storage_go_dependencies")

bb_storage_go_dependencies()

load("@com_github_bazelbuild_remote_apis//:repository_rules.bzl", "switched_rules_by_language")

switched_rules_by_language(
    name = "bazel_remote_apis_imports",
    go = True,
)

load("@bazel_tools//tools/build_defs/repo:git.bzl", "git_repository")

http_archive(
    name = "com_google_protobuf",
    sha256 = "761bfffc7d53cd01514fa237ca0d3aba5a3cfd8832a71808c0ccc447174fd0da",
    strip_prefix = "protobuf-3.11.1",
    urls = ["https://github.com/protocolbuffers/protobuf/releases/download/v3.11.1/protobuf-all-3.11.1.tar.gz"],
)

load("@com_google_protobuf//:protobuf_deps.bzl", "protobuf_deps")

protobuf_deps()

http_archive(
    name = "com_github_buildbarn_bb_deployments",
    sha256 = "cf910624a50d3f1f4c8af98d96f4ff7cbdd51f2c107315ac835256776a41df1c",
    strip_prefix = "bb-deployments-cf505b7f363ce87798a367d6741b8f550a5e077a",
    url = "https://github.com/buildbarn/bb-deployments/archive/cf505b7f363ce87798a367d6741b8f550a5e077a.tar.gz",
)

http_archive(
    name = "com_grail_bazel_toolchain",
    sha256 = "b3dec631fe2be45b3a7a8a4161dd07fadc68825842e8d6305ed35bc8560968ca",
    strip_prefix = "bazel-toolchain-0.5.1",
    urls = ["https://github.com/grailbio/bazel-toolchain/archive/0.5.1.tar.gz"],
)

load("@com_grail_bazel_toolchain//toolchain:rules.bzl", "llvm_toolchain")

llvm_toolchain(
    name = "llvm_toolchain",
    llvm_version = "9.0.0",
)
