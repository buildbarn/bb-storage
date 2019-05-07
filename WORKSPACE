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
    sha256 = "109a99384f9d08f9e75136d218ebaebc68cc810c56897aea2224c57932052d30",
    strip_prefix = "bazel-toolchains-94d31935a2c94fe7e7c7379a0f3393e181928ff7",
    urls = [
        "https://mirror.bazel.build/github.com/bazelbuild/bazel-toolchains/archive/94d31935a2c94fe7e7c7379a0f3393e181928ff7.tar.gz",
        "https://github.com/bazelbuild/bazel-toolchains/archive/94d31935a2c94fe7e7c7379a0f3393e181928ff7.tar.gz",
    ],
)

http_archive(
    name = "io_bazel_rules_docker",
    sha256 = "aed1c249d4ec8f703edddf35cbe9dfaca0b5f5ea6e4cd9e83e99f3b0d1136c3d",
    strip_prefix = "rules_docker-0.7.0",
    urls = ["https://github.com/bazelbuild/rules_docker/archive/v0.7.0.tar.gz"],
)

load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

http_archive(
    name = "io_bazel_rules_go",
    sha256 = "86ae934bd4c43b99893fc64be9d9fc684b81461581df7ea8fc291c816f5ee8c5",
    urls = ["https://github.com/bazelbuild/rules_go/releases/download/0.18.3/rules_go-0.18.3.tar.gz"],
)

http_archive(
    name = "bazel_gazelle",
    sha256 = "3c681998538231a2d24d0c07ed5a7658cb72bfb5fd4bf9911157c0e9ac6a2687",
    urls = ["https://github.com/bazelbuild/bazel-gazelle/releases/download/0.17.0/bazel-gazelle-0.17.0.tar.gz"],
)

load("@io_bazel_rules_docker//repositories:repositories.bzl", container_repositories = "repositories")

container_repositories()

load("@io_bazel_rules_go//go:deps.bzl", "go_register_toolchains", "go_rules_dependencies")

go_rules_dependencies()

go_register_toolchains()

load("@bazel_gazelle//:deps.bzl", "gazelle_dependencies", "go_repository")

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

go_repository(
    name = "dev_gocloud",
    commit = "a68836e8e108ad55d26e8e2d21579028090c8aa5",
    importpath = "gocloud.dev",
)

go_repository(
    name = "org_golang_google_api",
    commit = "612f6f6a5fdac9e861d1779dffe63a1e105fb55c",
    importpath = "google.golang.org/api",
)

go_repository(
    name = "com_google_cloud_go",
    commit = "09ad026a62f0561b7f7e276569eda11a6afc9773",
    importpath = "cloud.google.com/go",
)

go_repository(
    name = "io_opencensus_go",
    commit = "17d7955af9d42886455ce010dd46878208041a58",
    importpath = "go.opencensus.io",
)

go_repository(
    name = "org_golang_x_xerrors",
    commit = "385005612d73f6925de56cb1886917aeaf90e3c5",
    importpath = "golang.org/x/xerrors",
)

go_repository(
    name = "com_github_hashicorp_golang_lru",
    commit = "7087cb70de9f7a8bc0a10c375cb0d2280a8edf9c",
    importpath = "github.com/hashicorp/golang-lru",
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
