load("@aspect_bazel_lib//lib:transitions.bzl", "platform_transition_filegroup")
load("@rules_oci//oci:defs.bzl", "oci_image", "oci_image_index", "oci_push")
load("@rules_pkg//pkg:tar.bzl", "pkg_tar")

def multiarch_go_image(name, binary):
    """Create a container image with two variants of the given go_binary target.

    Args:
        name: resulting oci_image_index target
        binary: label of a go_binary target; it may be transitioned to another architecture
    """
    images = []
    tar_target = "_{}.tar".format(name)
    image_target = "_{}.image".format(name)

    pkg_tar(
        name = tar_target,
        srcs = [binary],
        include_runfiles = True,
        package_dir = "app",
    )

    oci_image(
        name = image_target,
        base = "@distroless_static",
        entrypoint = ["/app/{}".format(native.package_relative_label(binary).name)],
        tars = [tar_target],
    )

    oci_image_index(
        name = name,
        image = image_target,
        platforms = [
            "@io_bazel_rules_go//go/toolchain:linux_{}".format(arch)
            for arch in ["amd64", "arm64"]
        ],
    )

def container_push_official(name, image, component):
    oci_push(
        name = name,
        image = image,
        repository = "ghcr.io/buildbarn/" + component,
        remote_tags = "@com_github_buildbarn_bb_storage//tools:stamped_tags",
    )
