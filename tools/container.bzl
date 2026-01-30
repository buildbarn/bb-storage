load("@aspect_bazel_lib//lib:transitions.bzl", "platform_transition_filegroup")
load("@bazel_skylib//rules:write_file.bzl", "write_file")
load("@rules_img//img:image.bzl", "image_index", "image_manifest")
load("@rules_img//img:layer.bzl", "image_layer")
load("@rules_img//img:push.bzl", "image_push")
load("@rules_oci//oci:defs.bzl", "oci_image", "oci_image_index", "oci_push")
load("@rules_pkg//pkg:tar.bzl", "pkg_tar")

def multiarch_go_image(name, binary):
    """Create a container image with two variants of the given go_binary target.

    Args:
        name: resulting image_index target
        binary: label of a go_binary target; it may be transitioned to another architecture
    """

    tar_target = "_{}.tar".format(name)
    image_target = "_{}.image".format(name)

    image_layer(
        name = tar_target,
        srcs = {"app/{}".format(native.package_relative_label(binary).name): binary},
        # Don't build un-transitioned images, as the default target architecture might be unsupported
        # For example when building on linux-i386.
        tags = ["manual"],
    )

    image_manifest(
        name = image_target,
        base = Label("@distroless_static"),
        entrypoint = ["/app/{}".format(native.package_relative_label(binary).name)],
        layers = [tar_target],
        # Don't build un-transitioned images, as the default target architecture might be unsupported
        # For example when building on linux-i386.
        tags = ["manual"],
    )

    image_index(
        name = name,
        manifests = [image_target],
        platforms = [
            Label("//tools/platforms:linux_amd64"),
            Label("//tools/platforms:linux_amd64_v3"),
            Label("//tools/platforms:linux_arm64"),
        ],
        visibility = ["//visibility:public"],
        # Don't build container image unless explicitly requested, as
        # building all variants can be time-consuming.
        tags = ["manual"],
    )

def multiarch_go_image_with_windows(name, binary):
    """Create a container image with three variants of the given go_binary target.

    Args:
        name: resulting oci_image_index target
        binary: label of a go_binary target; it may be transitioned to another architecture
    """
    images = []
    tar_target = "_{}.tar".format(name)
    image_target = "_{}.image".format(name)
    unix_binary_entrypoint = ["/app/{}".format(native.package_relative_label(binary).name)]
    windows_binary_entrypoint = ["C:\\app\\{}".format(native.package_relative_label(binary).name)]

    pkg_tar(
        name = tar_target,
        srcs = [binary],
        include_runfiles = True,
        package_dir = "app",
    )

    write_file(
        name = image_target + "_entrypoint",
        content = select({
            "@rules_go//go/platform:linux_amd64": unix_binary_entrypoint,
            "@rules_go//go/platform:linux_arm64": unix_binary_entrypoint,
            "@rules_go//go/platform:windows_amd64": windows_binary_entrypoint,
        }),
        out = "entrypoints.txt"
    )

    oci_image(
        name = image_target,
        base = select({
            "@rules_go//go/platform:linux_amd64": Label("@distroless_static_oci"),
            "@rules_go//go/platform:linux_arm64": Label("@distroless_static_oci"),
            "@rules_go//go/platform:windows_amd64": Label("@nanoserver_oci"),
        }),
        entrypoint = image_target + "_entrypoint",
        tars = [tar_target],
        # Don't build un-transitioned images, as the default target architecture might be unsupported
        # For example when building on linux-i386.
        tags = ["manual"],
    )

    oci_image_index(
        name = name,
        images = [image_target],
        platforms = [
            Label("//tools/platforms:linux_amd64"),
            # TODO: re-enable when rules_oci supports this pattern
#            Label("//tools/platforms:linux_amd64_v3"),
            Label("//tools/platforms:linux_arm64"),
            Label("//tools/platforms:windows_amd64"),
        ]
    )

def container_push_official(name, image, component):
    image_push(
        name = name,
        image = image,
        registry = "ghcr.io",
        repository = "buildbarn/" + component,
        tag_file = "@com_github_buildbarn_bb_storage//tools:stamped_tags",
        # Don't build container image unless explicitly requested, as
        # building all variants can be time-consuming.
        tags = ["manual"],
    )

def container_push_official_with_windows(name, image, component):
    oci_push(
        name = name,
        image = image,
        repository = "ghcr.io/buildbarn/" + component,
        remote_tags = "@com_github_buildbarn_bb_storage//tools:stamped_tags",
        # Don't build container image unless explicitly requested, as
        # building all variants can be time-consuming.
        tags = ["manual"],
    )