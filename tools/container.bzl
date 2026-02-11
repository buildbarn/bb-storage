load("@rules_img//img:image.bzl", "image_index", "image_manifest")
load("@rules_img//img:layer.bzl", "image_layer")
load("@rules_img//img:push.bzl", "image_push")

def multiarch_go_image(name, binary):
    """Create a container image with two variants of the given go_binary target.

    Args:
        name: resulting image_index target
        binary: label of a go_binary target; it may be transitioned to another architecture
    """

    tar_target = "_{}.tar".format(name)
    image_target = "_{}.image".format(name)
    binary_name = native.package_relative_label(binary).name

    image_layer(
        name = tar_target,
        srcs = select({
            "@rules_go//go/platform:linux": {"app/" + binary_name: binary},
            "@rules_go//go/platform:windows": {"app/{}.exe".format(binary_name): binary},
        }),
        # Creating parent directories is not recommended
        # as the directory creation during layer extraction shadows layers
        # from parent directories instead of merging their contents.
        # On Windows, the extraction routine requires parent directories to exist.
        create_parent_directories = select({
            "@rules_go//go/platform:linux": "disabled",
            "@rules_go//go/platform:windows": "enabled",
        }),
        # Don't build un-transitioned images, as the default target
        # architecture might be unsupported For example when building on
        # linux-i386.
        tags = ["manual"],
    )

    image_manifest(
        name = image_target,
        base = select({
            "@rules_go//go/platform:linux": Label("@distroless_static"),
            "@rules_go//go/platform:windows": Label("@nanoserver"),
        }),
        entrypoint = select({
            "@rules_go//go/platform:linux": ["/app/" + binary_name],
            "@rules_go//go/platform:windows": ["C:\\app\\{}.exe".format(binary_name)],
        }),
        layers = [tar_target],
        # Don't build un-transitioned images, as the default target
        # architecture might be unsupported For example when building on
        # linux-i386.
        tags = ["manual"],
    )

    image_index(
        name = name,
        manifests = [image_target],
        platforms = [
            Label("//tools/platforms:linux_amd64"),
            Label("//tools/platforms:linux_amd64_v3"),
            Label("//tools/platforms:linux_arm64"),
            Label("//tools/platforms:windows_amd64"),
        ],
        visibility = ["//visibility:public"],
        # Don't build container image unless explicitly requested, as
        # building all variants can be time-consuming.
        tags = ["manual"],
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
