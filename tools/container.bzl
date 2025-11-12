load("@rules_img//img:layer.bzl", "image_layer")
load("@rules_img//img:image.bzl", "image_manifest", "image_index")
load("@rules_img//img:push.bzl", "image_push")

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
    )

def container_push_official(name, image, component):
    image_push(
        name = name,
        image = image,
        registry = "ghcr.io",
        repository = "buildbarn/" + component,
        tag_file = "@com_github_buildbarn_bb_storage//tools:stamped_tags",
    )
