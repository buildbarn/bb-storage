load("@io_bazel_rules_go//go:def.bzl", "go_binary")
load("@rules_pkg//:pkg.bzl", "pkg_tar")
load("@rules_oci//oci:defs.bzl", "oci_image", "oci_image_index")

def go_image(name, base = "@distroless_base", srcs = [], embed = [], tags = [], visibility = None, pure = "on", arch = "amd64", os = "linux", variant = None):
    go_binary(
        name = "{}_binary".format(name),
        srcs = srcs,
        embed = embed,
        goarch = arch,
        goos = os,
        pure = pure,
        tags = tags,
        visibility = visibility,
    )

    pkg_tar(
        name = "{}_layer".format(name),
        srcs = ["{}_binary".format(name)],
        tags = tags,
        visibility = visibility,
    )

    oci_image(
        name = name,
        base = "{base}_{os}_{arch}".format(base = base, os = os, arch = arch),
        entrypoint = ["/{}_binary".format(name)],
        tars = [
            "{}_layer".format(name),
        ],
        tags = tags,
        visibility = visibility,
    )

def go_image_index(name, platforms, **kwargs):
    """Creates a multi platform go image

    Args:
        name: Name of the target
        platforms: List of platforms to create
        **kwargs: Rest of arguments passed down to go_image
    """
    images = []
    for platform in platforms:

        platform_split = platform.split("/")
        os = platform_split[0]
        arch = platform_split[1]
        variant = platform_split[2] if len(platform_split) > 2 else None

        parts = [os, arch]
        if variant:
            parts.append(variant)

        go_image_name = "_".join(parts)
        images.append(go_image_name)

        go_image(
            name = go_image_name,
            os = os, 
            arch = arch,
            variant = variant,
            **kwargs
        )

    oci_image_index(
        name = name,
        images = images,
    )
    