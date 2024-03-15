load("@rules_oci//oci:defs.bzl", "oci_push")

def container_push_official(name, image, component):
    oci_push(
        name = name,
        image = image,
        repository = "ghcr.io/buildbarn/" + component,
        remote_tags = "//tools:stamped_tags",
    )
