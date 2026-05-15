load("@rules_img//img:push.bzl", "image_push")

def container_push_fork(name, image, component):
    image_push(
        name = name,
        image = image,
        registry = "ghcr.io",
        repository = "aron-muon/" + component,
        stamp = "enabled",
        tag_file = "@com_github_buildbarn_bb_storage//tools:stamped_tags",
        tags = ["manual"],
    )
