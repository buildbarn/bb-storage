load("@io_bazel_rules_docker//container:container.bzl", "container_push")

def container_push_official(name, image, component):
    container_push(
        name = name,
        format = "Docker",
        image = image,
        registry = "{BB_REGISTRY}",
        repository = "{BB_REPO}" + component,
        tag = "{BB_TAG}",
    )
