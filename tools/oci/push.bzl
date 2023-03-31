load("@rules_oci//oci:defs.bzl", "oci_push")
load("@aspect_bazel_lib//lib:jq.bzl", "jq")

def oci_push_official(name, image, component):
    jq(
        name = "{}_tags".format(name),
        srcs = [],
        out = "{}.tags.txt".format(name),
        args = ["--raw-output"],
        filter = "|".join([
            "$ARGS.named.STAMP as $stamp",
            """($stamp.BUILD_SCM_TIMESTAMP // "not-built-with-stamp") + "-" + ($stamp.BUILD_SCM_REVISION // "not-built-with-stamp")""",
        ]),
    )
    oci_push(
        name = name,
        image = image,
        repository = "ghcr.io/buildbarn/" + component,
        repotags = ":{}_tags".format(name),
    )
