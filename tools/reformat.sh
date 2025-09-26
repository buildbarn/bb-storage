#!/bin/bash

# --- begin runfiles.bash initialization v3 ---
# Copy-pasted from the Bazel Bash runfiles library v3.
set -uo pipefail; set +e; f=bazel_tools/tools/bash/runfiles/runfiles.bash
# shellcheck disable=SC1090
source "${RUNFILES_DIR:-/dev/null}/$f" 2>/dev/null || \
  source "$(grep -sm1 "^$f " "${RUNFILES_MANIFEST_FILE:-/dev/null}" | cut -f2- -d' ')" 2>/dev/null || \
  source "$0.runfiles/$f" 2>/dev/null || \
  source "$(grep -sm1 "^$f " "$0.runfiles_manifest" | cut -f2- -d' ')" 2>/dev/null || \
  source "$(grep -sm1 "^$f " "$0.exe.runfiles_manifest" | cut -f2- -d' ')" 2>/dev/null || \
  { echo>&2 "ERROR: cannot find $f"; exit 1; }; f=; set -e
# --- end runfiles.bash initialization v3 ---

# Start in the root directory
cd "$BUILD_WORKSPACE_DIRECTORY"

# Go dependencies
find . bazel-bin/pkg/proto -name '*.pb.go' -delete || true
bazel build -k --output_groups=go_generated_srcs $(bazel query --output=label 'kind("go_proto_library", //...)') || true
find bazel-bin/pkg/proto -name '*.pb.go' | while read f; do
  cat $f > $(echo $f | sed -e 's|.*/pkg/proto/|pkg/proto/|')
done
#go get -d -u ./... || true
go mod tidy || true

# Gazelle
find . -name '*.pb.go' -delete
rm -f $(find . -name '*.proto' | sed -e 's/[^/]*$/BUILD.bazel/')
bazel run //:gazelle

# bzlmod
bazel mod tidy

# Go
"$(rlocation gazelle++go_deps+cc_mvdan_gofumpt/gofumpt_/gofumpt)" -w -extra "$(pwd)"

# Protobuf
find . -name '*.proto' -exec "$(rlocation toolchains_llvm++llvm+llvm_toolchain_llvm/bin/clang-format)" -i {} +

# Generated .pb.go files
find bazel-bin/pkg/proto -name '*.pb.go' -delete || true
bazel build --output_groups=go_generated_srcs $(bazel query --output=label 'kind("go_proto_library", //...)')
find bazel-bin/pkg/proto -name '*.pb.go' | while read f; do
  cat $f > $(echo $f | sed -e 's|.*/pkg/proto/|pkg/proto/|')
done

# Files embedded into Go binaries
bazel build $(git grep '^[[:space:]]*//go:embed ' | sed -e 's|\(.*\)/.*//go:embed |//\1:|; s|"||g; s| .*||' | sort -u)
git grep '^[[:space:]]*//go:embed ' | sed -e 's|\(.*\)/.*//go:embed |\1/|' | while read o; do
  if [ -e "bazel-bin/$o" ]; then
    rm -rf "$o"
    cp -r "bazel-bin/$o" "$o"
    find "$o" -type f -exec chmod -x {} +
  fi
done
