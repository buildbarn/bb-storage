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

# Check required binaries exist
go="$(rlocation rules_go/go/tools/go_bin_runner/bin/go)"
gofumpt="$(rlocation cc_mvdan_gofumpt/gofumpt_/gofumpt)"
clang_format="$(rlocation llvm_toolchain_llvm/bin/clang-format)"
gazelle="$(rlocation com_github_buildbarn_bb_storage/gazelle)"

if [ ! -x "$go" ] || [ ! -x "$gofumpt" ] || [ ! -x "$clang_format" ] || [ ! -x "$gazelle" ]; then
  echo "Missing dependency binaries: \n"
  echo "go: $go"
  echo "goumpt: $gofumpt"
  echo "clang-format: $clang_format"
  echo "gazelle: $gazelle"
  exit 1
fi

# Start in the root directory
cd "$BUILD_WORKSPACE_DIRECTORY"

# Get the go module name
go_module_name=$($go list -m)

# Go dependencies
find bazel-bin/ -path "*${go_module_name}*" -name '*.pb.go' -delete || true
bazel build $(bazel query --output=label 'kind("go_proto_library", //...)') || true
find bazel-bin/ -path "*${go_module_name}*" -name '*.pb.go' | while read f; do
  cat "$f" > $(echo "$f" | sed -e "s|.*/${go_module_name}/||")
done

#$go get -d -u ./... || true
$go mod tidy || true

# Gazelle
find . -name '*.pb.go' -delete
rm -f $(find . -name '*.proto' | sed -e 's/[^/]*$/BUILD.bazel/')
$gazelle

# bzlmod
bazel mod tidy

# Go
$gofumpt -w -extra "$(pwd)"

# Protobuf
find . -name '*.proto' -exec "$clang_format" -i {} +

# Generated .pb.go files
find bazel-bin/ -path "*${go_module_name}*" -name '*.pb.go' -delete || true
bazel build --output_groups=go_generated_srcs $(bazel query --output=label 'kind("go_proto_library", //...)')
find bazel-bin/ -path "*${go_module_name}*" -name '*.pb.go' | while read f; do
  cat $f > $(echo $f | sed -e "s|.*/${go_module_name}/||")
done

# Files embedded into Go binaries
if git grep -q '^[[:space:]]*//go:embed '; then
  bazel build $(git grep '^[[:space:]]*//go:embed ' | sed -e 's|\(.*\)/.*//go:embed |//\1:|; s|"||g; s| .*||' | sort -u)
  git grep '^[[:space:]]*//go:embed ' | sed -e 's|\(.*\)/.*//go:embed |\1/|' | while read o; do
    if [ -e "bazel-bin/$o" ]; then
      rm -rf "$o"
      cp -r "bazel-bin/$o" "$o"
      find "$o" -type f -exec chmod -x {} +
    fi
  done
fi
