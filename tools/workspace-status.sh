#!/bin/sh -e

# Emit stamp values for every build, not just CI, so local builds end
# up with the same ${TIMESTAMP}-${SHA} image-tag format that the GitHub
# Actions workflow produces.
TS=$(git show -s --format=%ct HEAD)
SHA=$(git rev-parse --short HEAD)

if FORMATTED=$(TZ=UTC date -u -d "@${TS}" +%Y%m%dT%H%M%SZ 2>/dev/null); then
  :
else
  FORMATTED=$(TZ=UTC date -u -r "${TS}" +%Y%m%dT%H%M%SZ)
fi

echo "BUILD_SCM_REVISION ${SHA}"
echo "BUILD_SCM_TIMESTAMP ${FORMATTED}"
