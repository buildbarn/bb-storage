#!/bin/sh -e
BUILD_SCM_REVISION=$(git rev-parse --short HEAD)
BUILD_SCM_TIMESTAMP=$(TZ=UTC date --date "@$(git show -s --format=%ct HEAD)" +%Y%m%dT%H%M%SZ)
BB_DEFAULT_TAG='dev'
if git diff-index --quiet master --; then
  echo "BUILD_SCM_REVISION ${BUILD_SCM_REVISION}"
  echo "BUILD_SCM_TIMESTAMP ${BUILD_SCM_TIMESTAMP}"
  BB_DEFAULT_TAG="${BUILD_SCM_TIMESTAMP}-${BUILD_SCM_REVISION}"
fi
echo BB_REGISTRY ${BB_REGISTRY_OVERRIDE:-index.docker.io}
echo BB_REPO ${BB_REPO_OVERRIDE:-buildbarn/}
echo BB_TAG ${BB_TAG_OVERRIDE:-${BB_DEFAULT_TAG}}
