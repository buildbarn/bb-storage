#!/bin/sh -e

if test "${GITHUB_ACTIONS}" = "true"; then
  echo "STABLE_BUILD_SCM_REVISION $(git rev-parse --short HEAD)"
  echo "STABLE_BUILD_SCM_TIMESTAMP $(TZ=UTC date --date "@$(git show -s --format=%ct HEAD)" +%Y%m%dT%H%M%SZ)"
fi
