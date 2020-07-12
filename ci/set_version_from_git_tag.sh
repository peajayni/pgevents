#!/bin/bash
set -eufx

package="pgevents"
version_file="${package}/version.py"

fallback_version="0.0.0"
git_version=$(git tag --sort=creatordate | tail -1)
version=${git_version:=${fallback_version}}


echo "__version__ = \"${version}\"" > ${version_file}
