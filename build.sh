#!/usr/bin/env bash
# Build the install zip locally (same layout the GitHub release produces).
# Useful for the manual "zip upload" install path in Dispatcharr.
#   bash build.sh   ->   dist/failovarr-<version>.zip  (+ its sha256)
set -euo pipefail
cd "$(dirname "$0")"

VERSION=$(jq -r .version plugin.json 2>/dev/null || \
          grep -oP '"version"\s*:\s*"\K[^"]+' plugin.json | head -1)

rm -rf dist && mkdir -p dist/failovarr
cp plugin.py plugin.json logo.png README.md LICENSE CHANGELOG.md dist/failovarr/

ASSET="dist/failovarr-${VERSION}.zip"
(cd dist && zip -r "failovarr-${VERSION}.zip" failovarr >/dev/null)

echo "Built ${ASSET}"
if command -v sha256sum >/dev/null 2>&1; then sha256sum "${ASSET}"; else shasum -a 256 "${ASSET}"; fi
