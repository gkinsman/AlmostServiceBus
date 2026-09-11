#!/usr/bin/env bash
# Docker build for the emulator. Builds and publishes the app on the host (so the patched
# AMQPNetLite submodule source and the Vue dashboard are built with the local toolchain), then
# packages the published output into a runtime image. The Docker build context is only
# artifacts/publish, so there is no .dockerignore to maintain and the daemon never receives the
# whole repo. On Windows use Git Bash or WSL.
#
#   ./build-docker.sh                          # builds almostservicebus:local
#   ./build-docker.sh --tag my/image:1.2.3     # custom tag
#   ./build-docker.sh --configuration Debug    # publish configuration
#   ./build-docker.sh --skip-patches           # skip the submodule patch step
set -euo pipefail

# This script lives in docker/; the repo root is its parent.
script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
root="$(cd "$script_dir/.." && pwd)"

tag="almostservicebus:local"
configuration="Release"
skip_patches=0
while [ $# -gt 0 ]; do
    case "$1" in
        -t|--tag) tag="$2"; shift 2 ;;
        -c|--configuration) configuration="$2"; shift 2 ;;
        --skip-patches) skip_patches=1; shift ;;
        *) echo "Unknown option: $1" >&2; exit 2 ;;
    esac
done

publish_dir="$root/artifacts/publish"
host_project="$root/src/AlmostServiceBus.Host/AlmostServiceBus.Host.csproj"

# Apply the submodule patches idempotently: reverse-check first (already applied -> skip), else
# apply. Skips submodules that aren't checked out.
apply_patches() {
    [ -d "$root/patches" ] || return 0
    for dir in "$root"/patches/*/; do
        [ -d "$dir" ] || continue
        framework="$(basename "$dir")"
        submodule="$root/external/$framework"
        for patch in "$dir"*.patch; do
            [ -e "$patch" ] || continue
            name="$(basename "$patch")"
            if [ ! -d "$submodule" ]; then
                echo "  Skipped: $name ($framework not checked out)"
            elif git -C "$submodule" apply --reverse --check --ignore-whitespace "$patch" 2>/dev/null; then
                echo "  Skipped: $name (already applied)"
            elif git -C "$submodule" apply --ignore-whitespace "$patch" 2>/dev/null; then
                echo "  Applied: $name"
            else
                echo "  FAILED:  $name — could not apply to external/$framework" >&2
                exit 1
            fi
        done
    done
}

if [ "$skip_patches" -eq 0 ]; then
    echo "Applying submodule patches..."
    apply_patches
fi

echo "Publishing AlmostServiceBus.Host ($configuration)..."
rm -rf "$publish_dir"
dotnet publish "$host_project" -c "$configuration" -o "$publish_dir"

# Context is the published output only; the Dockerfile just COPYies it in.
echo "Building Docker image '$tag'..."
docker build -f "$script_dir/Dockerfile" -t "$tag" "$publish_dir"

echo "Done. Built image '$tag'."
