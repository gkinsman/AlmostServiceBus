# Releasing

A release is a `v*` git tag on `main`. CI tests the tagged commit and, if every
suite passes, publishes the NuGet packages. No version numbers live in any csproj.

## Steps

1. **Merge everything that should ship into `main`** and make sure CI is green there.
2. **Add the changelog section.** In [`CHANGELOG.md`](CHANGELOG.md), add
   `## [x.y.z] - YYYY-MM-DD` above the previous release, in the
   [Keep a Changelog](https://keepachangelog.com/) style the file already uses
   (`### Fixed` / `### Changed` / `### Added`, one bold line per change, then the
   details, with PR or issue numbers). Fold dependency-update PRs into one
   **Dependency updates** line. Merge it through a PR, either on its own
   (`docs(changelog): add x.y.z`) or with the last fix going into the release.
3. **Tag the merge commit and push the tag:**

   ```bash
   git fetch origin
   git tag vX.Y.Z origin/main
   git push origin vX.Y.Z
   ```

4. **Watch the CI run for the tag** (`gh run list --branch vX.Y.Z`). It runs the
   full matrix: internal suites, the Python/Node/Java client smoke tests,
   Wolverine, NServiceBus and MassTransit. If everything passes, **Pack & Publish**
   pushes the packages to NuGet.org.
5. **Check NuGet.org** for the new version of each package (indexing can take a
   few minutes).

## Choosing the version

[MinVer](https://github.com/adamralph/minver) derives the version from the tag
(`v` prefix). Builds between tags get a pre-release version such as
`0.7.3-alpha.0.4` (four commits after `v0.7.2`).

- **Patch** (`v0.7.3`): fixes and dependency updates.
- **Minor** (`v0.8.0`): new features, or behaviour changes that can surface errors
  that used to be hidden (the changelog should say so, as 0.7.0's does).
- **Major** (`v1.0.0`): not yet.

## What gets published

| Package | Project |
|---------|---------|
| `AlmostServiceBus` | `src/AlmostServiceBus.Core` |
| `AlmostServiceBus.TestHost` | `src/AlmostServiceBus.TestHost` |
| `AlmostServiceBus.Aspire.Hosting` | `src/AlmostServiceBus.Aspire.Hosting` |
| `AlmostServiceBus.Tool` (dotnet tool `almost-servicebus`) | `src/AlmostServiceBus.Host` |

NuGet.org login uses OIDC trusted publishing with the `NUGET_USER` repository
secret; there is no stored API key.

A release does **not** create a GitHub Release or publish a Docker image.
[`docker/build-docker.sh`](docker/build-docker.sh) builds an image locally.

## If publishing fails

Fix the cause, then rerun publishing for the existing tag without moving it: run
the **CI** workflow manually (`workflow_dispatch`) with `tag` set to `vX.Y.Z`. It
checks out the tag, reruns the tests, and pushes with `--skip-duplicate`, so
packages that already made it to NuGet.org are left alone.

Don't move or delete a tag once its packages are on NuGet.org: a published
version can be unlisted but never replaced. Release the fix as the next patch.
