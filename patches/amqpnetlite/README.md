# AMQPNetLite patches

Patches applied to the `external/amqpnetlite` submodule (Azure/amqpnetlite). They are
applied by [`apply-patches.ps1`](../../apply-patches.ps1) and, for container builds, by the
[`Dockerfile`](../../Dockerfile). When the submodule is checked out, the root
[`Directory.Build.props`](../../Directory.Build.props) auto-detects it and builds
`AlmostServiceBus.Core` against the patched source; otherwise the published `AMQPNetLite`
NuGet package is used. Set `UseAmqpNetLiteSource=false` to force the package even when the
submodule is present.

## Patches

### `0001-defer-listener-open-frame.patch`

Defers the listener's AMQP `Open` frame until the peer's protocol header arrives, so the
header and `Open` are sent as separate writes instead of being coalesced into one TCP
segment. Some clients — notably the Node.js `@azure/service-bus` / `rhea` client opening
several connections concurrently — fail to parse the coalesced bytes and hang until their
connection-open timeout (~60s).

Introduces an internal virtual `Connection.PipelineOpen` (default `true`) that
`ListenerConnection` overrides to `false`; client connections are unchanged.

**Submitted upstream:** https://github.com/Azure/amqpnetlite/pull/651
(branch `fix/defer-listener-open-frame`). Once merged and released, this patch and the
source-build wiring can be dropped in favour of the NuGet package.

### `0002-target-netstandard2.0-only.patch`

Drops the `netstandard1.3` target from `src/Amqp.csproj`, leaving `netstandard2.0`. This
avoids building the legacy target (which pulls obsolete packages) when compiling the
vendored source; `AlmostServiceBus.Core` targets `net10.0`, which resolves `netstandard2.0`.

## Regenerating

```bash
cd external/amqpnetlite
git diff > ../../patches/amqpnetlite/000X-<name>.patch
```
