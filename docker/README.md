# Running AlmostServiceBus in Docker

This folder contains everything needed to build and run the emulator as a container:

- [`Dockerfile`](Dockerfile) — a minimal runtime image that copies a pre-published build.
- [`build-docker.sh`](build-docker.sh) — publishes the app on the host and packages it into the image.

## How the build works

Unlike a typical multi-stage Dockerfile, the .NET/Node build runs **on the host**, not inside the
image. `build-docker.sh`:

1. Applies any submodule patches under the repo's `patches/` folder (idempotent; none are
   needed for the emulator itself today — they exist for the framework conformance suites).
2. Runs `dotnet publish` for `AlmostServiceBus.Host`, which also builds the Vue dashboard, into
   `artifacts/publish/` at the repo root.
3. Runs `docker build` with **`artifacts/publish/` as the build context**. Because the context is
   just the published output, the daemon never receives the whole repo and there is no
   `.dockerignore` to maintain — the `Dockerfile` is a single `COPY . ./`.

## Prerequisites

Because the build happens on the host, the machine running the script needs:

- .NET 10 SDK
- Node.js (for the dashboard build)
- Docker
- Bash — on Windows use Git Bash or WSL

## Building

Run from anywhere; the script resolves the repo root relative to itself:

```bash
./docker/build-docker.sh                       # builds almostservicebus:local
./docker/build-docker.sh --tag my/image:1.2.3  # custom image tag
./docker/build-docker.sh --configuration Debug # publish configuration (default Release)
./docker/build-docker.sh --skip-patches        # skip the submodule patch step
```

## Running

Expose the AMQP (5672), admin HTTP (5300), admin HTTPS (5301), and dashboard (15672) ports:

```bash
docker run --rm -p 5672:5672 -p 5300:5300 -p 5301:5301 -p 15672:15672 almostservicebus:local
```

From the host, connect to `localhost:5672` exactly as with the standalone tool:

```
Endpoint=sb://localhost:5672;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=emulator;UseDevelopmentEmulator=true
```

Using `RootManageSharedAccessKey` as the `SharedAccessKeyName` maps to the `default` namespace.

## Reaching the emulator from another container

When your app runs in the same Docker network it connects using the emulator's service/container
name rather than `localhost`. The emulator detects that it is running in a container and advertises
the right host automatically, and it treats its own container hostname as the `default` namespace,
so `RootManageSharedAccessKey` keeps working. For example, with Docker Compose:

```yaml
services:
  servicebus:
    image: almostservicebus:local
    ports:
      - "5672:5672"
      - "5300:5300"
      - "5301:5301"
      - "15672:15672"

  app:
    build: ./app
    depends_on: [servicebus]
    environment:
      # Host matches the service name above
      ConnectionStrings__ServiceBus: "Endpoint=sb://servicebus:5672;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=emulator;UseDevelopmentEmulator=true"
```

If clients reach the emulator under a name that differs from the container hostname, set `ASB_HOST`
to that name.

## Configuration

The image's entrypoint starts the host on the standard ports. Common overrides:

| Environment variable | Purpose |
|----------------------|---------|
| `ASB_HOST` | Host name the emulator advertises and treats as the `default` namespace (plus comma-separated aliases). Set this when clients reach the emulator under a name other than the container hostname. |
| `ASB_BIND_HOST` | Interface to bind (defaults to `0.0.0.0` in a container). |
| `AdminTlsPort` | HTTPS admin port (default `5301`; `0` disables it). |
| `AdminTlsCertDir` | Where TLS material is written/read (default `/certs`, a declared `VOLUME`). |
| `AdminTlsHosts` | Extra hostnames/IPs added to the auto-generated certificate's SANs (e.g. the Compose service name). |
| `AdminTlsCertBase64` | Base64-encoded PFX to use your own certificate instead of a generated one. |
| `AdminTlsCertPassword` | Password for the supplied PFX. |

The entrypoint passes `--Port 5672 --DashboardPort 15672 --AdminTlsPort 5301 --AdminTlsCertDir /certs`;
override by supplying your own arguments after the image name in `docker run`.

The `AdminTls*` options configure the HTTPS admin endpoint and certificates — see
[`../certs/README.md`](../certs/README.md) for the full reference.

## HTTPS admin endpoint (Node.js, Java, Python)

The Node.js, Java and Python **admin** clients only speak HTTPS, so the image exposes an HTTPS admin
endpoint on **port 5301** in addition to the plain-HTTP one on 5300 (used by .NET). The per-language
trust setup (Node.js / Java / Python / curl) and bring-your-own-certificate details live in
[`../certs/README.md`](../certs/README.md); the only Docker-specific parts are keeping the CA stable
and supplying a certificate via base64.

### Keep the CA stable across restarts

The image declares a `VOLUME /certs` and writes the generated CA there. Mount it so the CA (which
clients trust once) survives `docker run` cycles, and copy it out to hand to your clients:

```bash
docker run --rm -p 5672:5672 -p 5300:5300 -p 5301:5301 -p 15672:15672 \
  -v asb-certs:/certs almostservicebus:local

# grab the CA / Java truststore the clients need
docker run --rm -v asb-certs:/certs -w /certs busybox cat emulator-ca.crt > emulator-ca.crt
```

When clients reach the emulator by a name other than `localhost` (e.g. a Compose service name), add
it to the certificate SANs with `-e AdminTlsHosts=servicebus`.

### Bring your own certificate (base64)

A file path is awkward inside a container, so supply a PKCS#12/PFX as a base64 string:

```bash
CERT_B64=$(base64 -w0 mine.pfx)   # macOS: base64 -i mine.pfx | tr -d '\n'

docker run --rm -p 5672:5672 -p 5300:5300 -p 5301:5301 -p 15672:15672 \
  -e AdminTlsCertBase64="$CERT_B64" \
  -e AdminTlsCertPassword=secret \
  almostservicebus:local
```

The emulator serves your certificate on 5301 and still writes `emulator-ca.crt` /
`emulator-truststore.p12` (derived from it) into `/certs`, so the client trust setup is identical.
