# TLS certificates & the HTTPS admin endpoint

The emulator's **data plane** is always plain AMQP (`UseDevelopmentEmulator=true`), and the .NET
`ServiceBusAdministrationClient` talks to the **admin** plane over plain HTTP on port **5300**. That
is all .NET needs — no certificates, no setup.

The Node.js, Java and Python **admin** clients, however, only speak HTTPS. For them the emulator can
also expose an HTTPS admin endpoint on port **5301**. This endpoint is **opt-in**: enable it with
`--AdminTlsEnabled true` (or the `AdminTlsEnabled=true` env var). By default it is off and no
certificate is generated. This document explains the certificate it serves there, how to make each
language trust it, and how to supply your own certificate instead.

> The **data plane** (sending/receiving messages on 5672) never uses TLS in any language. Everything
> below is only about the **admin/management** client that creates queues, topics and subscriptions.

## What the emulator generates

When TLS is enabled (`--AdminTlsEnabled true`), on first start the emulator creates a local
development CA and a server certificate, writes them to the cert directory (default `<app>/certs`, or
`/certs` in the container), and **reuses them on every later start** — so you trust the CA exactly
**once**. Point a volume at the cert directory to keep it stable across restarts.

| File | Format | Who uses it |
|------|--------|-------------|
| `emulator-ca.crt` | PEM (public CA) | Node.js, Python, curl, .NET-on-Linux |
| `emulator-truststore.p12` | PKCS#12 truststore, password `changeit` | Java |
| `emulator-admin.pfx` | PKCS#12 (cert + key) | the emulator itself — you don't need this |

The certificate's SANs cover `localhost`, `127.0.0.1` and `::1` out of the box. If your clients reach
the emulator under another name (a Docker service name, a LAN host, …), add it with `--AdminTlsHosts`
(or the `AdminTlsHosts` env var) so the name ends up in the certificate — TLS verification fails if
the hostname isn't a SAN.

The startup banner prints the resolved paths and the exact per-client trust hints.

## Trusting the CA per language

Setup differs per SDK. Follow the guide for your language:

| Language | Guide | Notes |
|----------|-------|-------|
| Node.js | [nodejs.md](nodejs.md) | Trust via `NODE_EXTRA_CA_CERTS`. |
| Python | [python.md](python.md) | Trust via `REQUESTS_CA_BUNDLE` or `connection_verify`. |
| Java | [java.md](java.md) | Trust via a PKCS#12 truststore. **The SDK admin client only works on port 443** — see the guide. |
| .NET | [dotnet.md](dotnet.md) | No TLS needed — uses plain HTTP on 5300. |

All guides assume the admin endpoint `https://localhost:5301` and the connection string:

```
Endpoint=sb://localhost:5301;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=emulator;UseDevelopmentEmulator=true
```

## Bring your own certificate

To serve a certificate you control instead of the auto-generated one, point the emulator at it. The
emulator still derives and writes `emulator-ca.crt` and `emulator-truststore.p12` from your
certificate, so the per-language trust setup above is identical.

Make a self-signed certificate with openssl (include every hostname your clients use as a SAN):

```bash
openssl req -x509 -newkey rsa:2048 -keyout key.pem -out cert.pem -days 365 -nodes \
  -subj "/CN=my-emulator" \
  -addext "subjectAltName=DNS:localhost,DNS:servicebus,IP:127.0.0.1"
```

Start the emulator with it, as **PEM certificate + key**:

```bash
almost-servicebus --AdminTlsEnabled true --AdminTlsCertPath ./cert.pem --AdminTlsKeyPath ./key.pem
```

…or as a **PKCS#12/PFX bundle** (also bundle the issuing CA in the PFX if the cert is CA-signed, so
the emitted trust material contains the full chain):

```bash
openssl pkcs12 -export -out cert.pfx -inkey key.pem -in cert.pem -passout pass:secret
almost-servicebus --AdminTlsEnabled true --AdminTlsCertPath ./cert.pfx --AdminTlsCertPassword secret
```

### Certificate options

| Option (CLI arg / env var) | Purpose |
|----------------------------|---------|
| `--AdminTlsEnabled` / `AdminTlsEnabled` | Master switch for the HTTPS admin endpoint (default `false`). Must be `true` to bind the TLS listener and generate/load certificates. |
| `--AdminTlsPort` / `AdminTlsPort` | HTTPS admin port (default `5301`; only used when TLS is enabled; `0` also disables it). |
| `--AdminTlsCertDir` / `AdminTlsCertDir` | Where TLS material is written/read (default `<app>/certs`, `/certs` in Docker). |
| `--AdminTlsCertPath` / `AdminTlsCertPath` | Path to your certificate: a PKCS#12/PFX, or a PEM cert (pair with the key option). |
| `--AdminTlsKeyPath` / `AdminTlsKeyPath` | Path to the PEM private key, when the cert is PEM. |
| `--AdminTlsCertPassword` / `AdminTlsCertPassword` | Password for the supplied PFX (file or base64). |
| `--AdminTlsCertBase64` / `AdminTlsCertBase64` | Base64-encoded PFX — for supplying a cert via env var (containers). |
| `--AdminTlsHosts` / `AdminTlsHosts` | Extra hostnames/IPs added to the **auto-generated** certificate's SANs. |

## In Docker

A file path is awkward inside a container, so supply the PFX as a base64 string:

```bash
CERT_B64=$(base64 -w0 cert.pfx)   # macOS: base64 -i cert.pfx | tr -d '\n'

docker run --rm -p 5672:5672 -p 5300:5300 -p 5301:5301 -p 15672:15672 \
  -e AdminTlsEnabled=true \
  -e AdminTlsCertBase64="$CERT_B64" \
  -e AdminTlsCertPassword=secret \
  almostservicebus:local
```

To keep the auto-generated CA stable, mount a volume at `/certs` and copy the CA out for your
clients:

```bash
docker run --rm -p 5301:5301 -e AdminTlsEnabled=true -v asb-certs:/certs almostservicebus:local
docker run --rm -v asb-certs:/certs -w /certs busybox cat emulator-ca.crt > emulator-ca.crt
```

See [`../docker/README.md`](../docker/README.md) for the full Docker configuration.

## Disabling TLS

The HTTPS admin endpoint is **off by default** — nothing to disable. Only enable it if you use the
Node.js, Java or Python admin clients:

```bash
almost-servicebus --AdminTlsEnabled true
```

.NET (or anything hitting the REST API on 5300 directly) needs no TLS and should leave it off.
