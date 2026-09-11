# TLS certificates & the HTTPS admin endpoint

The emulator's **data plane** is always plain AMQP (`UseDevelopmentEmulator=true`), and the .NET
`ServiceBusAdministrationClient` talks to the **admin** plane over plain HTTP on port **5300**. That
is all .NET needs — no certificates, no setup.

The Node.js, Java and Python **admin** clients, however, only speak HTTPS. For them the emulator
also exposes an HTTPS admin endpoint on port **5301**. This document explains the certificate it
serves there, how to make each language trust it, and how to supply your own certificate instead.

> The **data plane** (sending/receiving messages on 5672) never uses TLS in any language. Everything
> below is only about the **admin/management** client that creates queues, topics and subscriptions.

## What the emulator generates

On first start the emulator creates a local development CA and a server certificate, writes them to
the cert directory (default `<app>/certs`, or `/certs` in the container), and **reuses them on every
later start** — so you trust the CA exactly **once**. Point a volume at the cert directory to keep it
stable across restarts.

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

All examples assume the admin endpoint `https://localhost:5301` and the connection string:

```
Endpoint=sb://localhost:5301;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=emulator;UseDevelopmentEmulator=true
```

### Node.js — `@azure/service-bus`

Node ignores the OS trust store and only trusts its own bundled CAs plus whatever
`NODE_EXTRA_CA_CERTS` points at. That variable must be set **before** the process starts (Node reads
it once at startup — you cannot change it at runtime).

```bash
export NODE_EXTRA_CA_CERTS=/path/to/certs/emulator-ca.crt
node app.js
```

```js
// app.js — requires @azure/service-bus 7.10.0-beta.2+ (earlier versions force HTTPS with no
// emulator awareness). UseDevelopmentEmulator=true makes the client accept the emulator endpoint.
const { ServiceBusAdministrationClient } = require("@azure/service-bus");

const admin = new ServiceBusAdministrationClient(
  "Endpoint=sb://localhost:5301;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=emulator;UseDevelopmentEmulator=true");

await admin.createQueue("my-queue");
```

Alternatively, append the CA to Node's built-in set programmatically before creating the client:

```js
const fs = require("node:fs");
const tls = require("node:tls");
// Extends the default roots for the whole process; do this once at startup.
tls.rootCertificates = [...tls.rootCertificates, fs.readFileSync("/path/to/certs/emulator-ca.crt", "utf8")];
```

### Python — `azure-servicebus`

The admin client accepts a CA bundle directly via `connection_verify`:

```python
from azure.servicebus.management import ServiceBusAdministrationClient

admin = ServiceBusAdministrationClient.from_connection_string(
    "Endpoint=sb://localhost:5301;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=emulator;UseDevelopmentEmulator=true",
    connection_verify="/path/to/certs/emulator-ca.crt",
)

admin.create_queue("my-queue")
```

Or point the underlying HTTP stack at the CA with an environment variable (no code change):

```bash
export SSL_CERT_FILE=/path/to/certs/emulator-ca.crt
# some setups honour REQUESTS_CA_BUNDLE instead / as well:
export REQUESTS_CA_BUNDLE=/path/to/certs/emulator-ca.crt
```

### Java — `azure-messaging-servicebus`

The emulator writes a ready-to-use PKCS#12 truststore. Hand it to the JVM with system properties:

```bash
java \
  -Djavax.net.ssl.trustStore=/path/to/certs/emulator-truststore.p12 \
  -Djavax.net.ssl.trustStoreType=PKCS12 \
  -Djavax.net.ssl.trustStorePassword=changeit \
  -jar app.jar
```

```java
ServiceBusAdministrationClient admin = new ServiceBusAdministrationClientBuilder()
    .connectionString("Endpoint=sb://localhost:5301;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=emulator;UseDevelopmentEmulator=true")
    .buildClient();

admin.createQueue("my-queue");
```

Setting `javax.net.ssl.trustStore` **replaces** the JVM's default trust store, so your app will no
longer trust public CAs. If the same process also calls other TLS services, import the CA into the
JDK's own trust store instead of overriding it:

```bash
keytool -importcert -noprompt \
  -alias almostservicebus \
  -file /path/to/certs/emulator-ca.crt \
  -keystore "$JAVA_HOME/lib/security/cacerts" \
  -storepass changeit
```

### curl / .NET

```bash
curl --cacert /path/to/certs/emulator-ca.crt \
  "https://localhost:5301/my-queue?api-version=2021-05"
```

.NET needs no TLS setup — the `ServiceBusAdministrationClient` uses the plain-HTTP admin port 5300
when `UseDevelopmentEmulator=true`. Only trust the CA if you deliberately point .NET at 5301.

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
almost-servicebus --AdminTlsCertPath ./cert.pem --AdminTlsKeyPath ./key.pem
```

…or as a **PKCS#12/PFX bundle** (also bundle the issuing CA in the PFX if the cert is CA-signed, so
the emitted trust material contains the full chain):

```bash
openssl pkcs12 -export -out cert.pfx -inkey key.pem -in cert.pem -passout pass:secret
almost-servicebus --AdminTlsCertPath ./cert.pfx --AdminTlsCertPassword secret
```

### Certificate options

| Option (CLI arg / env var) | Purpose |
|----------------------------|---------|
| `--AdminTlsPort` / `AdminTlsPort` | HTTPS admin port (default `5301`; `0` disables it). |
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
  -e AdminTlsCertBase64="$CERT_B64" \
  -e AdminTlsCertPassword=secret \
  almostservicebus:local
```

To keep the auto-generated CA stable, mount a volume at `/certs` and copy the CA out for your
clients:

```bash
docker run --rm -p 5301:5301 -v asb-certs:/certs almostservicebus:local
docker run --rm -v asb-certs:/certs -w /certs busybox cat emulator-ca.crt > emulator-ca.crt
```

See [`../docker/README.md`](../docker/README.md) for the full Docker configuration.

## Disabling TLS

If you only use .NET (or create entities over the REST API on 5300 directly), turn the HTTPS admin
endpoint off entirely:

```bash
almost-servicebus --AdminTlsPort 0
```
