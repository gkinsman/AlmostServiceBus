# Node.js admin client over TLS — `@azure/service-bus`

Trusting the emulator's HTTPS admin endpoint (port **5301**) from the Node.js SDK.

> Enable the endpoint first with `--AdminTlsEnabled true` (it is off by default). See
> [README.md](README.md) for what the emulator generates and where.

All examples assume the admin endpoint `https://localhost:5301` and the connection string:

```
Endpoint=sb://localhost:5301;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=emulator;UseDevelopmentEmulator=true
```

## Trust the CA

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

## Troubleshooting

- **`UNABLE_TO_VERIFY_LEAF_SIGNATURE`** — `NODE_EXTRA_CA_CERTS` isn't reaching the process, or points
  at the wrong file. Verify it is set in the exact shell that launches node, points at the **CA**
  (`emulator-ca.crt`, not the server cert), and that the file matches the running emulator's cert
  directory. In a monorepo runner (turbo/nx), the var may be filtered out unless you declare it as a
  pass-through env for the task.
- Never use `NODE_TLS_REJECT_UNAUTHORIZED=0` — it disables TLS verification process-wide.
