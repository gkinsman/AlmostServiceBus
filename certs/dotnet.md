# .NET admin client — `Azure.Messaging.ServiceBus`

.NET needs **no TLS setup**. When `UseDevelopmentEmulator=true`, the
`ServiceBusAdministrationClient` talks to the plain-HTTP admin port **5300** — no certificates, no
trust configuration.

```csharp
var admin = new ServiceBusAdministrationClient(
    "Endpoint=sb://localhost:5300;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=emulator;UseDevelopmentEmulator=true");

await admin.CreateQueueAsync("my-queue");
```

You do **not** need to enable the HTTPS admin endpoint (`--AdminTlsEnabled`) for .NET.

## Only if you deliberately point .NET at the HTTPS endpoint (5301)

If you enable TLS (`--AdminTlsEnabled true`) and target `https://localhost:5301` on purpose, trust
the CA the way the platform expects:

- **curl** — pass the CA per request:

  ```bash
  curl --cacert /path/to/certs/emulator-ca.crt \
    "https://localhost:5301/my-queue?api-version=2021-05"
  ```

- **.NET on Linux** — the runtime uses the OpenSSL trust store; install `emulator-ca.crt` into the
  system CA store (for example `/usr/local/share/ca-certificates/` + `update-ca-certificates` on
  Debian/Ubuntu), or configure a custom `HttpClient` handler that trusts it.
