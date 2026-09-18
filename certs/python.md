# Python admin client over TLS — `azure-servicebus`

Trusting the emulator's HTTPS admin endpoint (port **5301**) from the Python SDK.

> Enable the endpoint first with `--AdminTlsEnabled true` (it is off by default). See
> [README.md](README.md) for what the emulator generates and where.

All examples assume the admin endpoint `https://localhost:5301` and the connection string:

```
Endpoint=sb://localhost:5301;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=emulator;UseDevelopmentEmulator=true
```

## Trust the CA

The admin client accepts a CA bundle directly via `connection_verify`:

```python
from azure.servicebus.management import ServiceBusAdministrationClient

admin = ServiceBusAdministrationClient.from_connection_string(
    "Endpoint=sb://localhost:5301;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=emulator;UseDevelopmentEmulator=true",
    connection_verify="/path/to/certs/emulator-ca.crt",
)

admin.create_queue("my-queue")
```

Or point the underlying HTTP stack at the CA with an environment variable (no code change). The
`requests` transport honours `REQUESTS_CA_BUNDLE`:

```bash
export REQUESTS_CA_BUNDLE=/path/to/certs/emulator-ca.crt
# some setups honour SSL_CERT_FILE instead / as well:
export SSL_CERT_FILE=/path/to/certs/emulator-ca.crt
```

## Troubleshooting

- **`CERTIFICATE_VERIFY_FAILED: unable to get local issuer certificate`** — the CA isn't being
  trusted. Prefer `REQUESTS_CA_BUNDLE` (the transport reads it reliably); `SSL_CERT_FILE` and the
  `connection_verify` kwarg alone are not always sufficient depending on the installed transport.
- Make sure the CA file matches the running emulator's cert directory (a stale `emulator-ca.crt`
  from a previous run won't verify a freshly generated server cert).
