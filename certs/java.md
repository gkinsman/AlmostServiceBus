# Java admin client over TLS — `azure-messaging-servicebus`

Trusting the emulator's HTTPS admin endpoint from the Java SDK.

> Enable the endpoint first with `--AdminTlsEnabled true` (it is off by default). See
> [README.md](README.md) for what the emulator generates and where.

## ⚠️ The Java SDK admin client always dials port 443

Unlike Node.js and Python, the Java `ServiceBusAdministrationClientBuilder` **strips the port** from
the endpoint and always connects to the namespace host on **port 443**, ignoring both the
connection-string port and an explicit `.endpoint(...)` override. So the emulator's default admin
port (`5301`) is unreachable from the Java admin client.

To use the Java SDK admin client you must therefore **bind the emulator's HTTPS admin endpoint on
port 443** and use a connection string **without a custom port**:

```bash
# Port 443 is privileged — run with the necessary privileges (or map 443→5301 with a proxy).
almost-servicebus --AdminTlsEnabled true --AdminTlsPort 443
```

```
Endpoint=sb://localhost;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=emulator;UseDevelopmentEmulator=true
```

If you cannot bind 443, the SDK admin client is unusable; drive the Atom REST API directly over
`https://localhost:5301` with `java.net.http` instead. The data plane (send/receive on 5672) works
with the SDK regardless. The smoke test
[`../tests/client-sdk-smoke/java/.../AdminSmoke.java`](../tests/client-sdk-smoke/java/src/main/java/io/almostservicebus/smoke/AdminSmoke.java)
uses the SDK admin client against port 443.

## Trust the CA

The emulator writes a ready-to-use PKCS#12 truststore. Hand it to the JVM with system properties:

```bash
java \
  -Djavax.net.ssl.trustStore=/path/to/certs/emulator-truststore.p12 \
  -Djavax.net.ssl.trustStoreType=PKCS12 \
  -Djavax.net.ssl.trustStorePassword=changeit \
  -jar app.jar
```

```java
// Requires the emulator's HTTPS admin endpoint on port 443 (see the warning above), so the
// connection string endpoint has no port and the SDK's implicit :443 lands on the emulator.
ServiceBusAdministrationClient admin = new ServiceBusAdministrationClientBuilder()
    .connectionString("Endpoint=sb://localhost;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=emulator;UseDevelopmentEmulator=true")
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
