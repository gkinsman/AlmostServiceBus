# Dashboard UI — Design Spec

## Goal

A browser-based diagnostic dashboard for the Azure Service Bus Emulator. Lets developers inspect namespaces, browse queues/topics/subscriptions, watch messages flow in real time, peek at message bodies, and view dead-letter queues. Read-only observation with minimal admin actions (purge).

## Motivation

The emulator has no visibility into what's happening inside it. When debugging MassTransit topology or message routing, developers have to rely on app-side logging. A dashboard provides immediate visual feedback — what entities exist, what messages are flowing, what's stuck in dead-letter queues.

---

## Architecture

### Tech Stack

- **Frontend**: Vue 3 + TypeScript, served via Vite
- **Backend API**: JSON REST endpoints on the existing Kestrel HTTP server (alongside the Atom XML management API)
- **Real-time**: WebSocket connection from the Vue app to the emulator for live message events
- **Dev integration**: Vite.AspNetCore for HMR during development, static file serving in production

### How It Fits

The Vue app is served from the same Kestrel instance that handles the management API. In development, Vite.AspNetCore proxies to the Vite dev server for HMR. In production/release, the built Vue assets are embedded as static files.

The dashboard API is a separate set of JSON endpoints (prefixed `/api/dashboard/`) that read from the in-memory broker state. The WebSocket endpoint (`/ws/messages`) pushes message events as they flow through the broker.

---

## Layout

Three-panel layout:

### Left Panel — Entity Tree (300px)

- **Namespace tabs** at the top: one tab per namespace (default, app1, app2, etc.). Tabs appear dynamically as namespaces are created.
- **Search/filter** box below tabs.
- **Queues section**: flat list of queues. Dead-letter sub-queues nested under their parent with a red badge showing count.
- **Topics section**: topics grouped by common namespace prefix. For example, `MyApp.Messages.Domain.Orders/Events-OrderPlaced` and `MyApp.Messages.Domain.Orders/Events-OrderShipped` group under the collapsible heading `MyApp.Messages.Domain.Orders`. Subscriptions shown as children under each topic.
- **Footer**: entity counts (N queues, N topics, N subscriptions).

### Middle Panel — Message List (flexible)

Shows messages for the selected entity.

- **Entity header**: entity name, parent namespace path, entity type badge, Purge button.
- **Stats bar**: active message count, dead-letter count, consumer count.
- **Tab bar**: Messages | Dead Letter | Properties.
- **Live indicator**: green dot when WebSocket is connected.
- **Message rows**: each row shows message ID, relative timestamp ("2s ago"), and **scalar property tags** — top-level scalar values from the message body extracted and displayed as colored pills (e.g. `name: "George"`, `email: "user@example.com"`). This gives immediate visibility without opening the detail panel.

### Right Panel — Message Detail (flexible)

Opens when a message row is clicked.

- **Header**: message ID, Complete and Dead-letter action buttons.
- **Metadata grid**: Message ID, Enqueue Time, Content Type, Correlation ID, Delivery Count, Sequence Number.
- **Tab bar**: Body | App Properties | System Properties.
- **Body tab**: syntax-highlighted JSON viewer with the full message body.
- **App Properties tab**: key-value table of AMQP application properties.
- **System Properties tab**: key-value table of system properties (delivery count, enqueue time, sequence number, etc.).

---

## Dashboard API

JSON endpoints under `/api/dashboard/`. These are separate from the Atom XML management API — they're for the dashboard only.

### Endpoints

| Method | Path | Returns |
|--------|------|---------|
| GET | `/api/dashboard/namespaces` | List of namespace names |
| GET | `/api/dashboard/namespaces/{ns}/entities` | All queues, topics, subscriptions for a namespace with message counts |
| GET | `/api/dashboard/namespaces/{ns}/queues/{name}/messages` | Peek messages in a queue (non-destructive) |
| GET | `/api/dashboard/namespaces/{ns}/queues/{name}/deadletter` | Peek dead-letter messages |
| GET | `/api/dashboard/namespaces/{ns}/topics/{name}/subscriptions/{sub}/messages` | Peek subscription messages |
| DELETE | `/api/dashboard/namespaces/{ns}/queues/{name}/messages` | Purge queue |
| DELETE | `/api/dashboard/namespaces/{ns}/queues/{name}/deadletter` | Purge dead-letter queue |

### WebSocket

`/ws/messages` — after connecting, the client sends a JSON subscription message:

```json
{ "namespace": "default", "entity": "send-installer-login-email" }
```

The server pushes events as they happen:

```json
{ "type": "enqueue", "entity": "send-installer-login-email", "messageId": "...", "body": {...}, "timestamp": "..." }
{ "type": "complete", "entity": "send-installer-login-email", "messageId": "...", "timestamp": "..." }
{ "type": "deadletter", "entity": "send-installer-login-email", "messageId": "...", "timestamp": "..." }
```

The client can change subscription by sending a new subscription message.

---

## Vue App Structure

```
src/AlmostServiceBus.Dashboard/
├── index.html
├── vite.config.ts
├── package.json
├── tsconfig.json
├── src/
│   ├── App.vue                    # Three-panel layout shell
│   ├── main.ts                    # Entry point
│   ├── api/
│   │   ├── client.ts              # HTTP client for dashboard API
│   │   └── websocket.ts           # WebSocket connection manager
│   ├── components/
│   │   ├── EntityTree.vue         # Left panel: namespace tabs + tree
│   │   ├── NamespaceTabs.vue      # Tab bar for namespaces
│   │   ├── EntityGroup.vue        # Collapsible namespace group
│   │   ├── MessageList.vue        # Middle panel: message rows + stats
│   │   ├── MessageRow.vue         # Single message with scalar tags
│   │   ├── MessageDetail.vue      # Right panel: full message view
│   │   ├── JsonViewer.vue         # Syntax-highlighted JSON
│   │   └── StatsBar.vue           # Active/Dead/Consumers counters
│   ├── composables/
│   │   ├── useNamespaces.ts       # Namespace list + selection
│   │   ├── useEntities.ts         # Entity tree data + grouping logic
│   │   ├── useMessages.ts         # Message list + WebSocket integration
│   │   └── useMessageDetail.ts    # Selected message state
│   └── types/
│       └── index.ts               # TypeScript interfaces
```

### Entity Grouping Logic

Topics are grouped by splitting the entity name on `/` — the part before the slash is the namespace prefix, the part after is the event/command name. Topics sharing the same prefix are grouped under a collapsible heading. Single topics under a prefix are shown inline (no group needed).

### Scalar Tag Extraction

When displaying a message row, parse the body JSON. If the body has a `message` property (MassTransit envelope), use that as the source. Extract all top-level properties whose values are scalars (string, number, boolean) and display them as colored pills. Skip objects, arrays, and null values. Limit to 4-5 tags to avoid overflow.

---

## Integration with Host

### Development (Vite.AspNetCore)

```csharp
// Program.cs
if (app.Environment.IsDevelopment())
{
    app.UseViteDevelopmentServer(); // Proxies to Vite dev server with HMR
}
app.UseStaticFiles(); // Serves built Vue assets
app.MapDashboardApi(registry); // JSON API endpoints
app.MapDashboardWebSocket(registry); // WebSocket endpoint
```

### Production

The Vue app is built to `wwwroot/` and served as static files. No Vite dependency at runtime.

---

## Scope

### In Scope

- Three-panel dashboard layout (entity tree, message list, message detail)
- Namespace tabs with dynamic discovery
- Topic grouping by common namespace prefix
- Queue/topic/subscription browsing
- Message peek (non-destructive read)
- Scalar tag extraction for message row previews
- Syntax-highlighted JSON body viewer
- Message metadata display (headers, app properties, system properties)
- Dead-letter queue viewing
- Purge queue/dead-letter actions
- Real-time message flow via WebSocket
- Live indicator
- Vite.AspNetCore integration for development HMR
- Dark theme (Catppuccin Mocha-inspired, matching the mockup)

### Out of Scope

- Send test messages from the UI (the app does that)
- Message editing or replay
- Entity creation/deletion from the UI (use the SDK/MassTransit)
- Authentication/authorization on the dashboard
- Light theme
- Persistent message history (messages are in-memory only)
- Performance graphs or time-series charts
