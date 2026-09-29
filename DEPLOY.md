# Deploying to Cloudflare

Two builds share one payload contract and one OpenAPI spec:

| | Local Node server | Cloudflare Worker |
|---|---|---|
| Run | `npm start` | `npx wrangler deploy` |
| Entry | `server.js` (Express + `ws`) | `worker/src/index.js` (router + Durable Object) |
| State | in-process `latest` + `Set` of sockets | one Durable Object named `thermal-relay` |
| Telemetry push | `ws://host:8080/ws/device` or `POST /api/telemetry` | `wss://<sub>.workers.dev/ws/device` or `POST /api/telemetry` |
| Subscribe | `ws://host:8080/ws/client`, SSE, or polling | `wss://<sub>.workers.dev/ws/client` or polling |
| Docs | `http://localhost:8080/api-docs` | `https://<sub>.workers.dev/api-docs` |
| SSE | supported | **501** (a long-lived response pins a live isolate) |

`shared/telemetry.js` and `shared/openapi.js` are imported by both, so normalization and
docs can never drift between them.

## Why a Durable Object

Workers are ephemeral and share no memory between isolates. The device socket and every
subscriber socket can land in different isolates, so an in-memory `Set` cannot fan out and
a `latest` variable disappears between requests. Routing everything to one DO instance
gives a single point of coordination: it owns the sockets and persists the last reading to
SQLite-backed storage. Sockets are accepted with `ctx.acceptWebSocket()`, so when the object
is idle it hibernates and idle connections cost nothing; it wakes on the next reading.

## Deploy

```bash
npx wrangler login          # opens a browser, one-time
npx wrangler deploy
```

You get `https://thermal-relay.<your-subdomain>.workers.dev`. Verify:

```bash
curl https://thermal-relay.<sub>.workers.dev/api/health
npx wscat -c wss://thermal-relay.<sub>.workers.dev/ws/client
npx wscat -c wss://thermal-relay.<sub>.workers.dev/ws/device
```

Then push a reading and watch it appear in the client tab:

```bash
curl -X POST https://thermal-relay.<sub>.workers.dev/api/telemetry \
  -H "Content-Type: application/json" \
  -d '{"ts":1790678801340,"thermal":{"max":68.4},"targets":[{"label":"Heater","value":68.4}]}'
```

Iterate with `npm run cf:dev` (local workerd on :8787, no account needed) and
`npx wrangler tail` for live logs from the deployed Worker and its Durable Object.

## Point the device and frontend at it

ESP32-S3: `ws://<ip>:8080` becomes `wss://thermal-relay.<sub>.workers.dev/ws/device`.
The device sends the same JSON it always did:

```json
{"ts": 1790678801340, "thermal": {"max": 68.4}, "targets": [{"label": "Heater", "value": 68.4}]}
```

Frontend:

```js
const ws = new WebSocket('wss://thermal-relay.<sub>.workers.dev/ws/client');
ws.onmessage = (e) => render(JSON.parse(e.data));
```

Notes:
- Use `wss://` from an HTTPS page, or `location.origin` and swap the scheme, otherwise the
  browser blocks it as mixed content.
- A bare upgrade to `/` is still accepted as the device endpoint, so firmware hardcoded to
  the host root keeps working.
- The stored reading is replayed to each subscriber the moment it connects, so a page load
  renders the current temperature without waiting for the next sample.

## Free plan limits that matter here

| Limit | Free | Effect on this app |
|---|---|---|
| Requests/day | 100,000 | Each device push is one Durable Object request. At 1 Hz that's ~86k/day. Post at 1 Hz max, or batch. |
| CPU/request | 10 ms (Workers) / 30 s (DO) | Normalizing and forwarding a reading is microseconds. |
| Storage/account | 5 GB | Only one reading is stored. |
| DO classes | 100 | One is used. |
| Custom domains | none | `workers.dev` hostname only. |

WebSocket connections are not billed as requests while hibernating, but every message is
work. Nothing here needs the $5 plan unless you raise the sample rate.

## Before this faces the public internet

- `access-control-allow-origin: *` on every response, in both builds. Lock it to your
  frontend origin.
- No authentication: anyone can `POST /api/telemetry` or open a subscriber socket. Add a
  shared token checked in `worker/src/index.js` before routing.
- The Swagger page pulls Swagger UI from unpkg, so `workers.dev` needs no extra setup. If
  you later add a custom domain and want offline docs, vendor the assets.
- The DO name is fixed (`idFromName('thermal-relay')`), so every reader and writer shares
  one room. If you want per-device isolation, change `ROOM_NAME` to a name derived from a
  query param or header.
