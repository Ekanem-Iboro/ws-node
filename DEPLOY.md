# Deploying to Cloudflare

Live URL: **https://ws-node.sireemmy12.workers.dev** (Worker name `ws-node`, set in
`wrangler.jsonc`. The host string lives in `shared/config.js` as `BASE_URL` — change it there
and the OpenAPI spec, the local banner, and the Worker info endpoint all follow.)

Two builds share one payload contract and one OpenAPI spec:

| | Local Node server | Cloudflare Worker |
|---|---|---|
| Run | `npm start` | `npx wrangler deploy` |
| Entry | `server.js` (Express + `ws`) | `worker/src/index.js` (router + Durable Object) |
| State | in-process `latest` + `Set` of sockets | one Durable Object named `thermal-relay` |
| Telemetry push | `ws://localhost:8080/ws/device`, `ws://localhost:8080/ws/client`, or `POST /api/telemetry` | same, on `wss://ws-node.sireemmy12.workers.dev` |
| Subscribe | `ws://localhost:8080/ws/updates`, SSE, or polling | `wss://ws-node.sireemmy12.workers.dev/ws/updates` or polling |
| Docs | `http://localhost:8080/api-docs` | `https://ws-node.sireemmy12.workers.dev/api-docs` |
| SSE | supported | **501** (a long-lived response pins a live isolate) |

`shared/telemetry.mjs`, `shared/openapi.mjs`, and `shared/config.mjs` are imported by both, so
normalization, docs, and URLs cannot drift between them.

## Why a Durable Object

Workers are ephemeral and share no memory between isolates. The device socket and every
subscriber socket can land in different isolates, so an in-memory `Set` cannot fan out and a
`latest` variable disappears between requests. Routing everything to one DO instance gives a
single point of coordination: it owns the sockets and persists the last reading to
SQLite-backed storage. Sockets are accepted with `ctx.acceptWebSocket()`, so when the object
is idle it hibernates and idle connections cost nothing; it wakes on the next reading.

## Deploy

```bash
npx wrangler login          # opens a browser, one-time
npx wrangler deploy         # publishes to https://ws-node.sireemmy12.workers.dev
```

Verify:

```bash
curl https://ws-node.sireemmy12.workers.dev/api/health
npx wscat -c wss://ws-node.sireemmy12.workers.dev/ws/updates   # observer
npx wscat -c wss://ws-node.sireemmy12.workers.dev/ws/device    # publisher
```

Then push a reading and watch it appear in the observer tab:

```bash
curl -X POST https://ws-node.sireemmy12.workers.dev/api/telemetry \
  -H "Content-Type: application/json" \
  -d '{"ts":1790678801340,"thermal":{"max":68.4},"targets":[{"label":"Heater","value":68.4}]}'
```

Iterate with `npm run cf:dev` (local workerd on :8787, no account needed) and
`npx wrangler tail` for live logs from the deployed Worker and its Durable Object.

## Point the device and frontend at it

ESP32-S3: `ws://<ip>:8080` becomes `wss://ws-node.sireemmy12.workers.dev/ws/device`. The
device sends the same JSON it always did:

```json
{"ts": 1790678801340, "thermal": {"max": 68.4}, "targets": [{"label": "Heater", "value": 68.4}]}
```

Frontend — two sockets, because publishing and observing are separate:

```js
// observe: read-only feed of every accepted reading
const feed = new WebSocket('wss://ws-node.sireemmy12.workers.dev/ws/updates');
feed.onmessage = (e) => render(JSON.parse(e.data));

// publish: payloads are stored as the latest reading and fanned out to every observer
const pub = new WebSocket('wss://ws-node.sireemmy12.workers.dev/ws/client');
pub.onopen = () =>
  pub.send(JSON.stringify({ thermal: { max: 68.4 }, targets: [{ label: 'Heater', value: 68.4 }] }));
```

## Which socket to use

| Path | Who | Publish | Observe | Notes |
|---|---|---|---|---|
| `/ws/updates` | Anything displaying data | no | **yes** | **Use this one to display.** Read-only. |
| `/ws/client` | Browser publisher | yes | no | Receives nothing back, not even its own pushes. |
| `/ws/device` | ESP32-S3 | yes | no | Exclusive slot: a second device closes the first with code 4000. |
| `/` or `/ws/relay` | Anyone | yes | yes | The one exception. Kept for the original `new WebSocket('ws://host:8080')`. Never evicts the sensor. |

A socket either publishes or observes, never both. That is what stops a publisher from
seeing an echo of its own message. A message from any publisher is handled identically,
which is what the original `wss.on('message')` script did: parse JSON, normalize, store as
latest, fan out to every observer.

Because there is no distinction between a reading from the ESP32 and a push from a browser,
**any connected publisher can overwrite the stored value.** An observer cannot: anything
sent on `/ws/updates` is refused, and the socket is closed with code 4003.

Notes:
- Use `wss://` from an HTTPS page, or `location.origin` and swap the scheme, otherwise the
  browser blocks it as mixed content.
- A bare upgrade to `/` is still accepted as the device endpoint, so firmware hardcoded to
  the host root keeps working.
- The stored reading is replayed to each subscriber the moment it connects, so a page load
  renders the current temperature without waiting for the next sample.
- Nothing to configure on the `workers.dev` host: TLS is automatic and ports are 443 only.

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
- No authentication, and anyone can open a publisher socket: a stranger can inject fake
  temperatures into your display, and anyone connecting to `/ws/device` displaces your real
  sensor. Observers are harmless, but they are also unauthenticated. A shared token checked in
  `worker/src/index.js` before routing is the fix.
- The Swagger page pulls Swagger UI from unpkg, so `workers.dev` needs no extra setup. If
  you later add a custom domain and want offline docs, vendor the assets.
- The DO name is fixed (`idFromName('thermal-relay')`), so every reader and writer shares
  one room. If you want per-device isolation, derive `ROOM_NAME` in `worker/src/index.js`
  from a query param or header instead.
