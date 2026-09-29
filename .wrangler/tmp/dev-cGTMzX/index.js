var __defProp = Object.defineProperty;
var __name = (target, value) => __defProp(target, "name", { value, configurable: true });

// worker/src/room.js
import { DurableObject } from "cloudflare:workers";

// shared/telemetry.mjs
function toNumberOrNull(v) {
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}
__name(toNumberOrNull, "toNumberOrNull");
function normalize(raw) {
  const ts = Number(raw?.ts);
  return {
    ts: Number.isFinite(ts) ? ts : Date.now(),
    thermal: { max: toNumberOrNull(raw?.thermal?.max) },
    targets: Array.isArray(raw?.targets) ? raw.targets : []
  };
}
__name(normalize, "normalize");
function formatTargets(targets) {
  if (!Array.isArray(targets) || targets.length === 0) return "none";
  return targets.map((t) => `${t.label ?? t.name ?? "target"} ${t.value ?? t.temp ?? "?"}degC`).join(", ");
}
__name(formatTargets, "formatTargets");
function describe(p) {
  return `[${p.ts}ms] Thermal Max: ${p.thermal.max ?? "?"}degC | Targets: ${formatTargets(p.targets)}`;
}
__name(describe, "describe");

// worker/src/room.js
var LAST_KEY = "telemetry:latest";
var ROLES = {
  device: { receivesBroadcast: false },
  client: { receivesBroadcast: true },
  relay: { receivesBroadcast: true }
};
var Room = class extends DurableObject {
  static {
    __name(this, "Room");
  }
  constructor(ctx, env) {
    super(ctx, env);
    this.ctx = ctx;
    this.ctx.blockConcurrencyWhile(async () => {
      const stored = await this.ctx.storage.get(LAST_KEY);
      this.latest = stored?.data ?? null;
      this.latestAt = stored?.at ?? null;
    });
  }
  async fetch(request) {
    const url = new URL(request.url);
    switch (url.pathname) {
      case "/ws/device":
        return this.acceptSocket(request, "device");
      case "/ws/client":
        return this.acceptSocket(request, "client");
      case "/ws/relay":
        return this.acceptSocket(request, "relay");
      case "/ingest":
        return request.method === "POST" ? this.ingestViaHttp(request) : json({ error: "Method not allowed" }, 405);
      case "/latest":
        return json({ data: this.latest, latestAt: this.latestAt });
      case "/health":
        return json({
          status: "ok",
          runtime: "cloudflare-worker",
          deviceConnected: this.ctx.getWebSockets("device").length > 0,
          subscribers: {
            websocket: this.subscriberCount(),
            device: this.ctx.getWebSockets("device").length,
            sse: 0
          },
          latestAt: this.latestAt
        });
      default:
        return json({ error: "Not found" }, 404);
    }
  }
  // --- sockets ------------------------------------------------------------
  acceptSocket(request, role) {
    if ((request.headers.get("Upgrade") || "").toLowerCase() !== "websocket") {
      return json(
        {
          error: "This endpoint requires a WebSocket upgrade",
          hint: 'Connect a WebSocket to this path, e.g. new WebSocket("wss://<host>/ws/client"). A plain HTTP GET cannot join it.'
        },
        426
      );
    }
    if (role === "device") {
      for (const ws of this.ctx.getWebSockets("device")) {
        ws.close(4e3, "replaced by a new device connection");
      }
    }
    const [client, server] = Object.values(new WebSocketPair());
    this.ctx.acceptWebSocket(server, [role]);
    server.serializeAttachment({ role, connectedAt: Date.now() });
    console.log(`${LABELS[role]} connected.`);
    if (ROLES[role].receivesBroadcast && this.latest) server.send(JSON.stringify(this.latest));
    return new Response(null, { status: 101, webSocket: client });
  }
  // --- ingest -------------------------------------------------------------
  async ingestViaHttp(request) {
    let raw;
    try {
      raw = await request.json();
    } catch {
      return json({ error: "Body must be valid JSON" }, 400);
    }
    if (!raw || typeof raw !== "object" || Array.isArray(raw)) {
      return json({ error: "Body must be a telemetry JSON object" }, 400);
    }
    const { payload } = await this.ingest(raw);
    return json({ accepted: true, ts: payload.ts }, 202);
  }
  // Every socket that receives the broadcast: the frontends, not the sensor (so the ESP32
  // never sees an echo of its own reading).
  broadcastTargets() {
    return [
      ...this.ctx.getWebSockets("client"),
      ...this.ctx.getWebSockets("relay")
    ];
  }
  subscriberCount() {
    return this.broadcastTargets().length;
  }
  // The single fan-out point: every accepted reading, from any socket or from HTTP, ends here.
  async ingest(raw, source = "unknown") {
    const payload = normalize(raw);
    this.latest = payload;
    this.latestAt = Date.now();
    await this.ctx.storage.put(LAST_KEY, { data: payload, at: this.latestAt });
    console.log(`${describe(payload)} (via ${source})`);
    const msg = JSON.stringify(payload);
    let notified = 0;
    for (const ws of this.broadcastTargets()) {
      try {
        ws.send(msg);
        notified++;
      } catch {
      }
    }
    return { payload, notified };
  }
  // --- hibernation event handlers ----------------------------------------
  // Push path. Any role may send: the ESP32 and the frontend are treated identically,
  // which is what the original `wss.on('message')` script did.
  async webSocketMessage(ws, message) {
    const { role } = ws.deserializeAttachment() ?? {};
    const raw = typeof message === "string" ? message : new TextDecoder().decode(message);
    let parsed;
    try {
      parsed = JSON.parse(raw);
    } catch {
      console.error(`Invalid JSON from ${LABELS[role] ?? "socket"}:`, raw.slice(0, 200));
      return;
    }
    if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
      console.error(`Non-object payload from ${LABELS[role] ?? "socket"}:`, raw.slice(0, 200));
      return;
    }
    await this.ingest(parsed, `${LABELS[role] ?? "socket"} socket`);
  }
  async webSocketClose(ws, code) {
    const { role } = ws.deserializeAttachment() ?? {};
    if (code >= 1e3 && code <= 4999 && code !== 1005 && code !== 1006) {
      ws.close(code, "closing");
    }
    if (ROLES[role]?.receivesBroadcast) {
      console.log(`${LABELS[role]} disconnected (${Math.max(0, this.subscriberCount() - 1)} total).`);
    } else {
      console.log(`${LABELS[role] ?? "Socket"} disconnected.`);
    }
  }
  async webSocketError(ws, error) {
    const { role } = ws.deserializeAttachment() ?? {};
    console.error(`${LABELS[role] ?? "Socket"} error:`, error?.message ?? error);
  }
};
var LABELS = {
  device: "ESP32-S3",
  client: "Frontend",
  relay: "Relay client"
};
function json(body, status = 200) {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "content-type": "application/json; charset=utf-8" }
  });
}
__name(json, "json");

// shared/config.mjs
var BASE_URL = "https://ws-node.sireemmy12.workers.dev";
var WS_PATHS = {
  device: "/ws/device",
  // ESP32-S3 pushes telemetry here
  client: "/ws/client",
  // frontend subscribes and pushes here
  relay: "/ws/relay"
  // same, under an explicit name
};
var deviceWsUrl = /* @__PURE__ */ __name((base = BASE_URL) => `${base.replace(/^http/, "ws")}${WS_PATHS.device}`, "deviceWsUrl");
var clientWsUrl = /* @__PURE__ */ __name((base = BASE_URL) => `${base.replace(/^http/, "ws")}${WS_PATHS.client}`, "clientWsUrl");

// shared/openapi.mjs
function wsOperation({ summary, url, role, sends, receives, example, tags = ["websocket"], notes = [] }) {
  return {
    get: {
      tags,
      summary,
      description: [
        `**Full URL:** \`${url}\``,
        "",
        role,
        "",
        `**Send:** ${sends}`,
        "",
        `**Receive:** ${receives}`,
        "",
        "```js",
        example,
        "```",
        "",
        ...notes.flatMap((n) => [`- ${n}`, ""]),
        'This is a WebSocket upgrade, so "Try it out" does not apply: a plain GET returns `426 Upgrade Required`.'
      ].join("\n"),
      responses: {
        101: { description: "Switching Protocols - the socket is open" },
        426: { description: "Upgrade Required - this endpoint only speaks WebSocket" }
      }
    }
  };
}
__name(wsOperation, "wsOperation");
var openapi_default = {
  openapi: "3.0.3",
  info: {
    title: "Thermal Relay API",
    version: "1.0.0",
    description: [
      "Relay between an ESP32-S3 thermal sensor and a web frontend.",
      "",
      "### The frontend pushes too",
      "`/ws/client` is bidirectional. The browser sends a payload and it is ingested exactly like a reading from the ESP32:",
      "stored as the latest value and broadcast to every other frontend socket. This mirrors the original",
      "`wss.on('message')` script, which never distinguished who sent what.",
      "",
      "```js",
      "const ws = new WebSocket('" + clientWsUrl() + "');",
      "ws.onmessage = (e) => render(JSON.parse(e.data));",
      "ws.send(JSON.stringify({ thermal: { max: 68.4 }, targets: [{ label: 'Heater', value: 68.4 }] }));",
      "```",
      "",
      "### Data flow",
      "1. A reading arrives from the ESP32-S3 (`/ws/device`), from the frontend (`/ws/client`), or from an HTTP POST (`POST /api/telemetry`).",
      "2. The server normalizes it, stores it as the latest reading, and fans it out to every frontend socket and SSE subscriber.",
      "3. Frontends receive them on `/ws/client`, or read the latest value with `GET /api/telemetry` when they cannot hold a connection.",
      "",
      "### Which socket to use",
      "- `/ws/client` - the frontend. Pushes **and** receives. Use this one.",
      "- `/ws/device` - the ESP32-S3. Pushes, does not receive, so the sensor never sees an echo. Exclusive: a new device socket closes the previous one with code 4000.",
      "- `/` - a relay socket, kept for compatibility with the original `new WebSocket('ws://host:8080')`. Pushes and receives, and never evicts the sensor.",
      "",
      "### Hosting notes",
      "The Cloudflare Worker build (`worker/`) routes everything through one Durable Object named `thermal-relay`, which owns the client sockets and the stored reading so fan-out survives multiple isolates. On Workers the SSE endpoint is not available (it would pin a live isolate); use the WebSocket there."
    ].join("\n")
  },
  servers: [
    { url: "http://localhost:8080", description: "Local Node server" },
    { url: "http://localhost:8787", description: "Local wrangler dev" },
    { url: BASE_URL, description: "Cloudflare Workers (deployed)" }
  ],
  tags: [
    { name: "websocket", description: "WebSocket endpoints: the ESP32-S3 and the frontend both push here" },
    { name: "telemetry", description: "Thermal readings pushed by the device and read by the frontend" },
    { name: "health", description: "Server and connection status" }
  ],
  paths: {
    "/api/health": {
      get: {
        tags: ["health"],
        summary: "Server and connection status",
        responses: {
          200: {
            description: "Current status",
            content: {
              "application/json": {
                schema: {
                  type: "object",
                  properties: {
                    status: { type: "string", example: "ok" },
                    deviceConnected: { type: "boolean" },
                    subscribers: {
                      type: "object",
                      properties: {
                        websocket: { type: "integer" },
                        sse: { type: "integer" }
                      }
                    },
                    latestAt: { type: "integer", nullable: true, description: "Epoch ms of last reading" }
                  }
                }
              }
            }
          }
        }
      }
    },
    "/api/telemetry": {
      get: {
        tags: ["telemetry"],
        summary: "Latest reading (polling fallback for the frontend)",
        responses: {
          200: {
            description: "Most recent reading, or null if nothing has arrived yet",
            content: {
              "application/json": {
                schema: {
                  type: "object",
                  properties: {
                    data: { oneOf: [{ $ref: "#/components/schemas/Telemetry" }, { type: "null" }] },
                    latestAt: { type: "integer", nullable: true }
                  }
                }
              }
            }
          }
        }
      },
      post: {
        tags: ["telemetry"],
        summary: "Push a reading (HTTP alternative to the device WebSocket)",
        requestBody: {
          required: true,
          content: { "application/json": { schema: { $ref: "#/components/schemas/TelemetryInput" } } }
        },
        responses: {
          202: {
            description: "Accepted and broadcast to subscribers",
            content: {
              "application/json": {
                schema: {
                  type: "object",
                  properties: { accepted: { type: "boolean" }, ts: { type: "integer" } }
                }
              }
            }
          },
          400: { description: "Body was not a JSON object" }
        }
      }
    },
    "/ws/client": wsOperation({
      summary: "Frontend socket: push and receive telemetry",
      url: clientWsUrl(),
      role: "Browser frontend. Sends a payload to publish it, and receives every accepted reading.",
      sends: "Anything with a `thermal` field is stored as the latest reading and broadcast to all other frontend sockets.",
      receives: "Every accepted reading. The stored reading is replayed on connect.",
      example: "const ws = new WebSocket('" + clientWsUrl() + "');\nws.onmessage = (e) => render(JSON.parse(e.data));\nws.send(JSON.stringify({ thermal: { max: 68.4 }, targets: [{ label: 'Heater', value: 68.4 }] }));",
      tags: ["websocket"]
    }),
    "/ws/device": wsOperation({
      summary: "ESP32-S3 socket: push telemetry",
      url: deviceWsUrl(),
      role: "The sensor. Sends readings; does not receive the broadcast, so it never sees an echo of its own data.",
      sends: 'A telemetry object: { "ts": <ms>, "thermal": { "max": <number> }, "targets": [ { "label": "...", "value": <number> } ] }',
      receives: "Nothing. Device sockets are push-only.",
      example: '// ESP32 / Arduino\nWebSocketClient ws("' + deviceWsUrl() + '");\nws.connect();\nws.println("{\\"ts\\":" + millis() + ",\\"thermal\\":{\\"max\\":68.4}}");',
      tags: ["websocket"],
      notes: [
        "Connecting a second device socket closes the first with code 4000, so the sensor slot stays exclusive.",
        "A bare upgrade to / is accepted as a general relay socket and does not evict the device."
      ]
    }),
    "/api/telemetry/stream": {
      get: {
        tags: ["telemetry"],
        summary: "Live stream of readings (Server-Sent Events)",
        description: "Local Node server only: replays the current latest reading, then a keep-alive comment every 25s. The Cloudflare Worker returns 501 here because a long-lived response pins a live isolate; use the WebSocket instead. Use `EventSource` in the browser.",
        responses: {
          200: {
            description: "text/event-stream",
            content: {
              "text/event-stream": {
                schema: { type: "string", example: 'event: telemetry\ndata: {"ts":1790678801340,"thermal":{"max":68.4},"targets":[]}\n\n' }
              }
            }
          },
          501: { description: "Not available on the Cloudflare Worker build" }
        }
      }
    }
  },
  components: {
    schemas: {
      Target: {
        type: "object",
        properties: {
          label: { type: "string", example: "Heater" },
          value: { type: "number", example: 68.4, description: "Degrees Celsius" }
        }
      },
      TelemetryInput: {
        type: "object",
        properties: {
          ts: { type: "integer", example: 1790678801340, description: "Device timestamp in ms; defaults to server time" },
          thermal: { type: "object", properties: { max: { type: "number", example: 68.4 } } },
          targets: { type: "array", items: { $ref: "#/components/schemas/Target" } }
        }
      },
      Telemetry: {
        allOf: [{ $ref: "#/components/schemas/TelemetryInput" }],
        description: "Normalized form stored by the server and pushed to subscribers."
      }
    }
  },
  // Machine-readable mirror of the websocket entries in `paths`. Swagger UI ignores
  // vendor extensions, which is why those same endpoints are also declared in `paths`.
  "x-websocket": {
    note: "WebSocket endpoints are declared under `paths` as GET operations with a 101 response, because vendor extensions are not rendered by Swagger UI. This block is the machine-readable duplicate.",
    endpoints: {
      "/ws/client": {
        url: clientWsUrl(),
        role: "Frontend",
        bidirectional: true,
        sends: "Telemetry object; stored as latest and broadcast to other frontends.",
        receives: "Every accepted reading; the stored reading is replayed on connect."
      },
      "/ws/device": {
        url: deviceWsUrl(),
        role: "ESP32-S3",
        bidirectional: false,
        sends: "Telemetry object.",
        receives: "Nothing. Exclusive slot: a second device closes the first with code 4000."
      },
      "/": {
        url: BASE_URL.replace(/^http/, "ws") + "/",
        role: "Relay (legacy path from the original script)",
        bidirectional: true,
        sends: "Telemetry object.",
        receives: "Every accepted reading.",
        note: "Never evicts the device socket."
      }
    }
  }
};

// worker/src/index.js
var ROOM_NAME = "thermal-relay";
var DO_ORIGIN = "https://do.internal";
var WS_ROLE_BY_PATH = {
  [WS_PATHS.device]: "/ws/device",
  [WS_PATHS.client]: "/ws/client",
  [WS_PATHS.relay]: "/ws/relay",
  "/": "/ws/relay"
};
var WS_ONLY_PATHS = [WS_PATHS.device, WS_PATHS.client, WS_PATHS.relay];
function wsUrlFor(pathname) {
  const base = BASE_URL.replace(/^http/, "ws");
  return `${base}${pathname === "/" ? "/" : pathname}`;
}
__name(wsUrlFor, "wsUrlFor");
var src_default = {
  // This Worker is a thin router: it does no state work of its own, it forwards
  // everything to the one Durable Object instance that owns the sockets.
  async fetch(request, env) {
    const url = new URL(request.url);
    const { pathname } = url;
    try {
      if (request.method === "OPTIONS") return new Response(null, { status: 204, headers: cors() });
      const isUpgrade = (request.headers.get("Upgrade") || "").toLowerCase() === "websocket";
      if (isUpgrade && WS_ROLE_BY_PATH[pathname]) {
        return roomStub(env).fetch(DO_ORIGIN + WS_ROLE_BY_PATH[pathname], request);
      }
      if (!isUpgrade && WS_ONLY_PATHS.includes(pathname)) {
        return json2(
          {
            error: "This endpoint requires a WebSocket upgrade",
            hint: `Connect a WebSocket to ${url.origin}${pathname} - e.g. new WebSocket("${wsUrlFor(pathname)}"). A plain GET cannot join it.`
          },
          426
        );
      }
      if (pathname === "/api/telemetry" && request.method === "POST") {
        const body = await readJson(request);
        if (body.error) return json2({ error: body.error }, 400);
        const res = await roomStub(env).fetch(DO_ORIGIN + "/ingest", {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify(body.data)
        });
        return withCors(res);
      }
      if (pathname === "/api/telemetry" && request.method === "GET") {
        return withCors(await roomStub(env).fetch(DO_ORIGIN + "/latest"));
      }
      if (pathname === "/api/telemetry/stream") {
        return json2(
          {
            error: "SSE is not available on the Worker build",
            hint: "Use the WebSocket at ws(s)://<host>/ws/client, or poll GET /api/telemetry."
          },
          501
        );
      }
      if (pathname === "/api/health") {
        return withCors(await roomStub(env).fetch(DO_ORIGIN + "/health"));
      }
      if (pathname === "/api-docs.json") return json2(openapi_default);
      if (pathname === "/api-docs" || pathname === "/api-docs/") {
        return new Response(SWAGGER_HTML, {
          headers: { "content-type": "text/html; charset=utf-8", ...cors() }
        });
      }
      if (pathname === "/") {
        return json2({
          name: "thermal-relay (Cloudflare Worker)",
          baseUrl: BASE_URL,
          rest: ["POST /api/telemetry", "GET /api/telemetry", "GET /api/health"],
          websocket: { device: deviceWsUrl(), client: clientWsUrl() },
          sse: "not available on this runtime",
          docs: `${BASE_URL}/api-docs`
        });
      }
      return json2({ error: "Not found" }, 404);
    } catch (err) {
      console.error("Unhandled error:", err?.message ?? err);
      return json2({ error: "Internal error" }, 500);
    }
  }
};
function roomStub(env) {
  return env.THERMAL_ROOM.get(env.THERMAL_ROOM.idFromName(ROOM_NAME));
}
__name(roomStub, "roomStub");
async function readJson(request) {
  let data;
  try {
    data = await request.json();
  } catch {
    return { error: "Body must be valid JSON" };
  }
  if (!data || typeof data !== "object" || Array.isArray(data)) {
    return { error: "Body must be a telemetry JSON object" };
  }
  return { data };
}
__name(readJson, "readJson");
function cors() {
  return {
    "access-control-allow-origin": "*",
    "access-control-allow-methods": "GET,POST,OPTIONS",
    "access-control-allow-headers": "Content-Type"
  };
}
__name(cors, "cors");
function json2(body, status = 200) {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "content-type": "application/json; charset=utf-8", ...cors() }
  });
}
__name(json2, "json");
function withCors(res) {
  const headers = new Headers(res.headers);
  for (const [k, v] of Object.entries(cors())) headers.set(k, v);
  return new Response(res.body, { status: res.status, headers });
}
__name(withCors, "withCors");
var SWAGGER_HTML = `<!doctype html>
<html>
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>Thermal Relay API</title>
    <link rel="stylesheet" href="https://unpkg.com/swagger-ui-dist@5/swagger-ui.css" />
  </head>
  <body>
    <div id="swagger-ui"></div>
    <script src="https://unpkg.com/swagger-ui-dist@5/swagger-ui-bundle.js" crossorigin><\/script>
    <script>
      window.onload = () => SwaggerUIBundle({ url: '/api-docs.json', dom_id: '#swagger-ui' });
    <\/script>
  </body>
</html>`;

// node_modules/wrangler/templates/middleware/middleware-ensure-req-body-drained.ts
var drainBody = /* @__PURE__ */ __name(async (request, env, _ctx, middlewareCtx) => {
  try {
    return await middlewareCtx.next(request, env);
  } finally {
    try {
      if (request.body !== null && !request.bodyUsed) {
        const reader = request.body.getReader();
        while (!(await reader.read()).done) {
        }
      }
    } catch (e) {
      console.error("Failed to drain the unused request body.", e);
    }
  }
}, "drainBody");
var middleware_ensure_req_body_drained_default = drainBody;

// node_modules/wrangler/templates/middleware/middleware-miniflare3-json-error.ts
function reduceError(e) {
  return {
    name: e?.name,
    message: e?.message ?? String(e),
    stack: e?.stack,
    cause: e?.cause === void 0 ? void 0 : reduceError(e.cause)
  };
}
__name(reduceError, "reduceError");
var jsonError = /* @__PURE__ */ __name(async (request, env, _ctx, middlewareCtx) => {
  try {
    return await middlewareCtx.next(request, env);
  } catch (e) {
    const error = reduceError(e);
    const body = JSON.stringify(error);
    const headers = {
      "Content-Type": "application/json",
      "MF-Experimental-Error-Stack": "true"
    };
    const encoded = encodeURIComponent(body);
    if (encoded.length <= 8192) {
      headers["MF-Experimental-Error-Stack-Payload"] = encoded;
    }
    return new Response(body, { status: 500, headers });
  }
}, "jsonError");
var middleware_miniflare3_json_error_default = jsonError;

// .wrangler/tmp/bundle-VTiiZv/middleware-insertion-facade.js
var __INTERNAL_WRANGLER_MIDDLEWARE__ = [
  middleware_ensure_req_body_drained_default,
  middleware_miniflare3_json_error_default
];
var middleware_insertion_facade_default = src_default;

// node_modules/wrangler/templates/middleware/common.ts
var __facade_middleware__ = [];
function __facade_register__(...args) {
  __facade_middleware__.push(...args.flat());
}
__name(__facade_register__, "__facade_register__");
function __facade_invokeChain__(request, env, ctx, dispatch, middlewareChain) {
  const [head, ...tail] = middlewareChain;
  const middlewareCtx = {
    dispatch,
    next(newRequest, newEnv) {
      return __facade_invokeChain__(newRequest, newEnv, ctx, dispatch, tail);
    }
  };
  return head(request, env, ctx, middlewareCtx);
}
__name(__facade_invokeChain__, "__facade_invokeChain__");
function __facade_invoke__(request, env, ctx, dispatch, finalMiddleware) {
  return __facade_invokeChain__(request, env, ctx, dispatch, [
    ...__facade_middleware__,
    finalMiddleware
  ]);
}
__name(__facade_invoke__, "__facade_invoke__");

// .wrangler/tmp/bundle-VTiiZv/middleware-loader.entry.ts
var __Facade_ScheduledController__ = class ___Facade_ScheduledController__ {
  constructor(scheduledTime, cron, noRetry) {
    this.scheduledTime = scheduledTime;
    this.cron = cron;
    this.#noRetry = noRetry;
  }
  scheduledTime;
  cron;
  static {
    __name(this, "__Facade_ScheduledController__");
  }
  #noRetry;
  noRetry() {
    if (!(this instanceof ___Facade_ScheduledController__)) {
      throw new TypeError("Illegal invocation");
    }
    this.#noRetry();
  }
};
function wrapExportedHandler(worker) {
  if (__INTERNAL_WRANGLER_MIDDLEWARE__ === void 0 || __INTERNAL_WRANGLER_MIDDLEWARE__.length === 0) {
    return worker;
  }
  for (const middleware of __INTERNAL_WRANGLER_MIDDLEWARE__) {
    __facade_register__(middleware);
  }
  const fetchDispatcher = /* @__PURE__ */ __name(function(request, env, ctx) {
    if (worker.fetch === void 0) {
      throw new Error("Handler does not export a fetch() function.");
    }
    return worker.fetch(request, env, ctx);
  }, "fetchDispatcher");
  return {
    ...worker,
    fetch(request, env, ctx) {
      const dispatcher = /* @__PURE__ */ __name(function(type, init) {
        if (type === "scheduled" && worker.scheduled !== void 0) {
          const controller = new __Facade_ScheduledController__(
            Date.now(),
            init.cron ?? "",
            () => {
            }
          );
          return worker.scheduled(controller, env, ctx);
        }
      }, "dispatcher");
      return __facade_invoke__(request, env, ctx, dispatcher, fetchDispatcher);
    }
  };
}
__name(wrapExportedHandler, "wrapExportedHandler");
function wrapWorkerEntrypoint(klass) {
  if (__INTERNAL_WRANGLER_MIDDLEWARE__ === void 0 || __INTERNAL_WRANGLER_MIDDLEWARE__.length === 0) {
    return klass;
  }
  for (const middleware of __INTERNAL_WRANGLER_MIDDLEWARE__) {
    __facade_register__(middleware);
  }
  return class extends klass {
    #fetchDispatcher = /* @__PURE__ */ __name((request, env, ctx) => {
      this.env = env;
      this.ctx = ctx;
      if (super.fetch === void 0) {
        throw new Error("Entrypoint class does not define a fetch() function.");
      }
      return super.fetch(request);
    }, "#fetchDispatcher");
    #dispatcher = /* @__PURE__ */ __name((type, init) => {
      if (type === "scheduled" && super.scheduled !== void 0) {
        const controller = new __Facade_ScheduledController__(
          Date.now(),
          init.cron ?? "",
          () => {
          }
        );
        return super.scheduled(controller);
      }
    }, "#dispatcher");
    fetch(request) {
      return __facade_invoke__(
        request,
        this.env,
        this.ctx,
        this.#dispatcher,
        this.#fetchDispatcher
      );
    }
  };
}
__name(wrapWorkerEntrypoint, "wrapWorkerEntrypoint");
var WRAPPED_ENTRY;
if (typeof middleware_insertion_facade_default === "object") {
  WRAPPED_ENTRY = wrapExportedHandler(middleware_insertion_facade_default);
} else if (typeof middleware_insertion_facade_default === "function") {
  WRAPPED_ENTRY = wrapWorkerEntrypoint(middleware_insertion_facade_default);
}
var middleware_loader_entry_default = WRAPPED_ENTRY;
export {
  Room,
  __INTERNAL_WRANGLER_MIDDLEWARE__,
  middleware_loader_entry_default as default
};
//# sourceMappingURL=index.js.map
