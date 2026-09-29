var __defProp = Object.defineProperty;
var __name = (target, value) => __defProp(target, "name", { value, configurable: true });

// worker/src/room.js
import { DurableObject } from "cloudflare:workers";

// shared/telemetry.js
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
      case "/ingest":
        return request.method === "POST" ? this.ingestViaHttp(request) : json({ error: "Method not allowed" }, 405);
      case "/latest":
        return json({ data: this.latest, latestAt: this.latestAt });
      case "/health":
        return json({
          status: "ok",
          runtime: "cloudflare-worker",
          deviceConnected: this.ctx.getWebSockets("device").length > 0,
          subscribers: { websocket: this.ctx.getWebSockets("client").length, sse: 0 },
          latestAt: this.latestAt
        });
      default:
        return json({ error: "Not found" }, 404);
    }
  }
  // --- sockets ------------------------------------------------------------
  acceptSocket(request, role) {
    if ((request.headers.get("Upgrade") || "").toLowerCase() !== "websocket") {
      return json({ error: "Expected a WebSocket upgrade" }, 426);
    }
    if (role === "device") {
      for (const ws of this.ctx.getWebSockets("device")) {
        ws.close(4e3, "replaced by a new device connection");
      }
    }
    const [client, server] = Object.values(new WebSocketPair());
    this.ctx.acceptWebSocket(server, [role]);
    server.serializeAttachment({ role, connectedAt: Date.now() });
    if (role === "client") {
      console.log(`Frontend connected (${this.ctx.getWebSockets("client").length} total).`);
      if (this.latest) server.send(JSON.stringify(this.latest));
    } else {
      console.log("ESP32-S3 Connected!");
    }
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
  // The single fan-out point: every accepted reading, from either transport, ends here.
  async ingest(raw) {
    const payload = normalize(raw);
    this.latest = payload;
    this.latestAt = Date.now();
    await this.ctx.storage.put(LAST_KEY, { data: payload, at: this.latestAt });
    console.log(describe(payload));
    const msg = JSON.stringify(payload);
    let notified = 0;
    for (const ws of this.ctx.getWebSockets("client")) {
      try {
        ws.send(msg);
        notified++;
      } catch {
      }
    }
    return { payload, notified };
  }
  // --- hibernation event handlers ----------------------------------------
  async webSocketMessage(ws, message) {
    const { role } = ws.deserializeAttachment() ?? {};
    if (role !== "device") return;
    const raw = typeof message === "string" ? message : new TextDecoder().decode(message);
    let parsed;
    try {
      parsed = JSON.parse(raw);
    } catch {
      console.error("Invalid JSON from ESP32-S3:", raw.slice(0, 200));
      return;
    }
    await this.ingest(parsed);
  }
  async webSocketClose(ws, code) {
    const { role } = ws.deserializeAttachment() ?? {};
    if (code >= 1e3 && code <= 4999 && code !== 1005 && code !== 1006) {
      ws.close(code, "closing");
    }
    if (role === "device") console.log("ESP32-S3 Disconnected.");
    else {
      const remaining = Math.max(0, this.ctx.getWebSockets("client").length - 1);
      console.log(`Frontend disconnected (${remaining} total).`);
    }
  }
  async webSocketError(ws, error) {
    const { role } = ws.deserializeAttachment() ?? {};
    console.error(`${role ?? "client"} socket error:`, error?.message ?? error);
  }
};
function json(body, status = 200) {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "content-type": "application/json; charset=utf-8" }
  });
}
__name(json, "json");

// shared/openapi.js
var openapi_default = {
  openapi: "3.0.3",
  info: {
    title: "Thermal Relay API",
    version: "1.0.0",
    description: [
      "Relay between an ESP32-S3 thermal sensor and a web frontend.",
      "",
      "### Data flow",
      "1. The ESP32-S3 pushes a telemetry reading, either over a WebSocket (`ws://host/ws/device`) or an HTTP POST (`POST /api/telemetry`).",
      "2. The server normalizes it, stores it as the latest reading, and fans it out to every subscriber.",
      "3. Subscribers are WebSocket clients (`ws://host/ws/client`) or Server-Sent Event subscribers (`GET /api/telemetry/stream`).",
      "4. A frontend that cannot hold a connection reads the latest value with `GET /api/telemetry`.",
      "",
      "### Hosting notes",
      "The Cloudflare Worker build (`worker/`) routes everything through one Durable Object named `thermal-relay`, which owns the client sockets and the stored reading so fan-out survives multiple isolates. On Workers the SSE endpoint is not available (it would pin a live isolate); use the WebSocket there."
    ].join("\n")
  },
  servers: [
    { url: "http://localhost:8080", description: "Local Node server" },
    { url: "http://localhost:8787", description: "Local wrangler dev" },
    { url: "https://thermal-relay.<your-subdomain>.workers.dev", description: "Cloudflare Workers" }
  ],
  tags: [
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
  "x-websocket": {
    note: 'OpenAPI cannot describe WebSocket handshakes; documented here for reference. There is no "Try it out" for these - use a WS client, or use the REST endpoints instead.',
    endpoints: {
      "/ws/device": {
        method: "GET (HTTP Upgrade)",
        role: "ESP32-S3 pushes telemetry",
        serverHandshake: "101 Switching Protocols",
        send: { example: '{"ts":1790678801340,"thermal":{"max":68.4},"targets":[{"label":"Heater","value":68.4}]}' },
        receives: "Nothing meaningful; client sockets are read-only subscribers.",
        notes: [
          "Legacy: on the Node server a bare upgrade to / is still accepted as the device endpoint.",
          "On Cloudflare, connecting a second device closes the first one (code 4000)."
        ]
      },
      "/ws/client": {
        method: "GET (HTTP Upgrade)",
        role: "Frontend subscribes to telemetry",
        serverHandshake: "101 Switching Protocols",
        receives: "Every accepted telemetry payload, as JSON text. The latest reading is replayed on connect.",
        browserExample: "const ws = new WebSocket('ws://localhost:8080/ws/client');\nws.onmessage = (e) => console.log(JSON.parse(e.data));"
      }
    }
  }
};

// worker/src/index.js
var ROOM_NAME = "thermal-relay";
var DEVICE_PATH = "/ws/device";
var CLIENT_PATH = "/ws/client";
var DO_ORIGIN = "https://do.internal";
var src_default = {
  // This Worker is a thin router: it does no state work of its own, it forwards
  // everything to the one Durable Object instance that owns the sockets.
  async fetch(request, env) {
    const url = new URL(request.url);
    const { pathname } = url;
    try {
      if (request.method === "OPTIONS") return new Response(null, { status: 204, headers: cors() });
      if ((request.headers.get("Upgrade") || "").toLowerCase() === "websocket") {
        if (pathname !== DEVICE_PATH && pathname !== CLIENT_PATH && pathname !== "/") {
          return json2({ error: "Unknown WebSocket path" }, 404);
        }
        const target = pathname === CLIENT_PATH ? "/ws/client" : "/ws/device";
        return roomStub(env).fetch(DO_ORIGIN + target, request);
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
          rest: ["POST /api/telemetry", "GET /api/telemetry", "GET /api/health"],
          websocket: { device: DEVICE_PATH, client: CLIENT_PATH },
          sse: "not available on this runtime",
          docs: "/api-docs"
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

// .wrangler/tmp/bundle-WbIQHM/middleware-insertion-facade.js
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

// .wrangler/tmp/bundle-WbIQHM/middleware-loader.entry.ts
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
