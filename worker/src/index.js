import { Room } from './room.js';
import openapi from '../../shared/openapi.mjs';
import { BASE_URL, WS_PATHS, deviceWsUrl, clientWsUrl, updatesWsUrl } from '../../shared/config.mjs';

export { Room };

// Independent of the Worker name in wrangler.jsonc: this is the id of the one Durable
// Object instance that owns the sockets. Changing it would start a fresh empty room.
const ROOM_NAME = 'thermal-relay';
// Durable Object stubs only accept absolute URLs. The host is ignored; it is not a network hop.
const DO_ORIGIN = 'https://do.internal';

// Public path -> Durable Object path. The DO serves the same path names, so this is
// mostly an identity map; '/' is the exception, and it exists because the original script
// was `new WebSocket('ws://host:8080')` with no path at all.
const WS_ROLE_BY_PATH = {
  [WS_PATHS.device]: '/ws/device',
  [WS_PATHS.client]: '/ws/client',
  [WS_PATHS.updates]: '/ws/updates',
  [WS_PATHS.relay]: '/ws/relay',
  '/': '/ws/relay',
};

// Paths that only ever answer WebSocket upgrades, so a plain GET should return 426.
const WS_ONLY_PATHS = [WS_PATHS.device, WS_PATHS.client, WS_PATHS.updates, WS_PATHS.relay];

function wsUrlFor(pathname) {
  const base = BASE_URL.replace(/^http/, 'ws');
  return `${base}${pathname === '/' ? '/' : pathname}`;
}

export default {
  // This Worker is a thin router: it does no state work of its own, it forwards
  // everything to the one Durable Object instance that owns the sockets.
  async fetch(request, env) {
    const url = new URL(request.url);
    const { pathname } = url;

    try {
      if (request.method === 'OPTIONS') return new Response(null, { status: 204, headers: cors() });

      // --- WebSockets ------------------------------------------------------
      // '/' doubles as the service-info route, so it only counts as a socket path when an
      // upgrade header is actually present.
      const isUpgrade = (request.headers.get('Upgrade') || '').toLowerCase() === 'websocket';
      if (isUpgrade && WS_ROLE_BY_PATH[pathname]) {
        return roomStub(env).fetch(DO_ORIGIN + WS_ROLE_BY_PATH[pathname], request);
      }

      // A plain GET to a socket-only path: someone opened it in a tab or hit it with curl.
      if (!isUpgrade && WS_ONLY_PATHS.includes(pathname)) {
        return json(
          {
            error: 'This endpoint requires a WebSocket upgrade',
            hint: `Connect a WebSocket to ${url.origin}${pathname} - e.g. new WebSocket("${wsUrlFor(pathname)}"). A plain GET cannot join it.`,
          },
          426
        );
      }

      // --- REST ------------------------------------------------------------
      if (pathname === '/api/telemetry' && request.method === 'POST') {
        const body = await readJson(request);
        if (body.error) return json({ error: body.error }, 400);
        const res = await roomStub(env).fetch(DO_ORIGIN + '/ingest', {
          method: 'POST',
          headers: { 'content-type': 'application/json' },
          body: JSON.stringify(body.data),
        });
        return withCors(res);
      }

      if (pathname === '/api/telemetry' && request.method === 'GET') {
        return withCors(await roomStub(env).fetch(DO_ORIGIN + '/latest'));
      }

      if (pathname === '/api/telemetry/stream') {
        // A long-lived SSE response would pin a live isolate and burn free-plan CPU.
        return json(
          {
            error: 'SSE is not available on the Worker build',
            hint: 'Use the WebSocket at ws(s)://<host>/ws/client, or poll GET /api/telemetry.',
          },
          501
        );
      }

      if (pathname === '/api/health') {
        return withCors(await roomStub(env).fetch(DO_ORIGIN + '/health'));
      }

      // --- docs ------------------------------------------------------------
      if (pathname === '/api-docs.json') return json(openapi);

      if (pathname === '/api-docs' || pathname === '/api-docs/') {
        return new Response(SWAGGER_HTML, {
          headers: { 'content-type': 'text/html; charset=utf-8', ...cors() },
        });
      }

      if (pathname === '/') {
        return json({
          name: 'thermal-relay (Cloudflare Worker)',
          baseUrl: BASE_URL,
          rest: ['POST /api/telemetry', 'GET /api/telemetry', 'GET /api/health'],
          websocket: {
            publish: { device: deviceWsUrl(), client: clientWsUrl() },
            observe: { updates: updatesWsUrl() },
          },
          sse: 'not available on this runtime',
          docs: `${BASE_URL}/api-docs`,
        });
      }

      return json({ error: 'Not found' }, 404);
    } catch (err) {
      console.error('Unhandled error:', err?.message ?? err);
      return json({ error: 'Internal error' }, 500);
    }
  },
};

function roomStub(env) {
  return env.THERMAL_ROOM.get(env.THERMAL_ROOM.idFromName(ROOM_NAME));
}

async function readJson(request) {
  let data;
  try {
    data = await request.json();
  } catch {
    return { error: 'Body must be valid JSON' };
  }
  if (!data || typeof data !== 'object' || Array.isArray(data)) {
    return { error: 'Body must be a telemetry JSON object' };
  }
  return { data };
}

function cors() {
  return {
    'access-control-allow-origin': '*',
    'access-control-allow-methods': 'GET,POST,OPTIONS',
    'access-control-allow-headers': 'Content-Type',
  };
}

function json(body, status = 200) {
  return new Response(JSON.stringify(body), {
    status,
    headers: { 'content-type': 'application/json; charset=utf-8', ...cors() },
  });
}

// Copies CORS headers onto a Response that came from the Durable Object.
function withCors(res) {
  const headers = new Headers(res.headers);
  for (const [k, v] of Object.entries(cors())) headers.set(k, v);
  return new Response(res.body, { status: res.status, headers });
}

// Swagger UI can't be bundled into a Worker, so the page pulls it from a CDN and
// reads the spec from this same Worker.
const SWAGGER_HTML = `<!doctype html>
<html>
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>Thermal Relay API</title>
    <link rel="stylesheet" href="https://unpkg.com/swagger-ui-dist@5/swagger-ui.css" />
  </head>
  <body>
    <div id="swagger-ui"></div>
    <script src="https://unpkg.com/swagger-ui-dist@5/swagger-ui-bundle.js" crossorigin></script>
    <script>
      window.onload = () => SwaggerUIBundle({ url: '/api-docs.json', dom_id: '#swagger-ui' });
    </script>
  </body>
</html>`;
