const http = require('http');
const express = require('express');
const swaggerUi = require('swagger-ui-express');
const { WebSocketServer } = require('ws');
const { normalize, describe } = require('./shared/telemetry.mjs');
const openapi = require('./shared/openapi.mjs').default;
const { BASE_URL, WS_PATHS, deviceWsUrl, clientWsUrl } = require('./shared/config.mjs');

// One HTTP server serves everything:
//   REST   -> /api/*      (device pushes, frontend reads)
//   Docs   -> /api-docs   (Swagger UI) and /api-docs.json
//   WS     -> /ws/device  (ESP32-S3) and /ws/client (browser)
const HTTP_PORT = Number(process.env.PORT) || 8080;
const DEVICE_PATH = process.env.DEVICE_PATH || WS_PATHS.device;
const CLIENT_PATH = process.env.CLIENT_PATH || WS_PATHS.client;
const RELAY_PATH = WS_PATHS.relay;

// --- state ---------------------------------------------------------------
let latest = null; // most recent telemetry payload
let latestAt = null; // when it arrived
let deviceConnected = false;

const wsClients = new Set();
const sseClients = new Set();

// --- helpers -------------------------------------------------------------
function logTelemetry(p, source = 'unknown') {
  console.log(`${describe(p)} (via ${source})`);
}

// Single fan-out point: every accepted reading goes to frontend WS clients AND SSE clients.
// The device socket is deliberately excluded so the ESP32 never sees an echo of its own
// reading. `source` is only used for logging.
function ingest(raw, source = 'unknown') {
  const payload = normalize(raw);
  latest = payload;
  latestAt = Date.now();
  logTelemetry(payload, source);

  const msg = JSON.stringify(payload);
  for (const ws of wsClients) {
    if (ws.readyState === 1) ws.send(msg);
  }
  for (const res of sseClients) {
    res.write(`event: telemetry\ndata: ${msg}\n\n`);
  }
  return payload;
}

// Turns a raw socket frame into an ingest call, or logs why it could not.
function ingestFromSocket(raw, source) {
  let parsed;
  try {
    parsed = JSON.parse(raw);
  } catch {
    console.error(`Invalid JSON from ${source}:`, raw.slice(0, 200));
    return null;
  }
  if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) {
    console.error(`Non-object payload from ${source}:`, raw.slice(0, 200));
    return null;
  }
  return ingest(parsed, source);
}

// --- app -----------------------------------------------------------------
const app = express();
app.use(express.json({ limit: '64kb' }));
app.use(express.urlencoded({ extended: false }));

// Permissive CORS so a frontend dev server (e.g. Vite on :5173) can call it.
app.use((req, res, next) => {
  res.set('Access-Control-Allow-Origin', '*');
  res.set('Access-Control-Allow-Methods', 'GET,POST,OPTIONS');
  res.set('Access-Control-Allow-Headers', 'Content-Type');
  if (req.method === 'OPTIONS') return res.sendStatus(204);
  next();
});

app.get('/api/health', (req, res) => {
  res.json({
    status: 'ok',
    deviceConnected,
    subscribers: { websocket: wsClients.size, sse: sseClients.size },
    latestAt,
  });
});

app.post('/api/telemetry', (req, res) => {
  if (!req.body || typeof req.body !== 'object' || Array.isArray(req.body)) {
    return res.status(400).json({ error: 'Body must be a telemetry JSON object' });
  }
  const payload = ingest(req.body);
  res.status(202).json({ accepted: true, ts: payload.ts });
});

app.get('/api/telemetry', (req, res) => {
  res.json({ data: latest, latestAt });
});

app.get('/api/telemetry/stream', (req, res) => {
  res.writeHead(200, {
    'Content-Type': 'text/event-stream',
    'Cache-Control': 'no-cache, no-transform',
    Connection: 'keep-alive',
    'Access-Control-Allow-Origin': '*',
    'X-Accel-Buffering': 'no',
  });
  res.write('retry: 3000\n\n');

  sseClients.add(res);
  console.log(`SSE subscriber connected (${sseClients.size} total).`);

  if (latest) res.write(`event: telemetry\ndata: ${JSON.stringify(latest)}\n\n`);

  const ka = setInterval(() => res.write(': keep-alive\n\n'), 25000);
  req.on('close', () => {
    clearInterval(ka);
    sseClients.delete(res);
    console.log(`SSE subscriber disconnected (${sseClients.size} total).`);
  });
});

app.get('/', (req, res) => {
  res.json({
    name: 'thermal-relay',
    local: `http://localhost:${HTTP_PORT}`,
    deployed: BASE_URL,
    rest: '/api/telemetry (GET latest, POST push), /api/health, /api/telemetry/stream (SSE)',
    websocket: {
      device: DEVICE_PATH,
      client: CLIENT_PATH,
      relay: `${RELAY_PATH} (or '/' with no path)`,
      note: 'all sockets accept pushes; client and relay also receive the broadcast, device does not',
    },
    deployedWebsocket: { device: deviceWsUrl(), client: clientWsUrl() },
    docs: '/api-docs',
  });
});

app.use('/api-docs', swaggerUi.serve, swaggerUi.setup(openapi, { customSiteTitle: 'Thermal Relay API' }));
app.get('/api-docs.json', (req, res) => res.json(openapi));

// --- websocket plumbing --------------------------------------------------
const deviceWss = new WebSocketServer({ noServer: true });
const clientWss = new WebSocketServer({ noServer: true });

const relayWss = new WebSocketServer({ noServer: true });

// '/' is the path the original script used, with no path at all. On the Worker build it is
// a relay socket that pushes and receives but never evicts the sensor; here it is a
// separate server so the behaviour matches.
function whichServer(pathname) {
  if (pathname === DEVICE_PATH) return deviceWss;
  if (pathname === CLIENT_PATH) return clientWss;
  if (pathname === '/' || pathname === RELAY_PATH) return relayWss;
  return null;
}

const server = http.createServer(app);

// A socket-only path opened in a browser tab or hit with curl. Answer with an explanation
// instead of a bare 404, mirroring the Worker build.
const WS_ONLY_PATHS = new Set([DEVICE_PATH, CLIENT_PATH, RELAY_PATH]);
app.get([...WS_ONLY_PATHS], (req, res) => {
  res.status(426).json({
    error: 'This endpoint requires a WebSocket upgrade',
    hint: `Connect a WebSocket to http://localhost:${HTTP_PORT}${req.path} - a plain GET cannot join it.`,
  });
});

server.on('upgrade', (req, socket, head) => {
  const { pathname } = new URL(req.url, `http://${req.headers.host || 'localhost'}`);
  const wss = whichServer(pathname);
  if (!wss) {
    socket.write('HTTP/1.1 404 Not Found\r\nConnection: close\r\n\r\n');
    return socket.destroy();
  }
  wss.handleUpgrade(req, socket, head, (ws) => wss.emit('connection', ws, req));
});

deviceWss.on('connection', (ws) => {
  deviceConnected = true;
  console.log('ESP32-S3 Connected!');

  ws.on('message', (data) => ingestFromSocket(data.toString(), 'ESP32-S3 socket'));

  ws.on('close', () => {
    deviceConnected = false;
    console.log('ESP32-S3 Disconnected.');
  });
  ws.on('error', (err) => console.error('ESP32-S3 error:', err.message));
});

// Frontend sockets both push and receive. ws.send(...) from a browser is ingested exactly
// like a reading from the ESP32, then broadcast to every other frontend. '/' behaves the
// same way, which is what the original bare-host script did.
function registerFrontendSocket(wss, label) {
  wss.on('connection', (ws) => {
    wsClients.add(ws);
    console.log(`${label} connected (${wsClients.size} total).`);

    // Replay the stored reading so a page renders before the next sample arrives.
    if (latest) ws.send(JSON.stringify(latest));

    ws.on('message', (msg) => ingestFromSocket(msg.toString(), `${label} socket`));
    ws.on('close', () => {
      wsClients.delete(ws);
      console.log(`${label} disconnected (${wsClients.size} total).`);
    });
    ws.on('error', (err) => console.error(`${label} error:`, err.message));
  });
}

registerFrontendSocket(clientWss, 'Frontend');
registerFrontendSocket(relayWss, 'Relay client');

for (const [name, wss] of [['Device', deviceWss], ['Client', clientWss], ['Relay', relayWss]]) {
  wss.on('error', (err) => console.error(`${name} server error:`, err.message));
}

server.listen(HTTP_PORT, () => {
  const local = `http://localhost:${HTTP_PORT}`;
  console.log(`\n  Local server`);
  console.log(`    REST      -> ${local}/api/telemetry`);
  console.log(`    SSE       -> ${local}/api/telemetry/stream`);
  console.log(`    Swagger   -> ${local}/api-docs`);
  console.log(`    ESP32-S3  -> ws://localhost:${HTTP_PORT}${DEVICE_PATH}`);
  console.log(`    Frontend  -> ws://localhost:${HTTP_PORT}${CLIENT_PATH}`);
  console.log(`\n  Deployed (Cloudflare)`);
  console.log(`    Base      -> ${BASE_URL}`);
  console.log(`    REST      -> ${BASE_URL}/api/telemetry`);
  console.log(`    ESP32-S3  -> ${deviceWsUrl()}`);
  console.log(`    Frontend  -> ${clientWsUrl()}\n`);
});

process.on('SIGINT', () => {
  console.log('\nShutting down...');
  deviceWss.close();
  clientWss.close();
  server.close(() => process.exit(0));
});
