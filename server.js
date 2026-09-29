const http = require('http');
const express = require('express');
const swaggerUi = require('swagger-ui-express');
const { WebSocketServer } = require('ws');
const { normalize, describe } = require('./shared/telemetry.mjs');
const openapi = require('./shared/openapi.mjs').default;
const { BASE_URL, WS_PATHS, deviceWsUrl, clientWsUrl, updatesWsUrl } = require('./shared/config.mjs');

// One HTTP server serves everything:
//   REST   -> /api/*      (device pushes, frontend reads)
//   Docs   -> /api-docs   (Swagger UI) and /api-docs.json
//   WS     -> /ws/device  (ESP32-S3) and /ws/client (browser)
const HTTP_PORT = Number(process.env.PORT) || 8080;
const DEVICE_PATH = process.env.DEVICE_PATH || WS_PATHS.device;
const CLIENT_PATH = process.env.CLIENT_PATH || WS_PATHS.client;
const UPDATES_PATH = WS_PATHS.updates;
const RELAY_PATH = WS_PATHS.relay;

// --- state ---------------------------------------------------------------
let latest = null; // most recent telemetry payload
let latestAt = null; // when it arrived
let deviceConnected = false;

// Observer sockets only. Publisher sockets are never added, so a push is never echoed
// back to whoever sent it.
const wsClients = new Set();
const sseClients = new Set();

// --- helpers -------------------------------------------------------------
function logTelemetry(p, source = 'unknown') {
  console.log(`${describe(p)} (via ${source})`);
}

// Single fan-out point: every accepted reading goes to every observer WS socket AND to SSE
// subscribers. Publisher sockets are deliberately excluded so neither the ESP32 nor the
// frontend ever sees an echo of what it just sent. `source` is only used for logging.
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

// --- websocket plumbing --------------------------------------------------
// Declared before the routes because /api/health reports their client counts.
const deviceWss = new WebSocketServer({ noServer: true });
const clientWss = new WebSocketServer({ noServer: true });
const updatesWss = new WebSocketServer({ noServer: true });
const relayWss = new WebSocketServer({ noServer: true });

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
    publishers: { device: deviceWss.clients.size, client: clientWss.clients.size },
    subscribers: {
      websocket: wsClients.size,
      updates: updatesWss.clients.size,
      relay: relayWss.clients.size,
      sse: sseClients.size,
    },
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
      publish: { device: DEVICE_PATH, client: CLIENT_PATH },
      observe: { updates: UPDATES_PATH, relay: `${RELAY_PATH} (or '/' with no path)` },
      note: 'a socket either publishes or observes; publishers never receive an echo of their own push',
    },
    deployedWebsocket: {
      publish: { device: deviceWsUrl(), client: clientWsUrl() },
      observe: { updates: updatesWsUrl() },
    },
    docs: '/api-docs',
  });
});

app.use('/api-docs', swaggerUi.serve, swaggerUi.setup(openapi, { customSiteTitle: 'Thermal Relay API' }));
app.get('/api-docs.json', (req, res) => res.json(openapi));

// A socket either publishes or observes. This table is the local mirror of
// WS_ROLE_BY_PATH in worker/src/index.js.
//   /ws/device  - ESP32-S3, publishes, exclusive sensor slot
//   /ws/client  - browser publisher, publishes only
//   /ws/updates - read-only observer, receives everything
//   /ws/relay   - the bare '/' from the original script: publishes and observes
const WS_ROLES = {
  [DEVICE_PATH]: { wss: deviceWss, label: 'ESP32-S3' },
  [CLIENT_PATH]: { wss: clientWss, label: 'Client publisher' },
  [UPDATES_PATH]: { wss: updatesWss, label: 'Observer' },
  [RELAY_PATH]: { wss: relayWss, label: 'Relay client' },
  '/': { wss: relayWss, label: 'Relay client' },
};

function whichServer(pathname) {
  return WS_ROLES[pathname]?.wss ?? null;
}

const server = http.createServer(app);

// A socket-only path opened in a browser tab or hit with curl. Answer with an explanation
// instead of a bare 404, mirroring the Worker build.
const WS_ONLY_PATHS = new Set([DEVICE_PATH, CLIENT_PATH, UPDATES_PATH, RELAY_PATH]);
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

// A publisher socket pushes only. It is never added to wsClients, so it receives nothing
// back and never sees an echo of its own message.
function registerPublisherSocket(wss, label) {
  wss.on('connection', (ws) => {
    console.log(`${label} connected.`);
    ws.on('message', (msg) => ingestFromSocket(msg.toString(), `${label} socket`));
    ws.on('close', () => console.log(`${label} disconnected.`));
    ws.on('error', (err) => console.error(`${label} error:`, err.message));
  });
}

// An observer socket is read-only: it receives the broadcast, and anything it sends is
// refused so it cannot corrupt the stored value.
function registerObserverSocket(wss, label) {
  wss.on('connection', (ws) => {
    wsClients.add(ws);
    console.log(`${label} connected (${wsClients.size} total).`);

    // Replay the stored reading so an observer renders before the next sample arrives.
    if (latest) ws.send(JSON.stringify(latest));

    ws.on('message', () => {
      console.error(`${label} is read-only and cannot push; closing.`);
      ws.close(4003, 'this endpoint is read-only');
    });
    ws.on('close', () => {
      wsClients.delete(ws);
      console.log(`${label} disconnected (${wsClients.size} total).`);
    });
    ws.on('error', (err) => console.error(`${label} error:`, err.message));
  });
}

// The relay is the one role that does both, kept for compatibility with the original
// `new WebSocket('ws://host:8080')` script.
relayWss.on('connection', (ws) => {
  wsClients.add(ws);
  console.log(`Relay client connected (${wsClients.size} total).`);

  if (latest) ws.send(JSON.stringify(latest));

  ws.on('message', (msg) => ingestFromSocket(msg.toString(), 'Relay client socket'));
  ws.on('close', () => {
    wsClients.delete(ws);
    console.log(`Relay client disconnected (${wsClients.size} total).`);
  });
  ws.on('error', (err) => console.error('Relay client error:', err.message));
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

registerPublisherSocket(clientWss, 'Client publisher');
registerObserverSocket(updatesWss, 'Observer');

for (const [name, wss] of [
  ['Device', deviceWss],
  ['Client', clientWss],
  ['Updates', updatesWss],
  ['Relay', relayWss],
]) {
  wss.on('error', (err) => console.error(`${name} server error:`, err.message));
}

server.listen(HTTP_PORT, () => {
  const local = `http://localhost:${HTTP_PORT}`;
  console.log(`\n  Local server`);
  console.log(`    REST      -> ${local}/api/telemetry`);
  console.log(`    SSE       -> ${local}/api/telemetry/stream`);
  console.log(`    Swagger   -> ${local}/api-docs`);
  console.log(`    ESP32-S3  -> ws://localhost:${HTTP_PORT}${DEVICE_PATH}`);
  console.log(`    Publisher -> ws://localhost:${HTTP_PORT}${CLIENT_PATH}`);
  console.log(`    Observer  -> ws://localhost:${HTTP_PORT}${UPDATES_PATH}`);
  console.log(`\n  Deployed (Cloudflare)`);
  console.log(`    Base      -> ${BASE_URL}`);
  console.log(`    REST      -> ${BASE_URL}/api/telemetry`);
  console.log(`    ESP32-S3  -> ${deviceWsUrl()}`);
  console.log(`    Publisher -> ${clientWsUrl()}`);
  console.log(`    Observer  -> ${updatesWsUrl()}\n`);
});

process.on('SIGINT', () => {
  console.log('\nShutting down...');
  deviceWss.close();
  clientWss.close();
  server.close(() => process.exit(0));
});
