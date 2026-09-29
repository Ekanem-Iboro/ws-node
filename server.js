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

// --- state ---------------------------------------------------------------
let latest = null; // most recent telemetry payload
let latestAt = null; // when it arrived
let deviceConnected = false;

const wsClients = new Set();
const sseClients = new Set();

// --- helpers -------------------------------------------------------------
function logTelemetry(p) {
  console.log(describe(p));
}

// Single fan-out point: every accepted reading goes to WS clients AND SSE clients.
function ingest(raw) {
  const payload = normalize(raw);
  latest = payload;
  latestAt = Date.now();
  logTelemetry(payload);

  const msg = JSON.stringify(payload);
  for (const ws of wsClients) {
    if (ws.readyState === 1) ws.send(msg);
  }
  for (const res of sseClients) {
    res.write(`event: telemetry\ndata: ${msg}\n\n`);
  }
  return payload;
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
    websocket: { device: DEVICE_PATH, client: CLIENT_PATH },
    deployedWebsocket: { device: deviceWsUrl(), client: clientWsUrl() },
    docs: '/api-docs',
  });
});

app.use('/api-docs', swaggerUi.serve, swaggerUi.setup(openapi, { customSiteTitle: 'Thermal Relay API' }));
app.get('/api-docs.json', (req, res) => res.json(openapi));

// --- websocket plumbing --------------------------------------------------
const deviceWss = new WebSocketServer({ noServer: true });
const clientWss = new WebSocketServer({ noServer: true });

function whichServer(pathname) {
  if (pathname === DEVICE_PATH) return deviceWss;
  if (pathname === CLIENT_PATH) return clientWss;
  if (pathname === '/') return deviceWss; // legacy: bare host/ still means the ESP32
  return null;
}

const server = http.createServer(app);

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

  ws.on('message', (data) => {
    const raw = data.toString();
    try {
      ingest(JSON.parse(raw));
    } catch (err) {
      console.error('Invalid JSON from ESP32-S3:', raw);
    }
  });

  ws.on('close', () => {
    deviceConnected = false;
    console.log('ESP32-S3 Disconnected.');
  });
  ws.on('error', (err) => console.error('ESP32-S3 error:', err.message));
});

clientWss.on('connection', (ws) => {
  wsClients.add(ws);
  console.log(`Frontend connected (${wsClients.size} total).`);

  ws.on('message', (msg) => console.log('From client:', msg.toString()));
  ws.on('close', () => {
    wsClients.delete(ws);
    console.log(`Frontend disconnected (${wsClients.size} total).`);
  });
  ws.on('error', (err) => console.error('Client error:', err.message));
});

for (const [name, wss] of [['Device', deviceWss], ['Client', clientWss]]) {
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
