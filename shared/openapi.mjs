// OpenAPI 3.0 spec, shared by the local Node server and the Cloudflare Worker.
// WebSocket handshakes can't be expressed in OpenAPI, so they live under x-websocket.
import { BASE_URL, deviceWsUrl, clientWsUrl } from './config.mjs';

// A WebSocket handshake cannot be expressed in OpenAPI, but Swagger UI will happily render
// a GET operation with a documented 101 response, which is what a reader needs to see.
// "Try it out" is meaningless here: it issues a plain GET, which returns 426.
function wsOperation({ summary, url, role, sends, receives, example, tags = ['websocket'], notes = [] }) {
  return {
    get: {
      tags,
      summary,
      description: [
        `**Full URL:** \`${url}\``,
        '',
        role,
        '',
        `**Send:** ${sends}`,
        '',
        `**Receive:** ${receives}`,
        '',
        '```js',
        example,
        '```',
        '',
        ...notes.flatMap((n) => [`- ${n}`, '']),
        'This is a WebSocket upgrade, so "Try it out" does not apply: a plain GET returns `426 Upgrade Required`.',
      ].join('\n'),
      responses: {
        101: { description: 'Switching Protocols - the socket is open' },
        426: { description: 'Upgrade Required - this endpoint only speaks WebSocket' },
      },
    },
  };
}

export default {
  openapi: '3.0.3',
  info: {
    title: 'Thermal Relay API',
    version: '1.0.0',
    description: [
      'Relay between an ESP32-S3 thermal sensor and a web frontend.',
      '',
      '### The frontend pushes too',
      '`/ws/client` is bidirectional. The browser sends a payload and it is ingested exactly like a reading from the ESP32:',
      'stored as the latest value and broadcast to every other frontend socket. This mirrors the original',
      '`wss.on(\'message\')` script, which never distinguished who sent what.',
      '',
      '```js',
      "const ws = new WebSocket('" + clientWsUrl() + "');",
      'ws.onmessage = (e) => render(JSON.parse(e.data));',
      "ws.send(JSON.stringify({ thermal: { max: 68.4 }, targets: [{ label: 'Heater', value: 68.4 }] }));",
      '```',
      '',
      '### Data flow',
      '1. A reading arrives from the ESP32-S3 (`/ws/device`), from the frontend (`/ws/client`), or from an HTTP POST (`POST /api/telemetry`).',
      '2. The server normalizes it, stores it as the latest reading, and fans it out to every frontend socket and SSE subscriber.',
      '3. Frontends receive them on `/ws/client`, or read the latest value with `GET /api/telemetry` when they cannot hold a connection.',
      '',
      '### Which socket to use',
      '- `/ws/client` - the frontend. Pushes **and** receives. Use this one.',
      '- `/ws/device` - the ESP32-S3. Pushes, does not receive, so the sensor never sees an echo. Exclusive: a new device socket closes the previous one with code 4000.',
      '- `/` - a relay socket, kept for compatibility with the original `new WebSocket(\'ws://host:8080\')`. Pushes and receives, and never evicts the sensor.',
      '',
      '### Hosting notes',
      'The Cloudflare Worker build (`worker/`) routes everything through one Durable Object named `thermal-relay`, which owns the client sockets and the stored reading so fan-out survives multiple isolates. On Workers the SSE endpoint is not available (it would pin a live isolate); use the WebSocket there.',
    ].join('\n'),
  },
  servers: [
    { url: 'http://localhost:8080', description: 'Local Node server' },
    { url: 'http://localhost:8787', description: 'Local wrangler dev' },
    { url: BASE_URL, description: 'Cloudflare Workers (deployed)' },
  ],
  tags: [
    { name: 'websocket', description: 'WebSocket endpoints: the ESP32-S3 and the frontend both push here' },
    { name: 'telemetry', description: 'Thermal readings pushed by the device and read by the frontend' },
    { name: 'health', description: 'Server and connection status' },
  ],
  paths: {
    '/api/health': {
      get: {
        tags: ['health'],
        summary: 'Server and connection status',
        responses: {
          200: {
            description: 'Current status',
            content: {
              'application/json': {
                schema: {
                  type: 'object',
                  properties: {
                    status: { type: 'string', example: 'ok' },
                    deviceConnected: { type: 'boolean' },
                    subscribers: {
                      type: 'object',
                      properties: {
                        websocket: { type: 'integer' },
                        sse: { type: 'integer' },
                      },
                    },
                    latestAt: { type: 'integer', nullable: true, description: 'Epoch ms of last reading' },
                  },
                },
              },
            },
          },
        },
      },
    },
    '/api/telemetry': {
      get: {
        tags: ['telemetry'],
        summary: 'Latest reading (polling fallback for the frontend)',
        responses: {
          200: {
            description: 'Most recent reading, or null if nothing has arrived yet',
            content: {
              'application/json': {
                schema: {
                  type: 'object',
                  properties: {
                    data: { oneOf: [{ $ref: '#/components/schemas/Telemetry' }, { type: 'null' }] },
                    latestAt: { type: 'integer', nullable: true },
                  },
                },
              },
            },
          },
        },
      },
      post: {
        tags: ['telemetry'],
        summary: 'Push a reading (HTTP alternative to the device WebSocket)',
        requestBody: {
          required: true,
          content: { 'application/json': { schema: { $ref: '#/components/schemas/TelemetryInput' } } },
        },
        responses: {
          202: {
            description: 'Accepted and broadcast to subscribers',
            content: {
              'application/json': {
                schema: {
                  type: 'object',
                  properties: { accepted: { type: 'boolean' }, ts: { type: 'integer' } },
                },
              },
            },
          },
          400: { description: 'Body was not a JSON object' },
        },
      },
    },
    '/ws/client': wsOperation({
      summary: 'Frontend socket: push and receive telemetry',
      url: clientWsUrl(),
      role: 'Browser frontend. Sends a payload to publish it, and receives every accepted reading.',
      sends: 'Anything with a `thermal` field is stored as the latest reading and broadcast to all other frontend sockets.',
      receives: 'Every accepted reading. The stored reading is replayed on connect.',
      example:
        "const ws = new WebSocket('" + clientWsUrl() + "');\n" +
        'ws.onmessage = (e) => render(JSON.parse(e.data));\n' +
        "ws.send(JSON.stringify({ thermal: { max: 68.4 }, targets: [{ label: 'Heater', value: 68.4 }] }));",
      tags: ['websocket'],
    }),
    '/ws/device': wsOperation({
      summary: 'ESP32-S3 socket: push telemetry',
      url: deviceWsUrl(),
      role: 'The sensor. Sends readings; does not receive the broadcast, so it never sees an echo of its own data.',
      sends: 'A telemetry object: { "ts": <ms>, "thermal": { "max": <number> }, "targets": [ { "label": "...", "value": <number> } ] }',
      receives: 'Nothing. Device sockets are push-only.',
      example:
        '// ESP32 / Arduino\n' +
        'WebSocketClient ws("' + deviceWsUrl() + '");\n' +
        'ws.connect();\n' +
        'ws.println("{\\"ts\\":" + millis() + ",\\"thermal\\":{\\"max\\":68.4}}");',
      tags: ['websocket'],
      notes: [
        'Connecting a second device socket closes the first with code 4000, so the sensor slot stays exclusive.',
        'A bare upgrade to / is accepted as a general relay socket and does not evict the device.',
      ],
    }),
    '/api/telemetry/stream': {
      get: {
        tags: ['telemetry'],
        summary: 'Live stream of readings (Server-Sent Events)',
        description:
          'Local Node server only: replays the current latest reading, then a keep-alive comment every 25s. The Cloudflare Worker returns 501 here because a long-lived response pins a live isolate; use the WebSocket instead. Use `EventSource` in the browser.',
        responses: {
          200: {
            description: 'text/event-stream',
            content: {
              'text/event-stream': {
                schema: { type: 'string', example: 'event: telemetry\ndata: {"ts":1790678801340,"thermal":{"max":68.4},"targets":[]}\n\n' },
              },
            },
          },
          501: { description: 'Not available on the Cloudflare Worker build' },
        },
      },
    },
  },
  components: {
    schemas: {
      Target: {
        type: 'object',
        properties: {
          label: { type: 'string', example: 'Heater' },
          value: { type: 'number', example: 68.4, description: 'Degrees Celsius' },
        },
      },
      TelemetryInput: {
        type: 'object',
        properties: {
          ts: { type: 'integer', example: 1790678801340, description: 'Device timestamp in ms; defaults to server time' },
          thermal: { type: 'object', properties: { max: { type: 'number', example: 68.4 } } },
          targets: { type: 'array', items: { $ref: '#/components/schemas/Target' } },
        },
      },
      Telemetry: {
        allOf: [{ $ref: '#/components/schemas/TelemetryInput' }],
        description: 'Normalized form stored by the server and pushed to subscribers.',
      },
    },
  },
  // Machine-readable mirror of the websocket entries in `paths`. Swagger UI ignores
  // vendor extensions, which is why those same endpoints are also declared in `paths`.
  'x-websocket': {
    note: 'WebSocket endpoints are declared under `paths` as GET operations with a 101 response, because vendor extensions are not rendered by Swagger UI. This block is the machine-readable duplicate.',
    endpoints: {
      '/ws/client': {
        url: clientWsUrl(),
        role: 'Frontend',
        bidirectional: true,
        sends: 'Telemetry object; stored as latest and broadcast to other frontends.',
        receives: 'Every accepted reading; the stored reading is replayed on connect.',
      },
      '/ws/device': {
        url: deviceWsUrl(),
        role: 'ESP32-S3',
        bidirectional: false,
        sends: 'Telemetry object.',
        receives: 'Nothing. Exclusive slot: a second device closes the first with code 4000.',
      },
      '/': {
        url: BASE_URL.replace(/^http/, 'ws') + '/',
        role: 'Relay (legacy path from the original script)',
        bidirectional: true,
        sends: 'Telemetry object.',
        receives: 'Every accepted reading.',
        note: 'Never evicts the device socket.',
      },
    },
  },
};
