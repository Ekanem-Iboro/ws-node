// OpenAPI 3.0 spec, shared by the local Node server and the Cloudflare Worker.
// WebSocket handshakes can't be expressed in OpenAPI, so they live under x-websocket.
export default {
  openapi: '3.0.3',
  info: {
    title: 'Thermal Relay API',
    version: '1.0.0',
    description: [
      'Relay between an ESP32-S3 thermal sensor and a web frontend.',
      '',
      '### Data flow',
      '1. The ESP32-S3 pushes a telemetry reading, either over a WebSocket (`ws://host/ws/device`) or an HTTP POST (`POST /api/telemetry`).',
      '2. The server normalizes it, stores it as the latest reading, and fans it out to every subscriber.',
      '3. Subscribers are WebSocket clients (`ws://host/ws/client`) or Server-Sent Event subscribers (`GET /api/telemetry/stream`).',
      '4. A frontend that cannot hold a connection reads the latest value with `GET /api/telemetry`.',
      '',
      '### Hosting notes',
      'The Cloudflare Worker build (`worker/`) routes everything through one Durable Object named `thermal-relay`, which owns the client sockets and the stored reading so fan-out survives multiple isolates. On Workers the SSE endpoint is not available (it would pin a live isolate); use the WebSocket there.',
    ].join('\n'),
  },
  servers: [
    { url: 'http://localhost:8080', description: 'Local Node server' },
    { url: 'http://localhost:8787', description: 'Local wrangler dev' },
    { url: 'https://thermal-relay.<your-subdomain>.workers.dev', description: 'Cloudflare Workers' },
  ],
  tags: [
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
  'x-websocket': {
    note: 'OpenAPI cannot describe WebSocket handshakes; documented here for reference. There is no "Try it out" for these - use a WS client, or use the REST endpoints instead.',
    endpoints: {
      '/ws/device': {
        method: 'GET (HTTP Upgrade)',
        role: 'ESP32-S3 pushes telemetry',
        serverHandshake: '101 Switching Protocols',
        send: { example: '{"ts":1790678801340,"thermal":{"max":68.4},"targets":[{"label":"Heater","value":68.4}]}' },
        receives: 'Nothing meaningful; client sockets are read-only subscribers.',
        notes: [
          'Legacy: on the Node server a bare upgrade to / is still accepted as the device endpoint.',
          'On Cloudflare, connecting a second device closes the first one (code 4000).',
        ],
      },
      '/ws/client': {
        method: 'GET (HTTP Upgrade)',
        role: 'Frontend subscribes to telemetry',
        serverHandshake: '101 Switching Protocols',
        receives: 'Every accepted telemetry payload, as JSON text. The latest reading is replayed on connect.',
        browserExample: "const ws = new WebSocket('ws://localhost:8080/ws/client');\nws.onmessage = (e) => console.log(JSON.parse(e.data));",
      },
    },
  },
};
