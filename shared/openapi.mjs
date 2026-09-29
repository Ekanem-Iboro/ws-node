// OpenAPI 3.0 spec, shared by the local Node server and the Cloudflare Worker.
// WebSocket handshakes can't be expressed in OpenAPI, so they live under x-websocket.
import { BASE_URL, WS_PATHS, deviceWsUrl, clientWsUrl, updatesWsUrl } from './config.mjs';

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
      '### Publish and observe are separate sockets',
      'A socket either **publishes** a reading or **observes** them, never both. That is what stops a',
      'publisher from seeing an echo of its own message:',
      '',
      '```js',
      "// publish on /ws/client - no echo comes back",
      "const pub = new WebSocket('" + clientWsUrl() + "');",
      "pub.send(JSON.stringify({ thermal: { max: 68.4 }, targets: [{ label: 'Heater', value: 68.4 }] }));",
      '',
      "// observe on /ws/updates - receives every accepted reading, from any publisher",
      "const feed = new WebSocket('" + updatesWsUrl() + "');",
      'feed.onmessage = (e) => render(JSON.parse(e.data));',
      '```',
      '',
      'Anything sent on `/ws/updates` is refused: the socket is closed with code 4003 and the',
      'stored value is left untouched. An observer can therefore never corrupt the feed.',
      '',
      'This mirrors the original `wss.on(\'message\')` script, which never distinguished who sent what -',
      'a payload from a browser is ingested exactly like a reading from the ESP32.',
      '',
      '### Data flow',
      '1. A reading is published by the ESP32-S3 (`/ws/device`), by a browser (`/ws/client`), or over HTTP (`POST /api/telemetry`).',
      '2. The server normalizes it, stores it as the latest reading, and fans it out to every observer socket and SSE subscriber.',
      '3. Observers receive it on `/ws/updates`, or read the latest value with `GET /api/telemetry` when they cannot hold a connection.',
      '',
      '### Which socket to use',
      '- `/ws/updates` - **read-only observer.** Receives every accepted reading. Use this one to display data.',
      '- `/ws/client` - **browser publisher.** Pushes only; it receives nothing back, not even its own messages.',
      '- `/ws/device` - the ESP32-S3. Publishes only, same as `/ws/client`. Exclusive: a new device socket closes the previous one with code 4000.',
      '- `/` or `/ws/relay` - the one exception: publishes **and** observes. Kept for the original `new WebSocket(\'ws://host:8080\')`, and it never evicts the sensor.',
      '',
      '### Hosting notes',
      'The Cloudflare Worker build (`worker/`) routes everything through one Durable Object named `thermal-relay`, which owns the sockets and the stored reading so fan-out survives multiple isolates. On Workers the SSE endpoint is not available (it would pin a live isolate); use the WebSocket there.',
    ].join('\n'),
  },
  servers: [
    { url: 'http://localhost:8080', description: 'Local Node server' },
    { url: 'http://localhost:8787', description: 'Local wrangler dev' },
    { url: BASE_URL, description: 'Cloudflare Workers (deployed)' },
  ],
  tags: [
    { name: 'websocket', description: 'WebSocket endpoints. Sockets either publish a reading or observe them, never both.' },
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
                    deviceConnected: { type: 'boolean', description: 'True while an ESP32-S3 holds the /ws/device slot' },
                    publishers: {
                      type: 'object',
                      description: 'Sockets that may push. Counted but never sent data.',
                      properties: {
                        device: { type: 'integer' },
                        client: { type: 'integer' },
                      },
                    },
                    subscribers: {
                      type: 'object',
                      description: 'Read-only observers receiving the broadcast',
                      properties: {
                        websocket: { type: 'integer', description: 'Total observer WebSockets' },
                        updates: { type: 'integer', description: 'Sockets on /ws/updates' },
                        relay: { type: 'integer' },
                        sse: { type: 'integer', description: 'Always 0 on Cloudflare, which does not support SSE' },
                      },
                    },
                    latestAt: { type: 'integer', nullable: true, description: 'Epoch ms of last reading' },
                    persistence: {
                      type: 'object',
                      description:
                        'Durable Object storage status. The current reading is always served from memory, so `degraded` only means the last value is not persisted, not that the relay is down.',
                      properties: {
                        degraded: { type: 'boolean', description: 'True when storage writes are being refused (Free plan row budget spent). Resets 00:00 UTC.' },
                        lastPersistedAt: { type: 'integer', nullable: true, description: 'Epoch ms of the last successful write' },
                        intervalMs: { type: 'integer', example: 15000, description: 'Throttle between storage writes' },
                      },
                    },
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
    '/ws/updates': wsOperation({
      summary: 'Observer socket: read-only feed of every accepted reading',
      url: updatesWsUrl(),
      role: '**Read-only.** Receives every accepted reading, from the ESP32-S3, from browsers, and from HTTP posts. Cannot publish.',
      sends: 'Nothing. Anything sent here is refused: the socket is closed with code 4003 and the stored value is left untouched.',
      receives: 'Every accepted reading as JSON text. The last stored reading is replayed on connect, so a page renders before the next sample arrives.',
      example:
        "const feed = new WebSocket('" + updatesWsUrl() + "');\n" +
        'feed.onmessage = (e) => render(JSON.parse(e.data));\n' +
        'feed.onopen = () => console.log("listening");',
      tags: ['websocket'],
      notes: [
        'This is the socket to use for anything that displays data. Open as many as you like; none of them evict each other.',
        'A reading is only ever overwritten by a publisher, so an observer can never corrupt the feed.',
      ],
    }),
    '/ws/client': wsOperation({
      summary: 'Client publisher socket: push telemetry',
      url: clientWsUrl(),
      role: '**Publisher.** A browser pushes readings here. Receives nothing back, so it never sees an echo of its own message.',
      sends: 'A telemetry object: { "ts": <ms>, "thermal": { "max": <number> }, "targets": [ { "label": "...", "value": <number> } ] }',
      receives: 'Nothing. To display data, open a second socket on /ws/updates.',
      example:
        "const pub = new WebSocket('" + clientWsUrl() + "');\n" +
        'pub.onopen = () =>\n' +
        "  pub.send(JSON.stringify({ thermal: { max: 68.4 }, targets: [{ label: 'Heater', value: 68.4 }] }));",
      tags: ['websocket'],
      notes: [
        'A payload sent here is stored as the latest reading and broadcast to every observer, exactly like a reading from the ESP32-S3.',
        'send() is silently dropped while the socket is still CONNECTING. Wait for onopen, or check `readyState === 1`.',
      ],
    }),
    '/ws/device': wsOperation({
      summary: 'ESP32-S3 socket: push telemetry',
      url: deviceWsUrl(),
      role: 'The sensor. Publishes readings; receives nothing, so it never sees an echo of its own data.',
      sends: 'A telemetry object: { "ts": <ms>, "thermal": { "max": <number> }, "targets": [ { "label": "...", "value": <number> } ] }',
      receives: 'Nothing. Device sockets are publish-only.',
      example:
        '// ESP32 / Arduino\n' +
        'WebSocketClient ws("' + deviceWsUrl() + '");\n' +
        'ws.connect();\n' +
        'ws.println("{\\"ts\\":" + millis() + ",\\"thermal\\":{\\"max\\":68.4}}");',
      tags: ['websocket'],
      notes: [
        'Connecting a second device socket closes the first with code 4000, so the sensor slot stays exclusive.',
        'A bare upgrade to / is accepted as a relay socket and does not evict the device.',
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
    principle: 'A socket either publishes or observes, never both, so no publisher sees an echo of its own message.',
    endpoints: {
      '/ws/updates': {
        url: updatesWsUrl(),
        role: 'Observer',
        publishes: false,
        receivesBroadcast: true,
        sends: 'Nothing; a push is refused with close code 4003 and the stored value is untouched.',
        receives: 'Every accepted reading; the stored reading is replayed on connect.',
      },
      '/ws/client': {
        url: clientWsUrl(),
        role: 'Browser publisher',
        publishes: true,
        receivesBroadcast: false,
        sends: 'Telemetry object; stored as latest and broadcast to every observer.',
        receives: 'Nothing.',
      },
      '/ws/device': {
        url: deviceWsUrl(),
        role: 'ESP32-S3',
        publishes: true,
        receivesBroadcast: false,
        sends: 'Telemetry object.',
        receives: 'Nothing. Exclusive slot: a second device closes the first with code 4000.',
      },
      '/ws/relay': {
        url: BASE_URL.replace(/^http/, 'ws') + WS_PATHS.relay,
        role: 'Relay (legacy path from the original script)',
        publishes: true,
        receivesBroadcast: true,
        sends: 'Telemetry object.',
        receives: 'Every accepted reading.',
        note: 'The only socket that does both. Never evicts the device socket. Same behaviour on the bare "/" path.',
      },
    },
  },
};
