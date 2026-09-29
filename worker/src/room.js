import { DurableObject } from 'cloudflare:workers';
import { normalize, describe } from '../../shared/telemetry.mjs';

const LAST_KEY = 'telemetry:latest';

// Roles decide what a socket may do. A socket either publishes or observes, never both.
//   device  - ESP32-S3. Publishes only. Does not receive (no echo of its own reading).
//   client  - browser publisher. Publishes only; its own pushes do not come back to it.
//   relay   - the bare '/' path from the original script. Publishes and observes, and never
//             evicts, so a frontend pasting ws://<host>/ cannot kick the sensor.
//   updates - read-only observer. Receives every accepted reading and cannot corrupt state.
const ROLES = {
  device: { publishes: true, receivesBroadcast: false },
  client: { publishes: true, receivesBroadcast: false },
  relay: { publishes: true, receivesBroadcast: true },
  updates: { publishes: false, receivesBroadcast: true },
};

// One instance of this class is the whole relay: it holds the device socket, every
// subscriber socket, and the last reading. Workers isolates are ephemeral and there is
// no shared memory between them, so all coordination has to happen in here.
export class Room extends DurableObject {
  constructor(ctx, env) {
    super(ctx, env);
    this.ctx = ctx;
    // Constructor must stay cheap: it re-runs every time the object wakes from hibernation.
    this.ctx.blockConcurrencyWhile(async () => {
      const stored = await this.ctx.storage.get(LAST_KEY);
      this.latest = stored?.data ?? null;
      this.latestAt = stored?.at ?? null;
    });
  }

  async fetch(request) {
    const url = new URL(request.url);
    switch (url.pathname) {
      case '/ws/device':
        return this.acceptSocket(request, 'device');
      case '/ws/client':
        return this.acceptSocket(request, 'client');
      case '/ws/relay':
        return this.acceptSocket(request, 'relay');
      case '/ws/updates':
        return this.acceptSocket(request, 'updates');
      case '/ingest':
        return request.method === 'POST' ? this.ingestViaHttp(request) : json({ error: 'Method not allowed' }, 405);
      case '/latest':
        return json({ data: this.latest, latestAt: this.latestAt });
      case '/health':
        return json({
          status: 'ok',
          runtime: 'cloudflare-worker',
          deviceConnected: this.ctx.getWebSockets('device').length > 0,
          publishers: {
            device: this.ctx.getWebSockets('device').length,
            client: this.ctx.getWebSockets('client').length,
          },
          subscribers: {
            websocket: this.subscriberCount(),
            updates: this.ctx.getWebSockets('updates').length,
            relay: this.ctx.getWebSockets('relay').length,
            sse: 0,
          },
          latestAt: this.latestAt,
        });
      default:
        return json({ error: 'Not found' }, 404);
    }
  }

  // --- sockets ------------------------------------------------------------
  acceptSocket(request, role) {
    if ((request.headers.get('Upgrade') || '').toLowerCase() !== 'websocket') {
      return json(
        {
          error: 'This endpoint requires a WebSocket upgrade',
          hint: 'Connect a WebSocket to this path, e.g. new WebSocket("wss://<host>/ws/client"). A plain HTTP GET cannot join it.',
        },
        426
      );
    }

    // Only a device claims exclusive ownership of the sensor slot; a reconnecting sensor
    // replaces the old one. client and relay never evict, so extra tabs are harmless.
    if (role === 'device') {
      for (const ws of this.ctx.getWebSockets('device')) {
        ws.close(4000, 'replaced by a new device connection');
      }
    }

    const [client, server] = Object.values(new WebSocketPair());
    // acceptWebSocket (not ws.accept()) is what allows this object to hibernate while
    // subscribers stay connected, so idle cost stays near zero.
    this.ctx.acceptWebSocket(server, [role]);
    server.serializeAttachment({ role, connectedAt: Date.now() });

    console.log(`${LABELS[role]} connected.`);
    // Replay the stored reading so an observer renders before the next sample arrives.
    // A publisher gets nothing back, so it never sees an echo of what it just sent.
    if (ROLES[role].receivesBroadcast && this.latest) server.send(JSON.stringify(this.latest));

    return new Response(null, { status: 101, webSocket: client });
  }

  // --- ingest -------------------------------------------------------------
  async ingestViaHttp(request) {
    let raw;
    try {
      raw = await request.json();
    } catch {
      return json({ error: 'Body must be valid JSON' }, 400);
    }
    if (!raw || typeof raw !== 'object' || Array.isArray(raw)) {
      return json({ error: 'Body must be a telemetry JSON object' }, 400);
    }
    const { payload } = await this.ingest(raw);
    return json({ accepted: true, ts: payload.ts }, 202);
  }

  // Every socket that receives the broadcast: the observers. Publishers are excluded, so a
  // socket never sees an echo of its own message.
  broadcastTargets() {
    return [
      ...this.ctx.getWebSockets('updates'),
      ...this.ctx.getWebSockets('relay'),
    ];
  }

  subscriberCount() {
    return this.broadcastTargets().length;
  }

  // The single fan-out point: every accepted reading, from any socket or from HTTP, ends here.
  async ingest(raw, source = 'unknown') {
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
        // subscriber vanished mid-send; nothing to clean up, hibernation handles it
      }
    }
    return { payload, notified };
  }

  // --- hibernation event handlers ----------------------------------------
  // Publish path. A socket whose role allows publishing is ingested exactly like the
  // original `wss.on('message')` script: parse, normalize, store, broadcast.
  async webSocketMessage(ws, message) {
    const { role } = ws.deserializeAttachment() ?? {};
    const label = LABELS[role] ?? 'Socket';

    // A read-only observer cannot corrupt state, so refuse instead of ingesting. Close the
    // socket so the mistake is obvious on the client rather than silently ignored forever.
    if (!ROLES[role]?.publishes) {
      console.error(`${label} is read-only and cannot push; closing.`);
      ws.close(4003, 'this endpoint is read-only');
      return;
    }

    const raw = typeof message === 'string' ? message : new TextDecoder().decode(message);
    let parsed;
    try {
      parsed = JSON.parse(raw);
    } catch {
      console.error(`Invalid JSON from ${label}:`, raw.slice(0, 200));
      return;
    }

    if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) {
      console.error(`Non-object payload from ${label}:`, raw.slice(0, 200));
      return;
    }

    await this.ingest(parsed, `${label} socket`);
  }

  async webSocketClose(ws, code) {
    const { role } = ws.deserializeAttachment() ?? {};
    // 1005 (no status) and 1006 (abnormal) are reserved and cannot be sent back; echoing
    // them throws InvalidAccessError. Modern compat dates auto-reply to close frames anyway.
    if (code >= 1000 && code <= 4999 && code !== 1005 && code !== 1006) {
      ws.close(code, 'closing');
    }
    if (ROLES[role]?.receivesBroadcast) {
      // the closing socket is still listed here, so don't count it
      console.log(`${LABELS[role]} disconnected (${Math.max(0, this.subscriberCount() - 1)} total).`);
    } else {
      console.log(`${LABELS[role] ?? 'Socket'} disconnected.`);
    }
  }

  async webSocketError(ws, error) {
    const { role } = ws.deserializeAttachment() ?? {};
    console.error(`${LABELS[role] ?? 'Socket'} error:`, error?.message ?? error);
  }
}

const LABELS = {
  device: 'ESP32-S3',
  client: 'Client publisher',
  relay: 'Relay client',
  updates: 'Observer',
};

function json(body, status = 200) {
  return new Response(JSON.stringify(body), {
    status,
    headers: { 'content-type': 'application/json; charset=utf-8' },
  });
}
