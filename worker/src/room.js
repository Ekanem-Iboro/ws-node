import { DurableObject } from 'cloudflare:workers';
import { normalize, describe } from '../../shared/telemetry.mjs';

const LAST_KEY = 'telemetry:latest';

// Roles decide what a socket may do, not whether it can send. All three can push; the
// difference is who receives the broadcast and whether the socket evicts an earlier one.
//   device - ESP32-S3. Push telemetry. Does not receive the broadcast (no echo).
//   client - browser frontend. Pushes and receives everything.
//   relay  - the bare '/' path from the original script: pushes and receives everything,
//            and never evicts, so a frontend pasting ws://<host>/ cannot kick the sensor.
const ROLES = {
  device: { receivesBroadcast: false },
  client: { receivesBroadcast: true },
  relay: { receivesBroadcast: true },
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
      case '/ingest':
        return request.method === 'POST' ? this.ingestViaHttp(request) : json({ error: 'Method not allowed' }, 405);
      case '/latest':
        return json({ data: this.latest, latestAt: this.latestAt });
      case '/health':
        return json({
          status: 'ok',
          runtime: 'cloudflare-worker',
          deviceConnected: this.ctx.getWebSockets('device').length > 0,
          subscribers: {
            websocket: this.subscriberCount(),
            device: this.ctx.getWebSockets('device').length,
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
    // Replay the stored reading so a page renders before the next sample arrives.
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

  // Every socket that receives the broadcast: the frontends, not the sensor (so the ESP32
  // never sees an echo of its own reading).
  broadcastTargets() {
    return [
      ...this.ctx.getWebSockets('client'),
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
  // Push path. Any role may send: the ESP32 and the frontend are treated identically,
  // which is what the original `wss.on('message')` script did.
  async webSocketMessage(ws, message) {
    const { role } = ws.deserializeAttachment() ?? {};

    const raw = typeof message === 'string' ? message : new TextDecoder().decode(message);
    let parsed;
    try {
      parsed = JSON.parse(raw);
    } catch {
      console.error(`Invalid JSON from ${LABELS[role] ?? 'socket'}:`, raw.slice(0, 200));
      return;
    }

    if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) {
      console.error(`Non-object payload from ${LABELS[role] ?? 'socket'}:`, raw.slice(0, 200));
      return;
    }

    await this.ingest(parsed, `${LABELS[role] ?? 'socket'} socket`);
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
  client: 'Frontend',
  relay: 'Relay client',
};

function json(body, status = 200) {
  return new Response(JSON.stringify(body), {
    status,
    headers: { 'content-type': 'application/json; charset=utf-8' },
  });
}
