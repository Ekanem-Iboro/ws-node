import { DurableObject } from 'cloudflare:workers';
import { normalize, describe } from '../../shared/telemetry.mjs';

const LAST_KEY = 'telemetry:latest';

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
      case '/ingest':
        return request.method === 'POST' ? this.ingestViaHttp(request) : json({ error: 'Method not allowed' }, 405);
      case '/latest':
        return json({ data: this.latest, latestAt: this.latestAt });
      case '/health':
        return json({
          status: 'ok',
          runtime: 'cloudflare-worker',
          deviceConnected: this.ctx.getWebSockets('device').length > 0,
          subscribers: { websocket: this.ctx.getWebSockets('client').length, sse: 0 },
          latestAt: this.latestAt,
        });
      default:
        return json({ error: 'Not found' }, 404);
    }
  }

  // --- sockets ------------------------------------------------------------
  acceptSocket(request, role) {
    if ((request.headers.get('Upgrade') || '').toLowerCase() !== 'websocket') {
      return json({ error: 'Expected a WebSocket upgrade' }, 426);
    }

    // Only one device may own the feed; a reconnect replaces the old socket.
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

    if (role === 'client') {
      console.log(`Frontend connected (${this.ctx.getWebSockets('client').length} total).`);
      if (this.latest) server.send(JSON.stringify(this.latest)); // replay current value
    } else {
      console.log('ESP32-S3 Connected!');
    }

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

  // The single fan-out point: every accepted reading, from either transport, ends here.
  async ingest(raw) {
    const payload = normalize(raw);
    this.latest = payload;
    this.latestAt = Date.now();

    await this.ctx.storage.put(LAST_KEY, { data: payload, at: this.latestAt });
    console.log(describe(payload));

    const msg = JSON.stringify(payload);
    let notified = 0;
    for (const ws of this.ctx.getWebSockets('client')) {
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
  async webSocketMessage(ws, message) {
    const { role } = ws.deserializeAttachment() ?? {};
    if (role !== 'device') return; // subscribers are read-only

    const raw = typeof message === 'string' ? message : new TextDecoder().decode(message);
    let parsed;
    try {
      parsed = JSON.parse(raw);
    } catch {
      console.error('Invalid JSON from ESP32-S3:', raw.slice(0, 200));
      return;
    }
    await this.ingest(parsed);
  }

  async webSocketClose(ws, code) {
    const { role } = ws.deserializeAttachment() ?? {};
    // 1005 (no status) and 1006 (abnormal) are reserved and cannot be sent back; echoing
    // them throws InvalidAccessError. Modern compat dates auto-reply to close frames anyway.
    if (code >= 1000 && code <= 4999 && code !== 1005 && code !== 1006) {
      ws.close(code, 'closing');
    }
    if (role === 'device') console.log('ESP32-S3 Disconnected.');
    else {
      // the closing socket is still listed here, so don't count it
      const remaining = Math.max(0, this.ctx.getWebSockets('client').length - 1);
      console.log(`Frontend disconnected (${remaining} total).`);
    }
  }

  async webSocketError(ws, error) {
    const { role } = ws.deserializeAttachment() ?? {};
    console.error(`${role ?? 'client'} socket error:`, error?.message ?? error);
  }
}

function json(body, status = 200) {
  return new Response(JSON.stringify(body), {
    status,
    headers: { 'content-type': 'application/json; charset=utf-8' },
  });
}
