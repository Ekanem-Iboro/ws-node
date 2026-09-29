// Single source of truth for the public host. Change BASE_URL here and every consumer
// follows: the OpenAPI spec (both builds), the Worker's info endpoint, the local banner.
export const BASE_URL = 'https://ws-node.sireemmy12.workers.dev';

// WebSocket routes are path-based on every runtime: local Node, wrangler dev, and Cloudflare.
// A socket either publishes or observes. Publishers never receive, so no socket sees an
// echo of its own message.
export const WS_PATHS = {
  device: '/ws/device', // ESP32-S3 publishes (push-only, exclusive slot)
  client: '/ws/client', // browser publisher (push-only)
  updates: '/ws/updates', // read-only observer: receives everything, cannot publish
  relay: '/ws/relay', // legacy path from the original script: publishes and observes
};

// The bare '/' path also accepts an upgrade, because the original script connected to the
// host root with no path. It is a relay socket: it never evicts the device slot.
export const ROOT_WS_PATH = '/';

export const deviceWsUrl = (base = BASE_URL) =>
  `${base.replace(/^http/, 'ws')}${WS_PATHS.device}`;

export const clientWsUrl = (base = BASE_URL) =>
  `${base.replace(/^http/, 'ws')}${WS_PATHS.client}`;

export const updatesWsUrl = (base = BASE_URL) =>
  `${base.replace(/^http/, 'ws')}${WS_PATHS.updates}`;
