// Single source of truth for the public host. Change BASE_URL here and every consumer
// follows: the OpenAPI spec (both builds), the Worker's info endpoint, the local banner.
export const BASE_URL = 'https://ws-node.sireemmy12.workers.dev';

// WebSocket routes are path-based on every runtime: local Node, wrangler dev, and Cloudflare.
export const WS_PATHS = {
  device: '/ws/device', // ESP32-S3 pushes telemetry here
  client: '/ws/client', // frontend subscribes and pushes here
  relay: '/ws/relay', // same, under an explicit name
};

// The bare '/' path also accepts an upgrade, because the original script connected to the
// host root with no path. It is a relay socket: it never evicts the device slot.
export const ROOT_WS_PATH = '/';

export const deviceWsUrl = (base = BASE_URL) =>
  `${base.replace(/^http/, 'ws')}${WS_PATHS.device}`;

export const clientWsUrl = (base = BASE_URL) =>
  `${base.replace(/^http/, 'ws')}${WS_PATHS.client}`;
