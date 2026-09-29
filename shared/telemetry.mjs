// Shared between the local Node server (require) and the Cloudflare Worker (import).
// ESM on purpose: Node 24 can require() ESM, and Wrangler bundles ESM natively.

export function toNumberOrNull(v) {
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

// Normalizes whatever the ESP32 sends into the canonical shape the frontend receives.
export function normalize(raw) {
  const ts = Number(raw?.ts);
  return {
    ts: Number.isFinite(ts) ? ts : Date.now(),
    thermal: { max: toNumberOrNull(raw?.thermal?.max) },
    targets: Array.isArray(raw?.targets) ? raw.targets : [],
  };
}

export function formatTargets(targets) {
  if (!Array.isArray(targets) || targets.length === 0) return 'none';
  return targets
    .map((t) => `${t.label ?? t.name ?? 'target'} ${t.value ?? t.temp ?? '?'}degC`)
    .join(', ');
}

export function describe(p) {
  return `[${p.ts}ms] Thermal Max: ${p.thermal.max ?? '?'}degC | Targets: ${formatTargets(p.targets)}`;
}
