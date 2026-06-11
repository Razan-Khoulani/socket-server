const pricingSnapshots = new Map(); // rideId -> snapshot

const PRICING_SNAPSHOT_TTL_MS = Number.isFinite(
  Number(process.env.PRICING_SNAPSHOT_TTL_MS)
)
  ? Math.max(60_000, Number(process.env.PRICING_SNAPSHOT_TTL_MS))
  : 15 * 60 * 1000;

const toNumber = (v) => {
  if (v === null || v === undefined || v === "") return null;
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
};

const scheduleCleanup = (rideId) => {
  const current = pricingSnapshots.get(rideId);
  if (!current) return;

  if (current._cleanupTimer) {
    clearTimeout(current._cleanupTimer);
  }

  const timer = setTimeout(() => {
    const latest = pricingSnapshots.get(rideId);
    if (!latest) return;

    if (latest._cleanupTimer === timer) {
      pricingSnapshots.delete(rideId);
    }
  }, PRICING_SNAPSHOT_TTL_MS);

  pricingSnapshots.set(rideId, {
    ...current,
    _cleanupTimer: timer,
  });
};

const savePricingSnapshot = (rideId, snapshot = {}) => {
  const safeRideId = toNumber(rideId);
  if (!safeRideId || !snapshot || typeof snapshot !== "object") return null;

  const previous = pricingSnapshots.get(safeRideId) || {};
  const next = {
    ...previous,
    ...snapshot,
    ride_id: safeRideId,
    cached_at: Date.now(),
  };

  pricingSnapshots.set(safeRideId, next);
  scheduleCleanup(safeRideId);

  const { _cleanupTimer, ...publicSnapshot } = next;
  return publicSnapshot;
};

const getPricingSnapshot = (rideId) => {
  const safeRideId = toNumber(rideId);
  if (!safeRideId) return null;

  const current = pricingSnapshots.get(safeRideId);
  if (!current) return null;

  const { _cleanupTimer, ...publicSnapshot } = current;
  return publicSnapshot;
};

const clearPricingSnapshot = (rideId) => {
  const safeRideId = toNumber(rideId);
  if (!safeRideId) return;

  const current = pricingSnapshots.get(safeRideId);
  if (current?._cleanupTimer) {
    clearTimeout(current._cleanupTimer);
  }

  pricingSnapshots.delete(safeRideId);
};

module.exports = {
  savePricingSnapshot,
  getPricingSnapshot,
  clearPricingSnapshot,
};