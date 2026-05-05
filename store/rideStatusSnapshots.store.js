// store/rideStatusSnapshots.store.js
const rideStatusSnapshots = new Map(); // rideId -> snapshot

const SNAPSHOT_TTL_MS = Number.isFinite(
  Number(process.env.RIDE_STATUS_SNAPSHOT_TTL_MS)
)
  ? Number(process.env.RIDE_STATUS_SNAPSHOT_TTL_MS)
  : 6 * 60 * 60 * 1000; // 6 hours

const toNumber = (v) => {
  if (v === null || v === undefined || v === "") return null;
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
};

const statusOrder = (status) => {
  const s = toNumber(status);
  if (s == null) return null;
  if (s === 4 || s === 10) return 100;
  return s;
};

const scheduleCleanup = (rideId) => {
  const current = rideStatusSnapshots.get(rideId);
  if (!current) return;
  if (current._cleanupTimer) clearTimeout(current._cleanupTimer);

  const timer = setTimeout(() => {
    const latest = rideStatusSnapshots.get(rideId);
    if (!latest) return;
    if (latest._cleanupTimer === timer) {
      rideStatusSnapshots.delete(rideId);
    }
  }, SNAPSHOT_TTL_MS);

  rideStatusSnapshots.set(rideId, {
    ...current,
    _cleanupTimer: timer,
  });
};

const getRideStatusSnapshot = (rideId) => {
  const safeRideId = toNumber(rideId);
  if (!safeRideId) return null;
  const current = rideStatusSnapshots.get(safeRideId);
  if (!current) return null;
  const { _cleanupTimer, ...publicSnapshot } = current;
  return publicSnapshot;
};

const upsertRideStatusSnapshot = (rideId, patch = {}) => {
  const safeRideId = toNumber(rideId);
  if (!safeRideId) return null;

  const current = rideStatusSnapshots.get(safeRideId) || {};

  const incomingStatus = toNumber(patch?.ride_status);
  const currentStatus = toNumber(current?.ride_status);
  const incomingOrder = statusOrder(incomingStatus);
  const currentOrder = statusOrder(currentStatus);

  if (
    incomingOrder !== null &&
    currentOrder !== null &&
    incomingOrder < currentOrder
  ) {
    scheduleCleanup(safeRideId);
    return getRideStatusSnapshot(safeRideId);
  }

  // إضافة التقييم في حالة وجوده في `patch`
  const next = {
    ...current,
    ...patch,
    ride_id: safeRideId,
    ride_status: incomingStatus ?? currentStatus ?? null,
    rating: patch?.rating ?? current?.rating ?? null,  // إضافة التقييم هنا
    updated_at: toNumber(patch?.updated_at) ?? Date.now(),
  };

  rideStatusSnapshots.set(safeRideId, next);
  scheduleCleanup(safeRideId);
  return getRideStatusSnapshot(safeRideId);
};

const clearRideStatusSnapshot = (rideId) => {
  const safeRideId = toNumber(rideId);
  if (!safeRideId) return;
  const current = rideStatusSnapshots.get(safeRideId);
  if (current?._cleanupTimer) {
    clearTimeout(current._cleanupTimer);
  }
  rideStatusSnapshots.delete(safeRideId);
};

module.exports = {
  getRideStatusSnapshot,
  upsertRideStatusSnapshot,
  clearRideStatusSnapshot,
};
