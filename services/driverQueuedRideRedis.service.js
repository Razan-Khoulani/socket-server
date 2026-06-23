const { ensureClient } = require("./redisClient.service");

const DRIVER_QUEUED_RIDE_REDIS_ENABLED = String(
  process.env.DRIVER_QUEUED_RIDE_REDIS_ENABLED || "0"
) === "1";
const DRIVER_QUEUED_RIDE_REDIS_TTL_S = Number.isFinite(
  Number(process.env.DRIVER_QUEUED_RIDE_REDIS_TTL_S)
)
  ? Math.max(60, Math.floor(Number(process.env.DRIVER_QUEUED_RIDE_REDIS_TTL_S)))
  : 900;
const DRIVER_QUEUED_RIDE_REDIS_FLUSH_MS = Number.isFinite(
  Number(process.env.DRIVER_QUEUED_RIDE_REDIS_FLUSH_MS)
)
  ? Math.max(25, Math.floor(Number(process.env.DRIVER_QUEUED_RIDE_REDIS_FLUSH_MS)))
  : 150;
const DRIVER_QUEUED_RIDE_KEY_PREFIX =
  process.env.DRIVER_QUEUED_RIDE_KEY_PREFIX || "driver:queued-ride:";
const DRIVER_QUEUED_RIDE_CLEARED_KEY_PREFIX =
  process.env.DRIVER_QUEUED_RIDE_CLEARED_KEY_PREFIX || "driver:queued-ride:cleared:";

const pendingQueuedRides = new Map();
const flushTimers = new Map();
let initErrorLogged = false;

const isEnabled = () => DRIVER_QUEUED_RIDE_REDIS_ENABLED;

const toNumber = (value) => {
  if (value === null || value === undefined || value === "") return null;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
};

const logInitErrorOnce = (error) => {
  if (initErrorLogged) return;
  initErrorLogged = true;
  console.warn(
    "[driverQueuedRideRedis] disabled:",
    error?.message || error || "Redis init failed"
  );
};

const buildQueuedRideKey = (driverId) => {
  const safeDriverId = toNumber(driverId);
  return safeDriverId ? `${DRIVER_QUEUED_RIDE_KEY_PREFIX}${safeDriverId}` : null;
};

const buildClearedKey = (driverId) => {
  const safeDriverId = toNumber(driverId);
  return safeDriverId ? `${DRIVER_QUEUED_RIDE_CLEARED_KEY_PREFIX}${safeDriverId}` : null;
};

const normalizeQueuedRide = (driverId, queuedRide = {}) => {
  const safeDriverId = toNumber(driverId);
  const safeRideId = toNumber(queuedRide?.ride_id);
  if (
    !safeDriverId ||
    !safeRideId ||
    !queuedRide ||
    typeof queuedRide !== "object" ||
    Array.isArray(queuedRide)
  ) {
    return null;
  }

  return {
    driver_id: safeDriverId,
    queued_ride: {
      ...queuedRide,
      ride_id: safeRideId,
    },
    shadow_synced_at: Date.now(),
  };
};

const getFlushEntry = (driverId) => {
  const safeDriverId = toNumber(driverId);
  return safeDriverId ? flushTimers.get(safeDriverId) ?? null : null;
};

const clearFlushTimer = (driverId) => {
  const safeDriverId = toNumber(driverId);
  if (!safeDriverId) return;

  const timer = getFlushEntry(safeDriverId);
  if (!timer) return;

  clearTimeout(timer);
  flushTimers.delete(safeDriverId);
};

const maybeClearFlushTimer = (driverId) => {
  const safeDriverId = toNumber(driverId);
  if (!safeDriverId) return;
  if (pendingQueuedRides.has(safeDriverId)) return;
  clearFlushTimer(safeDriverId);
};

const readJsonValue = async (key) => {
  if (!key || !isEnabled()) return null;

  try {
    const client = await ensureClient();
    if (!client) return null;

    const raw = await client.get(key);
    if (!raw || typeof raw !== "string") return null;

    return JSON.parse(raw);
  } catch (_) {
    return null;
  }
};

const flushDriverQueuedRide = async (driverId) => {
  const safeDriverId = toNumber(driverId);
  if (!safeDriverId || !isEnabled()) return false;

  clearFlushTimer(safeDriverId);

  const queuedRidePayload = pendingQueuedRides.get(safeDriverId) ?? null;
  if (!queuedRidePayload) return false;

  pendingQueuedRides.delete(safeDriverId);

  try {
    const client = await ensureClient();
    if (!client) {
      logInitErrorOnce(new Error("Redis client unavailable"));
      return false;
    }

    const multi = client.multi();
    const queuedRideKey = buildQueuedRideKey(safeDriverId);
    const clearedKey = buildClearedKey(safeDriverId);

    if (queuedRideKey) {
      if (queuedRidePayload?.queued_ride?.ride_id) {
        multi.set(queuedRideKey, JSON.stringify(queuedRidePayload), {
          EX: DRIVER_QUEUED_RIDE_REDIS_TTL_S,
        });
      } else {
        multi.del(queuedRideKey);
      }
    }

    if (clearedKey && queuedRidePayload?.queued_ride?.ride_id) {
      multi.del(clearedKey);
    }

    await multi.exec();
    return true;
  } catch (error) {
    logInitErrorOnce(error);
    return false;
  }
};

const scheduleFlush = (driverId) => {
  const safeDriverId = toNumber(driverId);
  if (!safeDriverId || !isEnabled() || getFlushEntry(safeDriverId)) return;

  const timer = setTimeout(() => {
    flushTimers.delete(safeDriverId);
    void flushDriverQueuedRide(safeDriverId);
  }, DRIVER_QUEUED_RIDE_REDIS_FLUSH_MS);

  timer.unref?.();
  flushTimers.set(safeDriverId, timer);
};

const scheduleDriverQueuedRideSync = (driverId, queuedRide = {}) => {
  if (!isEnabled()) return;

  const normalizedPayload = normalizeQueuedRide(driverId, queuedRide);
  if (!normalizedPayload) return;

  pendingQueuedRides.set(normalizedPayload.driver_id, normalizedPayload);
  scheduleFlush(normalizedPayload.driver_id);
};

const clearDriverQueuedRide = async (driverId) => {
  const safeDriverId = toNumber(driverId);
  if (!safeDriverId || !isEnabled()) return false;

  pendingQueuedRides.delete(safeDriverId);
  maybeClearFlushTimer(safeDriverId);

  try {
    const client = await ensureClient();
    if (!client) {
      logInitErrorOnce(new Error("Redis client unavailable"));
      return false;
    }

    const multi = client.multi();
    const queuedRideKey = buildQueuedRideKey(safeDriverId);
    const clearedKey = buildClearedKey(safeDriverId);
    if (queuedRideKey) multi.del(queuedRideKey);
    if (clearedKey) {
      multi.set(clearedKey, String(Date.now()), {
        EX: DRIVER_QUEUED_RIDE_REDIS_TTL_S,
      });
    }
    await multi.exec();
    return true;
  } catch (error) {
    logInitErrorOnce(error);
    return false;
  }
};

const getDriverQueuedRide = async (driverId) => {
  const parsed = await readJsonValue(buildQueuedRideKey(driverId));
  if (!parsed?.queued_ride?.ride_id) return null;
  return normalizeQueuedRide(driverId, parsed.queued_ride);
};

const hasDriverQueuedRideClearedMarker = async (driverId) => {
  const key = buildClearedKey(driverId);
  if (!key || !isEnabled()) return false;

  try {
    const client = await ensureClient();
    if (!client) return false;
    return !!(await client.exists(key));
  } catch (_) {
    return false;
  }
};

module.exports = {
  isEnabled,
  scheduleDriverQueuedRideSync,
  clearDriverQueuedRide,
  getDriverQueuedRide,
  hasDriverQueuedRideClearedMarker,
};
