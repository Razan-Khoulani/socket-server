const { ensureClient } = require("./redisClient.service");

const ACTIVE_RIDE_REDIS_ENABLED = String(
  process.env.ACTIVE_RIDE_REDIS_ENABLED || "0"
) === "1";
const ACTIVE_RIDE_REDIS_TTL_S = Number.isFinite(
  Number(process.env.ACTIVE_RIDE_REDIS_TTL_S)
)
  ? Math.max(60, Math.floor(Number(process.env.ACTIVE_RIDE_REDIS_TTL_S)))
  : 900;
const ACTIVE_RIDE_REDIS_FLUSH_MS = Number.isFinite(
  Number(process.env.ACTIVE_RIDE_REDIS_FLUSH_MS)
)
  ? Math.max(25, Math.floor(Number(process.env.ACTIVE_RIDE_REDIS_FLUSH_MS)))
  : 100;
const ACTIVE_RIDE_BY_DRIVER_KEY_PREFIX =
  process.env.ACTIVE_RIDE_BY_DRIVER_KEY_PREFIX || "driver:active-ride:";
const ACTIVE_DRIVER_BY_RIDE_KEY_PREFIX =
  process.env.ACTIVE_DRIVER_BY_RIDE_KEY_PREFIX || "ride:active-driver:";
const ACTIVE_RIDE_DRIVER_CLEARED_KEY_PREFIX =
  process.env.ACTIVE_RIDE_DRIVER_CLEARED_KEY_PREFIX || "driver:active-ride:cleared:";
const ACTIVE_RIDE_RIDE_CLEARED_KEY_PREFIX =
  process.env.ACTIVE_RIDE_RIDE_CLEARED_KEY_PREFIX || "ride:active-driver:cleared:";

const pendingActiveRides = new Map();
const flushTimers = new Map();
let initErrorLogged = false;

const isEnabled = () => ACTIVE_RIDE_REDIS_ENABLED;

const toNumber = (value) => {
  if (value === null || value === undefined || value === "") return null;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
};

const logInitErrorOnce = (error) => {
  if (initErrorLogged) return;
  initErrorLogged = true;
  console.warn(
    "[activeRideRedis] disabled:",
    error?.message || error || "Redis init failed"
  );
};

const buildDriverKey = (driverId) => {
  const safeDriverId = toNumber(driverId);
  return safeDriverId ? `${ACTIVE_RIDE_BY_DRIVER_KEY_PREFIX}${safeDriverId}` : null;
};

const buildRideKey = (rideId) => {
  const safeRideId = toNumber(rideId);
  return safeRideId ? `${ACTIVE_DRIVER_BY_RIDE_KEY_PREFIX}${safeRideId}` : null;
};

const buildDriverClearedKey = (driverId) => {
  const safeDriverId = toNumber(driverId);
  return safeDriverId ? `${ACTIVE_RIDE_DRIVER_CLEARED_KEY_PREFIX}${safeDriverId}` : null;
};

const buildRideClearedKey = (rideId) => {
  const safeRideId = toNumber(rideId);
  return safeRideId ? `${ACTIVE_RIDE_RIDE_CLEARED_KEY_PREFIX}${safeRideId}` : null;
};

const normalizeActiveRide = (driverId, rideId, meta = {}) => {
  const safeDriverId = toNumber(driverId);
  const safeRideId = toNumber(rideId);
  if (!safeDriverId || !safeRideId) {
    return null;
  }

  const now = Date.now();
  return {
    driver_id: safeDriverId,
    ride_id: safeRideId,
    set_at: toNumber(meta?.set_at) ?? now,
    touched_at: toNumber(meta?.touched_at) ?? now,
    shadow_synced_at: now,
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
  if (pendingActiveRides.has(safeDriverId)) return;
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

const findPendingActiveRideByRideId = (rideId) => {
  const safeRideId = toNumber(rideId);
  if (!safeRideId) return null;

  for (const payload of pendingActiveRides.values()) {
    if (toNumber(payload?.ride_id) === safeRideId) {
      return payload;
    }
  }

  return null;
};

const flushActiveRide = async (driverId) => {
  const safeDriverId = toNumber(driverId);
  if (!safeDriverId || !isEnabled()) return false;

  clearFlushTimer(safeDriverId);

  const payload = pendingActiveRides.get(safeDriverId) ?? null;
  if (!payload?.ride_id) return false;

  pendingActiveRides.delete(safeDriverId);

  try {
    const client = await ensureClient();
    if (!client) {
      logInitErrorOnce(new Error("Redis client unavailable"));
      return false;
    }

    const multi = client.multi();
    const driverKey = buildDriverKey(safeDriverId);
    const rideKey = buildRideKey(payload.ride_id);
    const driverClearedKey = buildDriverClearedKey(safeDriverId);
    const rideClearedKey = buildRideClearedKey(payload.ride_id);

    if (driverKey) {
      multi.set(driverKey, JSON.stringify(payload), {
        EX: ACTIVE_RIDE_REDIS_TTL_S,
      });
    }

    if (rideKey) {
      multi.set(rideKey, JSON.stringify(payload), {
        EX: ACTIVE_RIDE_REDIS_TTL_S,
      });
    }

    if (driverClearedKey) {
      multi.del(driverClearedKey);
    }

    if (rideClearedKey) {
      multi.del(rideClearedKey);
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
    void flushActiveRide(safeDriverId);
  }, ACTIVE_RIDE_REDIS_FLUSH_MS);

  timer.unref?.();
  flushTimers.set(safeDriverId, timer);
};

const scheduleActiveRideSync = (driverId, rideId, meta = {}) => {
  if (!isEnabled()) return;

  const normalizedPayload = normalizeActiveRide(driverId, rideId, meta);
  if (!normalizedPayload) return;

  pendingActiveRides.set(normalizedPayload.driver_id, normalizedPayload);
  scheduleFlush(normalizedPayload.driver_id);
};

const resolveRideIdForDriverClear = async (driverId, providedRideId = null) => {
  const safeRideId = toNumber(providedRideId);
  if (safeRideId) return safeRideId;

  const pending = pendingActiveRides.get(toNumber(driverId));
  if (pending?.ride_id) return toNumber(pending.ride_id);

  const existing = await readJsonValue(buildDriverKey(driverId));
  return toNumber(existing?.ride_id);
};

const resolveDriverIdForRideClear = async (rideId, providedDriverId = null) => {
  const safeDriverId = toNumber(providedDriverId);
  if (safeDriverId) return safeDriverId;

  const pending = findPendingActiveRideByRideId(rideId);
  if (pending?.driver_id) return toNumber(pending.driver_id);

  const existing = await readJsonValue(buildRideKey(rideId));
  return toNumber(existing?.driver_id);
};

const clearActiveRideByDriver = async (driverId, rideId = null) => {
  const safeDriverId = toNumber(driverId);
  if (!safeDriverId || !isEnabled()) return false;

  const safeRideId = await resolveRideIdForDriverClear(safeDriverId, rideId);

  pendingActiveRides.delete(safeDriverId);
  maybeClearFlushTimer(safeDriverId);

  try {
    const client = await ensureClient();
    if (!client) {
      logInitErrorOnce(new Error("Redis client unavailable"));
      return false;
    }

    const multi = client.multi();
    const driverKey = buildDriverKey(safeDriverId);
    const driverClearedKey = buildDriverClearedKey(safeDriverId);

    if (driverKey) multi.del(driverKey);
    if (driverClearedKey) {
      multi.set(driverClearedKey, String(Date.now()), {
        EX: ACTIVE_RIDE_REDIS_TTL_S,
      });
    }

    if (safeRideId) {
      const rideKey = buildRideKey(safeRideId);
      const rideClearedKey = buildRideClearedKey(safeRideId);
      if (rideKey) multi.del(rideKey);
      if (rideClearedKey) {
        multi.set(rideClearedKey, String(Date.now()), {
          EX: ACTIVE_RIDE_REDIS_TTL_S,
        });
      }
    }

    await multi.exec();
    return true;
  } catch (error) {
    logInitErrorOnce(error);
    return false;
  }
};

const clearActiveRideByRideId = async (rideId, driverId = null) => {
  const safeRideId = toNumber(rideId);
  if (!safeRideId || !isEnabled()) return false;

  const safeDriverId = await resolveDriverIdForRideClear(safeRideId, driverId);
  if (safeDriverId) {
    pendingActiveRides.delete(safeDriverId);
    maybeClearFlushTimer(safeDriverId);
  }

  try {
    const client = await ensureClient();
    if (!client) {
      logInitErrorOnce(new Error("Redis client unavailable"));
      return false;
    }

    const multi = client.multi();
    const rideKey = buildRideKey(safeRideId);
    const rideClearedKey = buildRideClearedKey(safeRideId);

    if (rideKey) multi.del(rideKey);
    if (rideClearedKey) {
      multi.set(rideClearedKey, String(Date.now()), {
        EX: ACTIVE_RIDE_REDIS_TTL_S,
      });
    }

    if (safeDriverId) {
      const driverKey = buildDriverKey(safeDriverId);
      const driverClearedKey = buildDriverClearedKey(safeDriverId);
      if (driverKey) multi.del(driverKey);
      if (driverClearedKey) {
        multi.set(driverClearedKey, String(Date.now()), {
          EX: ACTIVE_RIDE_REDIS_TTL_S,
        });
      }
    }

    await multi.exec();
    return true;
  } catch (error) {
    logInitErrorOnce(error);
    return false;
  }
};

const getActiveRideByDriver = async (driverId) => {
  const parsed = await readJsonValue(buildDriverKey(driverId));
  return normalizeActiveRide(parsed?.driver_id, parsed?.ride_id, parsed);
};

const getActiveDriverByRide = async (rideId) => {
  const parsed = await readJsonValue(buildRideKey(rideId));
  return normalizeActiveRide(parsed?.driver_id, parsed?.ride_id, parsed);
};

const hasActiveRideClearedMarkerByDriver = async (driverId) => {
  const key = buildDriverClearedKey(driverId);
  if (!key || !isEnabled()) return false;

  try {
    const client = await ensureClient();
    if (!client) return false;
    return !!(await client.exists(key));
  } catch (_) {
    return false;
  }
};

const hasActiveRideClearedMarkerByRide = async (rideId) => {
  const key = buildRideClearedKey(rideId);
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
  scheduleActiveRideSync,
  clearActiveRideByDriver,
  clearActiveRideByRideId,
  getActiveRideByDriver,
  getActiveDriverByRide,
  hasActiveRideClearedMarkerByDriver,
  hasActiveRideClearedMarkerByRide,
};
