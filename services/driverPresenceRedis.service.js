const { ensureClient } = require("./redisClient.service");

const DRIVER_PRESENCE_REDIS_ENABLED = String(
  process.env.DRIVER_PRESENCE_REDIS_ENABLED || "0"
) === "1";
const DRIVER_PRESENCE_REDIS_TTL_S = Number.isFinite(
  Number(process.env.DRIVER_PRESENCE_REDIS_TTL_S)
)
  ? Math.max(30, Math.floor(Number(process.env.DRIVER_PRESENCE_REDIS_TTL_S)))
  : 180;
const DRIVER_PRESENCE_REDIS_FLUSH_MS = Number.isFinite(
  Number(process.env.DRIVER_PRESENCE_REDIS_FLUSH_MS)
)
  ? Math.max(25, Math.floor(Number(process.env.DRIVER_PRESENCE_REDIS_FLUSH_MS)))
  : 150;
const DRIVER_PRESENCE_KEY_PREFIX =
  process.env.DRIVER_PRESENCE_KEY_PREFIX || "driver:state:";
const DRIVER_ONLINE_SET_KEY =
  process.env.DRIVER_ONLINE_SET_KEY || "drivers:online";

const pendingStates = new Map();
const flushTimers = new Map();
let initErrorLogged = false;

const isEnabled = () => DRIVER_PRESENCE_REDIS_ENABLED;

const toNumber = (value) => {
  if (value === null || value === undefined || value === "") return null;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
};

const logInitErrorOnce = (error) => {
  if (initErrorLogged) return;
  initErrorLogged = true;
  console.warn(
    "[driverPresenceRedis] disabled:",
    error?.message || error || "Redis init failed"
  );
};

const buildDriverStateKey = (driverId) => {
  const safeDriverId = toNumber(driverId);
  return safeDriverId ? `${DRIVER_PRESENCE_KEY_PREFIX}${safeDriverId}` : null;
};

const normalizeDriverState = (driverId, state = {}) => {
  const safeDriverId = toNumber(driverId);
  if (!safeDriverId || !state || typeof state !== "object") return null;

  return {
    driver_id: safeDriverId,
    ...state,
    driver_id_shadow: safeDriverId,
    shadow_synced_at: Date.now(),
  };
};

const writeDriverState = async (driverId, state = {}) => {
  if (!isEnabled()) return false;

  const key = buildDriverStateKey(driverId);
  const normalizedState = normalizeDriverState(driverId, state);
  if (!key || !normalizedState) return false;

  try {
    const client = await ensureClient();
    if (!client) {
      logInitErrorOnce(new Error("Redis client unavailable"));
      return false;
    }

    const multi = client.multi();
    multi.set(key, JSON.stringify(normalizedState), {
      EX: DRIVER_PRESENCE_REDIS_TTL_S,
    });

    const isOnline = Number(normalizedState?.is_online ?? 1) === 1;
    if (isOnline) {
      multi.sAdd(DRIVER_ONLINE_SET_KEY, String(driverId));
    } else {
      multi.sRem(DRIVER_ONLINE_SET_KEY, String(driverId));
    }

    await multi.exec();
    return true;
  } catch (error) {
    logInitErrorOnce(error);
    return false;
  }
};

const flushDriverState = async (driverId) => {
  const safeDriverId = toNumber(driverId);
  if (!safeDriverId) return false;

  const state = pendingStates.get(safeDriverId);
  pendingStates.delete(safeDriverId);

  const timer = flushTimers.get(safeDriverId);
  if (timer) {
    clearTimeout(timer);
    flushTimers.delete(safeDriverId);
  }

  if (!state) return false;
  return writeDriverState(safeDriverId, state);
};

const scheduleDriverStateSync = (driverId, state = {}) => {
  if (!isEnabled()) return;

  const safeDriverId = toNumber(driverId);
  const normalizedState = normalizeDriverState(safeDriverId, state);
  if (!safeDriverId || !normalizedState) return;

  pendingStates.set(safeDriverId, normalizedState);

  const existingTimer = flushTimers.get(safeDriverId);
  if (existingTimer) return;

  const timer = setTimeout(() => {
    flushTimers.delete(safeDriverId);
    void flushDriverState(safeDriverId);
  }, DRIVER_PRESENCE_REDIS_FLUSH_MS);

  timer.unref?.();
  flushTimers.set(safeDriverId, timer);
};

const removeDriverState = async (driverId) => {
  if (!isEnabled()) return false;

  const safeDriverId = toNumber(driverId);
  const key = buildDriverStateKey(safeDriverId);
  if (!safeDriverId || !key) return false;

  pendingStates.delete(safeDriverId);

  const timer = flushTimers.get(safeDriverId);
  if (timer) {
    clearTimeout(timer);
    flushTimers.delete(safeDriverId);
  }

  try {
    const client = await ensureClient();
    if (!client) {
      logInitErrorOnce(new Error("Redis client unavailable"));
      return false;
    }

    const multi = client.multi();
    multi.del(key);
    multi.sRem(DRIVER_ONLINE_SET_KEY, String(safeDriverId));
    await multi.exec();
    return true;
  } catch (error) {
    logInitErrorOnce(error);
    return false;
  }
};

const getDriverState = async (driverId) => {
  if (!isEnabled()) return null;

  const key = buildDriverStateKey(driverId);
  if (!key) return null;

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

module.exports = {
  isEnabled,
  scheduleDriverStateSync,
  removeDriverState,
  getDriverState,
};
