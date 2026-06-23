const { ensureClient } = require("./redisClient.service");

const DRIVER_INBOX_REDIS_ENABLED = String(
  process.env.DRIVER_INBOX_REDIS_ENABLED || "0"
) === "1";
const DRIVER_INBOX_REDIS_TTL_S = Number.isFinite(
  Number(process.env.DRIVER_INBOX_REDIS_TTL_S)
)
  ? Math.max(60, Math.floor(Number(process.env.DRIVER_INBOX_REDIS_TTL_S)))
  : 900;
const DRIVER_INBOX_REDIS_FLUSH_MS = Number.isFinite(
  Number(process.env.DRIVER_INBOX_REDIS_FLUSH_MS)
)
  ? Math.max(25, Math.floor(Number(process.env.DRIVER_INBOX_REDIS_FLUSH_MS)))
  : 150;
const DRIVER_INBOX_KEY_PREFIX =
  process.env.DRIVER_INBOX_KEY_PREFIX || "driver:inbox:";
const DRIVER_INBOX_CLEARED_KEY_PREFIX =
  process.env.DRIVER_INBOX_CLEARED_KEY_PREFIX || "driver:inbox:cleared:";

const pendingInboxes = new Map();
const flushTimers = new Map();
let initErrorLogged = false;

const isEnabled = () => DRIVER_INBOX_REDIS_ENABLED;

const toNumber = (value) => {
  if (value === null || value === undefined || value === "") return null;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
};

const logInitErrorOnce = (error) => {
  if (initErrorLogged) return;
  initErrorLogged = true;
  console.warn(
    "[driverInboxRedis] disabled:",
    error?.message || error || "Redis init failed"
  );
};

const buildInboxKey = (driverId) => {
  const safeDriverId = toNumber(driverId);
  return safeDriverId ? `${DRIVER_INBOX_KEY_PREFIX}${safeDriverId}` : null;
};

const buildClearedKey = (driverId) => {
  const safeDriverId = toNumber(driverId);
  return safeDriverId ? `${DRIVER_INBOX_CLEARED_KEY_PREFIX}${safeDriverId}` : null;
};

const normalizeInboxRides = (rides = []) =>
  (Array.isArray(rides) ? rides : []).filter(
    (ride) => ride && typeof ride === "object" && !Array.isArray(ride)
  );

const normalizeDriverInbox = (driverId, rides = []) => {
  const safeDriverId = toNumber(driverId);
  if (!safeDriverId) return null;

  return {
    driver_id: safeDriverId,
    rides: normalizeInboxRides(rides),
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
  if (pendingInboxes.has(safeDriverId)) return;
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

const flushDriverInbox = async (driverId) => {
  const safeDriverId = toNumber(driverId);
  if (!safeDriverId || !isEnabled()) return false;

  clearFlushTimer(safeDriverId);

  const inboxPayload = pendingInboxes.get(safeDriverId) ?? null;
  if (!inboxPayload) return false;

  pendingInboxes.delete(safeDriverId);

  try {
    const client = await ensureClient();
    if (!client) {
      logInitErrorOnce(new Error("Redis client unavailable"));
      return false;
    }

    const multi = client.multi();
    const inboxKey = buildInboxKey(safeDriverId);
    const clearedKey = buildClearedKey(safeDriverId);

    if (inboxKey) {
      if (Array.isArray(inboxPayload.rides) && inboxPayload.rides.length > 0) {
        multi.set(inboxKey, JSON.stringify(inboxPayload), {
          EX: DRIVER_INBOX_REDIS_TTL_S,
        });
      } else {
        multi.del(inboxKey);
      }
    }

    if (
      clearedKey &&
      Array.isArray(inboxPayload.rides) &&
      inboxPayload.rides.length > 0
    ) {
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
    void flushDriverInbox(safeDriverId);
  }, DRIVER_INBOX_REDIS_FLUSH_MS);

  timer.unref?.();
  flushTimers.set(safeDriverId, timer);
};

const scheduleDriverInboxSync = (driverId, rides = []) => {
  if (!isEnabled()) return;

  const normalizedPayload = normalizeDriverInbox(driverId, rides);
  if (!normalizedPayload) return;

  pendingInboxes.set(normalizedPayload.driver_id, normalizedPayload);
  scheduleFlush(normalizedPayload.driver_id);
};

const clearDriverInbox = async (driverId) => {
  const safeDriverId = toNumber(driverId);
  if (!safeDriverId || !isEnabled()) return false;

  pendingInboxes.delete(safeDriverId);
  maybeClearFlushTimer(safeDriverId);

  try {
    const client = await ensureClient();
    if (!client) {
      logInitErrorOnce(new Error("Redis client unavailable"));
      return false;
    }

    const multi = client.multi();
    const inboxKey = buildInboxKey(safeDriverId);
    const clearedKey = buildClearedKey(safeDriverId);
    if (inboxKey) multi.del(inboxKey);
    if (clearedKey) {
      multi.set(clearedKey, String(Date.now()), {
        EX: DRIVER_INBOX_REDIS_TTL_S,
      });
    }
    await multi.exec();
    return true;
  } catch (error) {
    logInitErrorOnce(error);
    return false;
  }
};

const getDriverInbox = async (driverId) => {
  const parsed = await readJsonValue(buildInboxKey(driverId));
  if (!parsed || !Array.isArray(parsed.rides)) return null;
  return normalizeDriverInbox(driverId, parsed.rides);
};

const hasDriverInboxClearedMarker = async (driverId) => {
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
  scheduleDriverInboxSync,
  clearDriverInbox,
  getDriverInbox,
  hasDriverInboxClearedMarker,
};
