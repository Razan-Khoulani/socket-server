const { ensureClient } = require("./redisClient.service");

const RIDE_SNAPSHOT_REDIS_ENABLED = String(
  process.env.RIDE_SNAPSHOT_REDIS_ENABLED || "0"
) === "1";
const RIDE_SNAPSHOT_REDIS_TTL_S = Number.isFinite(
  Number(process.env.RIDE_SNAPSHOT_REDIS_TTL_S)
)
  ? Math.max(60, Math.floor(Number(process.env.RIDE_SNAPSHOT_REDIS_TTL_S)))
  : 900;
const RIDE_SNAPSHOT_REDIS_FLUSH_MS = Number.isFinite(
  Number(process.env.RIDE_SNAPSHOT_REDIS_FLUSH_MS)
)
  ? Math.max(25, Math.floor(Number(process.env.RIDE_SNAPSHOT_REDIS_FLUSH_MS)))
  : 150;
const RIDE_SNAPSHOT_KEY_PREFIX =
  process.env.RIDE_SNAPSHOT_KEY_PREFIX || "ride:snapshot:";
const RIDE_OWNER_KEY_PREFIX =
  process.env.RIDE_OWNER_KEY_PREFIX || "ride:owner:";
const RIDE_SNAPSHOT_CLEARED_KEY_PREFIX =
  process.env.RIDE_SNAPSHOT_CLEARED_KEY_PREFIX || "ride:snapshot:cleared:";
const RIDE_OWNER_CLEARED_KEY_PREFIX =
  process.env.RIDE_OWNER_CLEARED_KEY_PREFIX || "ride:owner:cleared:";

const pendingSnapshots = new Map();
const pendingOwners = new Map();
const flushTimers = new Map();
let initErrorLogged = false;

const isEnabled = () => RIDE_SNAPSHOT_REDIS_ENABLED;

const toNumber = (value) => {
  if (value === null || value === undefined || value === "") return null;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
};

const logInitErrorOnce = (error) => {
  if (initErrorLogged) return;
  initErrorLogged = true;
  console.warn(
    "[rideSnapshotRedis] disabled:",
    error?.message || error || "Redis init failed"
  );
};

const buildSnapshotKey = (rideId) => {
  const safeRideId = toNumber(rideId);
  return safeRideId ? `${RIDE_SNAPSHOT_KEY_PREFIX}${safeRideId}` : null;
};

const buildOwnerKey = (rideId) => {
  const safeRideId = toNumber(rideId);
  return safeRideId ? `${RIDE_OWNER_KEY_PREFIX}${safeRideId}` : null;
};

const buildSnapshotClearedKey = (rideId) => {
  const safeRideId = toNumber(rideId);
  return safeRideId ? `${RIDE_SNAPSHOT_CLEARED_KEY_PREFIX}${safeRideId}` : null;
};

const buildOwnerClearedKey = (rideId) => {
  const safeRideId = toNumber(rideId);
  return safeRideId ? `${RIDE_OWNER_CLEARED_KEY_PREFIX}${safeRideId}` : null;
};

const normalizeRideSnapshot = (rideId, rideDetails = {}) => {
  const safeRideId = toNumber(rideId);
  if (
    !safeRideId ||
    !rideDetails ||
    typeof rideDetails !== "object" ||
    Array.isArray(rideDetails)
  ) {
    return null;
  }

  return {
    ride_id: safeRideId,
    ride_details: rideDetails,
    shadow_synced_at: Date.now(),
  };
};

const normalizeRideOwner = (rideId, userId) => {
  const safeRideId = toNumber(rideId);
  const safeUserId = toNumber(userId);
  if (!safeRideId || !safeUserId) return null;

  return {
    ride_id: safeRideId,
    user_id: safeUserId,
    shadow_synced_at: Date.now(),
  };
};

const getFlushEntry = (rideId) => {
  const safeRideId = toNumber(rideId);
  return safeRideId ? flushTimers.get(safeRideId) ?? null : null;
};

const clearFlushTimer = (rideId) => {
  const safeRideId = toNumber(rideId);
  if (!safeRideId) return;

  const timer = getFlushEntry(safeRideId);
  if (!timer) return;

  clearTimeout(timer);
  flushTimers.delete(safeRideId);
};

const maybeClearFlushTimer = (rideId) => {
  const safeRideId = toNumber(rideId);
  if (!safeRideId) return;

  if (pendingSnapshots.has(safeRideId) || pendingOwners.has(safeRideId)) {
    return;
  }

  clearFlushTimer(safeRideId);
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

const flushRideState = async (rideId) => {
  const safeRideId = toNumber(rideId);
  if (!safeRideId || !isEnabled()) return false;

  clearFlushTimer(safeRideId);

  const snapshotPayload = pendingSnapshots.get(safeRideId) ?? null;
  const ownerPayload = pendingOwners.get(safeRideId) ?? null;
  if (!snapshotPayload && !ownerPayload) return false;

  pendingSnapshots.delete(safeRideId);
  pendingOwners.delete(safeRideId);

  try {
    const client = await ensureClient();
    if (!client) {
      logInitErrorOnce(new Error("Redis client unavailable"));
      return false;
    }

    const multi = client.multi();
    const snapshotKey = buildSnapshotKey(safeRideId);
    const ownerKey = buildOwnerKey(safeRideId);
    const snapshotClearedKey = buildSnapshotClearedKey(safeRideId);
    const ownerClearedKey = buildOwnerClearedKey(safeRideId);

    if (snapshotKey) {
      if (snapshotPayload?.ride_details) {
        multi.set(snapshotKey, JSON.stringify(snapshotPayload), {
          EX: RIDE_SNAPSHOT_REDIS_TTL_S,
        });
      } else {
        multi.del(snapshotKey);
      }
    }

    if (ownerKey) {
      if (ownerPayload?.user_id) {
        multi.set(ownerKey, JSON.stringify(ownerPayload), {
          EX: RIDE_SNAPSHOT_REDIS_TTL_S,
        });
      } else {
        multi.del(ownerKey);
      }
    }

    if (snapshotPayload?.ride_details && snapshotClearedKey) {
      multi.del(snapshotClearedKey);
    }

    if (ownerPayload?.user_id && ownerClearedKey) {
      multi.del(ownerClearedKey);
    }

    await multi.exec();
    return true;
  } catch (error) {
    logInitErrorOnce(error);
    return false;
  }
};

const scheduleFlush = (rideId) => {
  const safeRideId = toNumber(rideId);
  if (!safeRideId || !isEnabled() || getFlushEntry(safeRideId)) return;

  const timer = setTimeout(() => {
    flushTimers.delete(safeRideId);
    void flushRideState(safeRideId);
  }, RIDE_SNAPSHOT_REDIS_FLUSH_MS);

  timer.unref?.();
  flushTimers.set(safeRideId, timer);
};

const scheduleRideSnapshotSync = (rideId, rideDetails = {}) => {
  if (!isEnabled()) return;

  const normalizedPayload = normalizeRideSnapshot(rideId, rideDetails);
  if (!normalizedPayload) return;

  pendingSnapshots.set(normalizedPayload.ride_id, normalizedPayload);
  scheduleFlush(normalizedPayload.ride_id);
};

const scheduleRideOwnerSync = (rideId, userId) => {
  if (!isEnabled()) return;

  const normalizedPayload = normalizeRideOwner(rideId, userId);
  if (!normalizedPayload) return;

  pendingOwners.set(normalizedPayload.ride_id, normalizedPayload);
  scheduleFlush(normalizedPayload.ride_id);
};

const clearRideSnapshot = async (rideId) => {
  const safeRideId = toNumber(rideId);
  if (!safeRideId || !isEnabled()) return false;

  pendingSnapshots.delete(safeRideId);
  maybeClearFlushTimer(safeRideId);

  try {
    const client = await ensureClient();
    if (!client) {
      logInitErrorOnce(new Error("Redis client unavailable"));
      return false;
    }

    const multi = client.multi();
    const snapshotKey = buildSnapshotKey(safeRideId);
    const snapshotClearedKey = buildSnapshotClearedKey(safeRideId);
    if (snapshotKey) multi.del(snapshotKey);
    if (snapshotClearedKey) {
      multi.set(snapshotClearedKey, String(Date.now()), {
        EX: RIDE_SNAPSHOT_REDIS_TTL_S,
      });
    }
    await multi.exec();
    return true;
  } catch (error) {
    logInitErrorOnce(error);
    return false;
  }
};

const clearRideOwner = async (rideId) => {
  const safeRideId = toNumber(rideId);
  if (!safeRideId || !isEnabled()) return false;

  pendingOwners.delete(safeRideId);
  maybeClearFlushTimer(safeRideId);

  try {
    const client = await ensureClient();
    if (!client) {
      logInitErrorOnce(new Error("Redis client unavailable"));
      return false;
    }

    const multi = client.multi();
    const ownerKey = buildOwnerKey(safeRideId);
    const ownerClearedKey = buildOwnerClearedKey(safeRideId);
    if (ownerKey) multi.del(ownerKey);
    if (ownerClearedKey) {
      multi.set(ownerClearedKey, String(Date.now()), {
        EX: RIDE_SNAPSHOT_REDIS_TTL_S,
      });
    }
    await multi.exec();
    return true;
  } catch (error) {
    logInitErrorOnce(error);
    return false;
  }
};

const clearRideState = async (
  rideId,
  { clearSnapshot = true, clearOwner = true } = {}
) => {
  const safeRideId = toNumber(rideId);
  if (!safeRideId || !isEnabled()) return false;
  if (!clearSnapshot && !clearOwner) return false;

  if (clearSnapshot) {
    pendingSnapshots.delete(safeRideId);
  }
  if (clearOwner) {
    pendingOwners.delete(safeRideId);
  }
  maybeClearFlushTimer(safeRideId);

  try {
    const client = await ensureClient();
    if (!client) {
      logInitErrorOnce(new Error("Redis client unavailable"));
      return false;
    }

    const multi = client.multi();
    const snapshotKey = buildSnapshotKey(safeRideId);
    const ownerKey = buildOwnerKey(safeRideId);
    const snapshotClearedKey = buildSnapshotClearedKey(safeRideId);
    const ownerClearedKey = buildOwnerClearedKey(safeRideId);

    if (clearSnapshot && snapshotKey) {
      multi.del(snapshotKey);
    }
    if (clearOwner && ownerKey) {
      multi.del(ownerKey);
    }
    if (clearSnapshot && snapshotClearedKey) {
      multi.set(snapshotClearedKey, String(Date.now()), {
        EX: RIDE_SNAPSHOT_REDIS_TTL_S,
      });
    }
    if (clearOwner && ownerClearedKey) {
      multi.set(ownerClearedKey, String(Date.now()), {
        EX: RIDE_SNAPSHOT_REDIS_TTL_S,
      });
    }

    await multi.exec();
    return true;
  } catch (error) {
    logInitErrorOnce(error);
    return false;
  }
};

const getRideSnapshot = async (rideId) => {
  const parsed = await readJsonValue(buildSnapshotKey(rideId));
  if (!parsed?.ride_details || typeof parsed.ride_details !== "object") return null;
  return normalizeRideSnapshot(rideId, parsed.ride_details);
};

const getRideOwner = async (rideId) => {
  const parsed = await readJsonValue(buildOwnerKey(rideId));
  if (!parsed?.user_id) return null;
  return normalizeRideOwner(rideId, parsed.user_id);
};

const hasRideSnapshotClearedMarker = async (rideId) => {
  const key = buildSnapshotClearedKey(rideId);
  if (!key || !isEnabled()) return false;

  try {
    const client = await ensureClient();
    if (!client) return false;
    return !!(await client.exists(key));
  } catch (_) {
    return false;
  }
};

const hasRideOwnerClearedMarker = async (rideId) => {
  const key = buildOwnerClearedKey(rideId);
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
  scheduleRideSnapshotSync,
  scheduleRideOwnerSync,
  clearRideSnapshot,
  clearRideOwner,
  clearRideState,
  getRideSnapshot,
  getRideOwner,
  hasRideSnapshotClearedMarker,
  hasRideOwnerClearedMarker,
};
