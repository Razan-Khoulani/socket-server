const { ensureClient } = require("./redisClient.service");

const RIDE_DISPATCH_REDIS_ENABLED = String(
  process.env.RIDE_DISPATCH_REDIS_ENABLED || "0"
) === "1";
const RIDE_DISPATCH_REDIS_TTL_S = Number.isFinite(
  Number(process.env.RIDE_DISPATCH_REDIS_TTL_S)
)
  ? Math.max(60, Math.floor(Number(process.env.RIDE_DISPATCH_REDIS_TTL_S)))
  : 600;
const RIDE_DISPATCH_REDIS_FLUSH_MS = Number.isFinite(
  Number(process.env.RIDE_DISPATCH_REDIS_FLUSH_MS)
)
  ? Math.max(25, Math.floor(Number(process.env.RIDE_DISPATCH_REDIS_FLUSH_MS)))
  : 150;
const RIDE_DISPATCH_CANDIDATES_KEY_PREFIX =
  process.env.RIDE_DISPATCH_CANDIDATES_KEY_PREFIX || "ride:dispatch:candidates:";
const RIDE_DISPATCH_STATES_KEY_PREFIX =
  process.env.RIDE_DISPATCH_STATES_KEY_PREFIX || "ride:dispatch:states:";
const RIDE_DISPATCH_CLEARED_KEY_PREFIX =
  process.env.RIDE_DISPATCH_CLEARED_KEY_PREFIX || "ride:dispatch:cleared:";

const pendingCandidates = new Map();
const pendingStates = new Map();
const flushTimers = new Map();
let initErrorLogged = false;

const isEnabled = () => RIDE_DISPATCH_REDIS_ENABLED;

const toNumber = (value) => {
  if (value === null || value === undefined || value === "") return null;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
};

const logInitErrorOnce = (error) => {
  if (initErrorLogged) return;
  initErrorLogged = true;
  console.warn(
    "[rideDispatchRedis] disabled:",
    error?.message || error || "Redis init failed"
  );
};

const buildCandidatesKey = (rideId) => {
  const safeRideId = toNumber(rideId);
  return safeRideId ? `${RIDE_DISPATCH_CANDIDATES_KEY_PREFIX}${safeRideId}` : null;
};

const buildStatesKey = (rideId) => {
  const safeRideId = toNumber(rideId);
  return safeRideId ? `${RIDE_DISPATCH_STATES_KEY_PREFIX}${safeRideId}` : null;
};

const buildClearedKey = (rideId) => {
  const safeRideId = toNumber(rideId);
  return safeRideId ? `${RIDE_DISPATCH_CLEARED_KEY_PREFIX}${safeRideId}` : null;
};

const normalizeCandidateIds = (rideId, candidateIds = []) => {
  const safeRideId = toNumber(rideId);
  if (!safeRideId) return null;

  const normalizedCandidates = Array.from(
    new Set(
      (Array.isArray(candidateIds) ? candidateIds : [])
        .map((driverId) => toNumber(driverId))
        .filter((driverId) => !!driverId)
    )
  );

  return {
    ride_id: safeRideId,
    candidates: normalizedCandidates,
    shadow_synced_at: Date.now(),
  };
};

const normalizeDriverStates = (rideId, states = []) => {
  const safeRideId = toNumber(rideId);
  if (!safeRideId) return null;

  const normalizedStates = (Array.isArray(states) ? states : [])
    .filter((state) => state && typeof state === "object")
    .map((state) => ({
      ...state,
      driver_id: toNumber(state.driver_id),
    }))
    .filter((state) => !!state.driver_id);

  return {
    ride_id: safeRideId,
    states: normalizedStates,
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

const flushRideDispatchState = async (rideId) => {
  const safeRideId = toNumber(rideId);
  if (!safeRideId || !isEnabled()) return false;

  clearFlushTimer(safeRideId);

  const candidatePayload = pendingCandidates.get(safeRideId) ?? null;
  const statesPayload = pendingStates.get(safeRideId) ?? null;
  if (!candidatePayload && !statesPayload) return false;

  pendingCandidates.delete(safeRideId);
  pendingStates.delete(safeRideId);

  try {
    const client = await ensureClient();
    if (!client) {
      logInitErrorOnce(new Error("Redis client unavailable"));
      return false;
    }

    const multi = client.multi();
    const candidatesKey = buildCandidatesKey(safeRideId);
    const statesKey = buildStatesKey(safeRideId);
    const clearedKey = buildClearedKey(safeRideId);

    if (candidatesKey) {
      if (candidatePayload && Array.isArray(candidatePayload.candidates) && candidatePayload.candidates.length > 0) {
        multi.set(candidatesKey, JSON.stringify(candidatePayload), {
          EX: RIDE_DISPATCH_REDIS_TTL_S,
        });
      } else {
        multi.del(candidatesKey);
      }
    }

    if (statesKey) {
      if (statesPayload && Array.isArray(statesPayload.states) && statesPayload.states.length > 0) {
        multi.set(statesKey, JSON.stringify(statesPayload), {
          EX: RIDE_DISPATCH_REDIS_TTL_S,
        });
      } else {
        multi.del(statesKey);
      }
    }

    if (
      clearedKey &&
      (
        (candidatePayload && Array.isArray(candidatePayload.candidates) && candidatePayload.candidates.length > 0) ||
        (statesPayload && Array.isArray(statesPayload.states) && statesPayload.states.length > 0)
      )
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

const scheduleFlush = (rideId) => {
  const safeRideId = toNumber(rideId);
  if (!safeRideId || !isEnabled() || getFlushEntry(safeRideId)) return;

  const timer = setTimeout(() => {
    flushTimers.delete(safeRideId);
    void flushRideDispatchState(safeRideId);
  }, RIDE_DISPATCH_REDIS_FLUSH_MS);

  timer.unref?.();
  flushTimers.set(safeRideId, timer);
};

const scheduleCandidatesSync = (rideId, candidateIds = []) => {
  if (!isEnabled()) return;

  const normalizedPayload = normalizeCandidateIds(rideId, candidateIds);
  if (!normalizedPayload) return;

  pendingCandidates.set(normalizedPayload.ride_id, normalizedPayload);
  scheduleFlush(normalizedPayload.ride_id);
};

const scheduleDriverStatesSync = (rideId, states = []) => {
  if (!isEnabled()) return;

  const normalizedPayload = normalizeDriverStates(rideId, states);
  if (!normalizedPayload) return;

  pendingStates.set(normalizedPayload.ride_id, normalizedPayload);
  scheduleFlush(normalizedPayload.ride_id);
};

const clearRideDispatchState = async (rideId) => {
  const safeRideId = toNumber(rideId);
  if (!safeRideId || !isEnabled()) return false;

  pendingCandidates.delete(safeRideId);
  pendingStates.delete(safeRideId);
  clearFlushTimer(safeRideId);

  try {
    const client = await ensureClient();
    if (!client) {
      logInitErrorOnce(new Error("Redis client unavailable"));
      return false;
    }

    const multi = client.multi();
    const candidatesKey = buildCandidatesKey(safeRideId);
    const statesKey = buildStatesKey(safeRideId);
    const clearedKey = buildClearedKey(safeRideId);
    if (candidatesKey) multi.del(candidatesKey);
    if (statesKey) multi.del(statesKey);
    if (clearedKey) {
      multi.set(clearedKey, String(Date.now()), {
        EX: RIDE_DISPATCH_REDIS_TTL_S,
      });
    }
    await multi.exec();
    return true;
  } catch (error) {
    logInitErrorOnce(error);
    return false;
  }
};

const getRideCandidates = async (rideId) => {
  const parsed = await readJsonValue(buildCandidatesKey(rideId));
  if (!parsed || !Array.isArray(parsed.candidates)) return null;
  return normalizeCandidateIds(rideId, parsed.candidates);
};

const getRideDriverStates = async (rideId) => {
  const parsed = await readJsonValue(buildStatesKey(rideId));
  if (!parsed || !Array.isArray(parsed.states)) return null;
  return normalizeDriverStates(rideId, parsed.states);
};

const hasRideDispatchClearedMarker = async (rideId) => {
  const key = buildClearedKey(rideId);
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
  scheduleCandidatesSync,
  scheduleDriverStatesSync,
  clearRideDispatchState,
  getRideCandidates,
  getRideDriverStates,
  hasRideDispatchClearedMarker,
};
