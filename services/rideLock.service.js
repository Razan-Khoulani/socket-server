const {
  tryAcquireLock,
  releaseLock,
  forceReleaseLock,
} = require("./distributedLock.service");

const localLocks = new Map();

const toPositiveInteger = (value) => {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) return null;
  const normalized = Math.floor(parsed);
  return normalized > 0 ? normalized : null;
};

const normalizeTtlMs = (ttlMs, fallbackMs = 5000) => {
  const parsed = Number(ttlMs);
  if (!Number.isFinite(parsed)) return fallbackMs;
  return Math.max(250, Math.floor(parsed));
};

const buildLocalToken = () =>
  `local:${Date.now()}:${Math.random().toString(36).slice(2)}:${Math.random()
    .toString(36)
    .slice(2)}`;

const getLocalEntry = (key) => {
  const entry = localLocks.get(key);
  if (!entry) return null;

  if (entry.expiresAt <= Date.now()) {
    localLocks.delete(key);
    return null;
  }

  return entry;
};

const acquireLocalLock = (key, ttlMs) => {
  const safeTtlMs = normalizeTtlMs(ttlMs);
  const existing = getLocalEntry(key);
  if (existing) {
    return {
      acquired: false,
      handle: null,
      reason: "locked",
    };
  }

  const token = buildLocalToken();
  const expiresAt = Date.now() + safeTtlMs;
  localLocks.set(key, {
    token,
    expiresAt,
  });

  const timeout = setTimeout(() => {
    const current = localLocks.get(key);
    if (current && current.token === token) {
      localLocks.delete(key);
    }
  }, safeTtlMs);
  timeout.unref?.();

  return {
    acquired: true,
    handle: {
      backend: "local",
      key,
      token,
    },
    reason: null,
  };
};

const releaseLocalLock = (key, handle) => {
  if (!key || !handle?.token) return false;

  const current = localLocks.get(key);
  if (!current || current.token !== handle.token) {
    return false;
  }

  localLocks.delete(key);
  return true;
};

const forceReleaseLocalLock = (key) => {
  if (!key) return false;
  return localLocks.delete(key);
};

const acquireWithFallback = async (key, ttlMs) => {
  const distributedResult = await tryAcquireLock(key, ttlMs);
  if (distributedResult.acquired) {
    return {
      acquired: true,
      handle: {
        backend: "redis",
        key,
        token: distributedResult.token,
      },
      reason: null,
    };
  }

  if (distributedResult.reason === "locked") {
    return {
      acquired: false,
      handle: null,
      reason: "locked",
    };
  }

  return acquireLocalLock(key, ttlMs);
};

const releaseWithFallback = async (key, handle) => {
  if (!key || !handle) return false;

  if (handle.backend === "redis") {
    return releaseLock(key, handle.token);
  }

  return releaseLocalLock(key, handle);
};

const forceReleaseWithFallback = async (key) => {
  const localReleased = forceReleaseLocalLock(key);
  const redisReleased = await forceReleaseLock(key);
  return localReleased || redisReleased;
};

const rideAcceptKey = (rideId) => {
  const safeRideId = toPositiveInteger(rideId);
  return safeRideId ? `lock:ride:accept:${safeRideId}` : null;
};

const driverAcceptKey = (driverId) => {
  const safeDriverId = toPositiveInteger(driverId);
  return safeDriverId ? `lock:driver:accept:${safeDriverId}` : null;
};

const autoAcceptFirstBidKey = (rideId) => {
  const safeRideId = toPositiveInteger(rideId);
  return safeRideId ? `lock:ride:auto-accept-first-bid:${safeRideId}` : null;
};

const dispatchInFlightKey = (rideId) => {
  const safeRideId = toPositiveInteger(rideId);
  return safeRideId ? `lock:ride:dispatch-in-flight:${safeRideId}` : null;
};

const acquireRideAcceptLock = async (rideId, ttlMs) => {
  const key = rideAcceptKey(rideId);
  if (!key) return false;
  const result = await acquireWithFallback(key, ttlMs);
  return result.acquired === true;
};

const clearRideAcceptLock = async (rideId) => {
  const key = rideAcceptKey(rideId);
  if (!key) return false;
  return forceReleaseWithFallback(key);
};

const acquireDriverAcceptLock = async (driverId, ttlMs) => {
  const key = driverAcceptKey(driverId);
  if (!key) return null;
  const result = await acquireWithFallback(key, ttlMs);
  return result.acquired ? result.handle : null;
};

const releaseDriverAcceptLock = async (driverId, handle) => {
  const key = driverAcceptKey(driverId);
  if (!key) return false;
  return releaseWithFallback(key, handle);
};

const acquireAutoAcceptFirstBidLock = async (rideId, ttlMs) => {
  const key = autoAcceptFirstBidKey(rideId);
  if (!key) return null;
  const result = await acquireWithFallback(key, ttlMs);
  return result.acquired ? result.handle : null;
};

const releaseAutoAcceptFirstBidLock = async (rideId, handle) => {
  const key = autoAcceptFirstBidKey(rideId);
  if (!key) return false;
  return releaseWithFallback(key, handle);
};

const acquireDispatchInFlightLock = async (rideId, ttlMs) => {
  const key = dispatchInFlightKey(rideId);
  if (!key) return null;
  const result = await acquireWithFallback(key, ttlMs);
  return result.acquired ? result.handle : null;
};

const releaseDispatchInFlightLock = async (rideId, handle) => {
  const key = dispatchInFlightKey(rideId);
  if (!key) return false;
  return releaseWithFallback(key, handle);
};

const clearDispatchInFlightLock = async (rideId) => {
  const key = dispatchInFlightKey(rideId);
  if (!key) return false;
  return forceReleaseWithFallback(key);
};

module.exports = {
  acquireRideAcceptLock,
  clearRideAcceptLock,
  acquireDriverAcceptLock,
  releaseDriverAcceptLock,
  acquireAutoAcceptFirstBidLock,
  releaseAutoAcceptFirstBidLock,
  acquireDispatchInFlightLock,
  releaseDispatchInFlightLock,
  clearDispatchInFlightLock,
};
