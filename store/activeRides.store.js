// store/activeRides.store.js
const activeRideByDriver = new Map(); // driverId -> rideId
const activeDriverByRide = new Map(); // rideId -> driverId
const activeRideMetaByDriver = new Map(); // driverId -> { ride_id, set_at, touched_at }

const setActiveRide = (driverId, rideId) => {
  if (!driverId || !rideId) return;
  const now = Date.now();
  const existing = activeRideMetaByDriver.get(driverId) || null;
  activeRideByDriver.set(driverId, rideId);
  activeDriverByRide.set(rideId, driverId);
  if (existing && Number(existing.ride_id) === Number(rideId)) {
    activeRideMetaByDriver.set(driverId, {
      ...existing,
      touched_at: now,
    });
    return;
  }
  activeRideMetaByDriver.set(driverId, {
    ride_id: rideId,
    set_at: now,
    touched_at: now,
  });
};

const getActiveRideByDriver = (driverId) => {
  if (!driverId) return null;
  return activeRideByDriver.get(driverId) || null;
};

const getActiveDriverByRide = (rideId) => {
  if (!rideId) return null;
  return activeDriverByRide.get(rideId) || null;
};

const clearActiveRideByDriver = (driverId) => {
  if (!driverId) return;
  const rideId = activeRideByDriver.get(driverId);
  activeRideByDriver.delete(driverId);
  activeRideMetaByDriver.delete(driverId);
  if (rideId) activeDriverByRide.delete(rideId);
};

const clearActiveRideByRideId = (rideId) => {
  if (!rideId) return;
  const driverId = activeDriverByRide.get(rideId);
  activeDriverByRide.delete(rideId);
  if (driverId) {
    activeRideByDriver.delete(driverId);
    activeRideMetaByDriver.delete(driverId);
  }
};

const getActiveRideLockAgeMs = (driverId) => {
  if (!driverId) return null;
  const meta = activeRideMetaByDriver.get(driverId);
  if (!meta || !Number.isFinite(Number(meta.set_at))) return null;
  return Math.max(0, Date.now() - Number(meta.set_at));
};

module.exports = {
  setActiveRide,
  getActiveRideByDriver,
  getActiveDriverByRide,
  clearActiveRideByDriver,
  clearActiveRideByRideId,
  getActiveRideLockAgeMs,
};
