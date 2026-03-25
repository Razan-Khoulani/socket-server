// store/activeRides.store.js
const activeRideByDriver = new Map(); // driverId -> rideId
const activeDriverByRide = new Map(); // rideId -> driverId

const setActiveRide = (driverId, rideId) => {
  if (!driverId || !rideId) return;
  activeRideByDriver.set(driverId, rideId);
  activeDriverByRide.set(rideId, driverId);
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
  if (rideId) activeDriverByRide.delete(rideId);
};

const clearActiveRideByRideId = (rideId) => {
  if (!rideId) return;
  const driverId = activeDriverByRide.get(rideId);
  activeDriverByRide.delete(rideId);
  if (driverId) activeRideByDriver.delete(driverId);
};

module.exports = {
  setActiveRide,
  getActiveRideByDriver,
  getActiveDriverByRide,
  clearActiveRideByDriver,
  clearActiveRideByRideId,
};
