const db = require("../db");
const { getDistanceMeters } = require("../utils/geo.util");

const driverLocations = new Map();
const driverListeners = new Map();

exports.update = (driverId, lat, long) => {
  return new Promise((resolve, reject) => {
    db.query(
      `UPDATE transport_driver_details
       SET current_lat = ?, current_long = ?, updated_at = NOW()
       WHERE id = ?`,
      [lat, long, driverId],
      (err, result) => {
        if (err) return reject(err);
        resolve(result);
      }
    );
  });
};

exports.updateMemory = (driverId, lat, long) => {
  driverLocations.set(driverId, { lat, long, timestamp: Date.now() });

  if (driverListeners.has(driverId)) {
    driverListeners.get(driverId).forEach(cb => cb({ driver_id: driverId, lat, long, timestamp: Date.now() }));
  }
};

exports.getDriver = (driverId) => driverLocations.get(driverId);

exports.remove = (driverId) => {
  driverLocations.delete(driverId);
  driverListeners.delete(driverId);
};

exports.getNearbyDriversFromMemory = (lat, long, radius = 5000) => {
  const drivers = [];
  for (const [driverId, data] of driverLocations.entries()) {
    const distance = getDistanceMeters(lat, long, data.lat, data.long);
    if (distance <= radius) {
      drivers.push({ driver_id: driverId, lat: data.lat, long: data.long, distance });
    }
  }
  return drivers;
};
