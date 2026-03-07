// rideTracking.js

const { getDistanceMeters } = require("../utils/geo.util");
const { getActiveDriverByRide } = require("../store/activeRides.store");
const { getRoadDistanceMeters } = require("../services/osrm.service");

const rideLocations = new Map();

const envNumber = (key, fallback) => {
  const n = Number(process.env[key]);
  return Number.isFinite(n) ? n : fallback;
};

const AIR_TRIGGER_M = envNumber("RIDE_TRACK_AIR_TRIGGER_M", 250);
const ROAD_CHECK_EVERY_MS = envNumber("RIDE_TRACK_ROAD_CHECK_EVERY_MS", 7000);
const ENTER_AIR_M = envNumber("RIDE_TRACK_ENTER_AIR_M", 25);
const ENTER_ROAD_M = envNumber("RIDE_TRACK_ENTER_ROAD_M", 60);
const MIN_INSIDE_MS = envNumber("RIDE_TRACK_MIN_INSIDE_MS", 3000);

const toFiniteNumber = (value) => {
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
};

const emitLocation = (io, rideId, lat, long) => {
  io.to(`ride:${rideId}`).emit("ride:locationUpdate", {
    ride_id: rideId,
    lat,
    long,
  });
};

function startTracking(io, rideId, pickup, destination) {
  if (!rideId) return false;

  const pickupLat = toFiniteNumber(pickup?.lat);
  const pickupLong = toFiniteNumber(pickup?.long);
  if (pickupLat === null || pickupLong === null) return false;

  const destLat = toFiniteNumber(destination?.lat);
  const destLong = toFiniteNumber(destination?.long);
  const normalizedDestination =
    destLat !== null && destLong !== null ? { lat: destLat, long: destLong } : null;

  const existing = rideLocations.get(rideId);
  if (existing) {
    existing.lat = pickupLat;
    existing.long = pickupLong;
    if (normalizedDestination) {
      existing.destination = normalizedDestination;
    }

    rideLocations.set(rideId, existing);
    emitLocation(io, rideId, pickupLat, pickupLong);
    console.log(
      `[tracking][start] duplicate start for ride ${rideId}, refreshed memory only`
    );
    return false;
  }

  rideLocations.set(rideId, {
    lat: pickupLat,
    long: pickupLong,
    destination: normalizedDestination,
    hasEnteredDestZone: false,
    arrivedNotified: false,
    insideSince: null,
    passedNotified: false,
    lastRoadCheckAt: 0,
    lastRoadDistanceM: null,
    osrmInFlight: false,
  });

  console.log(
    `[tracking][start] ride=${rideId} lat=${pickupLat} long=${pickupLong}`
  );

  emitLocation(io, rideId, pickupLat, pickupLong);
  return true;
}

async function updateLocation(io, rideId, lat, long, options = {}) {
  const emitLocationEvent = options?.emit_location !== false;
  const loc = rideLocations.get(rideId);
  if (!loc) return false;

  const latNum = toFiniteNumber(lat);
  const longNum = toFiniteNumber(long);
  if (latNum === null || longNum === null) return false;

  loc.lat = latNum;
  loc.long = longNum;

  const now = Date.now();
  let roadDist = null;

  if (loc.destination && !loc.passedNotified) {
    const distAir = getDistanceMeters(
      latNum,
      longNum,
      loc.destination.lat,
      loc.destination.long
    );
    const distAirForLog = Number.isFinite(distAir) ? distAir : null;

    if (distAirForLog !== null && distAirForLog <= AIR_TRIGGER_M) {
      const canCheckRoad =
        (!loc.lastRoadCheckAt || now - loc.lastRoadCheckAt >= ROAD_CHECK_EVERY_MS) &&
        !loc.osrmInFlight;

      if (canCheckRoad) {
        loc.lastRoadCheckAt = now;
        loc.osrmInFlight = true;

        try {
          roadDist = await getRoadDistanceMeters({
            fromLat: latNum,
            fromLong: longNum,
            toLat: loc.destination.lat,
            toLong: loc.destination.long,
          });

          if (Number.isFinite(roadDist)) {
            loc.lastRoadDistanceM = roadDist;
          } else {
            roadDist = null;
          }
        } catch (err) {
          console.warn(`[ride:${rideId}] OSRM error:`, err?.message || err);
          roadDist = null;
        } finally {
          loc.osrmInFlight = false;
        }

        console.log(
          `[ride:${rideId}] air=${Math.round(distAirForLog)}m road=${roadDist ?? "null"}m`
        );
      } else {
        roadDist = loc.lastRoadDistanceM ?? null;
      }
    }
  }

  const roadDistForDecision =
    (Number.isFinite(roadDist) ? roadDist : null) ??
    (Number.isFinite(loc.lastRoadDistanceM) ? loc.lastRoadDistanceM : null);

  if (loc.destination && !loc.passedNotified) {
    const dist = getDistanceMeters(
      latNum,
      longNum,
      loc.destination.lat,
      loc.destination.long
    );

    const inside =
      Number.isFinite(dist) &&
      dist <= ENTER_AIR_M &&
      (roadDistForDecision == null ? true : roadDistForDecision <= ENTER_ROAD_M);

    if (inside) {
      if (!loc.insideSince) loc.insideSince = now;
      if (now - loc.insideSince >= MIN_INSIDE_MS) {
        if (!loc.hasEnteredDestZone) {
          loc.hasEnteredDestZone = true;
        }

        if (!loc.arrivedNotified) {
          loc.arrivedNotified = true;
          const driverId = getActiveDriverByRide(rideId);
          const arrivedEvt = {
            ride_id: rideId,
            lat: latNum,
            long: longNum,
            destination: loc.destination,
            distance_m: Math.round(dist),
            road_distance_m: roadDistForDecision,
            message: "Arrived at destination",
            at: now,
          };

          io.to(`ride:${rideId}`).emit("ride:arrivedDestination", arrivedEvt);
          if (driverId) {
            io.to(`driver:${driverId}`).emit("ride:arrivedDestination", arrivedEvt);
            console.log(
              `[emit->driver] event=ride:arrivedDestination ride=${rideId} driver=${driverId}`
            );
          } else {
            console.log(
              `[emit->driver][skip] event=ride:arrivedDestination ride=${rideId} driver=none`
            );
          }

          console.log(
            `[ride:${rideId}] arrived destination air=${Math.round(dist)}m road=${
              roadDistForDecision ?? "null"
            }m`
          );
        }
      }
    } else if (!loc.hasEnteredDestZone) {
      loc.insideSince = null;
    }

    if (loc.hasEnteredDestZone && inside) {
      loc.passedNotified = true;

      const driverId = getActiveDriverByRide(rideId);
      const passedEvt = {
        ride_id: rideId,
        lat: latNum,
        long: longNum,
        destination: loc.destination,
        distance_m: Math.round(dist),
        road_distance_m: roadDistForDecision,
        threshold_m: ENTER_AIR_M,
        trigger: "within_25m_destination",
        message: "Within 25m of destination",
        at: now,
      };

      io.to(`ride:${rideId}`).emit("ride:passedDestination", passedEvt);
      if (driverId) {
        io.to(`driver:${driverId}`).emit("ride:passedDestination", passedEvt);
        console.log(
          `[emit->driver] event=ride:passedDestination ride=${rideId} driver=${driverId}`
        );
      } else {
        console.log(
          `[emit->driver][skip] event=ride:passedDestination ride=${rideId} driver=none`
        );
      }

      console.log(
        `[ride:${rideId}] passed destination air=${Math.round(dist)}m road=${
          roadDistForDecision ?? "null"
        }m`
      );
    }
  }

  if (emitLocationEvent) {
    emitLocation(io, rideId, latNum, longNum);
  }

  console.log(`[tracking][update] ride=${rideId} lat=${latNum} long=${longNum}`);
  return true;
}

function arriveAtPickup(io, rideId, lat, long) {
  const latNum = toFiniteNumber(lat);
  const longNum = toFiniteNumber(long);
  if (latNum === null || longNum === null) return;

  io.to(`ride:${rideId}`).emit("ride:arrived", {
    ride_id: rideId,
    lat: latNum,
    long: longNum,
  });

  console.log(`[tracking][arrived] ride=${rideId} at pickup`);
}

function stopTracking(io, rideId) {
  if (!rideLocations.has(rideId)) return false;

  const lastLoc = rideLocations.get(rideId);
  rideLocations.delete(rideId);

  console.log(`[tracking][stop] ride=${rideId}`);

  emitLocation(io, rideId, lastLoc?.lat ?? null, lastLoc?.long ?? null);

  io.to(`ride:${rideId}`).emit("ride:completed", {
    ride_id: rideId,
    message: "Ride completed - End trip",
  });

  console.log(`[tracking][completed] ride=${rideId}`);
  return true;
}

module.exports = { startTracking, updateLocation, arriveAtPickup, stopTracking };
