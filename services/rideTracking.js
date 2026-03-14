﻿// rideTracking.js

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
const PASS_RADIUS_M = envNumber("RIDE_TRACK_PASS_RADIUS_M", 60);

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
    hasEnteredPassZone: false,
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
        roadDist = Number.isFinite(loc.lastRoadDistanceM)
          ? loc.lastRoadDistanceM
          : null;
      }
    }
  }

  const roadDistForDecision =
    (Number.isFinite(roadDist) ? roadDist : null) ??
    (Number.isFinite(loc.lastRoadDistanceM) ? loc.lastRoadDistanceM : null);

  if (loc.destination && !loc.passedNotified && roadDistForDecision !== null) {
    const inPassZone = roadDistForDecision <= PASS_RADIUS_M;

    if (inPassZone && !loc.hasEnteredPassZone) {
      loc.hasEnteredPassZone = true;

      console.log(
        `[ride:${rideId}] entered pass zone road=${Math.round(roadDistForDecision)}m`
      );
    }

    if (loc.hasEnteredPassZone && !inPassZone) {
      loc.passedNotified = true;

      const driverId = getActiveDriverByRide(rideId);
      const passedEvt = {
        ride_id: rideId,
        lat: latNum,
        long: longNum,
        destination: loc.destination,
        road_distance_m: Math.round(roadDistForDecision),
        threshold_m: PASS_RADIUS_M,
        trigger: "entered_60m_road_then_exited",
        message: "Driver passed destination zone",
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
        `[ride:${rideId}] passed destination zone road=${Math.round(roadDistForDecision)}m`
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