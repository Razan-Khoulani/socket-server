// rideTracking.js

const { getDistanceMeters } = require("../utils/geo.util");
const { getActiveDriverByRide } = require("../store/activeRides.store");
const { getRideStatusSnapshot } = require("../store/rideStatusSnapshots.store");
const axios = require("axios");
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
const LARAVEL_BASE_URL =
  process.env.LARAVEL_BASE_URL ||
  process.env.LARAVEL_URL ||
  "https://aiactive.co.uk/backend/backend-laravel/public";

const LARAVEL_GET_ROUTE_PATH =
  process.env.LARAVEL_GET_ROUTE_PATH || "/api/getRoute";

const LARAVEL_TIMEOUT_MS = Number.isFinite(Number(process.env.LARAVEL_TIMEOUT_MS))
  ? Number(process.env.LARAVEL_TIMEOUT_MS)
  : 10000;

  const extractRoadDistanceMetersFromRouteApi = (payload) => {
  if (!payload || typeof payload !== "object") return null;

  const directM = toFiniteNumber(
    payload?.distance_m ??
      payload?.distanceMeters ??
      payload?.distance_meters ??
      null
  );
  if (directM !== null) return directM;

  const km = toFiniteNumber(
    payload?.route ??
      payload?.distance_km ??
      payload?.total_distance ??
      null
  );
  if (km !== null) return Math.round(km * 1000);

  const nestedM = toFiniteNumber(
    payload?.routes?.[0]?.distanceMeters ??
      payload?.routes?.[0]?.legs?.[0]?.distance?.value ??
      null
  );
  if (nestedM !== null) return nestedM;

  return null;
};
async function getRoadDistanceMetersFromLaravel({
  fromLat,
  fromLong,
  toLat,
  toLong,
}) {
  const safeFromLat = toFiniteNumber(fromLat);
  const safeFromLong = toFiniteNumber(fromLong);
  const safeToLat = toFiniteNumber(toLat);
  const safeToLong = toFiniteNumber(toLong);

  if (
    safeFromLat === null ||
    safeFromLong === null ||
    safeToLat === null ||
    safeToLong === null
  ) {
    return null;
  }

  try {
    const response = await axios.get(
      `${LARAVEL_BASE_URL}${LARAVEL_GET_ROUTE_PATH}`,
      {
        params: {
          startLongitude: safeFromLong,
          startLatitude: safeFromLat,
          endLongitude: safeToLong,
          endLatitude: safeToLat,
          requested_at: new Date().toISOString(),
        },
        timeout: LARAVEL_TIMEOUT_MS,
      }
    );

    const routeApiData = response?.data ?? null;
    const roadDistanceM = extractRoadDistanceMetersFromRouteApi(routeApiData);

    console.log("[rideTracking][routeApi]", {
      fromLat: safeFromLat,
      fromLong: safeFromLong,
      toLat: safeToLat,
      toLong: safeToLong,
      road_distance_m: roadDistanceM,
    });

    return roadDistanceM;
  } catch (err) {
    console.warn(
      "[rideTracking][routeApi] failed:",
      err?.response?.data || err?.message || err
    );
    return null;
  }
}

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
  const preferredDriverId = Number.isFinite(Number(options?.driver_id))
    ? Number(options.driver_id)
    : null;
  const loc = rideLocations.get(rideId);
  if (!loc) return false;

  const latNum = toFiniteNumber(lat);
  const longNum = toFiniteNumber(long);
  if (latNum === null || longNum === null) return false;

  loc.lat = latNum;
  loc.long = longNum;

  const now = Date.now();
  let roadDist = null;
  let distAirForDecision = null;

  if (loc.destination && !loc.passedNotified) {
    const distAir = getDistanceMeters(
      latNum,
      longNum,
      loc.destination.lat,
      loc.destination.long
    );

    distAirForDecision = Number.isFinite(distAir) ? distAir : null;

    if (distAirForDecision !== null && distAirForDecision <= AIR_TRIGGER_M) {
      const canCheckRoad =
        (!loc.lastRoadCheckAt || now - loc.lastRoadCheckAt >= ROAD_CHECK_EVERY_MS) &&
        !loc.osrmInFlight;

      if (canCheckRoad) {
        loc.lastRoadCheckAt = now;
        loc.osrmInFlight = true;

        try {
          roadDist = await getRoadDistanceMetersFromLaravel({
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
          console.warn(`[ride:${rideId}] route api error:`, err?.message || err);
          roadDist = null;
        } finally {
          loc.osrmInFlight = false;
        }

        console.log(
          `[ride:${rideId}] air=${Math.round(distAirForDecision)}m road=${roadDist ?? "null"}m`
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
  const distanceForDecision =
    roadDistForDecision != null ? roadDistForDecision : distAirForDecision;
  const distanceSource = roadDistForDecision != null ? "road" : "air";

  if (loc.destination && !loc.passedNotified && distanceForDecision !== null) {
    const inPassZone = distanceForDecision <= PASS_RADIUS_M;

    if (inPassZone && !loc.hasEnteredPassZone) {
      loc.hasEnteredPassZone = true;

      console.log(
        `[ride:${rideId}] entered pass zone ${distanceSource}=${Math.round(
          distanceForDecision
        )}m`
      );
    }

    if (loc.hasEnteredPassZone && !inPassZone) {
      loc.passedNotified = true;

      const snap = getRideStatusSnapshot(rideId);
      const driverId =
        preferredDriverId ??
        getActiveDriverByRide(rideId) ??
        (Number.isFinite(Number(snap?.driver_id)) ? Number(snap.driver_id) : null);
      const passedEvt = {
        ride_id: rideId,
        lat: latNum,
        long: longNum,
        destination: loc.destination,
        distance_m: Math.round(distanceForDecision),
        distance_source: distanceSource,
        ...(roadDistForDecision != null
          ? { road_distance_m: Math.round(roadDistForDecision) }
          : {}),
        threshold_m: PASS_RADIUS_M,
        trigger: "entered_60m_road_then_exited",
        message: "Driver passed destination zone",
        at: now,
      };

      io.to(`ride:${rideId}`).emit("ride:passedDestination", passedEvt);

      if (driverId) {
        io.to(`driver:${driverId}`).emit("ride:passedDestination", passedEvt);
        console.log("[passed-destination][emit][tracking]", {
          ride_id: rideId,
          driver_id: driverId,
          distance_m: Math.round(distanceForDecision),
          threshold_m: PASS_RADIUS_M,
          distance_source: distanceSource,
          road_distance_m:
            roadDistForDecision != null ? Math.round(roadDistForDecision) : null,
        });
        console.log(
          `[emit->driver] event=ride:passedDestination ride=${rideId} driver=${driverId}`
        );
      } else {
        console.log("[passed-destination][emit][tracking]", {
          ride_id: rideId,
          driver_id: null,
          distance_m: Math.round(distanceForDecision),
          threshold_m: PASS_RADIUS_M,
          distance_source: distanceSource,
          road_distance_m:
            roadDistForDecision != null ? Math.round(roadDistForDecision) : null,
        });
        console.log(
          `[emit->driver][skip] event=ride:passedDestination ride=${rideId} driver=none`
        );
      }

      console.log(
        `[ride:${rideId}] passed destination zone ${distanceSource}=${Math.round(
          distanceForDecision
        )}m`
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
