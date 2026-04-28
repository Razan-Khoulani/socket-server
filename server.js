// server.js
require("dotenv").config();
const express = require("express");
const http = require("http");
const { Server } = require("socket.io");
const axios = require("axios");

console.log(`[BOOT] socket server source file: ${__filename}`);

const driverLocationService = require("./services/driverLocation.service");
const ENABLE_RIDE_TRACKING_SERVICE =
  process.env.ENABLE_RIDE_TRACKING_SERVICE === "1";
const rideTracking = ENABLE_RIDE_TRACKING_SERVICE
  ? require("./services/rideTracking")
  : null;
const {
  setActiveRide,
  clearActiveRideByRideId,
  getActiveRideByDriver,
  getActiveDriverByRide,
} = require("./store/activeRides.store");
const {
  startRideRoute,
  appendRidePoint,
  getRideRoutePoints,
  clearRideRoute,
} = require("./store/rideRoutes.store");
const { getDistanceMeters } = require("./utils/geo.util");
const {
  upsertRideStatusSnapshot,
  getRideStatusSnapshot,
} = require("./store/rideStatusSnapshots.store");
const {
  emitAdminDriverUpdate,
} = require("./services/adminDriverFeed.service");
const { getDriverAdminProfile } = require("./services/adminDriverProfile.service");

// ✅ sockets
const driverSocket = require("./sockets/driver.socket");
const biddingSocket = require("./sockets/bidding.socket");
const userSocket = require("./sockets/user.socket");
const adminSocket = require("./sockets/admin.socket");


const DEBUG_SOCKET_EVENTS = process.env.DEBUG_SOCKET_EVENTS === "1";
const debugSocketLog = (...args) => {
  if (!DEBUG_SOCKET_EVENTS) return;
  console.log(...args);
};
const LARAVEL_BASE_URL =
  process.env.LARAVEL_BASE_URL ||
  process.env.LARAVEL_URL ||
  "https://aiactive.co.uk/backend/backend-laravel/public";
const LARAVEL_DRIVER_INVOICE_PATH = "/api/driver/transport-ride-invoice";
const LARAVEL_TIMEOUT_MS = 7000;
const SOCKET_BIND_HOST =
  process.env.SOCKET_BIND_HOST || "0.0.0.0";
const SOCKET_BIND_PORT = Number(process.env.SOCKET_BIND_PORT) || 4000;
const INVOICE_STATUSES = new Set([7, 8, 9]);
const TRIP_SUMMARY_START_STATUSES = new Set([5]);
const TRIP_SUMMARY_COMPLETE_STATUSES = new Set([7, 8, 11]);
const SIMPLE_PASSED_DEST_DISTANCE_M = Number.isFinite(
  Number(process.env.SIMPLE_PASSED_DEST_DISTANCE_M)
)
  ? Number(process.env.SIMPLE_PASSED_DEST_DISTANCE_M)
  : 25;
// ride statuses treated as terminal flow states.
// Note: status 8 is handled as a transition state in terminal handling below.
const FINAL_STATUSES = new Set([4, 6 ,7, 8, 9, 11]);
// const parseStatusSet = (raw, fallback = []) => {
//   const source = String(raw ?? "")
//     .split(",")
//     .map((value) => String(value).trim())
//     .filter((value) => value.length > 0);
//   const parsed = source
//     .map((value) => Number(value))
//     .filter((value) => Number.isFinite(value));
//   return parsed.length > 0 ? new Set(parsed) : new Set(fallback);
// };
// Activate queued ride when current ride reaches one of these terminal statuses.
// Keep status 8 excluded because it is treated as a transition state.
// const QUEUE_ACTIVATE_TERMINAL_STATUSES = parseStatusSet(
//   process.env.QUEUE_ACTIVATE_TERMINAL_STATUSES,
//   [4, 7, 9, 11]
// );
// Keep status 6 route cache until payment statuses (7/8/9) so trip summary/invoice
// can still read full distance even after destination reached.
const ROUTE_CLEAR_STATUSES = new Set([4, 9, 11]);
const INVOICE_TTL_MS = 10 * 60 * 1000;
const sentInvoiceForRide = new Set();
const invoiceInFlight = new Set();
const INVOICE_RETRY_DELAY_MS = 1500;
const INVOICE_MAX_RETRIES = 2;
const INVOICE_MIN_DISTANCE_KM = Number.isFinite(
  Number(process.env.INVOICE_MIN_DISTANCE_KM)
)
  ? Math.max(0, Number(process.env.INVOICE_MIN_DISTANCE_KM))
  : 0;
const INVOICE_MIN_DURATION_SECONDS = Number.isFinite(
  Number(process.env.INVOICE_MIN_DURATION_SECONDS)
)
  ? Math.max(60, Math.floor(Number(process.env.INVOICE_MIN_DURATION_SECONDS)))
  : 60;
const TRIP_DISTANCE_MAX_DIRECT_RATIO = Number.isFinite(
  Number(process.env.TRIP_DISTANCE_MAX_DIRECT_RATIO)
)
  ? Math.max(1.5, Number(process.env.TRIP_DISTANCE_MAX_DIRECT_RATIO))
  : 5;
const STATUS_DEDUPE_TTL_MS = Number.isFinite(
  Number(process.env.STATUS_DEDUPE_TTL_MS)
)
  ? Number(process.env.STATUS_DEDUPE_TTL_MS)
  : 1500;
const ENDED_DEDUPE_TTL_MS = Number.isFinite(
  Number(process.env.ENDED_DEDUPE_TTL_MS)
)
  ? Number(process.env.ENDED_DEDUPE_TTL_MS)
  : 5000;
const SIMPLE_PASSED_DEDUPE_TTL_MS = Number.isFinite(
  Number(process.env.SIMPLE_PASSED_DEDUPE_TTL_MS)
)
  ? Number(process.env.SIMPLE_PASSED_DEDUPE_TTL_MS)
  : 5000;
const statusDedupe = new Map(); // key -> timestamp
const endedDedupe = new Map(); // rideId -> timestamp
const simplePassedDedupe = new Map(); // key -> timestamp
const rideTripMeta = new Map(); // rideId -> trip runtime snapshot
const TRIP_META_TTL_MS = Number.isFinite(Number(process.env.TRIP_META_TTL_MS))
  ? Number(process.env.TRIP_META_TTL_MS)
  : 60 * 60 * 1000;

const toFiniteNumber = (v) => {
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
};

const statusOrder = (status) => {
  const s = toFiniteNumber(status);
  if (s == null) return null;
  // Treat cancel/end terminal statuses as highest priority to block stale rewinds.
  if (s === 4 || s === 10 || s === 11) return 100;
  return s;
};

const getStoredRideStatus = (rideId) => {
  const meta = rideTripMeta.get(rideId);
  return toFiniteNumber(meta?.ride_status);
};

const isRideStatusRegression = (rideId, nextStatus) => {
  const nextOrder = statusOrder(nextStatus);
  if (nextOrder == null) return false;
  const prevOrder = statusOrder(getStoredRideStatus(rideId));
  if (prevOrder == null) return false;
  return nextOrder < prevOrder;
};

const rememberRideStatus = (rideId, status) => {
  if (!rideId) return;
  const current = rideTripMeta.get(rideId) || {};
  rideTripMeta.set(rideId, {
    ...current,
    ride_status: status,
  });
  upsertRideStatusSnapshot(rideId, {
    ride_status: status,
    updated_at: Date.now(),
    source: "rememberRideStatus",
  });
  scheduleTripMetaCleanup(rideId);
};

const round2 = (v) => {
  const n = toFiniteNumber(v);
  if (n == null) return null;
  return Math.round(n * 100) / 100;
};

const parseLatLongString = (value) => {
  if (typeof value !== "string" || !value.includes(",")) return null;
  const [rawLat, rawLong] = value.split(",").map((item) => item.trim());
  const lat = toFiniteNumber(rawLat);
  const lng = toFiniteNumber(rawLong);
  if (lat == null || lng == null) return null;
  return { lat, lng };
};

const resolveNamedPoint = (source, prefix) => {
  if (!source || typeof source !== "object") return null;

  const directLat = toFiniteNumber(
    source[`${prefix}_lat`] ??
      source[`${prefix}Lat`] ??
      source[prefix]?.lat ??
      source[prefix]?.latitude
  );
  const directLng = toFiniteNumber(
    source[`${prefix}_long`] ??
      source[`${prefix}Long`] ??
      source[prefix]?.lng ??
      source[prefix]?.long ??
      source[prefix]?.longitude
  );
  if (directLat != null && directLng != null) {
    return { lat: directLat, lng: directLng };
  }

  return parseLatLongString(
    source[`${prefix}_latlong`] ??
      source[`${prefix}LatLong`] ??
      source[prefix]?.latlong
  );
};

const formatDurationHms = (seconds) => {
  const total = Math.max(0, Math.floor(toFiniteNumber(seconds) ?? 0));
  const h = String(Math.floor(total / 3600)).padStart(2, "0");
  const m = String(Math.floor((total % 3600) / 60)).padStart(2, "0");
  const s = String(total % 60).padStart(2, "0");
  return `${h}:${m}:${s}`;
};

const scheduleTripMetaCleanup = (rideId) => {
  const current = rideTripMeta.get(rideId);
  if (!current) return;
  if (current._cleanupTimer) clearTimeout(current._cleanupTimer);
  const timer = setTimeout(() => {
    const latest = rideTripMeta.get(rideId);
    if (!latest) return;
    if (latest._cleanupTimer === timer) {
      rideTripMeta.delete(rideId);
    }
  }, TRIP_META_TTL_MS);
  rideTripMeta.set(rideId, { ...current, _cleanupTimer: timer });
};

const resolveDriverPoint = ({ driverId, lat, long }) => {
  let la = toFiniteNumber(lat);
  let lo = toFiniteNumber(long);
  if ((la == null || lo == null) && driverId) {
    const last = driverLocationService.getDriver(Number(driverId));
    if (la == null) la = toFiniteNumber(last?.lat);
    if (lo == null) lo = toFiniteNumber(last?.long);
  }
  return { lat: la, long: lo };
};

const computeRouteDistanceKm = (rideId) => {
  const points = getRideRoutePoints(rideId);
  if (!points || points.length < 2) return 0;
  let meters = 0;
  for (let i = 1; i < points.length; i += 1) {
    const prev = points[i - 1];
    const next = points[i];
    const seg = getDistanceMeters(
      toFiniteNumber(prev?.lat),
      toFiniteNumber(prev?.lng),
      toFiniteNumber(next?.lat),
      toFiniteNumber(next?.lng)
    );
    if (Number.isFinite(seg) && seg > 0) meters += seg;
  }
  return round2(meters / 1000) ?? 0;
};

const computeDirectDistanceKm = ({
  startLat,
  startLong,
  endLat,
  endLong,
}) => {
  const sLat = toFiniteNumber(startLat);
  const sLong = toFiniteNumber(startLong);
  const eLat = toFiniteNumber(endLat);
  const eLong = toFiniteNumber(endLong);
  if (sLat == null || sLong == null || eLat == null || eLong == null) return null;

  const meters = getDistanceMeters(sLat, sLong, eLat, eLong);
  if (!Number.isFinite(meters) || meters <= 0) return null;
  return round2(meters / 1000) ?? meters / 1000;
};

const resolveTripDistanceKm = ({
  computedDistanceKm,
  payloadDistanceKm,
  storedDistanceKm,
  directDistanceKm = null,
}) => {
  const computed = toFiniteNumber(computedDistanceKm);
  const payload = toFiniteNumber(payloadDistanceKm);
  const stored = toFiniteNumber(storedDistanceKm);
  const direct = toFiniteNumber(directDistanceKm);

  console.log("[distance][resolveTripDistanceKm]", {
    computedDistanceKm: computed,
    payloadDistanceKm: payload,
    storedDistanceKm: stored,
    directDistanceKm: direct,
  });

if (computed != null && computed > 0) {
  return round2(computed) ?? computed;
}

if (stored != null && stored > 0) {
  return round2(stored) ?? stored;
}

if (payload != null && payload > 0) {
  if (
    direct != null &&
    direct > 0 &&
    payload > direct * TRIP_DISTANCE_MAX_DIRECT_RATIO
  ) {
    console.log("[distance][payload-rejected]", {
      payloadDistanceKm: payload,
      directDistanceKm: direct,
      ratioLimit: TRIP_DISTANCE_MAX_DIRECT_RATIO,
    });
    return round2(direct) ?? direct;
  }

  return round2(payload) ?? payload;
}

if (direct != null && direct > 0) {
  return round2(direct) ?? direct;
}

return 0;
};

const extractInvoiceDistanceKm = (payload) => {
  if (!payload || typeof payload !== "object") return null;
  return (
    toFiniteNumber(payload?.total_distance) ??
    toFiniteNumber(payload?.trip_distance_km) ??
    toFiniteNumber(payload?.distance_km) ??
    toFiniteNumber(payload?.updated_total_distance_km) ??
    toFiniteNumber(payload?.invoice?.total_distance) ??
    toFiniteNumber(payload?.invoice?.trip_distance_km) ??
    toFiniteNumber(payload?.trip_summary?.distance_km) ??
    toFiniteNumber(payload?.invoice?.trip_summary?.distance_km) ??
    null
  );
};

const extractInvoiceTotalPay = (payload) => {
  if (!payload || typeof payload !== "object") return null;
  return (
    toFiniteNumber(payload?.total_pay) ??
    toFiniteNumber(payload?.total_amount) ??
    toFiniteNumber(payload?.final_price) ??
    toFiniteNumber(payload?.invoice?.total_pay) ??
    toFiniteNumber(payload?.invoice?.total_amount) ??
    toFiniteNumber(payload?.invoice?.final_price) ??
    toFiniteNumber(payload?.trip_summary?.final_price) ??
    toFiniteNumber(payload?.invoice?.trip_summary?.final_price) ??
    null
  );
};

const hasPositiveNumber = (value) => Number.isFinite(value) && value > 0;

const applyInvoiceMinimums = (distanceKm, durationSeconds) => {
  const parsedDistance = toFiniteNumber(distanceKm);
  const parsedDuration = toFiniteNumber(durationSeconds);

  const safeDistance = hasPositiveNumber(parsedDistance)
    ? Math.max(parsedDistance, INVOICE_MIN_DISTANCE_KM)
    : 0;
  const safeDurationSeconds = hasPositiveNumber(parsedDuration)
    ? Math.max(Math.round(parsedDuration), INVOICE_MIN_DURATION_SECONDS)
    : INVOICE_MIN_DURATION_SECONDS;

  return {
    distance_km: round2(safeDistance) ?? INVOICE_MIN_DISTANCE_KM,
    duration_seconds: safeDurationSeconds,
    duration_hms: formatDurationHms(safeDurationSeconds),
  };
};

const applyInvoiceFallbacks = ({ invoicePayload, tripSummary, statusPayload }) => {
  const safeInvoice =
    invoicePayload && typeof invoicePayload === "object" ? { ...invoicePayload } : {};
  const nestedInvoice =
    safeInvoice.invoice && typeof safeInvoice.invoice === "object"
      ? { ...safeInvoice.invoice }
      : null;

  const fallbackDistanceKm =
    extractInvoiceDistanceKm(safeInvoice) ??
    extractInvoiceDistanceKm(statusPayload) ??
    toFiniteNumber(tripSummary?.distance_km) ??
    null;
  const fallbackTotalPay =
    extractInvoiceTotalPay(safeInvoice) ??
    extractInvoiceTotalPay(statusPayload) ??
    toFiniteNumber(tripSummary?.final_price) ??
    null;

  if (hasPositiveNumber(fallbackDistanceKm)) {
    safeInvoice.total_distance = fallbackDistanceKm;
    safeInvoice.trip_distance_km = fallbackDistanceKm;
  }
  if (hasPositiveNumber(fallbackTotalPay)) {
    safeInvoice.total_pay = fallbackTotalPay;
  }

  if (tripSummary && typeof tripSummary === "object") {
    safeInvoice.trip_started_date_time = tripSummary.started_date_time;
    safeInvoice.trip_ended_date_time = tripSummary.ended_date_time;
    safeInvoice.trip_duration_seconds = tripSummary.duration_seconds;
    safeInvoice.trip_duration_hms = tripSummary.duration_hms;
    safeInvoice.trip_distance_km =
      hasPositiveNumber(toFiniteNumber(safeInvoice.trip_distance_km))
        ? toFiniteNumber(safeInvoice.trip_distance_km)
        : tripSummary.distance_km;
    safeInvoice.trip_final_price =
      hasPositiveNumber(toFiniteNumber(safeInvoice.trip_final_price))
        ? toFiniteNumber(safeInvoice.trip_final_price)
        : tripSummary.final_price;
  }

  const minimumMetrics = applyInvoiceMinimums(
    toFiniteNumber(safeInvoice.trip_distance_km ?? safeInvoice.total_distance),
    toFiniteNumber(safeInvoice.trip_duration_seconds ?? tripSummary?.duration_seconds)
  );
  safeInvoice.total_distance = minimumMetrics.distance_km;
  safeInvoice.trip_distance_km = minimumMetrics.distance_km;
  safeInvoice.trip_duration_seconds = minimumMetrics.duration_seconds;
  safeInvoice.trip_duration_hms = minimumMetrics.duration_hms;

  if (nestedInvoice) {
    nestedInvoice.total_distance = safeInvoice.total_distance;
    nestedInvoice.trip_distance_km =
      safeInvoice.trip_distance_km ?? safeInvoice.total_distance;
    nestedInvoice.trip_duration_seconds = safeInvoice.trip_duration_seconds;
    nestedInvoice.trip_duration_hms = safeInvoice.trip_duration_hms;
    if (hasPositiveNumber(toFiniteNumber(safeInvoice.total_pay))) {
      nestedInvoice.total_pay = safeInvoice.total_pay;
    }
    safeInvoice.invoice = nestedInvoice;
  }

  return safeInvoice;
};

const buildTripSummarySnapshot = ({ rideId, driverId, rideStatus, lat, long, payload }) => {
  const now = Date.now();
  const current = rideTripMeta.get(rideId) || {};
  const routePoints = getRideRoutePoints(rideId);
  const firstPoint = routePoints[0];
  const resolved = resolveDriverPoint({ driverId, lat, long });

  const startedAt =
    toFiniteNumber(current.started_at) ??
    toFiniteNumber(firstPoint?.at) ??
    null;
  if (startedAt == null) return null;

  const startLat =
    toFiniteNumber(current.start_lat) ??
    toFiniteNumber(firstPoint?.lat) ??
    resolved.lat ??
    null;
  const startLong =
    toFiniteNumber(current.start_long) ??
    toFiniteNumber(firstPoint?.lng) ??
    resolved.long ??
    null;

  let endedAt = toFiniteNumber(current.ended_at);
  if (
    endedAt == null &&
    (TRIP_SUMMARY_COMPLETE_STATUSES.has(rideStatus) || FINAL_STATUSES.has(rideStatus))
  ) {
    endedAt = now;
  }

  const endLat =
    toFiniteNumber(current.end_lat) ??
    (endedAt != null ? resolved.lat ?? startLat : null);
  const endLong =
    toFiniteNumber(current.end_long) ??
    (endedAt != null ? resolved.long ?? startLong : null);

  const computedDistanceKm = computeRouteDistanceKm(rideId);
  const directDistanceKm = computeDirectDistanceKm({
    startLat,
    startLong,
    endLat,
    endLong,
  });
  const payloadDistanceKm =
    toFiniteNumber(payload?.total_distance) ??
    toFiniteNumber(payload?.trip_distance_km) ??
    toFiniteNumber(payload?.distance_km) ??
    toFiniteNumber(payload?.updated_total_distance_km);
  const distanceKm = resolveTripDistanceKm({
    computedDistanceKm,
    payloadDistanceKm,
    storedDistanceKm: current.distance_km,
    directDistanceKm,
  });
  const payloadFinalPrice =
    toFiniteNumber(payload?.total_pay) ??
    toFiniteNumber(payload?.total_amount) ??
    toFiniteNumber(payload?.final_price);
  const storedFinalPrice = toFiniteNumber(current.final_price);
  const finalPrice =
    (storedFinalPrice != null && storedFinalPrice > 0
      ? storedFinalPrice
      : null) ??
    (payloadFinalPrice != null && payloadFinalPrice > 0
      ? payloadFinalPrice
      : null) ??
    storedFinalPrice ??
    payloadFinalPrice ??
    null;

  const durationSeconds =
    endedAt != null
      ? Math.max(0, Math.round((endedAt - startedAt) / 1000))
      : null;
  const completed = endedAt != null;
  const finalMetrics = completed
    ? applyInvoiceMinimums(distanceKm, durationSeconds)
    : {
        distance_km: distanceKm,
        duration_seconds: durationSeconds,
        duration_hms: durationSeconds != null ? formatDurationHms(durationSeconds) : null,
      };

  return {
    ride_id: rideId,
    driver_id: driverId ?? null,
    ride_status: rideStatus,
    stage: completed ? "completed" : "in_progress",
    started_at: startedAt,
    started_date_time: new Date(startedAt).toISOString(),
    ended_at: endedAt,
    ended_date_time: endedAt != null ? new Date(endedAt).toISOString() : null,
    start_location: {
      lat: startLat,
      long: startLong,
    },
    end_location: {
      lat: endLat,
      long: endLong,
    },
    duration_seconds: finalMetrics.duration_seconds,
    duration_hms: finalMetrics.duration_hms,
    distance_km: finalMetrics.distance_km,
    final_price: finalPrice ?? null,
    at: now,
  };
};

const logServerEmit = (event, room, payload) => {
  console.log(`[emit] event=${event} room=${room}`, payload);
};

const emitTripSummaryEvent = ({ rideId, driverId, rideStatus, lat, long, payload }) => {
  console.log(
    `[tripSummary][check] ride:${rideId} status:${rideStatus} driver:${driverId ?? "null"}`
  );

  const shouldStart = TRIP_SUMMARY_START_STATUSES.has(rideStatus);
  const shouldComplete = TRIP_SUMMARY_COMPLETE_STATUSES.has(rideStatus);

  if (!shouldStart && !shouldComplete) {
    console.log(
      `[tripSummary][skip] ride:${rideId} status:${rideStatus} not in start:[${Array.from(
        TRIP_SUMMARY_START_STATUSES
      ).join(",")}] complete:[${Array.from(TRIP_SUMMARY_COMPLETE_STATUSES).join(",")}]`
    );
    return;
  }

  const now = Date.now();
  const resolved = resolveDriverPoint({ driverId, lat, long });
  const current = rideTripMeta.get(rideId) || {};

  if (shouldStart) {
    current.driver_id = driverId ?? current.driver_id ?? null;
    current.ride_status = rideStatus;

    if (!current.started_at) {
      current.started_at = now;
      current.start_lat = resolved.lat;
      current.start_long = resolved.long;
    } else {
      if (current.start_lat == null) current.start_lat = resolved.lat;
      if (current.start_long == null) current.start_long = resolved.long;
    }

    rideTripMeta.set(rideId, current);
    scheduleTripMetaCleanup(rideId);

    const startedEvt = {
      ride_id: rideId,
      driver_id: driverId ?? null,
      ride_status: rideStatus,
      stage: "started",
      started_at: current.started_at,
      started_date_time: new Date(current.started_at).toISOString(),
      start_location: {
        lat: current.start_lat ?? null,
        long: current.start_long ?? null,
      },
      at: now,
    };

    const rideRoom = `ride:${rideId}`;
    io.to(rideRoom).emit("ride:tripSummary", startedEvt);
    logServerEmit("ride:tripSummary", rideRoom, startedEvt);

    if (driverId) {
      const driverRoom = `driver:${driverId}`;
      io.to(driverRoom).emit("ride:tripSummary", startedEvt);
      logServerEmit("ride:tripSummary", driverRoom, startedEvt);
    }
    return;
  }

  if (!current.started_at) {
    const routePoints = getRideRoutePoints(rideId);
    const firstPoint = routePoints[0];
    current.started_at = toFiniteNumber(firstPoint?.at) ?? now;
    current.start_lat =
      current.start_lat ??
      toFiniteNumber(firstPoint?.lat) ??
      resolved.lat;
    current.start_long =
      current.start_long ??
      toFiniteNumber(firstPoint?.lng) ??
      resolved.long;
  }

  current.ended_at = now;
  current.end_lat = resolved.lat;
  current.end_long = resolved.long;
  current.driver_id = driverId ?? current.driver_id ?? null;
  current.ride_status = rideStatus;

  const durationSeconds = Math.max(
    0,
    Math.round((current.ended_at - current.started_at) / 1000)
  );
  const computedDistanceKm = computeRouteDistanceKm(rideId);
  const directDistanceKm = computeDirectDistanceKm({
    startLat: current.start_lat,
    startLong: current.start_long,
    endLat: current.end_lat,
    endLong: current.end_long,
  });
  const payloadDistanceKm =
    toFiniteNumber(payload?.total_distance) ??
    toFiniteNumber(payload?.trip_distance_km) ??
    toFiniteNumber(payload?.distance_km) ??
    toFiniteNumber(payload?.updated_total_distance_km);
  const distanceKm = resolveTripDistanceKm({
    computedDistanceKm,
    payloadDistanceKm,
    storedDistanceKm: current.distance_km,
    directDistanceKm,
  });
  const minimumMetrics = applyInvoiceMinimums(distanceKm, durationSeconds);
  const finalPrice =
    toFiniteNumber(payload?.total_pay) ??
    toFiniteNumber(payload?.total_amount) ??
    toFiniteNumber(payload?.final_price);

  const completedEvt = {
    ride_id: rideId,
    driver_id: driverId ?? null,
    ride_status: rideStatus,
    stage: "completed",
    started_at: current.started_at,
    started_date_time: new Date(current.started_at).toISOString(),
    ended_at: current.ended_at,
    ended_date_time: new Date(current.ended_at).toISOString(),
    start_location: {
      lat: current.start_lat ?? null,
      long: current.start_long ?? null,
    },
    end_location: {
      lat: current.end_lat ?? null,
      long: current.end_long ?? null,
    },
    duration_seconds: minimumMetrics.duration_seconds,
    duration_hms: minimumMetrics.duration_hms,
    distance_km: minimumMetrics.distance_km,
    final_price: finalPrice,
    at: now,
  };

  current.distance_km = minimumMetrics.distance_km;
  current.duration_seconds = minimumMetrics.duration_seconds;
  current.final_price = finalPrice ?? null;

  rideTripMeta.set(rideId, current);
  scheduleTripMetaCleanup(rideId);

  const rideRoom = `ride:${rideId}`;
  io.to(rideRoom).emit("ride:tripSummary", completedEvt);
  logServerEmit("ride:tripSummary", rideRoom, completedEvt);

  if (driverId) {
    const driverRoom = `driver:${driverId}`;
    io.to(driverRoom).emit("ride:tripSummary", completedEvt);
    logServerEmit("ride:tripSummary", driverRoom, completedEvt);
  }
};

const shouldSkipDedupe = (map, key, ttlMs) => {
  const now = Date.now();
  const last = map.get(key);
  if (last && now - last < ttlMs) return true;
  map.set(key, now);
  setTimeout(() => {
    if (map.get(key) === now) map.delete(key);
  }, ttlMs);
  return false;
};

const app = express();
app.use(express.json());

const server = http.createServer(app);
const io = new Server(server, { cors: { origin: "*" } });

const markInvoiceSent = (rideId) => {
  sentInvoiceForRide.add(rideId);
  setTimeout(() => sentInvoiceForRide.delete(rideId), INVOICE_TTL_MS);
};

const tryEmitDriverInvoice = async ({
  rideId,
  driverId,
  rideStatus,
  statusPayload = null,
  retry = 0,
}) => {
  console.log(
    `[invoice][check] ride:${rideId} status:${rideStatus} driver:${driverId ?? "null"}`
  );

  if (!driverId) {
    console.log(`[invoice][skip] ride:${rideId} missing driver_id`);
    return;
  }
  if (sentInvoiceForRide.has(rideId)) {
    console.log(`[invoice][skip] ride:${rideId} already sent recently`);
    return;
  }
  if (invoiceInFlight.has(rideId)) {
    console.log(`[invoice][skip] ride:${rideId} request already in-flight`);
    return;
  }
  if (!INVOICE_STATUSES.has(rideStatus)) {
    console.log(
      `[invoice][skip] ride:${rideId} status:${rideStatus} not in [${Array.from(
        INVOICE_STATUSES
      ).join(",")}]`
    );
    return;
  }

  const meta = driverLocationService.getMeta(driverId) || {};

console.log("[invoice][meta-check]", {
  driver_id: driverId,
  driver_service_id: meta?.driver_service_id ?? null,
  has_access_token: !!meta?.access_token,
});
  const driverServiceId = Number(
    meta.driver_service_id ?? meta.driverServiceId ?? null
  );
  const accessToken =
    meta.access_token ??
    meta.driver_access_token ??
    meta.driverAccessToken ??
    null;

  if (!driverServiceId || !accessToken) {
    console.warn(
      `[invoice] missing driver_service_id/access_token for driver ${driverId}, ride ${rideId}`
    );
    return;
  }

  invoiceInFlight.add(rideId);

  try {
    const res = await axios.post(
      `${LARAVEL_BASE_URL}${LARAVEL_DRIVER_INVOICE_PATH}`,
      {
        driver_id: driverId,
        access_token: accessToken,
        driver_service_id: driverServiceId,
        ride_id: rideId,
      },
      { timeout: LARAVEL_TIMEOUT_MS }
    );

    let invoicePayload = res?.data ?? null;
    if (typeof invoicePayload === "string") {
      try {
        invoicePayload = JSON.parse(invoicePayload);
      } catch (_) {}
    }

    const mergedPayloadForSummary =
      (statusPayload && typeof statusPayload === "object") ||
      (invoicePayload && typeof invoicePayload === "object")
        ? {
            ...(statusPayload && typeof statusPayload === "object" ? statusPayload : {}),
            ...(invoicePayload && typeof invoicePayload === "object" ? invoicePayload : {}),
          }
        : null;

    const tripSummary = buildTripSummarySnapshot({
      rideId,
      driverId,
      rideStatus,
      payload: mergedPayloadForSummary,
    });

    invoicePayload = applyInvoiceFallbacks({
      invoicePayload,
      tripSummary,
      statusPayload,
    });

    const finalDistance = extractInvoiceDistanceKm(invoicePayload);
    const finalTotalPay = extractInvoiceTotalPay(invoicePayload);
    const stillDefaultLike =
      !hasPositiveNumber(finalDistance) && !hasPositiveNumber(finalTotalPay);

    if (stillDefaultLike && rideStatus === 7 && retry < INVOICE_MAX_RETRIES) {
      console.log(
        `[invoice][retry] ride:${rideId} status:${rideStatus} retry:${retry + 1} waiting for final invoice values`
      );
      setTimeout(() => {
        void tryEmitDriverInvoice({
          rideId,
          driverId,
          rideStatus,
          statusPayload,
          retry: retry + 1,
        });
      }, INVOICE_RETRY_DELAY_MS * (retry + 1));
      return;
    }

    markInvoiceSent(rideId);


    const invoiceEvt = {
      ride_id: rideId,
      ride_status: rideStatus,
      invoice: invoicePayload,
      trip_summary: tripSummary,
      at: Date.now(),
    };
    const driverRoom = `driver:${driverId}`;
    io.to(driverRoom).emit("ride:invoice", invoiceEvt);
    logServerEmit("ride:invoice", driverRoom, invoiceEvt);

    console.log(
      `[invoice] sent to driver ${driverId} for ride ${rideId} (status ${rideStatus})`
    );
  } catch (e) {
    console.error(
      "[invoice] API call failed:",
      e?.response?.data || e.message
    );
  } finally {
    invoiceInFlight.delete(rideId);
  }
};

// ────────────────────────────────────────────────
// Connection
// ────────────────────────────────────────────────
io.on("connection", (socket) => {
  console.log(`[NEW CONNECTION] socket.id = ${socket.id}`);
  if (DEBUG_SOCKET_EVENTS) {
    socket.onAny((event, ...args) => {
      console.log("[onAny] event:", event, "args:", args);
    });
  }

  // ✅ driver socket is the main handler
  driverSocket(io, socket);
  userSocket(io, socket); // ✅ لازم هالسطر
  biddingSocket(io, socket);
  adminSocket(io, socket);
  // ✅ bidding
  // Enable only if biddingSocket is implemented as a socket handler
  // biddingSocket(io, socket);

  socket.on("disconnect", () => {
    console.log(`[DISCONNECT] socket ${socket.id} disconnected`);
  });
});

// ────────────────────────────────────────────────
// Internal Endpoints (from Laravel)
// ────────────────────────────────────────────────

app.post("/events/internal/driver-status-updated", async (req, res) => {
  const { driver_id, old_status, new_status } = req.body;

  if (!driver_id) {
    console.warn("[driver-status-updated] Missing driver_id");
    return res.status(400).json({ status: 0, message: "driver_id required" });
  }

  console.log(
    `[STATUS UPDATE] driver:${driver_id} from ${old_status} to ${new_status}`
  );

  const room = `driver:${driver_id}`;
  const safeDriverId = Number(driver_id);

  if (Number(new_status) === 1) {
    io.in(room).socketsJoin("drivers:online");
    console.log(`[ONLINE] driver:${driver_id} joined drivers:online`);

    try {
      const profile = await getDriverAdminProfile({ driverId: safeDriverId });
      const profileLat = Number(profile?.current_lat);
      const profileLong = Number(profile?.current_long);

      if (Number.isFinite(profileLat) && Number.isFinite(profileLong)) {
        driverLocationService.updateMemory(safeDriverId, profileLat, profileLong);
      }

      driverLocationService.updateMeta(safeDriverId, {
        ...(profile || {}),
        dashboard_is_online: true,
        is_online: true,
        last_activity_at: Date.now(),
        updatedAt: Date.now(),
      });
    } catch (error) {
      console.warn("[driver-status-updated] admin profile sync failed", {
        driver_id: safeDriverId,
        error: error?.message || error,
      });
    }

    emitAdminDriverUpdate(io, safeDriverId);
  } else {
    const previousMeta = driverLocationService.getMeta(safeDriverId) || {};
    io.in(room).socketsLeave("drivers:online");
    console.log(`[OFFLINE] driver:${driver_id} left drivers:online`);

    const offlineNow = Date.now();
    driverLocationService.updateMeta(safeDriverId, {
      ...previousMeta,
      is_online: false,
      dashboard_is_online: false,
      socket_disconnected: true,
      last_activity_at: offlineNow,
      lastSeen: offlineNow,
      updatedAt: offlineNow,
    });
    console.log(`[MEMORY] driver:${driver_id} marked offline`);
    emitAdminDriverUpdate(io, safeDriverId);
  }
  

  io.emit("driver:status-updated", { driver_id, old_status, new_status });

  res.json({ status: 1 });

});

app.post("/events/internal/driver-location", async (req, res) => {
  const { driver_id, lat, lng, long } = req.body;

  if (!driver_id) {
    console.warn("[driver-location] Missing driver_id");
    return res.status(400).json({ status: 0, message: "driver_id required" });
  }

  const la = Number(lat);
  const lo = Number(lng ?? long);

  if (isNaN(la) || isNaN(lo)) {
    console.warn("[driver-location] Invalid coordinates");
    return res.status(400).json({ status: 0, message: "Invalid lat/long" });
  }

  console.log(
    `[LOCATION FROM LARAVEL] driver:${driver_id} -> lat:${la}, long:${lo}`
  );

  const driverId = Number(driver_id);
  const driverServiceId = Number(req.body.driver_service_id);
  const vehicleTypeId = Number(req.body.vehicle_type_id);
  const explicitStatusNumber = Number(req.body.current_status ?? req.body.new_status);
  const hasExplicitStatus = Number.isFinite(explicitStatusNumber);

  // ✅ update memory
  driverLocationService.updateMemory(driverId, la, lo);

  try {
    const profile = await getDriverAdminProfile({
      driverId,
      driverServiceId,
    });

    if (profile) {
      driverLocationService.updateMeta(driverId, {
        ...profile,
        updatedAt: Date.now(),
      });
    }
  } catch (error) {
    console.warn("[driver-location] admin profile sync failed", {
      driver_id: driverId,
      driver_service_id: Number.isFinite(driverServiceId) ? driverServiceId : null,
      error: error?.message || error,
    });
  }

  // ✅ mark online only if socket is connected to driver room
  const existingMeta = driverLocationService.getMeta(driverId) || {};
  const driverRoom = `driver:${driverId}`;
  const room = io.sockets.adapter.rooms.get(driverRoom);
  const isOnlineByRoom = !!room && room.size > 0;
  const profileStatusNumber = Number(
    existingMeta.current_status ??
      existingMeta.driver_current_status ??
      existingMeta.new_status ??
      existingMeta.provider_current_status
  );
  const hasProfileStatus = Number.isFinite(profileStatusNumber);
  const resolvedOnline = hasExplicitStatus
    ? explicitStatusNumber === 1
    : hasProfileStatus
    ? profileStatusNumber === 1
    : (existingMeta.is_online === false ? false : isOnlineByRoom);
  const resolvedDashboardOnline = hasExplicitStatus
    ? explicitStatusNumber === 1
    : hasProfileStatus
    ? profileStatusNumber === 1
    : (existingMeta.dashboard_is_online === false ? false : isOnlineByRoom);

  driverLocationService.updateMeta(driverId, {
    ...(Number.isFinite(driverServiceId) ? { driver_service_id: driverServiceId } : {}),
    ...(Number.isFinite(vehicleTypeId) ? { vehicle_type_id: vehicleTypeId } : {}),
    dashboard_is_online: resolvedDashboardOnline,
    last_activity_at: Date.now(),
    is_online: resolvedOnline,
    lastSeen: resolvedOnline ? undefined : Date.now(),
    updatedAt: Date.now(),
  });

  // ✅ broadcast to driver's room
  emitAdminDriverUpdate(io, driverId);
  io.to(`driver:${driverId}`).emit("driver:moved", {
    driver_id: driverId,
    lat: la,
    long: lo,
    timestamp: Date.now(),
  });

  const activeRideId = getActiveRideByDriver(driverId);
  if (activeRideId) {
    const now = Date.now();
    appendRidePoint(activeRideId, { lat: la, lng: lo, at: now });
    io.to(`ride:${activeRideId}`).emit("ride:locationUpdate", {
      ride_id: activeRideId,
      driver_id: driverId,
      lat: la,
      long: lo,
      at: now,
    });
  }

  res.json({ status: 1 });
});

app.post("/events/internal/ride-bid-dispatch", async (req, res) => {
  const dispatchPayload =
    req.body && typeof req.body === "object"
      ? { ...req.body, force_new_search_window: 1 }
      : { force_new_search_window: 1 };

  console.log(
    "[ride-bid-dispatch] Incoming request from Laravel",
    dispatchPayload
  );
  console.log("[dispatch][ride-bid-dispatch]", {
    ride_id: dispatchPayload?.ride_id ?? null,
    service_type_id: dispatchPayload?.service_type_id ?? null,
    radius: dispatchPayload?.radius ?? null,
    dispatch_timeout_s: dispatchPayload?.dispatch_timeout_s ?? null,
    dispatch_radius_stages_m: dispatchPayload?.dispatch_radius_stages_m ?? null,
    force_new_search_window: dispatchPayload?.force_new_search_window ?? null,
    additional_remarks:
      dispatchPayload?.additional_remarks ??
      dispatchPayload?.additional_remark ??
      dispatchPayload?.additional_request ??
      null,
  });

  try {
    const ok = await biddingSocket.restartRideDispatch(io, dispatchPayload);
    res.json({ status: ok ? 1 : 0 });
  } catch (e) {
    console.error("[ride-bid-dispatch] Failed:", e.message);
    res.status(500).json({ status: 0, message: "Dispatch failed" });
  }
});
app.post("/events/internal/ride-user-accepted", (req, res) => {
  const {
    ride_id,
    driver_id,
    provider_id,
    driver_detail_id,
    driver_service_id,
    offered_price,
    message,
    ride_details,
  } = req.body;
  const rideId = Number(ride_id);
  const providerId = Number(provider_id);
  const driverId = Number.isFinite(providerId) ? providerId : Number(driver_id);
  const driverDetailId = Number(driver_detail_id);
  const driverServiceId = Number(driver_service_id);
  const parsedPrice = offered_price == null ? null : Number(offered_price);
  const finalPrice = Number.isFinite(parsedPrice) ? parsedPrice : offered_price ?? null;

  if (!Number.isFinite(rideId) || !Number.isFinite(driverId)) {
    console.warn("[ride-user-accepted] Missing ride_id/provider_id");
    return res
      .status(400)
      .json({ status: 0, message: "ride_id & provider_id/driver_id required" });
  }

  console.log(
    `[ride-user-accepted] ride:${rideId} -> driver:${driverId} price:${finalPrice}`
  );

  upsertRideStatusSnapshot(rideId, {
    ride_status: 1,
    driver_id: driverId,
    provider_id: driverId,
    ...(Number.isFinite(driverDetailId)
      ? { driver_detail_id: driverDetailId, driver_details_id: driverDetailId }
      : {}),
    ...(Number.isFinite(driverServiceId) ? { driver_service_id: driverServiceId } : {}),
    updated_at: Date.now(),
    source: "ride-user-accepted",
  });

  const rideDetailsPayload =
    ride_details && typeof ride_details === "object"
      ? {
          ...ride_details,
          provider_id: driverId,
          ...(Number.isFinite(driverDetailId)
            ? { driver_detail_id: driverDetailId, driver_details_id: driverDetailId }
            : {}),
          ...(Number.isFinite(driverServiceId) ? { driver_service_id: driverServiceId } : {}),
        }
      : null;

  if (typeof biddingSocket.finalizeAcceptedRide === "function") {
    biddingSocket.finalizeAcceptedRide(io, rideId, driverId, finalPrice, {
      message: message || "User accepted the offer",
      rideDetails: rideDetailsPayload,
      driverIdentity: {
        provider_id: driverId,
        driver_detail_id: Number.isFinite(driverDetailId) ? driverDetailId : null,
        driver_service_id: Number.isFinite(driverServiceId) ? driverServiceId : null,
      },
    });
  } else {
    console.warn("[ride-user-accepted] finalizeAcceptedRide is not available");
  }

  return res.json({ status: 1 });
});

app.post("/events/internal/ride-extra-distance-accepted", (req, res) => {
  try {
    const body = req.body || {};
    const rideNum = Number(body.ride_id);
    if (!Number.isFinite(rideNum) || rideNum <= 0) {
      return res.status(400).json({ status: 0, message: "ride_id required" });
    }

    const providerNum = Number(body.provider_id);
    const driverNum = Number(body.driver_id);
    const resolvedDriverId =
      (Number.isFinite(providerNum) && providerNum > 0
        ? providerNum
        : Number.isFinite(driverNum) && driverNum > 0
        ? driverNum
        : getActiveDriverByRide(rideNum)) ?? null;
    let invoicePayload =
      body.invoice && typeof body.invoice === "object" ? body.invoice : null;
    if (!invoicePayload && typeof body.invoice === "string") {
      try {
        invoicePayload = JSON.parse(body.invoice);
      } catch (_) {}
    }
    const tripSummary =
      body.trip_summary && typeof body.trip_summary === "object"
        ? body.trip_summary
        : invoicePayload &&
          typeof invoicePayload === "object" &&
          invoicePayload.trip_summary &&
          typeof invoicePayload.trip_summary === "object"
        ? invoicePayload.trip_summary
        : null;

    const evt = {
      ride_id: rideNum,
      ride_status: Number.isFinite(Number(body.ride_status))
        ? Number(body.ride_status)
        : null,
      adjustment_id: Number.isFinite(Number(body.adjustment_id))
        ? Number(body.adjustment_id)
        : null,
      previous_total_distance: Number.isFinite(Number(body.previous_total_distance))
        ? Number(body.previous_total_distance)
        : null,
      extra_distance_km: Number.isFinite(Number(body.extra_distance_km))
        ? Number(body.extra_distance_km)
        : null,
      updated_total_distance: Number.isFinite(Number(body.updated_total_distance))
        ? Number(body.updated_total_distance)
        : null,
      cost_per_km: Number.isFinite(Number(body.cost_per_km))
        ? Number(body.cost_per_km)
        : null,
      previous_total_pay: Number.isFinite(Number(body.previous_total_pay))
        ? Number(body.previous_total_pay)
        : null,
      extra_fare_amount: Number.isFinite(Number(body.extra_fare_amount))
        ? Number(body.extra_fare_amount)
        : null,
      updated_total_pay: Number.isFinite(Number(body.updated_total_pay))
        ? Number(body.updated_total_pay)
        : null,
      invoice: invoicePayload,
      trip_summary: tripSummary,
      source: "laravel:accept-not-reached-destination",
      at: Number.isFinite(Number(body.at)) ? Number(body.at) : Date.now(),
    };
    const rideRoom = `ride:${rideNum}`;

    io.to(rideRoom).emit("ride:extraDistanceAccepted", evt);
    logServerEmit("ride:extraDistanceAccepted", rideRoom, evt);
    io.to(rideRoom).emit("ride:passedDestinationAccepted", evt);
    logServerEmit("ride:passedDestinationAccepted", rideRoom, evt);
    if (resolvedDriverId) {
      const driverRoom = `driver:${resolvedDriverId}`;
      io.to(driverRoom).emit("ride:extraDistanceAccepted", evt);
      logServerEmit("ride:extraDistanceAccepted", driverRoom, evt);
      io.to(driverRoom).emit("ride:passedDestinationAccepted", evt);
      logServerEmit("ride:passedDestinationAccepted", driverRoom, evt);

      if (invoicePayload && typeof invoicePayload === "object") {
         invoicePayload = applyInvoiceFallbacks({
    invoicePayload,
    tripSummary,
    statusPayload: evt,
  });
        const invoiceEvt = {
          ride_id: rideNum,
          ride_status: evt.ride_status,
          invoice: invoicePayload,
          trip_summary: tripSummary,
          at: evt.at,
          source: evt.source,
        };
        io.to(driverRoom).emit("ride:invoice", invoiceEvt);
        logServerEmit("ride:invoice", driverRoom, invoiceEvt);
      }
    }

    console.log(
      `[extra-distance][emit] ride:${rideNum} driver:${resolvedDriverId ?? "none"} events:ride:extraDistanceAccepted,ride:passedDestinationAccepted${invoicePayload ? ",ride:invoice" : ""} adjustment:${evt.adjustment_id ?? "none"} extra_km:${evt.extra_distance_km ?? "null"} extra_fare:${evt.extra_fare_amount ?? "null"} invoice:${invoicePayload ? "yes" : "no"}`
    );
    return res.json({ status: 1 });
  } catch (e) {
    console.error("❌ /events/internal/ride-extra-distance-accepted error:", e);
    return res.status(500).json({ status: 0, message: "Server error" });
  }
});

app.post("/events/internal/ride-trip-summary", (req, res) => {
  try {
    const { ride_id, driver_id, ride_status } = req.body || {};
    if (ride_id == null) {
      return res.status(400).json({ status: 0, message: "ride_id required" });
    }

    const rideId = Number(ride_id);
    if (!Number.isFinite(rideId) || rideId <= 0) {
      return res.status(400).json({ status: 0, message: "invalid ride_id" });
    }

    const current = rideTripMeta.get(rideId) || {};
    const driverId = Number.isFinite(Number(driver_id))
      ? Number(driver_id)
      : Number.isFinite(Number(current.driver_id))
      ? Number(current.driver_id)
      : null;

    const rideStatus = Number.isFinite(Number(ride_status))
      ? Number(ride_status)
      : Number.isFinite(Number(current.ride_status))
      ? Number(current.ride_status)
      : 8;

    const summary = buildTripSummarySnapshot({
      rideId,
      driverId,
      rideStatus,
      payload: null,
    });

    if (!summary) {
      return res.json({
        status: 0,
        message: "trip summary not found",
        ride_id: rideId,
      });
    }

    return res.json({
      status: 1,
      ride_id: rideId,
      trip_summary: summary,
    });
  } catch (e) {
    console.error("❌ /events/internal/ride-trip-summary error:", e);
    return res.status(500).json({ status: 0, message: "Server error" });
  }
});

// =========================
// Laravel -> Ride status updated (single realtime event)
// =========================
app.post("/events/internal/ride-status-updated", (req, res) => {
  try {
    const { ride_id, driver_id, ride_status, lat, long, payload, user_id } = req.body;

    if (ride_id == null || ride_status == null) {
      return res
        .status(400)
        .json({ status: 0, message: "ride_id & ride_status required" });
    }

    console.log("[internal][ride-status-updated]", {
      ride_id,
      driver_id: driver_id ?? null,
      ride_status,
      user_id: user_id ?? payload?.user_id ?? null,
    });

    const rideId = Number(ride_id);
    const driverNum = driver_id != null ? Number(driver_id) : null;
    const driverId = Number.isFinite(driverNum) ? driverNum : null;
    const status = Number(ride_status);
    const la = lat != null ? Number(lat) : null;
    const lo = long != null ? Number(long) : null;

    if (!Number.isFinite(rideId) || rideId <= 0 || !Number.isFinite(status)) {
      return res
        .status(400)
        .json({ status: 0, message: "invalid ride_id/ride_status" });
    }

    if (isRideStatusRegression(rideId, status)) {
      const latestStatus = getStoredRideStatus(rideId);
      console.log("[status][regression-skip]", {
        ride_id: rideId,
        incoming_status: status,
        latest_status: latestStatus,
      });
      return res.json({
        status: 1,
        ignored: 1,
        ride_id: rideId,
        ride_status: latestStatus ?? status,
      });
    }

    rememberRideStatus(rideId, status);

    if (status === 5) {
      const pickupPoint = resolveNamedPoint(payload, "pickup");
      const routeOpts = pickupPoint ? { pickup: pickupPoint } : {};
      if (Number.isFinite(la) && Number.isFinite(lo)) {
        startRideRoute(
          rideId,
          { lat: la, lng: lo, at: Date.now() },
          routeOpts
        );
      } else {
        startRideRoute(rideId, null, routeOpts);
      }
    }
    const userId =
      user_id ??
      payload?.user_id ??
      payload?.user_details?.user_id ??
      payload?.user?.id ??
      (typeof biddingSocket.getUserIdForRide === "function"
        ? biddingSocket.getUserIdForRide(rideId)
        : null);
    const cancelBy =
      typeof payload?.cancel_by === "string" && payload.cancel_by.trim() !== ""
        ? payload.cancel_by.trim()
        : null;
    const cancelReasonId = Number.isFinite(Number(payload?.cancel_reason_id))
      ? Number(payload.cancel_reason_id)
      : Number.isFinite(Number(payload?.reason_id))
      ? Number(payload.reason_id)
      : null;

    const evt = {
      ride_id: rideId,
      ride_status: status,
      ...(driverId ? { driver_id: driverId } : {}),
      ...(Number.isFinite(Number(userId)) ? { user_id: Number(userId) } : {}),
      ...(cancelBy ? { cancel_by: cancelBy } : {}),
      ...(cancelReasonId != null ? { reason_id: cancelReasonId } : {}),
    };
    const wayPointStatus = Number.isFinite(
      Number(payload?.way_point_status ?? payload?.wayPointStatus)
    )
      ? Number(payload?.way_point_status ?? payload?.wayPointStatus)
      : null;

    upsertRideStatusSnapshot(rideId, {
      ...evt,
      ...(wayPointStatus != null ? { way_point_status: wayPointStatus } : {}),
      updated_at: Date.now(),
      source: "ride-status-updated",
    });

    const latestStatusSnapshot = getRideStatusSnapshot(rideId);
    const adminDriverIdFromSnapshot = Number.isFinite(
      Number(latestStatusSnapshot?.driver_id)
    )
      ? Number(latestStatusSnapshot.driver_id)
      : null;
    const adminDriverId =
      driverId ?? getActiveDriverByRide(rideId) ?? adminDriverIdFromSnapshot ?? null;

    if (adminDriverId) {
const shouldReleaseRideRef =
  FINAL_STATUSES.has(status) && status !== 8;    const storedDriverPoint = driverLocationService.getDriver(adminDriverId) || {};
      const refreshLat =
        la != null ? la : toFiniteNumber(storedDriverPoint?.lat);
      const refreshLong =
        lo != null ? lo : toFiniteNumber(storedDriverPoint?.long);

      if (refreshLat != null && refreshLong != null) {
        // Refresh driver location timestamp on ride state transitions so
        // newly-finished drivers remain dispatchable without a manual
        // offline/online toggle.
        driverLocationService.updateMemory(adminDriverId, refreshLat, refreshLong);
      }

      driverLocationService.updateMeta(adminDriverId, {
        current_ride_id: shouldReleaseRideRef ? null : rideId,
        current_ride_status: status,
        raw_ride_status: status,
        latest_ride_status: status,
        last_activity_at: Date.now(),
        updatedAt: Date.now(),
      });
      emitAdminDriverUpdate(io, adminDriverId);
    }

    if (status === 4) {
      if (typeof biddingSocket.markRideCancelled === "function") {
        biddingSocket.markRideCancelled(io, rideId);
      } else if (typeof biddingSocket.removeRideFromAllInboxes === "function") {
        biddingSocket.removeRideFromAllInboxes(io, rideId);
        clearActiveRideByRideId(rideId);
      }
    }

    let destinationLat = toFiniteNumber(
      payload?.destination_lat ?? payload?.destination?.lat ?? payload?.destinationLat
    );
    let destinationLong = toFiniteNumber(
      payload?.destination_long ??
        payload?.destination?.long ??
        payload?.destinationLong
    );
    if (
      (destinationLat == null || destinationLong == null) &&
      typeof payload?.destination_latlong === "string"
    ) {
      const [rawDestLat, rawDestLong] = payload.destination_latlong
        .split(",")
        .map((v) => v.trim());
      if (destinationLat == null) destinationLat = toFiniteNumber(rawDestLat);
      if (destinationLong == null) destinationLong = toFiniteNumber(rawDestLong);
    }

    if (
      Number.isFinite(la) &&
      Number.isFinite(lo) &&
      destinationLat != null &&
      destinationLong != null
    ) {
      const currentDistanceM = getDistanceMeters(la, lo, destinationLat, destinationLong);
      const simpleKey = `${rideId}:near-destination`;
      const isDupSimplePassed = shouldSkipDedupe(
        simplePassedDedupe,
        simpleKey,
        SIMPLE_PASSED_DEDUPE_TTL_MS
      );
      if (
        Number.isFinite(currentDistanceM) &&
        currentDistanceM <= SIMPLE_PASSED_DEST_DISTANCE_M &&
        !isDupSimplePassed
      ) {
        const snap = getRideStatusSnapshot(rideId);
        const targetDriverId =
          driverId ??
          getActiveDriverByRide(rideId) ??
          (Number.isFinite(Number(snap?.driver_id)) ? Number(snap.driver_id) : null);
        const simplePassedEvt = {
          ride_id: rideId,
          ride_status: status,
          lat: la,
          long: lo,
          destination: {
            lat: destinationLat,
            long: destinationLong,
          },
          distance_m: Math.round(currentDistanceM),
          threshold_m: SIMPLE_PASSED_DEST_DISTANCE_M,
          trigger: "near_destination_simple",
          message: "near destination",
          source: "ride-status-updated",
          at: Date.now(),
        };

        io.to(`ride:${rideId}`).emit("ride:passedDestination", simplePassedEvt);
        const rideRoom = io.sockets.adapter.rooms.get(`ride:${rideId}`);
        const rideRoomCount = rideRoom ? rideRoom.size : 0;

        if (targetDriverId) {
          io.to(`driver:${targetDriverId}`).emit(
            "ride:passedDestination",
            simplePassedEvt
          );
          const driverRoom = io.sockets.adapter.rooms.get(`driver:${targetDriverId}`);
          const driverRoomCount = driverRoom ? driverRoom.size : 0;
          console.log("[passed-destination][emit][status]", {
            ride_id: rideId,
            driver_id: targetDriverId,
            distance_m: Math.round(currentDistanceM),
            threshold_m: SIMPLE_PASSED_DEST_DISTANCE_M,
            ride_room_sockets: rideRoomCount,
            driver_room_sockets: driverRoomCount,
            source: "ride-status-updated",
          });
          console.log(
            `[status][emit] ride:passedDestination ride:${rideId} driver:${targetDriverId} dist:${Math.round(
              currentDistanceM
            )}m -> rideRoom(sockets:${rideRoomCount}) driverRoom(sockets:${driverRoomCount})`
          );
        } else {
          console.log("[passed-destination][emit][status]", {
            ride_id: rideId,
            driver_id: null,
            distance_m: Math.round(currentDistanceM),
            threshold_m: SIMPLE_PASSED_DEST_DISTANCE_M,
            ride_room_sockets: rideRoomCount,
            driver_room_sockets: 0,
            source: "ride-status-updated",
          });
          console.log(
            `[status][emit] ride:passedDestination ride:${rideId} driver:none dist:${Math.round(
              currentDistanceM
            )}m -> rideRoom(sockets:${rideRoomCount})`
          );
        }
      } else {
        console.log("[passed-destination][skip][status]", {
          ride_id: rideId,
          driver_id: driverId ?? null,
          ride_status: status,
          distance_m: Number.isFinite(currentDistanceM)
            ? Math.round(currentDistanceM)
            : null,
          threshold_m: SIMPLE_PASSED_DEST_DISTANCE_M,
          deduped: !!isDupSimplePassed,
          reason: !Number.isFinite(currentDistanceM)
            ? "invalid-distance"
            : isDupSimplePassed
            ? "deduped"
            : currentDistanceM > SIMPLE_PASSED_DEST_DISTANCE_M
            ? "too-far-from-destination"
            : "not-emitted",
          destination_lat: destinationLat,
          destination_long: destinationLong,
          current_lat: la,
          current_long: lo,
        });
      }
    } else {
      console.log("[passed-destination][skip][status]", {
        ride_id: rideId,
        driver_id: driverId ?? null,
        ride_status: status,
        reason: "missing-current-or-destination-coordinates",
        destination_lat: destinationLat,
        destination_long: destinationLong,
        current_lat: la,
        current_long: lo,
      });
    }

    const statusKey = `${rideId}:${status}`;
    const isDupStatus = shouldSkipDedupe(
      statusDedupe,
      statusKey,
      STATUS_DEDUPE_TTL_MS
    );

    // ✅ keep active ride mapping in sync with driver status updates
if (
  driverId &&
  (status === 6 || !FINAL_STATUSES.has(status))
) {
  setActiveRide(driverId, rideId);
}

    if (!isDupStatus) {
      emitTripSummaryEvent({
        rideId,
        driverId,
        rideStatus: status,
        lat: la,
        long: lo,
        payload,
      });

      io.to(`ride:${rideId}`).emit("ride:statusUpdated", evt);
      const room = io.sockets.adapter.rooms.get(`ride:${rideId}`);
      const count = room ? room.size : 0;
      console.log(
        `[status][emit] ride:${rideId} status:${status} -> room:ride:${rideId} (sockets:${count})`
      );

      if (status === 4) {
        const cancelledEvt = {
          ...evt,
          ended: true,
          at: Date.now(),
        };
        upsertRideStatusSnapshot(rideId, {
          ...cancelledEvt,
          ...(wayPointStatus != null ? { way_point_status: wayPointStatus } : {}),
          updated_at: Date.now(),
          source: "ride-cancelled",
        });
        io.to(`ride:${rideId}`).emit("ride:cancelled", cancelledEvt);
        if (driverId) {
          io.to(`driver:${driverId}`).emit("ride:cancelled", cancelledEvt);
        }
      }
    } else {
      console.log(
        `[status][dedupe] skip ride:statusUpdated ride:${rideId} status:${status}`
      );
    }
    if (userId) {
      const uid = Number(userId);
      console.log(`[status][emit] ride:${rideId} status:${status} -> user:${uid} (no direct room emit)`);
    } else {
      console.log(`[status][emit] ride:${rideId} status:${status} -> user:unknown`);
    }
    if (driverId) {
      if (!isDupStatus) {
        io.to(`driver:${driverId}`).emit("ride:statusUpdated", evt);
        const droom = io.sockets.adapter.rooms.get(`driver:${driverId}`);
        const dcount = droom ? droom.size : 0;
        console.log(
          `[status][emit] ride:${rideId} status:${status} -> room:driver:${driverId} (sockets:${dcount})`
        );
      }
      void tryEmitDriverInvoice({
        rideId,
        driverId,
        rideStatus: status,
        statusPayload: payload,
      });
    }

    // ✅ If ride reached a terminal status, handle active mapping + notify
    if (FINAL_STATUSES.has(status)) {
      const activeDriverIdBeforeClear =
        driverId ?? getActiveDriverByRide(rideId) ?? null;
const isTransitionStatus = status === 7 || status === 8;
const shouldActivateQueuedRide = status === 9;

      if (isTransitionStatus) {
        console.log("[ride-status][transition-status]", {
          ride_id: rideId,
          status,
          activeDriverIdBeforeClear,
message: "skip clear/close; waiting for final completion status 9",        });
      } else if (shouldActivateQueuedRide) {
        clearActiveRideByRideId(rideId);

        const activated =
          activeDriverIdBeforeClear &&
          typeof biddingSocket.activateQueuedRideForDriver === "function"
            ? biddingSocket.activateQueuedRideForDriver(io, activeDriverIdBeforeClear)
            : false;

          console.log("[ride-status][queue-activation-after-terminal]", {
            ride_id: rideId,
            status,
            activeDriverIdBeforeClear,
            activated,
          });

        if (!activated && typeof biddingSocket.closeRideBidding === "function") {
          biddingSocket.closeRideBidding(io, rideId);
          console.log("[ride-status][queue-activation-fallback-close]", {
            ride_id: rideId,
            status,
          });
        }
      } else {
        clearActiveRideByRideId(rideId);

        if (status !== 4 && typeof biddingSocket.closeRideBidding === "function") {
          biddingSocket.closeRideBidding(io, rideId);
        }

        console.log("[ride-status][terminal-close]", {
          ride_id: rideId,
          status,
          activeDriverIdBeforeClear,
        });
      }

      if (ROUTE_CLEAR_STATUSES.has(status)) {
        clearRideRoute(rideId);
      } else {
        console.log(
          `[status][route-cache] keep route for ride:${rideId} status:${status}`
        );
      }

      const endEvt = { ...evt, ended: true };
      upsertRideStatusSnapshot(rideId, {
        ...endEvt,
        ...(wayPointStatus != null ? { way_point_status: wayPointStatus } : {}),
        updated_at: Date.now(),
        source: "ride-ended",
      });
const shouldEmitEndedEvent = !isTransitionStatus;      if (shouldEmitEndedEvent) {
        const isDupEnded = shouldSkipDedupe(
          endedDedupe,
          rideId,
          ENDED_DEDUPE_TTL_MS
        );
        if (!isDupEnded) {
          io.to(`ride:${rideId}`).emit("ride:ended", endEvt);
          const emitDriverId = driverId ?? activeDriverIdBeforeClear ?? null;
          if (emitDriverId) {
            io.to(`driver:${emitDriverId}`).emit("ride:ended", endEvt);
          }
        } else {
          console.log(`[status][dedupe] skip ride:ended ride:${rideId}`);
        }
      } else {
        console.log(
          `[status][skip] ride:ended ride:${rideId} status:${status} (transition status)`
        );
      }
    }

    return res.json({ status: 1 });
  } catch (e) {
    console.error("❌ /events/internal/ride-status-updated error:", e);
    return res.status(500).json({ status: 0, message: "Server error" });
  }
});




// =========================
// Laravel -> Start tracking
// =========================
app.post("/ride/start-tracking", (req, res) => {
  try {
    if (!ENABLE_RIDE_TRACKING_SERVICE) {
      return res.json({
        status: 1,
        message: "Tracking service disabled",
      });
    }

    const body = req.body || {};
    const {
      ride_id,
      pickup_lat,
      pickup_long,
      destination_lat,
      destination_long,
      current_lat,
      current_long,
    } = body;
    console.log("[tracking][start] request:", body);
    if (
      ride_id == null ||
      pickup_lat == null ||
      pickup_long == null ||
      destination_lat == null ||
      destination_long == null
    ) {
      return res.status(400).json({ status: 0, message: "Missing parameters" });
    }

    if (!io) {
      return res.status(500).json({ status: 0, message: "io not initialized" });
    }

    const rideId = Number(ride_id);
    const pickup = { lat: Number(pickup_lat), long: Number(pickup_long) };
    const destination = {
      lat: Number(destination_lat),
      long: Number(destination_long),
    };
    const currentLat = toFiniteNumber(current_lat);
    const currentLong = toFiniteNumber(current_long);

    if (currentLat != null && currentLong != null) {
      startRideRoute(
        rideId,
        { lat: currentLat, lng: currentLong, at: Date.now() },
        { pickup: { lat: pickup.lat, lng: pickup.long } }
      );
    } else {
      startRideRoute(rideId, null, {
        pickup: { lat: pickup.lat, lng: pickup.long },
      });
    }

    // بدء التتبع باستخدام الإحداثيات من الذاكرة
    rideTracking.startTracking(io, rideId, pickup, destination);

    console.log(`[tracking][start] ride ${rideId} started`);

    return res.json({
      status: 1,
      message: "Tracking started",
      ride_id: rideId,
    });
  } catch (e) {
    console.error("❌ /start-tracking error:", e);
    return res.status(500).json({ status: 0, message: "Server error" });
  }
});

// =========================
// Laravel -> Stop tracking
// =========================
app.post("/ride/stop-tracking", (req, res) => {
  try {
    if (!ENABLE_RIDE_TRACKING_SERVICE) {
      return res.json({
        status: 1,
        message: "Tracking service disabled",
      });
    }

    const body = req.body || {};
    const { ride_id } = body;

    if (ride_id == null) {
      return res.status(400).json({ status: 0, message: "ride_id required" });
    }

    if (!io) {
      return res.status(500).json({ status: 0, message: "io not initialized" });
    }

    const rideId = Number(ride_id);
    const stopped = rideTracking.stopTracking(io, rideId);
    // Do not clear active ride mapping here.
    // Terminal status handling (especially 8 -> 9 transition) owns this lifecycle.

    console.log(`[tracking][stop] ride ${rideId} stopped`);

    return res.json({
      status: 1,
      message: stopped
        ? "Tracking stopped"
        : "No active tracking for this ride",
      ride_id: rideId,
    });
  } catch (e) {
    console.error("❌ /stop-tracking error:", e);
    return res.status(500).json({ status: 0, message: "Server error" });
  }
});

// =========================
// Laravel -> Update location
// =========================
app.post("/ride/update-location", (req, res) => {
  try {
    const body = req.body || {};
    const { ride_id, driver_id, lat, long } = body;

    if (ride_id == null || lat == null || long == null) {
      return res.status(400).json({ status: 0, message: "Missing parameters" });
    }

    if (!io) {
      return res.status(500).json({ status: 0, message: "io not initialized" });
    }

    const rideId = Number(ride_id);
    const driverIdNum = Number(driver_id);
    const la = Number(lat);
    const lo = Number(long);

    if (isNaN(rideId) || isNaN(la) || isNaN(lo)) {
      return res.status(400).json({ status: 0, message: "Invalid numeric values" });
    }

    if (!ENABLE_RIDE_TRACKING_SERVICE) {
      io.to(`ride:${rideId}`).emit("ride:locationUpdate", {
        ride_id: rideId,
        lat: la,
        long: lo,
      });
      return res.json({
        status: 1,
        message: "Location updated (direct mode)",
        ride_id: rideId,
      });
    }

    rideTracking.updateLocation(io, rideId, la, lo, {
      driver_id: Number.isFinite(driverIdNum) ? driverIdNum : null,
    });

    return res.json({ status: 1, message: "Location updated", ride_id: rideId });
  } catch (e) {
    console.error("❌ /ride/update-location error:", e);
    return res.status(500).json({ status: 0, message: "Server error" });
  }
});

// =========================
// Laravel -> Driver ARRIVED
// =========================
app.post("/ride/arrived", (req, res) => {
  try {
    const body = req.body || {};
    const { ride_id, driver_id, lat, long, arrived_at } = body;

    if (ride_id == null) {
      return res.status(400).json({ status: 0, message: "ride_id required" });
    }

    if (!io) {
      return res.status(500).json({ status: 0, message: "io not initialized" });
    }

    const payload = {
      ride_id: Number(ride_id),
      driver_id: driver_id != null ? Number(driver_id) : null,
      lat: lat != null ? Number(lat) : null,
      long: long != null ? Number(long) : null,
      arrived_at: arrived_at ?? Date.now(),
    };

    if (!ENABLE_RIDE_TRACKING_SERVICE) {
      io.to(`ride:${payload.ride_id}`).emit("ride:arrived", payload);
    } else if (payload.lat != null && payload.long != null) {
      rideTracking.arriveAtPickup(io, payload.ride_id, payload.lat, payload.long);
    } else {
      io.to(`ride:${payload.ride_id}`).emit("ride:arrived", payload);
    }

    // بث للسائق
    if (payload.driver_id) {
      io.to(`driver:${payload.driver_id}`).emit("ride:arrived:ack", payload);
    }

    console.log(
      `[tracking][arrived] ride=${payload.ride_id} driver=${payload.driver_id} lat=${payload.lat} long=${payload.long}`
    );

    return res.json({
      status: 1,
      message: "Arrived event broadcasted",
      ride_id: payload.ride_id,
    });
  } catch (e) {
    console.error("❌ /ride/arrived error:", e);
    return res.status(500).json({ status: 0, message: "Server error" });
  }
});

app.post("/webhooks/ride-cancelled", (req, res) => {
  try {
    const body = req.body || {};
    const { ride_id, user_id, reason_id, driver_id } = body;

    if (ride_id == null) {
      return res.status(400).json({ status: 0, message: "ride_id required" });
    }

    const rideId = Number(ride_id);
    if (!Number.isFinite(rideId) || rideId <= 0) {
      return res.status(400).json({ status: 0, message: "invalid ride_id" });
    }

    const driverId = Number.isFinite(Number(driver_id))
      ? Number(driver_id)
      : getActiveDriverByRide(rideId) ?? null;
    const userId = Number.isFinite(Number(user_id)) ? Number(user_id) : null;
    const reasonId = Number.isFinite(Number(reason_id)) ? Number(reason_id) : null;
    const at = Date.now();

    const statusEvt = {
      ride_id: rideId,
      ride_status: 4,
    };
    const cancelledEvt = {
      ride_id: rideId,
      ride_status: 4,
      user_id: userId,
      driver_id: driverId,
      reason_id: reasonId,
      cancel_by: "user",
      ended: true,
      at,
    };

    rememberRideStatus(rideId, 4);
    upsertRideStatusSnapshot(rideId, {
      ...cancelledEvt,
      updated_at: at,
      source: "webhook-cancelled",
    });

    if (typeof biddingSocket.markRideCancelled === "function") {
      biddingSocket.markRideCancelled(io, rideId);
    } else {
      if (typeof biddingSocket.removeRideFromAllInboxes === "function") {
        biddingSocket.removeRideFromAllInboxes(io, rideId);
      }
      clearActiveRideByRideId(rideId);
    }
    clearRideRoute(rideId);

    io.to(`ride:${rideId}`).emit("ride:statusUpdated", statusEvt);
    io.to(`ride:${rideId}`).emit("ride:cancelled", cancelledEvt);
    io.to(`ride:${rideId}`).emit("ride:ended", cancelledEvt);

    if (driverId) {
      io.to(`driver:${driverId}`).emit("ride:statusUpdated", statusEvt);
      io.to(`driver:${driverId}`).emit("ride:cancelled", cancelledEvt);
      io.to(`driver:${driverId}`).emit("ride:ended", cancelledEvt);
    }

    console.log(
      `[cancel-webhook] ride:${rideId} user:${userId ?? "null"} driver:${driverId ?? "null"} reason:${reasonId ?? "null"}`
    );

    return res.json({ status: 1, ride_id: rideId });
  } catch (e) {
    console.error("❌ /webhooks/ride-cancelled error:", e);
    return res.status(500).json({ status: 0, message: "Server error" });
  }
});



app.post("/events/internal/ride-dispatch-retry", async (req, res) => {
  const {
    ride_id,
    pickup_lat,
    pickup_long,
    radius = null,
    dispatch_timeout_s = null,
    user_bid_price = null,
    min_fare_amount = null,
  } = req.body;

  if (!ride_id || pickup_lat == null || pickup_long == null) {
    console.warn("[ride-dispatch-retry] Missing required fields");
    return res.status(400).json({ status: 0, message: "Missing fields" });
  }

  const rideId = Number(ride_id);
  const lat = Number(pickup_lat);
  const long = Number(pickup_long);
  const parsedRadius = Number(radius);
  const rad = Number.isFinite(parsedRadius) && parsedRadius > 0 ? parsedRadius : null;
  const parsedDispatchTimeout = Number(dispatch_timeout_s);
  const dispatchTimeoutSec =
    Number.isFinite(parsedDispatchTimeout) && parsedDispatchTimeout > 0
      ? Math.floor(parsedDispatchTimeout)
      : null;
  const base = user_bid_price ? Number(user_bid_price) : null;
  const min = min_fare_amount ? Number(min_fare_amount) : null;

  if (isNaN(rideId) || isNaN(lat) || isNaN(long)) {
    console.warn("[ride-dispatch-retry] Invalid numeric values");
    return res
      .status(400)
      .json({ status: 0, message: "Invalid numeric values" });
  }

  console.log("[ride-dispatch-retry]", {
    ride_id: rideId,
    radius: rad,
    dispatch_timeout_s: dispatchTimeoutSec,
  });

  const nearby =
    rad !== null
      ? driverLocationService.getNearbyDriversFromMemory(lat, long, rad)
      : [];
  const ok = await biddingSocket.dispatchToNearbyDrivers(io, {
    ...req.body,
    ride_id: rideId,
    pickup_lat: lat,
    pickup_long: long,
    user_bid_price: base,
    min_fare_amount: min,
    ...(rad !== null ? { radius: rad } : {}),
    ...(dispatchTimeoutSec !== null ? { dispatch_timeout_s: dispatchTimeoutSec } : {}),
  });

  res.json({
    status: ok ? 1 : 0,
    drivers_reached: nearby.length,
    ride_id: rideId,
  });
});

app.post("/events/internal/driver-has-pending-rides", (req, res) => {
  const driverId = Number(req.body?.driver_id);
  if (!Number.isFinite(driverId) || driverId <= 0) {
    return res.status(400).json({
      status: 0,
      message: "driver_id required",
      has_data: false,
      total: 0,
    });
  }

  if (typeof biddingSocket.getDriverInboxStats !== "function") {
    return res.status(500).json({
      status: 0,
      message: "Inbox stats not available",
      has_data: false,
      total: 0,
    });
  }

  const stats = biddingSocket.getDriverInboxStats(driverId);
  return res.json({
    status: 1,
    driver_id: driverId,
    has_data: !!stats?.has_data,
    total: Number(stats?.total || 0),
    ride_ids: Array.isArray(stats?.ride_ids) ? stats.ride_ids : [],
  });
});

// ────────────────────────────────────────────────
// Run server
// ────────────────────────────────────────────────
server.listen(SOCKET_BIND_PORT, SOCKET_BIND_HOST, () => {
  console.log(
    `🚀 Socket server running at http://${SOCKET_BIND_HOST}:${SOCKET_BIND_PORT}`
  );
  console.log(
    `[tracking] mode=${
      ENABLE_RIDE_TRACKING_SERVICE ? "rideTracking-service" : "direct-location-only"
    }`
  );
  console.log("Started at:", new Date().toLocaleString());
});
