// server.js
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

// ✅ sockets
const driverSocket = require("./sockets/driver.socket");
const biddingSocket = require("./sockets/bidding.socket");
const userSocket = require("./sockets/user.socket");


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
const TRIP_SUMMARY_COMPLETE_STATUSES = new Set([7, 8]);
const SIMPLE_PASSED_DEST_DISTANCE_M = Number.isFinite(
  Number(process.env.SIMPLE_PASSED_DEST_DISTANCE_M)
)
  ? Number(process.env.SIMPLE_PASSED_DEST_DISTANCE_M)
  : 25;
// ride statuses that should be treated as final (clear active ride mapping)
const FINAL_STATUSES = new Set([4, 6, 7, 8, 9]);
// Keep status 6 route cache until payment statuses (7/8/9) so trip summary/invoice
// can still read full distance even after destination reached.
const ROUTE_CLEAR_STATUSES = new Set([4, 7, 8, 9]);
const INVOICE_TTL_MS = 10 * 60 * 1000;
const sentInvoiceForRide = new Set();
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

const round2 = (v) => {
  const n = toFiniteNumber(v);
  if (n == null) return null;
  return Math.round(n * 100) / 100;
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

const resolveTripDistanceKm = ({
  computedDistanceKm,
  payloadDistanceKm,
  storedDistanceKm,
}) => {
  const computed = toFiniteNumber(computedDistanceKm);
  const payload = toFiniteNumber(payloadDistanceKm);
  const stored = toFiniteNumber(storedDistanceKm);
  const positive = [computed, payload, stored].filter(
    (v) => v != null && v > 0
  );
  if (positive.length) {
    return round2(Math.max(...positive)) ?? Math.max(...positive);
  }
  return round2(computed ?? payload ?? stored ?? 0) ?? 0;
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
  const payloadDistanceKm =
    toFiniteNumber(payload?.total_distance) ??
    toFiniteNumber(payload?.trip_distance_km) ??
    toFiniteNumber(payload?.distance_km) ??
    toFiniteNumber(payload?.updated_total_distance_km);
  const distanceKm = resolveTripDistanceKm({
    computedDistanceKm,
    payloadDistanceKm,
    storedDistanceKm: current.distance_km,
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

  return {
    ride_id: rideId,
    driver_id: driverId ?? null,
    ride_status: rideStatus,
    stage: endedAt != null ? "completed" : "in_progress",
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
    duration_seconds: durationSeconds,
    duration_hms: durationSeconds != null ? formatDurationHms(durationSeconds) : null,
    distance_km: distanceKm,
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
  const payloadDistanceKm =
    toFiniteNumber(payload?.total_distance) ??
    toFiniteNumber(payload?.trip_distance_km) ??
    toFiniteNumber(payload?.distance_km) ??
    toFiniteNumber(payload?.updated_total_distance_km);
  const distanceKm = resolveTripDistanceKm({
    computedDistanceKm,
    payloadDistanceKm,
    storedDistanceKm: current.distance_km,
  });
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
    duration_seconds: durationSeconds,
    duration_hms: formatDurationHms(durationSeconds),
    distance_km: distanceKm,
    final_price: finalPrice,
    at: now,
  };

  current.distance_km = distanceKm;
  current.duration_seconds = durationSeconds;
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

const tryEmitDriverInvoice = async ({ rideId, driverId, rideStatus }) => {
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
  if (!INVOICE_STATUSES.has(rideStatus)) {
    console.log(
      `[invoice][skip] ride:${rideId} status:${rideStatus} not in [${Array.from(
        INVOICE_STATUSES
      ).join(",")}]`
    );
    return;
  }

  const meta = driverLocationService.getMeta(driverId) || {};
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

  markInvoiceSent(rideId);

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

    const tripSummary = buildTripSummarySnapshot({
      rideId,
      driverId,
      rideStatus,
      payload: typeof invoicePayload === "object" ? invoicePayload : null,
    });

    if (tripSummary && invoicePayload && typeof invoicePayload === "object") {
      const invoiceDistance = toFiniteNumber(invoicePayload.total_distance);
      if ((invoiceDistance == null || invoiceDistance <= 0) && tripSummary.distance_km != null) {
        invoicePayload.total_distance = tripSummary.distance_km;
      }

      const invoiceTotalPay = toFiniteNumber(invoicePayload.total_pay);
      if ((invoiceTotalPay == null || invoiceTotalPay <= 0) && tripSummary.final_price != null) {
        invoicePayload.total_pay = tripSummary.final_price;
      }

      invoicePayload.trip_started_date_time = tripSummary.started_date_time;
      invoicePayload.trip_ended_date_time = tripSummary.ended_date_time;
      invoicePayload.trip_duration_seconds = tripSummary.duration_seconds;
      invoicePayload.trip_duration_hms = tripSummary.duration_hms;
      invoicePayload.trip_distance_km = tripSummary.distance_km;
      invoicePayload.trip_final_price = tripSummary.final_price;
    }

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
    sentInvoiceForRide.delete(rideId);
    console.error(
      "[invoice] API call failed:",
      e?.response?.data || e.message
    );
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

app.post("/events/internal/driver-status-updated", (req, res) => {
  const { driver_id, old_status, new_status } = req.body;

  if (!driver_id) {
    console.warn("[driver-status-updated] Missing driver_id");
    return res.status(400).json({ status: 0, message: "driver_id required" });
  }

  console.log(
    `[STATUS UPDATE] driver:${driver_id} from ${old_status} to ${new_status}`
  );

  const room = `driver:${driver_id}`;

  if (Number(new_status) === 1) {
    io.in(room).socketsJoin("drivers:online");
    console.log(`[ONLINE] driver:${driver_id} joined drivers:online`);
  } else {
    io.in(room).socketsLeave("drivers:online");
    console.log(`[OFFLINE] driver:${driver_id} left drivers:online`);

    // ✅ add this line so offline drivers stop appearing in nearby
    driverLocationService.remove(Number(driver_id));
    console.log(`[MEMORY] driver:${driver_id} removed from memory`);
  }
  

  io.emit("driver:status-updated", { driver_id, old_status, new_status });

  res.json({ status: 1 });

});

app.post("/events/internal/driver-location", (req, res) => {
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

  // ✅ update memory
  driverLocationService.updateMemory(driverId, la, lo);

  // ✅ mark online only if socket is connected to driver room
  const driverRoom = `driver:${driverId}`;
  const room = io.sockets.adapter.rooms.get(driverRoom);
  const isOnline = !!room && room.size > 0;
  driverLocationService.updateMeta(driverId, {
    is_online: isOnline,
    lastSeen: isOnline ? undefined : Date.now(),
  });

  // ✅ broadcast to driver's room
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

app.post("/events/internal/ride-bid-dispatch", (req, res) => {
  console.log(
    "[ride-bid-dispatch] Incoming request from Laravel",
    req.body
  );
  console.log("[dispatch][ride-bid-dispatch]", {
    ride_id: req.body?.ride_id ?? null,
    service_type_id: req.body?.service_type_id ?? null,
    radius: req.body?.radius ?? null,
  });
  try {
    biddingSocket.dispatchToNearbyDrivers(io, req.body);
    res.json({ status: 1 });
  } catch (e) {
    console.error("[ride-bid-dispatch] Failed:", e.message);
    res.status(500).json({ status: 0, message: "Dispatch failed" });
  }
});
app.post("/events/internal/ride-user-accepted", (req, res) => {
  const { ride_id, driver_id, offered_price, message, ride_details } = req.body;
  const rideId = Number(ride_id);
  const driverId = Number(driver_id);
  const parsedPrice = offered_price == null ? null : Number(offered_price);
  const finalPrice = Number.isFinite(parsedPrice) ? parsedPrice : offered_price ?? null;

  if (!Number.isFinite(rideId) || !Number.isFinite(driverId)) {
    console.warn("[ride-user-accepted] Missing ride_id/driver_id");
    return res
      .status(400)
      .json({ status: 0, message: "ride_id & driver_id required" });
  }

  console.log(
    `[ride-user-accepted] ride:${rideId} -> driver:${driverId} price:${finalPrice}`
  );

  if (typeof biddingSocket.finalizeAcceptedRide === "function") {
    biddingSocket.finalizeAcceptedRide(io, rideId, driverId, finalPrice, {
      message: message || "User accepted the offer",
      rideDetails: ride_details ?? null,
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
      source: "laravel:accept-not-reached-destination",
      at: Number.isFinite(Number(body.at)) ? Number(body.at) : Date.now(),
    };

    io.to(`ride:${rideNum}`).emit("ride:extraDistanceAccepted", evt);
    io.to(`ride:${rideNum}`).emit("ride:passedDestinationAccepted", evt);
    if (resolvedDriverId) {
      io.to(`driver:${resolvedDriverId}`).emit("ride:extraDistanceAccepted", evt);
      io.to(`driver:${resolvedDriverId}`).emit("ride:passedDestinationAccepted", evt);
    }

    console.log(
      `[extra-distance][emit] ride:${rideNum} driver:${resolvedDriverId ?? "none"} events:ride:extraDistanceAccepted,ride:passedDestinationAccepted adjustment:${evt.adjustment_id ?? "none"} extra_km:${evt.extra_distance_km ?? "null"} extra_fare:${evt.extra_fare_amount ?? "null"}`
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

    if (status === 5) {
      if (Number.isFinite(la) && Number.isFinite(lo)) {
        startRideRoute(rideId, { lat: la, lng: lo, at: Date.now() });
      } else {
        startRideRoute(rideId);
      }
    }
    const evt = {
      ride_id: rideId,
      ride_status: status,
    };

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
        const targetDriverId = driverId ?? getActiveDriverByRide(rideId) ?? null;
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
          console.log(
            `[status][emit] ride:passedDestination ride:${rideId} driver:${targetDriverId} dist:${Math.round(
              currentDistanceM
            )}m -> rideRoom(sockets:${rideRoomCount}) driverRoom(sockets:${driverRoomCount})`
          );
        } else {
          console.log(
            `[status][emit] ride:passedDestination ride:${rideId} driver:none dist:${Math.round(
              currentDistanceM
            )}m -> rideRoom(sockets:${rideRoomCount})`
          );
        }
      }
    }

    const statusKey = `${rideId}:${status}`;
    const isDupStatus = shouldSkipDedupe(
      statusDedupe,
      statusKey,
      STATUS_DEDUPE_TTL_MS
    );

    // ✅ keep active ride mapping in sync with driver status updates
    if (driverId && !FINAL_STATUSES.has(status)) {
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
    } else {
      console.log(
        `[status][dedupe] skip ride:statusUpdated ride:${rideId} status:${status}`
      );
    }
    const userId =
      user_id ??
      payload?.user_id ??
      payload?.user_details?.user_id ??
      payload?.user?.id ??
      (typeof biddingSocket.getUserIdForRide === "function"
        ? biddingSocket.getUserIdForRide(rideId)
        : null);
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
      void tryEmitDriverInvoice({ rideId, driverId, rideStatus: status });
    }

    // ✅ If ride reached a terminal status, clear active mapping + notify
    if (FINAL_STATUSES.has(status)) {
      clearActiveRideByRideId(rideId);
      if (ROUTE_CLEAR_STATUSES.has(status)) {
        clearRideRoute(rideId);
      } else {
        console.log(
          `[status][route-cache] keep route for ride:${rideId} status:${status}`
        );
      }

      if (typeof biddingSocket.closeRideBidding === "function") {
        biddingSocket.closeRideBidding(io, rideId);
      }

      const endEvt = { ...evt, ended: true };
      const isDupEnded = shouldSkipDedupe(
        endedDedupe,
        rideId,
        ENDED_DEDUPE_TTL_MS
      );
      if (!isDupEnded) {
        io.to(`ride:${rideId}`).emit("ride:ended", endEvt);
        if (driverId) {
          io.to(`driver:${driverId}`).emit("ride:ended", endEvt);
        }
      } else {
        console.log(`[status][dedupe] skip ride:ended ride:${rideId}`);
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
    clearActiveRideByRideId(rideId);

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
    const { ride_id, lat, long } = body;

    if (ride_id == null || lat == null || long == null) {
      return res.status(400).json({ status: 0, message: "Missing parameters" });
    }

    if (!io) {
      return res.status(500).json({ status: 0, message: "io not initialized" });
    }

    const rideId = Number(ride_id);
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

    rideTracking.updateLocation(io, rideId, la, lo);

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



app.post("/events/internal/ride-dispatch-retry", (req, res) => {
  const {
    ride_id,
    pickup_lat,
    pickup_long,
    radius = 5000,
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
  const rad = Number(radius) || 5000;
  const base = user_bid_price ? Number(user_bid_price) : null;
  const min = min_fare_amount ? Number(min_fare_amount) : null;

  if (isNaN(rideId) || isNaN(lat) || isNaN(long)) {
    console.warn("[ride-dispatch-retry] Invalid numeric values");
    return res
      .status(400)
      .json({ status: 0, message: "Invalid numeric values" });
  }

  console.log(
    `Dispatching ride ${rideId} to nearby drivers (radius: ${rad}m)`
  );

  const nearby = driverLocationService.getNearbyDriversFromMemory(lat, long, rad);
  const ok = biddingSocket.dispatchToNearbyDrivers(io, {
    ...req.body,
    ride_id: rideId,
    pickup_lat: lat,
    pickup_long: long,
    radius: rad,
    user_bid_price: base,
    min_fare_amount: min,
  });

  res.json({
    status: ok ? 1 : 0,
    drivers_reached: nearby.length,
    ride_id: rideId,
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


