// sockets/driver100.socket.js 
const driverLocationService = require("../services/driverLocation.service");
const axios = require("axios");
const { getActiveRideByDriver } = require("../store/activeRides.store");
const { getUserDetails, getUserDetailsByToken } = require("../store/users.store");
const {
  startRideRoute,
  appendRidePoint,
  getRideRoutePoints,
  clearRideRoute,
} = require("../store/rideRoutes.store");
const { getDistanceMeters } = require("../utils/geo.util");
const { emitAdminDriverUpdate } = require("../services/adminDriverFeed.service");
const biddingSocket = require("./bidding.socket");
const ENABLE_RIDE_TRACKING_SERVICE =
  process.env.ENABLE_RIDE_TRACKING_SERVICE === "1";
const rideTracking = ENABLE_RIDE_TRACKING_SERVICE
  ? require("../services/rideTracking")
  : null;

// 🔧 Settings
const DB_UPDATE_EVERY_MS = 0; // set to 0 to disable direct DB writes from Node (recommended)
const LARAVEL_LOCATION_PUSH_EVERY_MS = 10000; // how often to push driver location to Laravel API
const LARAVEL_BASE_URL =
  process.env.LARAVEL_BASE_URL ||
  process.env.LARAVEL_URL ||
  "https://aiactive.co.uk/backend/backend-laravel/public";
const LARAVEL_TIMEOUT_MS = 7000;
const extraDistanceSessions = new Map(); // rideId -> { driverId, acceptedAt, baselineRoutePointCount, baselineTotalDistanceKm, acceptedLat, acceptedLong, settled }
const STATUS_DEDUPE_TTL_MS = Number.isFinite(
  Number(process.env.STATUS_DEDUPE_TTL_MS)
)
  ? Number(process.env.STATUS_DEDUPE_TTL_MS)
  : 4000;
const LOCATION_MAX_JUMP_METERS = Number.isFinite(
  Number(process.env.LOCATION_MAX_JUMP_METERS)
)
  ? Math.max(80, Number(process.env.LOCATION_MAX_JUMP_METERS))
  : 250;
const LOCATION_MAX_JUMP_WINDOW_MS = Number.isFinite(
  Number(process.env.LOCATION_MAX_JUMP_WINDOW_MS)
)
  ? Math.max(1000, Number(process.env.LOCATION_MAX_JUMP_WINDOW_MS))
  : 4000;
const LOCATION_MAX_SPEED_MPS = Number.isFinite(
  Number(process.env.LOCATION_MAX_SPEED_MPS)
)
  ? Math.max(10, Number(process.env.LOCATION_MAX_SPEED_MPS))
  : 45;
const LOCATION_DUPLICATE_WINDOW_MS = Number.isFinite(
  Number(process.env.LOCATION_DUPLICATE_WINDOW_MS)
)
  ? Math.max(500, Number(process.env.LOCATION_DUPLICATE_WINDOW_MS))
  : 1500;
// Keep status=6 active for extra-distance tracking until status=7 settlement.
const FINAL_RIDE_STATUSES = new Set([4, 6, 7, 8, 9, 10]);

const lastRideStatusByKey = new Map(); // key -> { status, at }
const lastAcceptedLocationByDriver = new Map(); // driverId -> { lat, long, at }
const statusOrder = (status) => {
  const n = Number(status);
  if (!Number.isFinite(n)) return null;
  if (n === 4 || n === 10) return 100;
  return n;
};
const pendingRelocationByDriver = new Map(); // driverId -> { lat, long, at, reason }
const shouldSkipDuplicateStatus = (key, status) => {
  const now = Date.now();
  const prev = lastRideStatusByKey.get(key);
  if (prev) {
    if (prev.status === status && now - prev.at < STATUS_DEDUPE_TTL_MS) {
      return true;
    }
    const prevOrder = statusOrder(prev.status);
    const nextOrder = statusOrder(status);
    if (prevOrder != null && nextOrder != null && nextOrder < prevOrder) {
      return true;
    }
  }
  lastRideStatusByKey.set(key, { status, at: now });
  return false;
};

const RELOCATION_CONFIRM_WINDOW_MS = Number.isFinite(
  Number(process.env.RELOCATION_CONFIRM_WINDOW_MS)
)
  ? Math.max(1000, Number(process.env.RELOCATION_CONFIRM_WINDOW_MS))
  : 4000;

const RELOCATION_CONFIRM_MAX_METERS = Number.isFinite(
  Number(process.env.RELOCATION_CONFIRM_MAX_METERS)
)
  ? Math.max(30, Number(process.env.RELOCATION_CONFIRM_MAX_METERS))
  : 120;

const validateDriverLocationPoint = (driverId, lat, long, at = Date.now()) => {
  if (!driverId || !Number.isFinite(lat) || !Number.isFinite(long)) {
    return { ok: false, reason: "invalid-input" };
  }

  const prev = lastAcceptedLocationByDriver.get(driverId);
  if (!prev) {
    pendingRelocationByDriver.delete(driverId);
    lastAcceptedLocationByDriver.set(driverId, { lat, long, at });
    return { ok: true };
  }

  const elapsedMs = Math.max(0, at - (Number(prev.at) || 0));
  const distMeters = getDistanceMeters(prev.lat, prev.long, lat, long);
  if (!Number.isFinite(distMeters)) {
    return { ok: false, reason: "invalid-distance" };
  }

  if (distMeters < 3 && elapsedMs < LOCATION_DUPLICATE_WINDOW_MS) {
    return { ok: false, reason: "duplicate-jitter", elapsedMs, distMeters };
  }

  let speedMps = null;
  if (elapsedMs > 0) {
    speedMps = distMeters / (elapsedMs / 1000);
  }

  const isJump =
    elapsedMs > 0 &&
    distMeters > LOCATION_MAX_JUMP_METERS &&
    elapsedMs < LOCATION_MAX_JUMP_WINDOW_MS;

  const isSpeed =
    elapsedMs > 0 &&
    Number.isFinite(speedMps) &&
    speedMps > LOCATION_MAX_SPEED_MPS;

  if (isJump || isSpeed) {
    const pending = pendingRelocationByDriver.get(driverId);

    if (pending) {
      const pendingAgeMs = Math.max(0, at - (Number(pending.at) || 0));
      const pendingDistMeters = getDistanceMeters(
        pending.lat,
        pending.long,
        lat,
        long
      );

      if (
        Number.isFinite(pendingDistMeters) &&
        pendingDistMeters <= RELOCATION_CONFIRM_MAX_METERS &&
        pendingAgeMs <= RELOCATION_CONFIRM_WINDOW_MS
      ) {
        pendingRelocationByDriver.delete(driverId);
        lastAcceptedLocationByDriver.set(driverId, { lat, long, at });
        return {
          ok: true,
          reanchored: true,
          reason: "relocation-confirmed",
          elapsedMs,
          distMeters,
          speedMps,
        };
      }
    }

    pendingRelocationByDriver.set(driverId, {
      lat,
      long,
      at,
      reason: isJump ? "jump" : "speed",
    });

    return {
      ok: false,
      reason: isJump ? "jump" : "speed",
      elapsedMs,
      distMeters,
      speedMps,
    };
  }

  pendingRelocationByDriver.delete(driverId);
  lastAcceptedLocationByDriver.set(driverId, { lat, long, at });
  return { ok: true };
};
const getDriverActiveRideId = (driverId) => {
  return getActiveRideByDriver(driverId) ?? toNumber(socket.activeRideId) ?? null;
};

const rejectOfflineIfBusy = (driverId, targetSocket = socket) => {
  const activeRideId = getDriverActiveRideId(driverId);
 
  if (!activeRideId) return false;

  targetSocket.emit("driver:offlineRejected", {
    status: 0,
    message: "Cannot go offline while ride is active",
    ride_id: activeRideId,
    at: Date.now(),
  });

  return true;
};

module.exports = (io, socket) => {
  // ─────────────────────────────
  // Helpers
  // ─────────────────────────────
  const toNumber = (v) => {
    const n = Number(v);
    return Number.isFinite(n) ? n : null;
  };

  const round2 = (value) => {
    const n = toNumber(value);
    if (n == null) return null;
    return Math.round(n * 100) / 100;
  };

  const parseLatLongString = (value) => {
    if (typeof value !== "string" || !value.includes(",")) return null;
    const [rawLat, rawLong] = value.split(",").map((item) => item.trim());
    const lat = toNumber(rawLat);
    const lng = toNumber(rawLong);
    if (lat == null || lng == null) return null;
    return { lat, lng };
  };

  const extractNamedPoint = (source, prefix) => {
    if (!source || typeof source !== "object") return null;

    const candidates = [source, source.meta, source.ride_details].filter(
      (item) => item && typeof item === "object"
    );

    for (const candidate of candidates) {
      const directLat = toNumber(
        candidate[`${prefix}_lat`] ??
          candidate[`${prefix}Lat`] ??
          candidate[prefix]?.lat ??
          candidate[prefix]?.latitude
      );
      const directLng = toNumber(
        candidate[`${prefix}_long`] ??
          candidate[`${prefix}Long`] ??
          candidate[prefix]?.lng ??
          candidate[prefix]?.long ??
          candidate[prefix]?.longitude
      );
      if (directLat != null && directLng != null) {
        return { lat: directLat, lng: directLng };
      }

      const latLongPoint = parseLatLongString(
        candidate[`${prefix}_latlong`] ??
          candidate[`${prefix}LatLong`] ??
          candidate[prefix]?.latlong
      );
      if (latLongPoint) return latLongPoint;
    }

    return null;
  };

  const computeDistanceKmFromPoints = (points = []) => {
    if (!Array.isArray(points) || points.length < 2) return 0;

    let meters = 0;
    for (let i = 1; i < points.length; i += 1) {
      const prev = points[i - 1];
      const next = points[i];
      const seg = getDistanceMeters(
        toNumber(prev?.lat),
        toNumber(prev?.lng),
        toNumber(next?.lat),
        toNumber(next?.lng)
      );
      if (Number.isFinite(seg) && seg > 0) {
        meters += seg;
      }
    }

    return Number.isFinite(meters) && meters > 0 ? round2(meters / 1000) : 0;
  };

  const computeRouteDistanceKm = (rideId) => {
    const points = getRideRoutePoints(rideId);
    if (!Array.isArray(points) || points.length < 2) return null;

    let meters = 0;
    for (let i = 1; i < points.length; i += 1) {
      const prev = points[i - 1];
      const next = points[i];
      const segmentMeters = getDistanceMeters(
        toNumber(prev?.lat),
        toNumber(prev?.lng),
        toNumber(next?.lat),
        toNumber(next?.lng)
      );
      if (Number.isFinite(segmentMeters) && segmentMeters > 0) {
        meters += segmentMeters;
      }
    }

    if (!Number.isFinite(meters) || meters <= 0) return null;
    return round2(meters / 1000);
  };

  const isAcceptedDecision = (value) => {
    if (value === true || value === 1) return true;
    if (typeof value === "string") {
      const normalized = value.trim().toLowerCase();
      return [
        "1",
        "true",
        "yes",
        "y",
        "ok",
        "confirm",
        "accepted",
      ].includes(normalized);
    }
    return false;
  };

  const driverRoom = (driverId) => `driver:${driverId}`;

  const bindDriverOnce = (newDriverId) => {
    if (!socket.driverId) {
      socket.driverId = newDriverId;
      return true;
    }
    return socket.driverId === newDriverId;
  };

  const emitCandidatesSummaryForDriver = (driverId) => {
    if (
      typeof biddingSocket.emitCandidatesSummaryForDriverStateChange ===
      "function"
    ) {
      biddingSocket.emitCandidatesSummaryForDriverStateChange(io, driverId);
    }
  };

  const logRooms = (label) => {
    const roomsMap = io.sockets.adapter.rooms;
    const customRooms = [];

    for (const [roomName, socketSet] of roomsMap.entries()) {
      const isPrivateSocketRoom = io.sockets.sockets.has(roomName); // room == socket.id
      if (!isPrivateSocketRoom) {
        customRooms.push({ room: roomName, socketsCount: socketSet.size });
      }
    }

    console.log("========== ROOMS DEBUG ==========");
    console.log("📌", label);
    console.log("🏷️ Custom rooms:", customRooms.length ? customRooms : "none");
    console.log("=================================\n");
  };

  // ─────────────────────────────
  // State per socket
  // ─────────────────────────────
  socket.laravelLocationInterval = null;
  socket.activeRideId = null;

  // ─────────────────────────────
  // Events
  // ─────────────────────────────

  socket.on("driver-online", async (payload = {}) => {
    const {
      driver_id,
      lat,
      long,
      access_token,
      driver_service_id,
      service_type_id,
      service_category_id,
      vehicle_type_id,

      // ✅ NEW: from frontend (optional)
      driver_gender,
      child_seat,
      handicap,
    } = payload || {};

    const driverId = toNumber(driver_id);
    const la = toNumber(lat);
    const lo = toNumber(long);
    if (!driverId || la === null || lo === null) return;
    if (!bindDriverOnce(driverId)) return;

    const payloadServiceTypeId = toNumber(
      service_type_id ?? vehicle_type_id ?? payload?.service_type ?? null
    );
    const payloadServiceCategoryId = toNumber(service_category_id ?? null);

    socket.driverServiceId = toNumber(driver_service_id) ?? null;
    socket.driverDetailId = toNumber(payload?.driver_detail_id ?? null);
    socket.driverAccessToken = access_token ?? null;
    socket.driverServiceCategoryId = payloadServiceCategoryId ?? null;

    socket.join(driverRoom(driverId));
    console.log("✅ driver joined room", driverRoom(driverId), "socket:", socket.id);

    // ✅ خزّن أولياً لوكيشن + online (بدون upsert)
    const onlineNow = Date.now();
    driverLocationService.updateMemory(driverId, la, lo);
    lastAcceptedLocationByDriver.set(driverId, {
      lat: la,
      long: lo,
      at: onlineNow,
    });

    const baseMeta = {
      driver_id: driverId,
      provider_id: driverId,
      is_online: true,
      dashboard_is_online: true,
      socket_disconnected: false,
      driver_service_id: toNumber(driver_service_id) ?? null,
      driver_detail_id: toNumber(payload?.driver_detail_id ?? null),
      updatedAt: onlineNow,
    };

    if (Number.isFinite(payloadServiceTypeId)) {
      baseMeta.service_type_id = payloadServiceTypeId;
    }
    if (Number.isFinite(payloadServiceCategoryId)) {
      baseMeta.service_category_id = payloadServiceCategoryId;
    }
    if (access_token) baseMeta.access_token = access_token;

    // ✅ NEW: store frontend-only flags (optional)
    const g = toNumber(driver_gender);
    if (g === 1 || g === 2) baseMeta.driver_gender = g;

    const cs = toNumber(child_seat);
    if (cs === 0 || cs === 1) baseMeta.child_seat = cs;

    const hc = toNumber(handicap);
    if (hc === 0 || hc === 1) baseMeta.handicap = hc;

    driverLocationService.updateMeta(driverId, baseMeta);

    // ✅ جيب معلومات النوع + كل بيانات السيارة من Laravel وخزّنها بالميموري (كما هو)
    if (!access_token) {
      console.warn("[driver-online] Missing access_token; skipping Laravel update-current-status");
    } else {
      try {
        const res = await axios.post(
          `${LARAVEL_BASE_URL}/api/driver/update-current-status`,
          {
            driver_id: driverId,
            update_status: 1,
            access_token,
            driver_service_id,
            current_lat: la,
            current_long: lo,
          },
          { timeout: LARAVEL_TIMEOUT_MS }
        );

        let d = res?.data || {};
        if (typeof d === "string") {
          try {
            d = JSON.parse(d);
          } catch (_) {}
        }

        // النوع الذي يفلتر عليه اليوزر
        const service_type_id = Number(
          d.service_type_id ??
            d.transport_vehicle_type?.service_type_id ??
            payloadServiceTypeId ??
            baseMeta.service_type_id ??
            null
        );

        // معلومات النوع (Economy/VIP...) + icon
        const vehicle_type_name =
          d.vehicle_type_name ??
          d.transport_vehicle_type?.vehicle_type_name ??
          "";
        const vehicle_type_icon =
          d.vehicle_type_icon ??
          d.transport_vehicle_type?.vehicle_type_icon ??
          "";

        const service_category_id = Number(
          d.service_category_id ??
            payloadServiceCategoryId ??
            baseMeta.service_category_id ??
            null
        );
        const resolvedDriverServiceId = toNumber(
          d.driver_service_id ?? baseMeta.driver_service_id ?? socket.driverServiceId ?? null
        );
        const resolvedDriverDetailId = toNumber(
          d.driver_detail_id ?? d.driver_details_id ?? baseMeta.driver_detail_id ?? socket.driverDetailId ?? null
        );
        const resolvedProviderId = toNumber(d.provider_id ?? d.driver_id ?? driverId);
        if (Number.isFinite(service_category_id)) {
          socket.driverServiceCategoryId = service_category_id;
        }
        if (Number.isFinite(resolvedDriverServiceId)) {
          socket.driverServiceId = resolvedDriverServiceId;
        }
        if (Number.isFinite(resolvedDriverDetailId)) {
          socket.driverDetailId = resolvedDriverDetailId;
        }

        // ✅ كل بيانات السيارة (حسب الريسبونس اللي عندك)
        const vehicle_company = d.vehicle_company ?? "";
        const plat_no = d.plat_no ?? "";
        const model_year = d.model_year ?? null;
        const model_name = d.model_name ?? "";
        const vehicle_color = d.vehicle_color ?? "";

        // بيانات سائق مفيدة للعرض
        const driver_name = d.driver_name ?? "";
        const rating = d.rating ?? null;
        const phone =
          d.phone ??
          d.contact_number ??
          d.mobile ??
          "";
        const country_code =
          d.country_code ??
          d.countryCode ??
          d.mobile_country_code ??
          "";
        const driver_image =
          d.driver_image ??
          d.driver_image_url ??
          d.profile_image ??
          d.avatar ??
          d.image ??
          null;
        const driver_gender = toNumber(
          d.driver_gender ?? d.gender ?? baseMeta.driver_gender ?? null
        );
        const child_seat = toNumber(
          d.child_seat ??
            d.child_seat_accessibility ??
            d.smoking_value ??
            d.smoking ??
            baseMeta.child_seat ??
            null
        );
        const handicap = toNumber(
          d.handicap ?? d.handicap_accessibility ?? baseMeta.handicap ?? null
        );

        // حالة السائق (من الريسبونس إذا بدك تكون أدق)
        const currentStatus = Number(d.new_status ?? d.driver_current_status ?? 1);

        const metaUpdate = {
          // status/meta
          is_online: currentStatus === 1,
          dashboard_is_online: currentStatus === 1,
          updatedAt: Date.now(),
          ...(Number.isFinite(resolvedProviderId) ? { provider_id: resolvedProviderId } : {}),
          ...(Number.isFinite(resolvedDriverServiceId)
            ? { driver_service_id: resolvedDriverServiceId }
            : {}),
          ...(Number.isFinite(resolvedDriverDetailId)
            ? {
                driver_detail_id: resolvedDriverDetailId,
                driver_details_id: resolvedDriverDetailId,
              }
            : {}),
          ...(Number.isFinite(service_type_id) ? { service_type_id } : {}),
          ...(Number.isFinite(service_category_id) ? { service_category_id } : {}),

          // type display
          ...(vehicle_type_name ? { vehicle_type_name } : {}),
          ...(vehicle_type_icon ? { vehicle_type_icon } : {}),

          // vehicle info
          ...(vehicle_company ? { vehicle_company } : {}),
          ...(plat_no ? { plat_no } : {}),
          ...(model_year ? { model_year } : {}),
          ...(model_name ? { model_name } : {}),
          ...(vehicle_color ? { vehicle_color } : {}),

          // driver info
          ...(driver_name ? { driver_name } : {}),
          ...(rating != null ? { rating } : {}),
          ...(phone ? { phone } : {}),
          ...(country_code ? { country_code } : {}),
          ...(driver_image ? { driver_image } : {}),
          ...(driver_gender === 1 || driver_gender === 2 ? { driver_gender } : {}),
          ...(child_seat === 0 || child_seat === 1 ? { child_seat } : {}),
          ...(handicap === 0 || handicap === 1 ? { handicap } : {}),
        };

        driverLocationService.updateMeta(driverId, metaUpdate);

        console.log("✅ Driver online stored:", {
          driverId,
          service_type_id: Number.isFinite(service_type_id) ? service_type_id : null,
        });
      } catch (e) {
        console.error("❌ update-current-status failed:", e?.response?.data || e.message);
      }
    }

    // ✅ ابعث المرشحين مباشرة عند أونلاين
    emitCandidatesSummaryForDriver(driverId);
    emitAdminDriverUpdate(io, driverId);
    socket.emit("driver:ready", {
      driver_id: driverId,
      provider_id: driverId,
      driver_service_id: socket.driverServiceId ?? null,
      driver_detail_id: socket.driverDetailId ?? null,
    });
  });

  socket.on("update-location", ({ lat, long }) => {
    console.log("[update-location] payload:", { lat, long });
    if (!socket.driverId) return;

    const la = toNumber(lat);
    const lo = toNumber(long);
    if (la === null || lo === null) return;
    const now = Date.now();

    const locationCheck = validateDriverLocationPoint(
      socket.driverId,
      la,
      lo,
      now
    );
    if (!locationCheck.ok) {
      console.log("[update-location][filtered]", {
        driver_id: socket.driverId,
        lat: la,
        long: lo,
        reason: locationCheck.reason,
        elapsed_ms: locationCheck.elapsedMs ?? null,
        distance_m: locationCheck.distMeters ?? null,
        speed_mps: Number.isFinite(locationCheck.speedMps)
          ? round2(locationCheck.speedMps)
          : null,
      });
      return;
    }

    // ✅ source of truth: service only
    driverLocationService.updateMemory(socket.driverId, la, lo);

    // ✅ mark as online on any location ping
    driverLocationService.updateMeta(socket.driverId, {
      is_online: true,
      dashboard_is_online: true,
      socket_disconnected: false,
      updatedAt: now,
    });

    // ✅ ابعث تحديث المرشحين مع كل update-location مقبول
    emitCandidatesSummaryForDriver(socket.driverId);
    console.log(
      "[update-location] payload:",
      { lat: la, long: lo },
      "socket:",
      socket.id,
      "driverId:",
      socket.driverId
    );

    const payload = {
      driver_id: socket.driverId,
      lat: la,
      long: lo,
      timestamp: now,
    };

    emitAdminDriverUpdate(io, socket.driverId);
    io.to(driverRoom(socket.driverId)).emit("driver:moved", payload);

    const activeRideId =
      getActiveRideByDriver(socket.driverId) ?? toNumber(socket.activeRideId);
    if (activeRideId) {
      appendRidePoint(activeRideId, { lat: la, lng: lo, at: now });
      io.to(`ride:${activeRideId}`).emit("ride:locationUpdate", {
        ride_id: activeRideId,
        driver_id: socket.driverId,
        lat: la,
        long: lo,
        at: now,
      });

      if (ENABLE_RIDE_TRACKING_SERVICE && rideTracking) {
        void rideTracking
          .updateLocation(io, activeRideId, la, lo, {
            driver_id: socket.driverId,
            emit_location: false,
          })
          .catch((error) => {
            console.error(
              "[tracking][socket-update-location] failed:",
              error?.message || error
            );
          });
      }
    }
  });

  // ✅ Driver updates ride status via Socket -> Laravel API -> internal socket event
 socket.on("driver:updateRideStatus", async (payload = {}) => {
  console.log("[ride-status][driver:updateRideStatus] incoming", {
    ride_id: payload?.ride_id ?? null,
    ride_status: payload?.ride_status ?? null,
    way_point_status: payload?.way_point_status ?? null,
    driver_id: socket.driverId ?? payload?.driver_id ?? null,
  });

  const driverId = toNumber(socket.driverId) ?? toNumber(payload?.driver_id);
  if (!driverId) {
    console.log("[ride-status][driver:updateRideStatus] missing driver_id");
    return;
  }

  if (!bindDriverOnce(driverId)) {
    console.log(
      `[ride-status][driver:updateRideStatus] driver_id mismatch: socket=${socket.driverId} payload=${payload?.driver_id}`
    );
    return;
  }

  const driverMeta = driverLocationService.getMeta(driverId) || {};
  const rideId =
    toNumber(payload?.ride_id) ?? getActiveRideByDriver(driverId) ?? null;
  const rideDetails =
    rideId && typeof biddingSocket.getRideDetails === "function"
      ? biddingSocket.getRideDetails(rideId)
      : null;
  const rideStatus = toNumber(payload?.ride_status);
  const wayPointStatusRaw = toNumber(payload?.way_point_status);
  const wayPointStatus =
    Number.isFinite(wayPointStatusRaw) ? wayPointStatusRaw : 0;
  const serviceCategoryId =
    toNumber(rideDetails?.service_category_id) ??
    toNumber(payload?.service_category_id) ??
    toNumber(socket.driverServiceCategoryId) ??
    toNumber(driverMeta?.service_category_id);
  const pickupPoint =
    extractNamedPoint(rideDetails, "pickup") ??
    extractNamedPoint(payload, "pickup");
  const routeOpts = pickupPoint ? { pickup: pickupPoint } : {};

  if (!rideId || rideStatus == null || wayPointStatus == null || !serviceCategoryId) {
    console.log("[ride-status][driver:updateRideStatus] missing required fields", {
      ride_id: rideId,
      ride_status: rideStatus,
      way_point_status: wayPointStatus,
      service_category_id: serviceCategoryId,
    });
    return;
  }

  if (!FINAL_RIDE_STATUSES.has(rideStatus)) {
    socket.activeRideId = rideId;
  }

  const dedupeKey = `${driverId}:${rideId}`;
  if (shouldSkipDuplicateStatus(dedupeKey, rideStatus)) {
    console.log("[ride-status][driver:updateRideStatus] duplicate ignored", {
      ride_id: rideId,
      ride_status: rideStatus,
      driver_id: driverId,
    });
    return;
  }

  // ✅ send pre-update info to driver (route/eta if available)
  const rideUserId = toNumber(rideDetails?.user_id);
  const rideToken =
    rideDetails?.token ?? rideDetails?.user_details?.user_token ?? null;
  const storedUser = rideUserId ? getUserDetails(rideUserId) : null;
  const storedByToken =
    !storedUser && rideToken ? getUserDetailsByToken(rideToken) : null;

  const routeKm =
    toNumber(payload?.route ?? null) ??
    toNumber(rideDetails?.route ?? rideDetails?.meta?.route ?? null) ??
    toNumber(storedUser?.route ?? storedByToken?.route ?? null);
  const etaMin =
    toNumber(payload?.eta_min ?? null) ??
    toNumber(rideDetails?.eta_min ?? rideDetails?.meta?.eta_min ?? null) ??
    toNumber(storedUser?.eta_min ?? storedByToken?.eta_min ?? null);

  io.to(driverRoom(driverId)).emit("ride:statusPreUpdate", {
    ride_id: rideId,
    ride_status: rideStatus,
    ...(routeKm !== null ? { route: routeKm } : {}),
    ...(etaMin !== null ? { eta_min: etaMin } : {}),
    at: Date.now(),
  });

  const driverServiceId =
    toNumber(socket.driverServiceId) ??
    toNumber(payload?.driver_service_id) ??
    toNumber(driverMeta?.driver_service_id);
  const accessToken =
    socket.driverAccessToken ??
    payload?.access_token ??
    driverMeta?.access_token ??
    null;

  if (!driverServiceId || !accessToken) {
    console.log(
      `[ride-status][driver:updateRideStatus] missing driver_service_id/access_token (driver ${driverId})`
    );
    return;
  }

  let currentLat = toNumber(payload?.current_lat ?? payload?.lat);
  let currentLong = toNumber(payload?.current_long ?? payload?.long);
  if (!Number.isFinite(currentLat) || !Number.isFinite(currentLong)) {
    const lastLoc = driverLocationService.getDriver(driverId);
    if (!Number.isFinite(currentLat)) currentLat = toNumber(lastLoc?.lat);
    if (!Number.isFinite(currentLong)) currentLong = toNumber(lastLoc?.long);
  }

  // ✅ start/append route tracking for running rides
  if (rideStatus === 5) {
    if (Number.isFinite(currentLat) && Number.isFinite(currentLong)) {
      startRideRoute(
        rideId,
        { lat: currentLat, lng: currentLong, at: Date.now() },
        routeOpts
      );
    } else {
      startRideRoute(rideId, null, routeOpts);
    }
  }
  if (rideStatus === 6) {
    if (Number.isFinite(currentLat) && Number.isFinite(currentLong)) {
      appendRidePoint(rideId, { lat: currentLat, lng: currentLong, at: Date.now() });
    }
  }

  const extraSession = extraDistanceSessions.get(rideId);
  let fetchedTotalAmount = null;

if (
  rideStatus === 6 &&
  extraSession &&
  extraSession.driverId === driverId &&
  extraSession.settled !== true
) {
  try {
    const fullRoutePoints = getRideRoutePoints(rideId);
    const finalTotalDistanceKm = computeDistanceKmFromPoints(fullRoutePoints);
    const baselineTotalDistanceKm = round2(extraSession.baselineTotalDistanceKm ?? 0);
    const extraDistanceKm = round2(
      Math.max(0, (finalTotalDistanceKm ?? 0) - (baselineTotalDistanceKm ?? 0))
    );

    console.log("[extra-distance][finalize-before-status-7]", {
      ride_id: rideId,
      driver_id: driverId,
      baseline_total_distance_km: baselineTotalDistanceKm,
      final_total_distance_km: finalTotalDistanceKm,
      extra_distance_km: extraDistanceKm,
      route_points_count: Array.isArray(fullRoutePoints) ? fullRoutePoints.length : 0,
    });

if (Array.isArray(fullRoutePoints) && fullRoutePoints.length >= 2) {
const acceptExtraPayload = {
  driver_id: driverId,
  access_token: accessToken,
  driver_service_id: driverServiceId,
  ride_id: rideId,
  route_lat_long_list: JSON.stringify(fullRoutePoints),
};

      const extraRes = await axios.post(
        `${LARAVEL_BASE_URL}/api/driver/accept-not-reached-destination`,
        acceptExtraPayload,
        { timeout: LARAVEL_TIMEOUT_MS }
      );

      let extraData = extraRes?.data ?? null;
      if (typeof extraData === "string") {
        try {
          extraData = JSON.parse(extraData);
        } catch (_) {}
      }

      if (extraData?.status !== 1) {
        console.log("[extra-distance][finalize-before-status-7] backend rejected", {
          ride_id: rideId,
          driver_id: driverId,
          response: extraData,
        });
        return;
      }

      let invoiceData =
        extraData?.invoice && typeof extraData?.invoice === "object"
          ? extraData.invoice
          : null;

      if (!invoiceData && (extraData?.status == null || Number(extraData?.status) === 1)) {
        try {
          const invoiceRes = await axios.post(
            `${LARAVEL_BASE_URL}/api/driver/transport-ride-invoice`,
            {
              driver_id: driverId,
              access_token: accessToken,
              driver_service_id: driverServiceId,
              ride_id: rideId,
            },
            { timeout: LARAVEL_TIMEOUT_MS }
          );

          invoiceData = invoiceRes?.data ?? null;
          if (typeof invoiceData === "string") {
            try {
              invoiceData = JSON.parse(invoiceData);
            } catch (_) {}
          }
        } catch (invoiceErr) {
          console.warn(
            "[extra-distance][finalize-before-status-7] invoice refresh failed:",
            invoiceErr?.response?.data || invoiceErr?.message || invoiceErr
          );
        }
      }

      const ack = {
        status: extraData?.status ?? 1,
        accepted: 1,
        ride_id: rideId,
        request: {
          extra_distance_km: acceptExtraPayload.extra_distance_km ?? null,
          updated_total_distance_km:
            acceptExtraPayload.updated_total_distance_km ?? null,
        },
        response: extraData,
        ...(invoiceData && typeof invoiceData === "object"
          ? { invoice: invoiceData }
          : {}),
        at: Date.now(),
      };

      socket.emit("ride:passedDestinationDecisionAck", ack);
      io.to(driverRoom(driverId)).emit("ride:passedDestinationDecisionAck", ack);

      if (invoiceData && typeof invoiceData === "object") {
        const invoiceEvt = {
          ride_id: rideId,
          ride_status:
            toNumber(invoiceData?.ride_status) ??
            toNumber(extraData?.ride_status) ??
            null,
          invoice: invoiceData,
          source: "driver:passedDestinationDecision",
          at: Date.now(),
        };

        socket.emit("ride:invoice", invoiceEvt);
        io.to(driverRoom(driverId)).emit("ride:invoice", invoiceEvt);
      }

      extraSession.settled = true;
      extraDistanceSessions.set(rideId, extraSession);
    } else {
      extraSession.settled = true;
      extraDistanceSessions.set(rideId, extraSession);
    }
  } catch (e) {
    console.error("[extra-distance][finalize-before-status-7] failed", {
      ride_id: rideId,
      driver_id: driverId,
      message: e?.message ?? null,
      data: e?.response?.data ?? null,
    });
    return;
  }
}

  // ✅ Auto-fetch total_amount for status=7 if missing
  if (rideStatus === 7 && payload?.total_amount == null) {
    try {
      const res = await axios.post(
        `${LARAVEL_BASE_URL}/api/driver/transport-ride-invoice`,
        {
          driver_id: driverId,
          access_token: accessToken,
          driver_service_id: driverServiceId,
          ride_id: rideId,
        },
        { timeout: LARAVEL_TIMEOUT_MS }
      );
      let data = res?.data;
      if (typeof data === "string") {
        try {
          data = JSON.parse(data);
        } catch (_) {}
      }
      if (data?.status === 1) {
        const n = Number(data?.total_pay ?? data?.totalPay ?? null);
        if (Number.isFinite(n)) fetchedTotalAmount = n;
      }
    } catch (e) {
      console.warn(
        "[ride-status][driver:updateRideStatus] invoice fetch failed:",
        e?.response?.data || e?.message || e
      );
    }

    if (fetchedTotalAmount == null) {
      console.log(
        "[ride-status][driver:updateRideStatus] missing total_amount; invoice not available",
        { ride_id: rideId, driver_id: driverId }
      );
      return;
    }
  }

  const apiPayload = {
    driver_id: driverId,
    access_token: accessToken,
    driver_service_id: driverServiceId,
    service_category_id: serviceCategoryId,
    ride_id: rideId,
    ride_status: rideStatus,
    way_point_status: wayPointStatus,
  };

  if (payload?.hail_ride_status != null) apiPayload.hail_ride_status = payload.hail_ride_status;
  if (Number.isFinite(currentLat)) apiPayload.current_lat = currentLat;
  if (Number.isFinite(currentLong)) apiPayload.current_long = currentLong;

  const optionalKeys = [
    "reason_id",
    "otp",
    "destination_address",
    "destination_lat",
    "destination_long",
    "estimated_time",
    "route_lat_long_list",
    "total_amount",
    "toll_charge",
    "no_of_toll",
  ];
  for (const key of optionalKeys) {
    if (payload?.[key] != null) {
      apiPayload[key] = payload[key];
      continue;
    }
    if (!rideDetails) continue;

    if (key === "destination_address" && rideDetails.destination_address != null) {
      apiPayload.destination_address = rideDetails.destination_address;
      continue;
    }
    if (key === "destination_lat" && rideDetails.destination_lat != null) {
      apiPayload.destination_lat = rideDetails.destination_lat;
      continue;
    }
    if (key === "destination_long" && rideDetails.destination_long != null) {
      apiPayload.destination_long = rideDetails.destination_long;
      continue;
    }

    const metaFallback =
      rideDetails.meta && rideDetails.meta[key] != null ? rideDetails.meta[key] : null;
    if (metaFallback != null) apiPayload[key] = metaFallback;
  }

  if (rideStatus === 7 && fetchedTotalAmount != null) {
    apiPayload.total_amount = fetchedTotalAmount;
  }

  if (rideStatus === 6 && apiPayload.route_lat_long_list == null) {
    let routePoints = getRideRoutePoints(rideId);
    if (!routePoints.length && Number.isFinite(currentLat) && Number.isFinite(currentLong)) {
      startRideRoute(rideId, null, routeOpts);
      appendRidePoint(
        rideId,
        { lat: currentLat, lng: currentLong, at: Date.now() },
        routeOpts
      );
      routePoints = getRideRoutePoints(rideId);
    }
    if (routePoints.length) {
      apiPayload.route_lat_long_list = JSON.stringify(routePoints);
    }
  }


  try {
    console.log("[ride-status][driver:updateRideStatus] api payload", apiPayload);
    const res = await axios.post(
      `${LARAVEL_BASE_URL}/api/driver/update-ride-status`,
      apiPayload,
      { timeout: LARAVEL_TIMEOUT_MS }
    );
    console.log("[ride-status][driver:updateRideStatus] api ok", {
      ride_id: rideId,
      ride_status: rideStatus,
      status: res?.status ?? null,
      body: res?.data ?? null,
    });
    const apiStatus = res?.data?.status;
    if (apiStatus != null && apiStatus !== 1) {
      console.log("[ride-status][driver:updateRideStatus] api returned non-success status", {
        ride_id: rideId,
        ride_status: rideStatus,
        api_status: apiStatus,
      });
      return;
    }

    if (FINAL_RIDE_STATUSES.has(rideStatus)) {
      socket.activeRideId = null;
      extraDistanceSessions.delete(rideId);

      console.log("[ride-status][driver:updateRideStatus] terminal status acknowledged", {
        driver_id: driverId,
        ride_id: rideId,
        ride_status: rideStatus,
        note: "cleanup/queue activation handled by /events/internal/ride-status-updated",
      });
    }

    const rideUserId = toNumber(rideDetails?.user_id);
    const rideToken =
      rideDetails?.token ?? rideDetails?.user_details?.user_token ?? null;
    const storedUser = rideUserId ? getUserDetails(rideUserId) : null;
    const storedByToken =
      !storedUser && rideToken ? getUserDetailsByToken(rideToken) : null;

    const routeKm =
      toNumber(payload?.route ?? null) ??
      toNumber(rideDetails?.route ?? rideDetails?.meta?.route ?? null) ??
      toNumber(storedUser?.route ?? storedByToken?.route ?? null);
    const etaMin =
      toNumber(payload?.eta_min ?? null) ??
      toNumber(rideDetails?.eta_min ?? rideDetails?.meta?.eta_min ?? null) ??
      toNumber(storedUser?.eta_min ?? storedByToken?.eta_min ?? null);

    const optimisticEvt = {
      ride_id: rideId,
      ride_status: rideStatus,
      optimistic: true,
      ...(routeKm !== null ? { route: routeKm } : {}),
      ...(etaMin !== null ? { eta_min: etaMin } : {}),
    };

    // ✅ DEBUG: print emitted payload of ride:statusUpdated
    console.log("[emit][ride:statusUpdated]", {
      toRideRoom: `ride:${rideId}`,
      toDriverRoom: driverRoom(driverId),
      payload: optimisticEvt,
    });
    io.to(`ride:${rideId}`).emit("ride:statusUpdated", optimisticEvt);
    io.to(driverRoom(driverId)).emit("ride:statusUpdated", optimisticEvt);
    console.log("[ride-status][driver:updateRideStatus] optimistic emit", {
      ride_id: rideId,
      ride_status: rideStatus,
    });
  } catch (e) {
    console.error("[ride-status][driver:updateRideStatus] api failed", {
      ride_id: rideId,
      driver_id: driverId,
      ride_status: rideStatus,
      status: e?.response?.status ?? null,
      data: e?.response?.data ?? null,
      message: e?.message ?? null,
    });

    if (rideStatus === 9) {
      console.log("[ride-status][fallback][status=9] starting fallback", {
        ride_id: rideId,
        driver_id: driverId,
        hasActivator:
          typeof biddingSocket.activateQueuedRideForDriver === "function",
      });

      const activated =
        typeof biddingSocket.activateQueuedRideForDriver === "function"
          ? biddingSocket.activateQueuedRideForDriver(io, driverId)
          : false;

      console.log("[ride-status][fallback][status=9] result", {
        ride_id: rideId,
        driver_id: driverId,
        activated,
      });

      if (activated) {
        socket.activeRideId = null;
      }
    }
  }
});

  socket.on("driver:passedDestinationDecision", async (payload = {}) => {
    console.log("[ride-status][driver:passedDestinationDecision] incoming", {
      socket_id: socket.id,
      socket_driver_id: socket.driverId ?? null,
      payload_driver_id: payload?.driver_id ?? null,
      ride_id: payload?.ride_id ?? null,
      accepted: payload?.accepted ?? payload?.yes ?? payload?.answer ?? null,
      extra_distance_km: payload?.extra_distance_km ?? null,
      updated_total_distance_km:
        payload?.updated_total_distance_km ??
        payload?.total_distance_km ??
        payload?.total_distance ??
        null,
    });

    const driverId = toNumber(socket.driverId) ?? toNumber(payload?.driver_id);
    if (!driverId) {
      socket.emit("ride:passedDestinationDecisionAck", {
        status: 0,
        accepted: 0,
        message: "driver_id required",
        at: Date.now(),
      });
      return;
    }

    if (!bindDriverOnce(driverId)) {
      socket.emit("ride:passedDestinationDecisionAck", {
        status: 0,
        accepted: 0,
        ride_id: toNumber(payload?.ride_id) ?? null,
        message: "driver_id mismatch",
        at: Date.now(),
      });
      return;
    }

    const rideId =
      toNumber(payload?.ride_id) ?? getActiveRideByDriver(driverId) ?? null;
    if (!rideId) {
      socket.emit("ride:passedDestinationDecisionAck", {
        status: 0,
        accepted: 0,
        message: "ride_id required",
        at: Date.now(),
      });
      return;
    }

    const accepted = isAcceptedDecision(
      payload?.accepted ?? payload?.yes ?? payload?.answer
    );
    if (!accepted) {
      extraDistanceSessions.delete(rideId);

      const ack = {
        status: 1,
        accepted: 0,
        ride_id: rideId,
        message: "Driver did not accept extra distance",
        at: Date.now(),
      };
      socket.emit("ride:passedDestinationDecisionAck", ack);
      io.to(driverRoom(driverId)).emit("ride:passedDestinationDecisionAck", ack);
      return;
    }

    const currentLat =
      toNumber(payload?.current_lat ?? payload?.lat) ??
      toNumber(driverLocationService.getDriver(driverId)?.lat);

    const currentLong =
      toNumber(payload?.current_long ?? payload?.long) ??
      toNumber(driverLocationService.getDriver(driverId)?.long);

    let routePoints = getRideRoutePoints(rideId);
    if (!routePoints.length && Number.isFinite(currentLat) && Number.isFinite(currentLong)) {
      appendRidePoint(rideId, {
        lat: currentLat,
        lng: currentLong,
        at: Date.now(),
      });
      routePoints = getRideRoutePoints(rideId);
    }

    const baselineRoutePointCount = Array.isArray(routePoints) ? routePoints.length : 0;
    const baselineTotalDistanceKm = computeDistanceKmFromPoints(routePoints);

    extraDistanceSessions.set(rideId, {
      driverId,
      acceptedAt: Date.now(),
      baselineRoutePointCount,
      baselineTotalDistanceKm,
      acceptedLat: Number.isFinite(currentLat) ? currentLat : null,
      acceptedLong: Number.isFinite(currentLong) ? currentLong : null,
      settled: false,
    });

    const ack = {
      status: 1,
      accepted: 1,
      ride_id: rideId,
      message: "Extra distance tracking started",
      baseline_route_point_count: baselineRoutePointCount,
      baseline_total_distance_km: baselineTotalDistanceKm,
      at: Date.now(),
    };

    socket.emit("ride:passedDestinationDecisionAck", ack);
    io.to(driverRoom(driverId)).emit("ride:passedDestinationDecisionAck", ack);

    io.to(`ride:${rideId}`).emit("ride:passedDestinationAccepted", {
      ride_id: rideId,
      driver_id: driverId,
      tracking_started: 1,
      baseline_route_point_count: baselineRoutePointCount,
      baseline_total_distance_km: baselineTotalDistanceKm,
      at: Date.now(),
    });

    io.to(driverRoom(driverId)).emit("ride:passedDestinationAccepted", {
      ride_id: rideId,
      driver_id: driverId,
      tracking_started: 1,
      baseline_route_point_count: baselineRoutePointCount,
      baseline_total_distance_km: baselineTotalDistanceKm,
      at: Date.now(),
    });
  });

  socket.on("disconnect", () => {
    if (socket.dbInterval) {
      clearInterval(socket.dbInterval);
      socket.dbInterval = null;
    }

    if (socket.laravelLocationInterval) {
      clearInterval(socket.laravelLocationInterval);
      socket.laravelLocationInterval = null;
    }

    if (socket.driverId) {
      const now = Date.now();
      const driverId = socket.driverId;
      const activeRideId =
        getActiveRideByDriver(driverId) ?? toNumber(socket.activeRideId);

      const prefix = `${driverId}:`;
      for (const key of lastRideStatusByKey.keys()) {
        if (key.startsWith(prefix)) lastRideStatusByKey.delete(key);
      }
      lastAcceptedLocationByDriver.delete(driverId);
      pendingRelocationByDriver.delete(driverId);

      if (activeRideId) {
        // ✅ إذا عنده رحلة شغالة: لا تحوله offline
        driverLocationService.updateMeta(driverId, {
          is_online: true,
          dashboard_is_online: true,
          socket_disconnected: true,
          lastSeen: now,
          updatedAt: now,
        });

        console.log(
          `🟡 Driver ${driverId} disconnected but kept online because active ride ${activeRideId} is running (socket: ${socket.id})`
        );
      } else {
        // ✅ إذا ما عنده رحلة: يصير offline طبيعي
        driverLocationService.updateMeta(driverId, {
          is_online: false,
          dashboard_is_online: false,
          socket_disconnected: true,
          lastSeen: now,
          updatedAt: now,
        });

        console.log(`⚫ Driver ${driverId} went offline (socket: ${socket.id})`);
      }

      emitCandidatesSummaryForDriver(driverId);
      emitAdminDriverUpdate(io, driverId);
      logRooms(`after disconnect driver:${driverId}`);
    }
  });
};
