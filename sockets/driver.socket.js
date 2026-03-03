// sockets/driver.socket.js
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
const biddingSocket = require("./bidding.socket");

// 🔧 Settings
const DB_UPDATE_EVERY_MS = 0; // set to 0 to disable direct DB writes from Node (recommended)
const LARAVEL_LOCATION_PUSH_EVERY_MS = 10000; // how often to push driver location to Laravel API
const LARAVEL_BASE_URL =
  process.env.LARAVEL_BASE_URL ||
  process.env.LARAVEL_URL ||
  "https://aiactive.co.uk/backend/backend-laravel/public";
const LARAVEL_TIMEOUT_MS = 7000;
const STATUS_DEDUPE_TTL_MS = Number.isFinite(
  Number(process.env.STATUS_DEDUPE_TTL_MS)
)
  ? Number(process.env.STATUS_DEDUPE_TTL_MS)
  : 4000;
const FINAL_RIDE_STATUSES = new Set([4, 6, 7, 8, 9, 10]);

const lastRideStatusByKey = new Map(); // key -> { status, at }
const shouldSkipDuplicateStatus = (key, status) => {
  const now = Date.now();
  const prev = lastRideStatusByKey.get(key);
  if (prev && prev.status === status && now - prev.at < STATUS_DEDUPE_TTL_MS) {
    return true;
  }
  lastRideStatusByKey.set(key, { status, at: now });
  return false;
};

module.exports = (io, socket) => {
  // ─────────────────────────────
  // Helpers
  // ─────────────────────────────
  const toNumber = (v) => {
    const n = Number(v);
    return Number.isFinite(n) ? n : null;
  };

  const driverRoom = (driverId) => `driver:${driverId}`;

  const bindDriverOnce = (newDriverId) => {
    if (!socket.driverId) {
      socket.driverId = newDriverId;
      return true;
    }
    return socket.driverId === newDriverId;
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
    socket.driverAccessToken = access_token ?? null;
    socket.driverServiceCategoryId = payloadServiceCategoryId ?? null;

    socket.join(driverRoom(driverId));
    console.log("✅ driver joined room", driverRoom(driverId), "socket:", socket.id);

    // ✅ خزّن أولياً لوكيشن + online (بدون upsert)
    driverLocationService.updateMemory(driverId, la, lo);

    const baseMeta = {
      driver_id: driverId,
      is_online: true,
      driver_service_id: toNumber(driver_service_id) ?? null,
      updatedAt: Date.now(),
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
        if (Number.isFinite(service_category_id)) {
          socket.driverServiceCategoryId = service_category_id;
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
          d.child_seat ?? d.child_seat_accessibility ?? baseMeta.child_seat ?? null
        );
        const handicap = toNumber(
          d.handicap ?? d.handicap_accessibility ?? baseMeta.handicap ?? null
        );

        // حالة السائق (من الريسبونس إذا بدك تكون أدق)
        const currentStatus = Number(d.new_status ?? d.driver_current_status ?? 1);

        const metaUpdate = {
          // status/meta
          is_online: currentStatus === 1,
          updatedAt: Date.now(),
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

    socket.emit("driver:ready", { driver_id: driverId });
  });

  socket.on("update-location", ({ lat, long }) => {
    console.log("[update-location] payload:", { lat, long });
    if (!socket.driverId) return;

    const la = toNumber(lat);
    const lo = toNumber(long);
    if (la === null || lo === null) return;

    // ✅ source of truth: service only
    driverLocationService.updateMemory(socket.driverId, la, lo);

    // ✅ mark as online on any location ping
    driverLocationService.updateMeta(socket.driverId, {
      is_online: true,
      updatedAt: Date.now(),
    });

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
      timestamp: Date.now(),
    };

    io.to(driverRoom(socket.driverId)).emit("driver:moved", payload);

    const activeRideId = getActiveRideByDriver(socket.driverId);
    if (activeRideId) {
      appendRidePoint(activeRideId, { lat: la, lng: lo, at: Date.now() });
      io.to(`ride:${activeRideId}`).emit("ride:locationUpdate", {
        ride_id: activeRideId,
        driver_id: socket.driverId,
        lat: la,
        long: lo,
        at: Date.now(),
      });
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

    if (!rideId || rideStatus == null || wayPointStatus == null || !serviceCategoryId) {
      console.log("[ride-status][driver:updateRideStatus] missing required fields", {
        ride_id: rideId,
        ride_status: rideStatus,
        way_point_status: wayPointStatus,
        service_category_id: serviceCategoryId,
      });
      return;
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

    // ✅ Auto-fetch total_amount for status=7 if missing
    let fetchedTotalAmount = null;
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

    // ✅ start/append route tracking for running rides
    if (rideStatus === 5) {
      if (Number.isFinite(currentLat) && Number.isFinite(currentLong)) {
        startRideRoute(rideId, { lat: currentLat, lng: currentLong, at: Date.now() });
      } else {
        startRideRoute(rideId);
      }
    }
    if (rideStatus === 6) {
      if (Number.isFinite(currentLat) && Number.isFinite(currentLong)) {
        appendRidePoint(rideId, { lat: currentLat, lng: currentLong, at: Date.now() });
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
      "total_distance",
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
        startRideRoute(rideId);
        appendRidePoint(rideId, { lat: currentLat, lng: currentLong, at: Date.now() });
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
  // ✅ IMPORTANT: release driver from active ride
  const { clearActiveRideByDriver, clearActiveRideByRideId } = require("../store/activeRides.store");

  clearActiveRideByDriver(driverId);
  clearActiveRideByRideId(rideId);

  // ✅ optional: close bidding cleanly (removes inbox/candidates if any left)
  if (typeof biddingSocket.closeRideBidding === "function") {
    biddingSocket.closeRideBidding(io, rideId, { clearUser: true });
  }

  console.log(`[ride-status] cleared active ride: driver=${driverId} ride=${rideId} status=${rideStatus}`);
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
      console.error("[ride-status][driver:updateRideStatus] api failed:", e?.response?.data || e.message);
    }
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
      const prefix = `${socket.driverId}:`;
      for (const key of lastRideStatusByKey.keys()) {
        if (key.startsWith(prefix)) lastRideStatusByKey.delete(key);
      }

      driverLocationService.updateMeta(socket.driverId, {
        is_online: false,
        lastSeen: Date.now(),
        updatedAt: Date.now(),
      });

      console.log(`⚫ Driver ${socket.driverId} went offline (socket: ${socket.id})`);
      logRooms(`after disconnect driver:${socket.driverId}`);
    }
  });
};
