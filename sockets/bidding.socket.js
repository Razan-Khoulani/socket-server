// sockets/bidding.socket.js
const driverLocationService = require("../services/driverLocation.service");
const axios = require("axios"); // لضمان استدعاء Laravel API عند قبول العرض
const { getDistanceMeters } = require("../utils/geo.util");
const { getUserDetails, getUserDetailsByToken } = require("../store/users.store");
const {
  clearActiveRideByRideId,
  getActiveRideByDriver,
  getActiveDriverByRide,
  setActiveRide,
  clearActiveRideByDriver,
} = require("../store/activeRides.store");
const LARAVEL_GET_ROUTE_PATH =
  process.env.LARAVEL_GET_ROUTE_PATH || "/api/getRoute";

const DEBUG_EVENTS = process.env.DEBUG_SOCKET_EVENTS === "1";
const debugLog = (event, payload, socketId) => {
  if (!DEBUG_EVENTS) return;
  console.log("[bidding.socket]", event, "socket:", socketId, "payload:", payload);
};

// ✅ In-memory Maps (no Redis)
const rideCandidates = new Map(); // rideId -> Set(driverId)
const driverRideInbox = new Map(); // driverId -> Map(rideId -> ridePayload)
const driverLastBidStatus = new Map(); // driverId -> { rideId, responded }
const acceptLocks = new Map(); // rideId -> timestamp (prevent double-accept race)

// ✅ NEW: per-driver patch sequence (ordering)
const driverPatchSeq = new Map(); // driverId -> number

const ACCEPT_LOCK_TTL_MS = Number.isFinite(Number(process.env.ACCEPT_LOCK_TTL_MS))
  ? Number(process.env.ACCEPT_LOCK_TTL_MS)
  : 5000;

const rideRoom = (rideId) => `ride:${rideId}`;
const driverRoom = (driverId) => `driver:${driverId}`;

const toNumber = (v) => {
  if (v === null || v === undefined || v === "") return null;
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
};
const toRouteMetricNumber = (v) => {
  if (v === null || v === undefined || v === "") return null;
  if (typeof v === "number") return Number.isFinite(v) ? v : null;
  if (typeof v === "string") {
    const strict = Number(v);
    if (Number.isFinite(strict)) return strict;

    // Supports API values like "25 min" or "9.23 km"
    const normalized = v.replace(",", ".").trim();
    const parsed = parseFloat(normalized);
    return Number.isFinite(parsed) ? parsed : null;
  }
  return null;
};
const pickFirstValue = (...values) => {
  for (const v of values) {
    if (v !== undefined && v !== null && v !== "") return v;
  }
  return null;
};

const round2 = (v) => (Number.isFinite(v) ? Math.round(v * 100) / 100 : null);

const buildPriceBounds = (baseFare) => {
  const base = toNumber(baseFare);
  if (base === null) {
    return {
      base_fare: null,
      min_price: null,
      max_price: null,
    };
  }

  return {
    base_fare: round2(base),
    min_price: round2(base / 2),
    max_price: round2(base * 2),
  };
};

const getRidePriceBounds = (payload = {}) => {
  if (!payload || typeof payload !== "object") {
    return { base_fare: null, min_price: null, max_price: null };
  }

  const explicitMin = toNumber(payload?.min_price ?? payload?.meta?.min_price ?? null);
  const explicitMax = toNumber(payload?.max_price ?? payload?.meta?.max_price ?? null);
  const explicitBase = toNumber(
    payload?.base_fare ??
      payload?.estimated_fare ??
      payload?.meta?.base_fare ??
      payload?.meta?.estimated_fare ??
      null
  );

  if (explicitBase !== null) {
    const built = buildPriceBounds(explicitBase);
    return {
      base_fare: built.base_fare,
      min_price: explicitMin ?? built.min_price,
      max_price: explicitMax ?? built.max_price,
    };
  }

  const fallbackBase = toNumber(
    payload?.user_bid_price ?? payload?.updatedPrice ?? payload?.min_fare_amount ?? null
  );

  const built = buildPriceBounds(fallbackBase);

  return {
    base_fare: built.base_fare,
    min_price: explicitMin ?? built.min_price,
    max_price: explicitMax ?? built.max_price,
  };
};

const isPriceWithinBounds = (price, bounds) => {
  const p = toNumber(price);
  const min = toNumber(bounds?.min_price);
  const max = toNumber(bounds?.max_price);

  if (p === null || min === null || max === null) return true;
  return p >= min && p <= max;
};

const emitPriceValidationError = (io, target, payload = {}) => {
  io.to(target).emit("ride:priceValidationError", {
    ride_id: payload.ride_id ?? null,
    attempted_price: payload.attempted_price ?? null,
    min_price: payload.min_price ?? null,
    max_price: payload.max_price ?? null,
    actor: payload.actor ?? null,
    message: payload.message ?? "Price is outside allowed range",
    at: Date.now(),
  });
};

const normalizeDuration = (v) => toNumber(v);
const getRideDurationRaw = (payload = null) => {
  if (!payload || typeof payload !== "object") return null;
  return pickFirstValue(
    payload?.ride_details?.duration,
    payload?.duration,
    payload?.meta?.duration,
    payload?.route_api_duration_min,
    payload?.meta?.route_api_duration_min,
    payload?.eta_min,
    payload?.meta?.eta_min
  );
};
const getRideDurationMinutes = (payload = null) => {
  if (!payload || typeof payload !== "object") return null;
  return normalizeDuration(
    payload?.ride_details?.duration ??
      payload?.duration ??
      payload?.meta?.duration ??
      payload?.route_api_duration_min ??
      payload?.meta?.route_api_duration_min ??
      payload?.eta_min ??
      payload?.meta?.eta_min ??
      null
  );
};
const getRideDistanceKm = (payload = null) => {
  if (!payload || typeof payload !== "object") return null;
  const km = pickFirstValue(
    payload?.distance ??
      payload?.route_api_distance_km ??
      payload?.driver_to_pickup_distance_km ??
      payload?.ride_details?.route_api_distance_km ??
      payload?.meta?.route_api_distance_km
  );
  return km ?? null;
};
const getRideRouteApiDurationRaw = (payload = null) => {
  if (!payload || typeof payload !== "object") return null;
  return pickFirstValue(
    toRouteMetricNumber(payload?.meta?.route_api_data?.duration),
    toRouteMetricNumber(payload?.route_api_duration_min),
    toRouteMetricNumber(payload?.meta?.route_api_duration_min),
    toRouteMetricNumber(payload?.ride_details?.route_api_duration_min)
  );
};
const getRideRouteApiDistanceKmRaw = (payload = null) => {
  if (!payload || typeof payload !== "object") return null;
  return pickFirstValue(
    toRouteMetricNumber(payload?.meta?.route_api_data?.route),
    toRouteMetricNumber(payload?.meta?.route_api_data?.distance_km),
    toRouteMetricNumber(payload?.meta?.route_api_data?.total_distance),
    toRouteMetricNumber(payload?.route_api_distance_km),
    toRouteMetricNumber(payload?.ride_details?.route_api_distance_km),
    toRouteMetricNumber(payload?.meta?.route_api_distance_km)
  );
};
const normalizeRideMetrics = (payload = {}) => {
  if (!payload || typeof payload !== "object") return payload;

  const duration = getRideDurationRaw(payload);
  const distanceKm = getRideDistanceKm(payload);
  const rideDetails =
    payload.ride_details && typeof payload.ride_details === "object"
      ? { ...payload.ride_details }
      : {};
  const meta =
    payload.meta && typeof payload.meta === "object" ? { ...payload.meta } : {};

  if (duration !== null) {
    rideDetails.duration = duration;
    meta.duration = duration;
  }
  if (distanceKm !== null) {
    rideDetails.route_api_distance_km = distanceKm;
    meta.route_api_distance_km = distanceKm;
  }

  return {
    ...payload,
    ...(duration !== null ? { duration, route_api_duration_min: duration } : {}),
    ...(distanceKm !== null
      ? { distance: distanceKm, route_api_distance_km: distanceKm }
      : {}),
    ride_details: rideDetails,
    meta,
  };
};
const DRIVER_TO_PICKUP_SPEED_KMPH = Number.isFinite(
  Number(process.env.DRIVER_TO_PICKUP_SPEED_KMPH)
)
  ? Math.max(5, Number(process.env.DRIVER_TO_PICKUP_SPEED_KMPH))
  : 28;

// ✅ UPDATED: timeout صار 90 ثانية (ALL TIMER VALUES RETURNED IN SECONDS)
const RIDE_TIMEOUT_S = Number.isFinite(Number(process.env.RIDE_TIMEOUT_S))
  ? Number(process.env.RIDE_TIMEOUT_S)
  : 90; // ✅ fixed 90 seconds

const CANCELLED_RIDE_TTL_MS = 10 * 60 * 1000;

// ✅ NEW: TTL لتنظيف الانبوكس من الرحلات القديمة (حتى لو ما وصل cancel/accept)
const INBOX_ENTRY_TTL_MS = Number.isFinite(Number(process.env.INBOX_ENTRY_TTL_MS))
  ? Number(process.env.INBOX_ENTRY_TTL_MS)
  : 10 * 60 * 1000; // default 10 minutes (survive short network drops)

const MIN_DISPATCH_RADIUS_METERS = 200;
const ROAD_RADIUS_METERS = Number.isFinite(Number(process.env.ROAD_RADIUS_METERS))
  ? Math.max(100, Number(process.env.ROAD_RADIUS_METERS))
  : 5000;

const AIR_CANDIDATE_RADIUS_METERS = Number.isFinite(Number(process.env.AIR_CANDIDATE_RADIUS_METERS))
  ? Math.max(ROAD_RADIUS_METERS, Number(process.env.AIR_CANDIDATE_RADIUS_METERS))
  : 8000;

const MAX_ROAD_FILTER_CANDIDATES = Number.isFinite(Number(process.env.MAX_ROAD_FILTER_CANDIDATES))
  ? Math.max(1, Number(process.env.MAX_ROAD_FILTER_CANDIDATES))
  : 25;
const MAX_DISPATCH_RADIUS_METERS = Number.isFinite(Number(process.env.MAX_DISPATCH_RADIUS_METERS))
  ? Number(process.env.MAX_DISPATCH_RADIUS_METERS)
  : 7000;

const MAX_DISPATCH_CANDIDATES = Number.isFinite(Number(process.env.MAX_DISPATCH_CANDIDATES))
  ? Number(process.env.MAX_DISPATCH_CANDIDATES)
  : 30;

const MAX_DRIVER_LOCATION_AGE_MS = Number.isFinite(Number(process.env.MAX_DRIVER_LOCATION_AGE_MS))
  ? Number(process.env.MAX_DRIVER_LOCATION_AGE_MS)
  : 2 * 60 * 1000;

const LARAVEL_BASE_URL =
  process.env.LARAVEL_BASE_URL ||
  process.env.LARAVEL_URL ||
  "https://aiactive.co.uk/backend/backend-laravel/public";

const LARAVEL_ACCEPT_BID_PATH = "/api/customer/transport/accept-bid";
const LARAVEL_DRIVER_BID_PATH = "/api/driver/bid-offer";
const LARAVEL_ACCEPT_BID_TIMEOUT_MS = Number.isFinite(
  Number(process.env.LARAVEL_ACCEPT_BID_TIMEOUT_MS)
)
  ? Number(process.env.LARAVEL_ACCEPT_BID_TIMEOUT_MS)
  : 7000;

// ─────────────────────────────
// ✅ Timer helpers (SECONDS ONLY + SERVER AUTHORITATIVE)
// ─────────────────────────────
/**
 * ✅ Timer payload (server-authoritative) — ALL IN SECONDS
 * - server_time: epoch seconds
 * - expires_at: epoch seconds
 * - timeout_ms: duration seconds (kept key name to avoid breaking old clients)
 */
const nowSec = () => Math.floor(Date.now() / 1000);

const makeTimer = (durationSec = RIDE_TIMEOUT_S) => {
  const now = nowSec();
  const durSec = Math.max(
    0,
    Math.floor(Number.isFinite(Number(durationSec)) ? Number(durationSec) : RIDE_TIMEOUT_S)
  );
  return {
    server_time: now, // seconds
    expires_at: now + durSec, // seconds
    timeout_ms: durSec, // seconds (kept key name)
  };
};

// ─────────────────────────────
// ✅ Patch system helpers
// ─────────────────────────────
const nextDriverSeq = (driverId) => {
  const cur = driverPatchSeq.get(driverId) || 0;
  const next = cur + 1;
  driverPatchSeq.set(driverId, next);
  return next;
};

/**
 * ✅ Patch event (delta) — no full inbox resend
 * ops:
 *  - { op: "upsert", ride: <ridePayload> }
 *  - { op: "remove", ride_id: <id> }
 */
function emitDriverPatch(io, driverId, ops = []) {
  if (!driverId || !ops || ops.length === 0) return;
  const safeOps = ops.map((op) => {
    if (!op || typeof op !== "object") return op;
    if (op.op !== "upsert" || !op.ride || typeof op.ride !== "object") return op;
    return {
      ...op,
      ride: sanitizeRidePayloadForClient(op.ride),
    };
  });
  io.to(driverRoom(driverId)).emit("driver:rides:patch", {
    driver_id: driverId,
    ops: safeOps,
    seq: nextDriverSeq(driverId),
    at: Date.now(),
  });
}

function buildRideCandidatesSummary(rideId) {
  const candidateSet = rideCandidates.get(rideId);
  if (!candidateSet || candidateSet.size === 0) return [];

  const rideSnapshot = getRideDetails(rideId) || null;
  const requestedServiceTypeId = toNumber(rideSnapshot?.service_type_id ?? null);

  const typesMap = new Map();

  for (const driverId of candidateSet.values()) {
    const driver = driverLocationService.getDriver(driverId);
    const meta = driverLocationService.getMeta(driverId) || {};

    if (!driver) continue;

    const isOnline = Number(driver?.is_online ?? meta?.is_online ?? 1) === 1;
    if (!isOnline) continue;

    const activeRide = getActiveRideByDriver(driverId);
    if (activeRide && activeRide !== rideId) continue;

    const typeId = toNumber(driver?.service_type_id ?? meta?.service_type_id);
    if (!typeId) continue;

    if (requestedServiceTypeId && typeId !== requestedServiceTypeId) continue;

    const serviceCategoryId = toNumber(
      driver?.service_category_id ?? meta?.service_category_id
    );

    const vehicleTypeName = driver?.vehicle_type_name ?? meta?.vehicle_type_name ?? "";
    const vehicleTypeIcon = driver?.vehicle_type_icon ?? meta?.vehicle_type_icon ?? null;
    const driverImage = driver?.driver_image ?? meta?.driver_image ?? null;
    const lat = toNumber(driver?.lat);
    const long = toNumber(driver?.long);

    if (!typesMap.has(typeId)) {
      typesMap.set(typeId, {
        service_type_id: typeId,
        service_category_id: serviceCategoryId ?? null,
        vehicle_type_name: vehicleTypeName,
        vehicle_type_icon: vehicleTypeIcon,
        drivers_count: 1,
        drivers: [
          {
            driver_id: driverId,
            lat,
            long,
            driver_image: driverImage,
          },
        ],
      });
    } else {
      const existing = typesMap.get(typeId);
      existing.drivers_count += 1;
      existing.drivers.push({
        driver_id: driverId,
        lat,
        long,
        driver_image: driverImage,
      });
    }
  }

  return Array.from(typesMap.values());
}

function emitRideCandidatesSummary(io, rideId) {
  const vehicleTypes = buildRideCandidatesSummary(rideId);

  io.to(rideRoom(rideId)).emit("ride:candidatesSummary", {
    ride_id: rideId,
    vehicle_types: vehicleTypes,
    total_vehicle_types: vehicleTypes.length,
    total_drivers: vehicleTypes.reduce(
      (sum, item) => sum + (toNumber(item?.drivers_count) ?? 0),
      0
    ),
    at: Date.now(),
  });
}

const fetchDriverMetaFromApi = async (driverId, accessToken, driverServiceId) => {
  if (!driverId || !accessToken || !driverServiceId) return null;
  try {
    const res = await axios.post(
      `${LARAVEL_BASE_URL}/api/driver/update-current-status`,
      {
        driver_id: driverId,
        access_token: accessToken,
        driver_service_id: driverServiceId,
        update_status: 1,
      },
      { timeout: 7000 }
    );

    let d = res?.data || {};
    if (typeof d === "string") {
      try {
        d = JSON.parse(d);
      } catch (_) {}
    }

    const driverName = d.driver_name ?? null;
    const vehicleType = d.vehicle_type_name ?? null;
    const vehicleNumber = d.plat_no ?? null;
    const rating = d.rating ?? null;
    const driverImage =
      d.driver_image ?? d.profile_image ?? d.driver_image_url ?? d.avatar ?? d.image ?? null;

    const metaUpdate = {
      ...(driverName ? { driver_name: driverName } : {}),
      ...(vehicleType ? { vehicle_type_name: vehicleType } : {}),
      ...(vehicleNumber ? { plat_no: vehicleNumber } : {}),
      ...(rating != null ? { rating } : {}),
      ...(driverImage ? { driver_image: driverImage } : {}),
    };

    if (Object.keys(metaUpdate).length > 0) {
      driverLocationService.updateMeta(driverId, metaUpdate);
    }

    return {
      driver_name: driverName,
      vehicle_type: vehicleType,
      vehicle_number: vehicleNumber,
      rating,
      driver_image: driverImage,
    };
  } catch (e) {
    return null;
  }
};

const isDriverDetailsEmpty = (d) => {
  if (!d) return true;
  return (
    d.driver_name == null &&
    d.vehicle_type == null &&
    d.vehicle_number == null &&
    d.rating == null &&
    d.driver_image == null
  );
};

// User -> ride owner map (rideId -> userId)
const rideOwnerByRide = new Map(); // rideId -> userId

const getUserIdFromDispatch = (data, userDetails) => {
  const uid = toNumber(
    data?.user_id ?? data?.customer_id ?? data?.passenger_id ?? userDetails?.user_id
  );
  return uid || null;
};

const setUserActiveRide = (userId, rideId) => {
  if (!userId || !rideId) return;
  rideOwnerByRide.set(rideId, userId);
};

const clearUserRideByRideId = (rideId) => {
  if (!rideId) return;
  rideOwnerByRide.delete(rideId);
};

const getUserIdForRide = (rideId) => {
  if (!rideId) return null;
  return rideOwnerByRide.get(rideId) ?? null;
};

const getActiveRideIdForUser = (userId) => {
  const safeUserId = toNumber(userId);
  if (!safeUserId) return null;

  for (const [rideId, ownerId] of rideOwnerByRide.entries()) {
    if (toNumber(ownerId) === safeUserId) {
      return toNumber(rideId) ?? null;
    }
  }
  return null;
};

const touchUserActiveRide = (userId, rideId) => {
  const safeUserId = toNumber(userId);
  const safeRideId = toNumber(rideId);
  if (!safeUserId || !safeRideId) return false;
  setUserActiveRide(safeUserId, safeRideId);
  return true;
};

// ✅ NEW: unify token extraction (fix retry cases where token name differs)
const getTokenFromAny = (data, built = null, src = null) => {
  return (
    data?.token ??
    data?.access_token ??
    data?.user_token ??
    data?.user_details?.user_token ??
    built?.user_token ??
    src?.user_token ??
    src?.token ??
    null
  );
};

const buildUserDetails = (data) => {
  const src =
    (data?.user_details && typeof data.user_details === "object" && data.user_details) ||
    (data?.user && typeof data.user === "object" && data.user) ||
    (data?.customer && typeof data.customer === "object" && data.customer) ||
    null;

  let userId = toNumber(
    src?.user_id ?? src?.id ?? src?.customer_id ?? data?.user_id ?? data?.customer_id
  );

  // ✅ NEW: token fallback (supports retry payloads using access_token / user_token / user_details.user_token)
  const token = getTokenFromAny(data, null, src);

  const userName =
    src?.user_name ??
    src?.name ??
    src?.customer_name ??
    data?.user_name ??
    data?.customer_name ??
    null;

  const genderRaw = src?.gender ?? src?.user_gender ?? data?.gender ?? data?.user_gender ?? null;

  const userGender =
    genderRaw === "" || genderRaw == null
      ? null
      : Number.isFinite(Number(genderRaw))
      ? Number(genderRaw)
      : genderRaw;

  const countryCode =
    src?.select_country_code ?? data?.select_country_code ?? src?.country_code ?? null;

  const contactNumber =
    src?.contact_number ??
    src?.user_phone ??
    src?.phone ??
    src?.mobile ??
    data?.contact_number ??
    data?.user_phone ??
    data?.customer_phone ??
    data?.phone ??
    null;

  const userImage =
    src?.profile_image ??
    src?.user_image ??
    src?.image ??
    src?.avatar ??
    data?.profile_image ??
    data?.user_image ??
    data?.customer_image ??
    null;

  const stored = userId ? getUserDetails(userId) : null;
  const storedByToken = !stored && token ? getUserDetailsByToken(token) : null;

  if (!userId && storedByToken?.user_id) {
    userId = toNumber(storedByToken.user_id) ?? userId;
  }

  const details = {
    user_id: userId ?? stored?.user_id ?? storedByToken?.user_id ?? null,
    user_name: userName ?? stored?.user_name ?? storedByToken?.user_name ?? null,
    user_gender: userGender ?? stored?.user_gender ?? storedByToken?.user_gender ?? null,
    user_phone: contactNumber ?? stored?.user_phone ?? storedByToken?.user_phone ?? null,
    user_country_code:
      countryCode ?? stored?.user_country_code ?? storedByToken?.user_country_code ?? null,
    user_phone_full:
      contactNumber && countryCode
        ? `${countryCode}${contactNumber}`
        : stored?.user_phone_full ?? storedByToken?.user_phone_full ?? null,
    user_image: userImage ?? stored?.user_image ?? storedByToken?.user_image ?? null,

    // ✅ NEW: keep token in user_details snapshot (helps later merges on retry)
    user_token: token ?? stored?.user_token ?? stored?.token ?? storedByToken?.user_token ?? null,
    token: token ?? stored?.user_token ?? stored?.token ?? storedByToken?.user_token ?? null,
  };

  if (
    !details.user_id &&
    !details.user_name &&
    !details.user_phone &&
    !details.user_image &&
    !details.user_token
  ) {
    return null;
  }
  return details;
};

const buildCustomerPayload = (payload = {}, userDetails = null) => {
  const details =
    userDetails && typeof userDetails === "object"
      ? userDetails
      : payload?.user_details && typeof payload.user_details === "object"
      ? payload.user_details
      : null;

  const customerId = toNumber(
    details?.user_id ??
      payload?.user_id ??
      payload?.customer_id ??
      payload?.passenger_id ??
      payload?.user_details?.user_id
  );

  const customerName =
    details?.user_name ??
    payload?.user_name ??
    payload?.customer_name ??
    payload?.passenger_name ??
    null;

  const customerGenderRaw =
    details?.user_gender ??
    payload?.user_gender ??
    payload?.customer_gender ??
    payload?.gender ??
    null;
  const customerGender =
    customerGenderRaw === "" || customerGenderRaw == null
      ? null
      : Number.isFinite(Number(customerGenderRaw))
      ? Number(customerGenderRaw)
      : customerGenderRaw;

  const customerCountryCode =
    details?.user_country_code ??
    payload?.user_country_code ??
    payload?.customer_country_code ??
    payload?.country_code ??
    payload?.select_country_code ??
    null;

  const customerPhone =
    details?.user_phone ??
    payload?.user_phone ??
    payload?.customer_phone ??
    payload?.contact_number ??
    payload?.phone ??
    null;

  const customerPhoneFull =
    details?.user_phone_full ??
    payload?.user_phone_full ??
    payload?.customer_phone_full ??
    (customerCountryCode && customerPhone ? `${customerCountryCode}${customerPhone}` : null);

  const customerImage =
    details?.user_image ??
    payload?.user_image ??
    payload?.customer_image ??
    payload?.profile_image ??
    payload?.avatar ??
    null;

  const customerToken =
    details?.user_token ??
    details?.token ??
    payload?.user_token ??
    payload?.token ??
    payload?.access_token ??
    null;

  if (
    !customerId &&
    !customerName &&
    !customerPhone &&
    !customerImage &&
    !customerToken &&
    !details
  ) {
    return null;
  }

  return {
    user_id: customerId ?? null,
    user_name: customerName ?? null,
    user_gender: customerGender ?? null,
    user_country_code: customerCountryCode ?? null,
    user_phone: customerPhone ?? null,
    user_phone_full: customerPhoneFull ?? null,
    user_image: customerImage ?? null,
    user_token: customerToken ?? null,
    token: customerToken ?? null,
  };
};

const attachCustomerFields = (payload = {}, userDetails = null) => {
  if (!payload || typeof payload !== "object") return payload;

  const customer = buildCustomerPayload(payload, userDetails);
  return {
    ...payload,
    customer: customer ?? null,
    customer_details: customer ?? null,
    customer_id: customer?.user_id ?? null,
    customer_name: customer?.user_name ?? null,
    customer_gender: customer?.user_gender ?? null,
    customer_country_code: customer?.user_country_code ?? null,
    customer_phone: customer?.user_phone ?? null,
    customer_phone_full: customer?.user_phone_full ?? null,
    customer_image: customer?.user_image ?? null,
  };
};

const sanitizeRidePayloadForClient = (payload = {}) => {
  if (!payload || typeof payload !== "object") return payload;
  const {
    user_details,
    customer,
    customer_details,
    customer_id,
    customer_name,
    customer_gender,
    customer_country_code,
    customer_phone,
    customer_phone_full,
    customer_image,
    ...rest
  } = payload;
  return rest;
};

// ✅ rides cancelled (block dispatch)
const cancelledRides = new Set(); // rideId

// ─────────────────────────────
// Inbox helpers
// ─────────────────────────────
const rideTimers = new Map(); // rideId -> setTimeout ID

// الخريطة لحفظ تفاصيل الرحلات في الذاكرة (إذا احتجتها لاحقاً)
const rideDetailsMap = new Map();
function saveRideDetails(rideId, rideDetails) {
  rideDetailsMap.set(rideId, rideDetails);
  console.log(`Details for ride ${rideId} saved in memory.`);
}
function getRideDetails(rideId) {
  return rideDetailsMap.get(rideId);
}

function isAcceptLocked(rideId) {
  if (!rideId) return false;
  const now = Date.now();
  const last = acceptLocks.get(rideId);
  if (last && now - last < ACCEPT_LOCK_TTL_MS) {
    return true;
  }
  acceptLocks.set(rideId, now);
  setTimeout(() => {
    if (acceptLocks.get(rideId) === now) acceptLocks.delete(rideId);
  }, ACCEPT_LOCK_TTL_MS);
  return false;
}

function finalizeAcceptedRide(io, rideId, driverId, finalPrice, options = {}) {
  const { message = "User accepted the offer", rideDetails = null } = options || {};

  if (isAcceptLocked(rideId)) {
    console.log(`⚠️ accept dedupe: ride ${rideId} already processed recently`);
    return;
  }
  const activeDriverId = getActiveDriverByRide(rideId);
  if (activeDriverId && activeDriverId === driverId) {
    console.log(`ℹ️ ride ${rideId} already accepted by driver ${driverId}`);
    return;
  }
  if (activeDriverId && activeDriverId !== driverId) {
    console.log(
      `⚠️ ride ${rideId} already accepted by driver ${activeDriverId}, ignoring accept from ${driverId}`
    );
    return;
  }

  clearActiveRideByDriver(driverId);
  setActiveRide(driverId, rideId);

  const snapshot =
    rideDetails ?? (typeof getRideDetails === "function" ? getRideDetails(rideId) : null);
  const routeKm = toNumber(snapshot?.route ?? snapshot?.meta?.route ?? null);
  const duration = getRideDurationMinutes(snapshot);
  const routeApiDistanceKm = toNumber(
    snapshot?.ride_details?.route_api_distance_km ??
      snapshot?.route_api_distance_km ??
      snapshot?.meta?.route_api_distance_km ??
      null
  );
  const etaMin = duration;

  const rideDetailsPayload = snapshot
    ? (() => {
        const cleanedMeta = snapshot.meta ? { ...snapshot.meta } : null;
        const cleanedRideDetails =
          snapshot.ride_details && typeof snapshot.ride_details === "object"
            ? { ...snapshot.ride_details }
            : null;
        if (cleanedMeta) {
          delete cleanedMeta.route;
          delete cleanedMeta.eta_min;
        }
        if (cleanedRideDetails) {
          delete cleanedRideDetails.eta_min;
        }
        return {
          ...snapshot,
          ...(routeKm !== null ? { route: routeKm } : {}),
          ...(duration !== null ? { duration } : {}),
          ...(etaMin !== null ? { eta_min: etaMin } : {}),
          ...(routeApiDistanceKm !== null ? { route_api_distance_km: routeApiDistanceKm } : {}),
          ...(cleanedRideDetails || duration !== null || routeApiDistanceKm !== null
            ? {
                ride_details: {
                  ...(cleanedRideDetails ?? {}),
                  ...(duration !== null ? { duration } : {}),
                  ...(routeApiDistanceKm !== null ? { route_api_distance_km: routeApiDistanceKm } : {}),
                },
              }
            : {}),
          ...(cleanedMeta ? { meta: cleanedMeta } : {}),
        };
      })()
    : routeKm !== null || etaMin !== null || duration !== null || routeApiDistanceKm !== null
    ? {
        ...(routeKm !== null ? { route: routeKm } : {}),
        ...(duration !== null ? { duration } : {}),
        ...(etaMin !== null ? { eta_min: etaMin } : {}),
        ...(routeApiDistanceKm !== null ? { route_api_distance_km: routeApiDistanceKm } : {}),
        ...(duration !== null || routeApiDistanceKm !== null
          ? {
              ride_details: {
                ...(duration !== null ? { duration } : {}),
                ...(routeApiDistanceKm !== null ? { route_api_distance_km: routeApiDistanceKm } : {}),
              },
            }
          : {}),
      }
    : null;

  const acceptedPayload = {
    ride_id: rideId,
    driver_id: driverId,
    offered_price: finalPrice,
    message,
    at: Date.now(),
  };
  if (rideDetailsPayload) acceptedPayload.ride_details = rideDetailsPayload;

  io.to(driverRoom(driverId)).emit("ride:userAccepted", acceptedPayload);
  io.to(rideRoom(rideId)).emit("ride:userAccepted", acceptedPayload);
  io.to(rideRoom(rideId)).emit("ride:trackingStarted", {
    ride_id: rideId,
    driver_id: driverId,
    at: Date.now(),
  });
  closeRideBidding(io, rideId, { clearUser: false });

  const d = driverLocationService.getDriver(driverId);
  if (d?.lat != null && d?.long != null) {
    io.to(rideRoom(rideId)).emit("ride:locationUpdate", {
      ride_id: rideId,
      driver_id: driverId,
      lat: d.lat,
      long: d.long,
      at: Date.now(),
    });
  }

  console.log(`✅ ride accepted -> ride ${rideId} driver ${driverId} price ${finalPrice}`);
}

function cancelRideTimeout(rideId) {
  const timer = rideTimers.get(rideId);
  if (timer) {
    clearTimeout(timer);
    rideTimers.delete(rideId);
  }
}

/**
 * ✅ UPDATED: startRideTimeout accepts seconds (NOT ms)
 */
function startRideTimeout(io, rideId, durationSec = RIDE_TIMEOUT_S) {
  cancelRideTimeout(rideId);

  const sec = Math.max(0, toNumber(durationSec) ?? RIDE_TIMEOUT_S);
  const ms = sec * 1000;

  const timer = setTimeout(() => {
    console.log(`🛑 Ride ${rideId} has timed out. Removing from memory.`);
    // ✅ timeout -> remove from all inboxes + candidates
    removeRideFromAllInboxes(io, rideId);
    rideCandidates.delete(rideId);
  }, ms);

  rideTimers.set(rideId, timer);
}

/**
 * ✅ NEW: unified timer refresh (always 90s, server authoritative)
 */
function refreshRideTimer(io, rideId, options = {}) {
  const { update_snapshot = true, patch_inboxes = true } = options || {};
  if (!rideId) return null;

  const timer = makeTimer(RIDE_TIMEOUT_S); // ✅ seconds

  // restart real server timeout to match expires_at (seconds)
  startRideTimeout(io, rideId, Math.max(0, timer.expires_at - nowSec()));

  // update snapshot
  if (update_snapshot) {
    const snap = getRideDetails(rideId);
    if (snap && typeof snap === "object") {
      saveRideDetails(rideId, {
        ...snap,
        ...timer,
      });
    }
  }

  // update inbox entries
  if (patch_inboxes) {
    for (const [driverId, box] of driverRideInbox.entries()) {
      if (!box?.has?.(rideId)) continue;
      const current = box.get(rideId);
      if (!current || typeof current !== "object") continue;

      const updated = {
        ...current,
        ...timer,
        _ts: Date.now(), // ✅ keep freshness
      };
      const updatedWithCustomer = attachCustomerFields(updated, current?.user_details ?? null);

      box.set(rideId, updatedWithCustomer);
      emitDriverPatch(io, driverId, [{ op: "upsert", ride: updatedWithCustomer }]);
    }
  }

  return timer;
}

function inboxUpsert(driverId, rideId, payload) {
  if (!driverRideInbox.has(driverId)) driverRideInbox.set(driverId, new Map());
  const prepared = attachCustomerFields(payload, payload?.user_details ?? null);
  driverRideInbox.get(driverId).set(rideId, {
    ...prepared,
    ride_id: rideId,
    _ts: Date.now(), // ✅ مهم للتنظيف
  });
}

function inboxRemove(driverId, rideId) {
  const box = driverRideInbox.get(driverId);
  if (!box) return false;
  const existed = box.delete(rideId);
  if (box.size === 0) driverRideInbox.delete(driverId);
  return existed;
}

function clearDriverBidStatus(driverId, rideId) {
  const lastBid = driverLastBidStatus.get(driverId);
  if (lastBid && lastBid.rideId === rideId) {
    driverLastBidStatus.delete(driverId);
  }
}

// ✅ prune inbox entries by TTL (removes stale rides) + emit REMOVE patches
function pruneDriverInbox(io, driverId) {
  const box = driverRideInbox.get(driverId);
  if (!box) return;

  const now = Date.now();
  const removedOps = [];

  for (const [rideId, ride] of box.entries()) {
    const ts = toNumber(ride?._ts) ?? 0;
    if (ts && now - ts > INBOX_ENTRY_TTL_MS) {
      box.delete(rideId);
      clearDriverBidStatus(driverId, rideId);
      removedOps.push({ op: "remove", ride_id: rideId });
      console.log(`🧹 [inbox prune] removed old ride ${rideId} from driver ${driverId}`);
    }
  }

  if (box.size === 0) driverRideInbox.delete(driverId);

  if (removedOps.length > 0) {
    emitDriverPatch(io, driverId, removedOps);
    console.log(`🧹 [inbox prune] driver ${driverId}: removed ${removedOps.length} stale ride(s)`);
  }
}

// ✅ periodic global prune (handles idle memory leaks)
setInterval(() => {
  try {
    for (const driverId of driverRideInbox.keys()) {
      const box = driverRideInbox.get(driverId);
      if (!box) continue;
      const now = Date.now();
      for (const [rideId, ride] of box.entries()) {
        const ts = toNumber(ride?._ts) ?? 0;
        if (ts && now - ts > INBOX_ENTRY_TTL_MS) {
          box.delete(rideId);
          clearDriverBidStatus(driverId, rideId);
        }
      }
      if (box.size === 0) driverRideInbox.delete(driverId);
    }
  } catch (e) {
    console.log("⚠️ [inbox prune] interval error:", e?.message || e);
  }
}, 30 * 1000);

function inboxList(driverId, limit = 30) {
  const box = driverRideInbox.get(driverId);
  if (!box) return [];
  return Array.from(box.values())
    .sort((a, b) => (b._ts || 0) - (a._ts || 0))
    .slice(0, limit);
}

function getFullRideSnapshot(rideId, driverId = null) {
  const fromMap = getRideDetails(rideId);
  if (fromMap) return fromMap;

  if (driverId) {
    const fromDriverInbox = driverRideInbox.get(driverId)?.get(rideId);
    if (fromDriverInbox) return fromDriverInbox;
  }

  return findRideInInboxes(rideId);
}

// ✅ Snapshot ONLY (used on driver:getRidesList)
function emitDriverInbox(io, driverId, eventName = "driver:rides:list") {
  pruneDriverInbox(io, driverId);

  const list = inboxList(driverId, 30).map((ride) => {
    const normalized = normalizeRideMetrics(ride);
    const routeApiDuration = getRideRouteApiDurationRaw(normalized);
    const routeApiDistanceKm = getRideRouteApiDistanceKmRaw(normalized);

    const withRouteApi = {
      ...normalized,
      ...(routeApiDuration !== null
        ? {
            duration: routeApiDuration,
            eta_min: routeApiDuration,
            route_api_duration_min: routeApiDuration,
          }
        : {}),
      ...(routeApiDistanceKm !== null
        ? { distance: routeApiDistanceKm, route_api_distance_km: routeApiDistanceKm }
        : {}),
      ride_details: {
        ...(normalized.ride_details && typeof normalized.ride_details === "object"
          ? normalized.ride_details
          : {}),
        ...(routeApiDuration !== null ? { duration: routeApiDuration } : {}),
        ...(routeApiDistanceKm !== null ? { route_api_distance_km: routeApiDistanceKm } : {}),
      },
      meta: {
        ...(normalized.meta && typeof normalized.meta === "object" ? normalized.meta : {}),
        ...(routeApiDuration !== null
          ? {
              duration: routeApiDuration,
              eta_min: routeApiDuration,
              route_api_duration_min: routeApiDuration,
            }
          : {}),
        ...(routeApiDistanceKm !== null ? { route_api_distance_km: routeApiDistanceKm } : {}),
      },
    };

    return sanitizeRidePayloadForClient(withRouteApi);
  });
  console.log("[driver:rides:list] payload", {
    driver_id: driverId,
    rides: list.map((ride) => ({
      ride_id: ride?.ride_id ?? null,
      duration: getRideRouteApiDurationRaw(ride) ?? getRideDurationRaw(ride),
      route_api_distance_km: getRideRouteApiDistanceKmRaw(ride) ?? getRideDistanceKm(ride),
    })),
  });
  io.to(driverRoom(driverId)).emit(eventName, {
    driver_id: driverId,
    rides: list,
    total: list.length,
    at: Date.now(),
  });
}

function updateRideUserDetailsInInbox(io, rideId, userDetails) {
  if (!rideId || !userDetails) return;

  const next = {
    user_id: userDetails.user_id ?? null,
    user_name: userDetails.user_name ?? null,
    user_gender: userDetails.user_gender ?? null,
    user_image: userDetails.user_image ?? null,

    // (اختياري) لو بدك كمان ينعكس بسرعة بالـ patch
    user_phone: userDetails.user_phone ?? null,
    user_country_code: userDetails.user_country_code ?? null,
    user_phone_full: userDetails.user_phone_full ?? null,
    user_token: userDetails.user_token ?? userDetails.token ?? null,
  };

  for (const [driverId, box] of driverRideInbox.entries()) {
    const ride = box.get(rideId);
    if (!ride) continue;

    const updated = attachCustomerFields(
      {
        ...ride,
        user_id: next.user_id ?? ride.user_id ?? null,
        user_name: next.user_name ?? ride.user_name ?? null,
        user_gender: next.user_gender ?? ride.user_gender ?? null,
        user_image: next.user_image ?? ride.user_image ?? null,

        user_phone: next.user_phone ?? ride.user_phone ?? null,
        user_country_code: next.user_country_code ?? ride.user_country_code ?? null,
        user_phone_full: next.user_phone_full ?? ride.user_phone_full ?? null,

        token: next.user_token ?? ride.token ?? null,
        user_details: {
          ...(ride.user_details && typeof ride.user_details === "object" ? ride.user_details : {}),
          ...userDetails,
          user_token: next.user_token ?? userDetails?.user_token ?? userDetails?.token ?? null,
          token: next.user_token ?? userDetails?.user_token ?? userDetails?.token ?? null,
        },

        _ts: Date.now(),
      },
      userDetails
    );

    box.set(rideId, updated);

    emitDriverPatch(io, driverId, [{ op: "upsert", ride: updated }]);
  }
}

function refreshUserDetailsForUserId(io, userId, userDetails) {
  if (!userId || !userDetails) return;

  for (const [driverId, box] of driverRideInbox.entries()) {
    const ops = [];
    for (const [rideId, ride] of box.entries()) {
      if (ride?.user_id !== userId) continue;

      const updated = attachCustomerFields(
        {
          ...ride,
          user_id: userDetails.user_id ?? ride.user_id ?? null,
          user_name: userDetails.user_name ?? ride.user_name ?? null,
          user_gender: userDetails.user_gender ?? ride.user_gender ?? null,
          user_image: userDetails.user_image ?? ride.user_image ?? null,

          user_phone: userDetails.user_phone ?? ride.user_phone ?? null,
          user_country_code: userDetails.user_country_code ?? ride.user_country_code ?? null,
          user_phone_full: userDetails.user_phone_full ?? ride.user_phone_full ?? null,

          token: userDetails.user_token ?? userDetails.token ?? ride.token ?? null,
          user_details: {
            ...(ride.user_details && typeof ride.user_details === "object" ? ride.user_details : {}),
            ...userDetails,
            user_token: userDetails.user_token ?? userDetails.token ?? null,
            token: userDetails.user_token ?? userDetails.token ?? null,
          },

          _ts: Date.now(),
        },
        userDetails
      );
      box.set(rideId, updated);
      ops.push({ op: "upsert", ride: updated });
    }
    if (ops.length > 0) emitDriverPatch(io, driverId, ops);
  }
}

function findRideInInboxes(rideId) {
  for (const [, box] of driverRideInbox.entries()) {
    const ride = box.get(rideId);
    if (ride) return ride;
  }
  return null;
}

function getRideSnapshotForRedispatch(rideId) {
  return getRideDetails(rideId) || findRideInInboxes(rideId);
}

// ✅ remove ride from all drivers inboxes (safe global scan) + PATCH remove
function removeRideFromAllInboxes(io, rideId) {
  cancelRideTimeout(rideId);
  clearUserRideByRideId(rideId);
  rideDetailsMap.delete(rideId);
  acceptLocks.delete(rideId);

  for (const [driverId, box] of driverRideInbox.entries()) {
    if (box.has(rideId)) {
      box.delete(rideId);
      clearDriverBidStatus(driverId, rideId);

      if (box.size === 0) driverRideInbox.delete(driverId);

      emitDriverPatch(io, driverId, [{ op: "remove", ride_id: rideId }]);
    }
  }

  rideCandidates.delete(rideId);
}

// ✅ Close bidding for a ride (remove from all inboxes) + PATCH remove
function closeRideBidding(io, rideId, opts = {}) {
  cancelRideTimeout(rideId);

  const clearUser = opts.clearUser !== false;
  if (clearUser) clearUserRideByRideId(rideId);
  rideDetailsMap.delete(rideId);
  acceptLocks.delete(rideId);

  for (const [driverId, box] of driverRideInbox.entries()) {
    if (box.has(rideId)) {
      box.delete(rideId);
      clearDriverBidStatus(driverId, rideId);
      if (box.size === 0) driverRideInbox.delete(driverId);

      emitDriverPatch(io, driverId, [{ op: "remove", ride_id: rideId }]);
    }
  }

  rideCandidates.delete(rideId);
}

// ─────────────────────────────
// Dispatch
// ─────────────────────────────
async function dispatchToNearbyDrivers(io, data) {
  const rideId = toNumber(data?.ride_id ?? data?.id);
  if (!rideId) return false;

  // ✅ fallback: if dispatch payload missing user info, try snapshot memory by rideId
  const snap0 = getRideDetails(rideId);
  if (snap0 && typeof snap0 === "object") {
    data = {
      ...snap0, // has user_id/token if previously saved
      ...data, // incoming overrides snapshot
      user_details: data?.user_details ?? snap0?.user_details ?? null,
    };
  }

  const activeDriverId = getActiveDriverByRide(rideId);
  if (activeDriverId) {
    console.log(`🛑 Dispatch blocked: ride ${rideId} already accepted by driver ${activeDriverId}`);
    return false;
  }

  if (cancelledRides.has(rideId)) {
    console.log(`🛑 Dispatch blocked: ride ${rideId} is cancelled`);
    return false;
  }

  const rawRadius = toNumber(data?.radius) ?? ROAD_RADIUS_METERS;
  const roadRadius = Math.max(
    MIN_DISPATCH_RADIUS_METERS,
    Math.min(rawRadius, MAX_DISPATCH_RADIUS_METERS)
  );

  const airCandidateRadius = Math.max(roadRadius, AIR_CANDIDATE_RADIUS_METERS);

  if (rawRadius !== roadRadius) {
    console.log(`[dispatch] road radius clamped for ride ${rideId}: ${rawRadius} -> ${roadRadius}`);
  }

  const lat = toNumber(data?.pickup_lat);
  const long = toNumber(data?.pickup_long);

  const serviceTypeId = toNumber(data?.service_type_id) ?? null;
  const base = toNumber(data?.user_bid_price);
  const min = toNumber(data?.min_fare_amount);
  const priceBounds = getRidePriceBounds(data);

  // ✅ build/merge user details from payload + store + token (retry-safe)
  const built = buildUserDetails(data);
  const tokenTmp = getTokenFromAny(data, built, data?.user_details ?? null);

  const userIdTmp = toNumber(
    data?.user_id ?? data?.customer_id ?? data?.passenger_id ?? built?.user_id
  );
  const fromStore = userIdTmp ? getUserDetails(userIdTmp) : null;
  const fromToken = !fromStore && tokenTmp ? getUserDetailsByToken(tokenTmp) : null;

  let userDetails = built ?? fromStore ?? fromToken ?? null;

  // ✅ extra fallback: if still null, try snapshot user_details
  if (!userDetails) {
    const snap = getRideDetails(rideId);
    const snapToken = snap?.token ?? snap?.user_details?.user_token ?? snap?.user_details?.token ?? null;
    const snapUserId = toNumber(snap?.user_id ?? snap?.user_details?.user_id ?? null);

    userDetails =
      (snapUserId ? getUserDetails(snapUserId) : null) ??
      (snapToken ? getUserDetailsByToken(snapToken) : null) ??
      snap?.user_details ??
      null;
  }
  const userId = getUserIdFromDispatch(data, userDetails);

  const isPriceUpdated = !!data?.isPriceUpdated;
  const updatedPrice = toNumber(data?.updatedPrice ?? null);
  const updatedAt = toNumber(data?.updatedAt ?? null);

  const storedUser = userId ? getUserDetails(userId) : null;
  const storedByToken = !storedUser && tokenTmp ? getUserDetailsByToken(tokenTmp) : null;

  const routeKm = toNumber(
    data?.route ??
      data?.route_km ??
      data?.routeKm ??
      data?.meta?.route ??
      storedUser?.route ??
      storedByToken?.route ??
      null
  );
  const etaMin = toNumber(
    data?.eta_min ??
      data?.etaMin ??
      data?.meta?.eta_min ??
      storedUser?.eta_min ??
      storedByToken?.eta_min ??
      null
  );

  // ✅ optional filter requirements (apply only if provided)
  const requiredGender = toNumber(
    data?.required_driver_gender ?? data?.required_gender ?? data?.driver_gender ?? null
  );
  const needChildSeat = toNumber(
    data?.need_child_seat ??
      data?.child_seat ??
      data?.require_child_seat ??
      data?.smoking ??
      null
  );
  const needHandicap = toNumber(
    data?.need_handicap ?? data?.handicap ?? data?.require_handicap ?? null
  );

  if (lat === null || long === null) {
    console.log("[dispatch] Invalid dispatch data:", {
      ride_id: rideId,
      pickup_lat: data?.pickup_lat,
      pickup_long: data?.pickup_long,
    });
    return false;
  }
  const destinationLat = toNumber(data?.destination_lat);
  const destinationLong = toNumber(data?.destination_long);

  let routeApiData = null;
  let routeApiDurationMin = pickFirstValue(
    data?.duration,
    data?.route_api_duration_min,
    data?.meta?.duration,
    data?.meta?.route_api_duration_min,
    data?.eta_min,
    data?.meta?.eta_min
  );
  let routeApiDistanceKm = pickFirstValue(
    data?.route_api_distance_km,
    data?.distance,
    data?.meta?.route_api_distance_km
  );

  const hasFrontendRouteValues = routeApiDurationMin !== null || routeApiDistanceKm !== null;
  const preferFrontendRouteMetrics =
    data?.route_metrics_source === "frontend" ||
    data?.route_data_source === "frontend" ||
    toNumber(data?.prefer_frontend_route_metrics) === 1;
  const shouldFetchRouteApi =
    destinationLat !== null &&
    destinationLong !== null &&
    (!preferFrontendRouteMetrics || routeApiDurationMin === null || routeApiDistanceKm === null);

  // Fetch route API when needed; keep frontend metrics if explicitly preferred.
  if (shouldFetchRouteApi) {
    routeApiData = await fetchRouteDataByCoords({
      startLongitude: long,
      startLatitude: lat,
      endLongitude: destinationLong,
      endLatitude: destinationLat,
      requested_at: new Date().toISOString(),
    });

    const fetchedRouteApiDurationMin = toRouteMetricNumber(routeApiData?.duration);
    const fetchedRouteApiDistanceKm =
      toRouteMetricNumber(routeApiData?.route) ??
      toRouteMetricNumber(routeApiData?.distance_km) ??
      toRouteMetricNumber(routeApiData?.total_distance) ??
      null;

    if (
      fetchedRouteApiDurationMin !== null &&
      (!preferFrontendRouteMetrics || routeApiDurationMin === null)
    ) {
      routeApiDurationMin = fetchedRouteApiDurationMin;
    }
    if (
      fetchedRouteApiDistanceKm !== null &&
      (!preferFrontendRouteMetrics || routeApiDistanceKm === null)
    ) {
      routeApiDistanceKm = fetchedRouteApiDistanceKm;
    }

    console.log("[dispatch][routeApi]", {
      ride_id: rideId,
      source: preferFrontendRouteMetrics ? "frontend-preferred" : "api",
      route_api_duration_min: routeApiDurationMin,
      route_api_distance_km: routeApiDistanceKm,
    });
  } else if (destinationLat !== null && destinationLong !== null && hasFrontendRouteValues) {
    console.log("[dispatch][routeApi] using frontend values", {
      ride_id: rideId,
      route_api_duration_min: routeApiDurationMin,
      route_api_distance_km: routeApiDistanceKm,
    });
  } else if (hasFrontendRouteValues) {
    console.log("[dispatch][routeApi] fallback to existing values (missing destination)", {
      ride_id: rideId,
      route_api_duration_min: routeApiDurationMin,
      route_api_distance_km: routeApiDistanceKm,
    });
  }
  const finalRouteApiDurationMin =
    routeApiDurationMin !== null ? routeApiDurationMin : null;
  const finalRouteApiDistanceKm =
    routeApiDistanceKm !== null ? routeApiDistanceKm : null;
  const finalEtaMin = finalRouteApiDurationMin !== null ? finalRouteApiDurationMin : etaMin;

  const nearbyAir = driverLocationService.getNearbyDriversFromMemory(lat, long, airCandidateRadius, {
    only_online: true,
    service_type_id: serviceTypeId,
    max_age_ms: MAX_DRIVER_LOCATION_AGE_MS,
    required_gender: requiredGender,
    need_child_seat: needChildSeat,
    need_handicap: needHandicap,
  });

  const availableAir = nearbyAir.filter((d) => {
    const dId = toNumber(d?.driver_id);
    if (!dId) return false;
    const activeRide = getActiveRideByDriver(dId);
    return !activeRide;
  });

  const roadFiltered = await filterDriversByRoadRadius(
    availableAir,
    lat,
    long,
    roadRadius
  );

  const candidates =
    MAX_DISPATCH_CANDIDATES > 0
      ? roadFiltered.slice(0, MAX_DISPATCH_CANDIDATES)
      : roadFiltered;
  console.log("[dispatch][dispatchToNearbyDrivers]", {
    ride_id: rideId,
    road_radius_m: roadRadius,
    air_candidate_radius_m: airCandidateRadius,
    service_type_id: serviceTypeId ?? null,
    nearby_air: nearbyAir.length,
    available_air: availableAir.length,
    road_filtered: roadFiltered.length,
    candidates: candidates.length,
    required_gender: requiredGender ?? null,
    need_child_seat: needChildSeat ?? null,
    need_handicap: needHandicap ?? null,
    has_user_details: !!userDetails,
    token_present: !!tokenTmp,
  });

  // ✅ TIMER (SERVER ONLY): ALWAYS CREATE 90s TIMER (NO incoming server_time/expires_at)
  const timer = makeTimer(RIDE_TIMEOUT_S);

  // ✅ start server timeout (seconds)
  startRideTimeout(io, rideId, RIDE_TIMEOUT_S);

  rideCandidates.set(rideId, new Set(candidates.map((d) => d.driver_id)));
  if (userId) setUserActiveRide(userId, rideId);

  const baseMeta =
    data?.meta && typeof data.meta === "object" && !Array.isArray(data.meta) ? data.meta : {};

  const ridePayloadBase = {
    ride_id: rideId,

    // ✅ FLAT user fields (important for retry + merges)
    user_id: userId ?? userDetails?.user_id ?? null,
    user_name: userDetails?.user_name ?? null,
    user_gender: userDetails?.user_gender ?? null,
    user_image: userDetails?.user_image ?? null,
    user_phone: userDetails?.user_phone ?? null,
    user_country_code: userDetails?.user_country_code ?? null,
    user_phone_full: userDetails?.user_phone_full ?? null,

    // ✅ keep token for later accept/merge paths
    token: tokenTmp ?? null,

    pickup_lat: lat,
    pickup_long: long,
    pickup_address: data.pickup_address ?? null,

    destination_lat: toNumber(data.destination_lat),
    destination_long: toNumber(data.destination_long),
    destination_address: data.destination_address ?? null,

    radius: roadRadius,
    user_bid_price: base,
    min_fare_amount: min,
    base_fare: priceBounds.base_fare,
    min_price: priceBounds.min_price,
    max_price: priceBounds.max_price,

    service_type_id: toNumber(data.service_type_id) ?? null,
    service_category_id: toNumber(data.service_category_id) ?? null,
    created_at: data.created_at ?? null,

    ...(routeKm !== null ? { route: routeKm } : {}),
    ...(finalEtaMin !== null ? { eta_min: finalEtaMin } : {}),
    duration: finalRouteApiDurationMin,
    route_api_distance_km: finalRouteApiDistanceKm,
    ride_details: {
      duration: finalRouteApiDurationMin,
      route_api_distance_km: finalRouteApiDistanceKm,
    },

    meta: {
      ...baseMeta,
      ...(routeKm !== null ? { route: routeKm } : {}),
      ...(finalEtaMin !== null ? { eta_min: finalEtaMin } : {}),
      duration: finalRouteApiDurationMin,
      route_api_distance_km: finalRouteApiDistanceKm,
      ...(priceBounds.base_fare !== null ? { base_fare: priceBounds.base_fare } : {}),
      ...(priceBounds.min_price !== null ? { min_price: priceBounds.min_price } : {}),
      ...(priceBounds.max_price !== null ? { max_price: priceBounds.max_price } : {}),
      ...(routeApiData && typeof routeApiData === "object"
        ? { route_api_data: routeApiData }
        : {}),
    },

    // ✅ full object too
    user_details: userDetails
      ? {
          ...userDetails,
          user_token: userDetails?.user_token ?? userDetails?.token ?? tokenTmp ?? null,
          token: userDetails?.user_token ?? userDetails?.token ?? tokenTmp ?? null,
        }
      : null,

    ...(isPriceUpdated ? { isPriceUpdated: true } : {}),
    ...(updatedPrice !== null ? { updatedPrice } : {}),
    ...(updatedAt !== null ? { updatedAt } : {}),

    // ✅ timer fields (SECONDS)
    ...timer,
  };
  const ridePayload = attachCustomerFields(ridePayloadBase, ridePayloadBase.user_details ?? userDetails);

  // ✅ keep a snapshot for future re-dispatch (retry-safe + includes flat user fields + token)
  saveRideDetails(
    rideId,
    attachCustomerFields(
      {
        ride_id: rideId,

        pickup_lat: lat,
        pickup_long: long,
        pickup_address: data.pickup_address ?? null,

        destination_lat: toNumber(data.destination_lat),
        destination_long: toNumber(data.destination_long),
        destination_address: data.destination_address ?? null,

        radius: roadRadius,
        user_bid_price: base,
        min_fare_amount: min,
        base_fare: priceBounds.base_fare,
        min_price: priceBounds.min_price,
        max_price: priceBounds.max_price,
        service_type_id: serviceTypeId,
        service_category_id: toNumber(data.service_category_id) ?? null,
        created_at: data.created_at ?? null,

        ...(routeKm !== null ? { route: routeKm } : {}),
        ...(finalEtaMin !== null ? { eta_min: finalEtaMin } : {}),
        duration: finalRouteApiDurationMin,
        route_api_distance_km: finalRouteApiDistanceKm,
        ride_details: {
          duration: finalRouteApiDurationMin,
          route_api_distance_km: finalRouteApiDistanceKm,
        },

        meta: {
          ...baseMeta,
          ...(routeKm !== null ? { route: routeKm } : {}),
          ...(finalEtaMin !== null ? { eta_min: finalEtaMin } : {}),
          duration: finalRouteApiDurationMin,
          route_api_distance_km: finalRouteApiDistanceKm,
          ...(priceBounds.base_fare !== null ? { base_fare: priceBounds.base_fare } : {}),
          ...(priceBounds.min_price !== null ? { min_price: priceBounds.min_price } : {}),
          ...(priceBounds.max_price !== null ? { max_price: priceBounds.max_price } : {}),
          ...(routeApiData && typeof routeApiData === "object"
            ? { route_api_data: routeApiData }
            : {}),
        },

        // ✅ user flat fields
        user_id: userId ?? userDetails?.user_id ?? null,
        user_name: userDetails?.user_name ?? null,
        user_gender: userDetails?.user_gender ?? null,
        user_image: userDetails?.user_image ?? null,
        user_phone: userDetails?.user_phone ?? null,
        user_country_code: userDetails?.user_country_code ?? null,
        user_phone_full: userDetails?.user_phone_full ?? null,

        token: tokenTmp ?? null,
        user_details: userDetails
          ? {
              ...userDetails,
              user_token: userDetails?.user_token ?? userDetails?.token ?? tokenTmp ?? null,
              token: userDetails?.user_token ?? userDetails?.token ?? tokenTmp ?? null,
            }
          : null,

        ...(isPriceUpdated ? { isPriceUpdated: true } : {}),
        ...(updatedPrice !== null ? { updatedPrice } : {}),
        ...(updatedAt !== null ? { updatedAt } : {}),

        // ✅ timer fields (SECONDS)
        ...timer,
      },
      userDetails
    )
  );

  // Keep bidRequest user fields stable even when one source is partially missing.
  const bidReqUserId =
    toNumber(ridePayload?.user_id) ??
    toNumber(data?.user_id) ??
    toNumber(userDetails?.user_id) ??
    null;
  const bidReqStoredUser = bidReqUserId ? getUserDetails(bidReqUserId) : null;
  const bidReqStoredByToken =
    !bidReqStoredUser && tokenTmp ? getUserDetailsByToken(tokenTmp) : null;

  const bidReqUserName =
    ridePayload?.user_name ??
    data?.user_name ??
    userDetails?.user_name ??
    bidReqStoredUser?.user_name ??
    bidReqStoredByToken?.user_name ??
    null;
  const bidReqUserGender =
    ridePayload?.user_gender ??
    data?.user_gender ??
    userDetails?.user_gender ??
    bidReqStoredUser?.user_gender ??
    bidReqStoredByToken?.user_gender ??
    null;
  const bidReqUserImage =
    ridePayload?.user_image ??
    data?.user_image ??
    userDetails?.user_image ??
    bidReqStoredUser?.user_image ??
    bidReqStoredByToken?.user_image ??
    null;
  const bidReqUserPhone =
    ridePayload?.user_phone ??
    data?.user_phone ??
    userDetails?.user_phone ??
    bidReqStoredUser?.user_phone ??
    bidReqStoredByToken?.user_phone ??
    null;
  const bidReqUserCountryCode =
    ridePayload?.user_country_code ??
    data?.user_country_code ??
    userDetails?.user_country_code ??
    bidReqStoredUser?.user_country_code ??
    bidReqStoredByToken?.user_country_code ??
    null;
  const bidReqUserPhoneFull =
    ridePayload?.user_phone_full ??
    data?.user_phone_full ??
    userDetails?.user_phone_full ??
    bidReqStoredUser?.user_phone_full ??
    bidReqStoredByToken?.user_phone_full ??
    (bidReqUserCountryCode && bidReqUserPhone
      ? `${bidReqUserCountryCode}${bidReqUserPhone}`
      : null);

  candidates.forEach((d) => {
    const room = driverRoom(d.driver_id);
    const bidRequestPayload = sanitizeRidePayloadForClient({
      ride_id: rideId,

      pickup_lat: lat,
      pickup_long: long,
      pickup_address: data.pickup_address ?? null,

      destination_lat: toNumber(data.destination_lat),
      destination_long: toNumber(data.destination_long),
      destination_address: data.destination_address ?? null,

      radius: roadRadius,
      user_bid_price: base,
      min_fare_amount: min,
      base_fare: priceBounds.base_fare,
      min_price: priceBounds.min_price,
      max_price: priceBounds.max_price,

      user_id: bidReqUserId,
      user_name: bidReqUserName,
      user_gender: bidReqUserGender,
      user_image: bidReqUserImage,
      user_phone: bidReqUserPhone,
      user_country_code: bidReqUserCountryCode,
      user_phone_full: bidReqUserPhoneFull,

      token: tokenTmp ?? null,

      ...(d.driver_to_pickup_distance_m != null
        ? { driver_to_pickup_distance_m: d.driver_to_pickup_distance_m }
        : {}),
      ...(d.driver_to_pickup_distance_km != null
        ? { driver_to_pickup_distance_km: d.driver_to_pickup_distance_km }
        : {}),
      ...(d.driver_to_pickup_duration_s != null
        ? { driver_to_pickup_duration_s: d.driver_to_pickup_duration_s }
        : {}),
      ...(d.driver_to_pickup_duration_min != null
        ? {
            driver_to_pickup_duration_min: d.driver_to_pickup_duration_min,
            estimated_arrival_min: d.driver_to_pickup_duration_min,
          }
        : {}),

      ...(finalEtaMin !== null ? { eta_min: finalEtaMin } : {}),
      duration: finalRouteApiDurationMin,
      route_api_distance_km: finalRouteApiDistanceKm,
      ride_details: {
        duration: finalRouteApiDurationMin,
        route_api_distance_km: finalRouteApiDistanceKm,
      },

      ...(isPriceUpdated ? { isPriceUpdated: true } : {}),
      ...(updatedPrice !== null ? { updatedPrice } : {}),
      ...(updatedAt !== null ? { updatedAt } : {}),

      ...timer,
    });
    console.log("[ride:bidRequest] payload", {
      driver_id: d.driver_id,
      ride_id: bidRequestPayload?.ride_id ?? null,
      duration:
        bidRequestPayload?.ride_details?.duration ?? bidRequestPayload?.duration ?? null,
      route_api_distance_km: bidRequestPayload?.route_api_distance_km ?? null,
    });

    io.to(room).emit("ride:bidRequest", bidRequestPayload);

    const ridePayloadForDriver = attachCustomerFields(
      {
        ...ridePayload,
        radius: roadRadius,

        ...(d.driver_to_pickup_distance_m != null
          ? { driver_to_pickup_distance_m: d.driver_to_pickup_distance_m }
          : {}),
        ...(d.driver_to_pickup_distance_km != null
          ? { driver_to_pickup_distance_km: d.driver_to_pickup_distance_km }
          : {}),
        ...(d.driver_to_pickup_duration_s != null
          ? { driver_to_pickup_duration_s: d.driver_to_pickup_duration_s }
          : {}),
        ...(d.driver_to_pickup_duration_min != null
          ? {
              driver_to_pickup_duration_min: d.driver_to_pickup_duration_min,
              estimated_arrival_min: d.driver_to_pickup_duration_min,
            }
          : {}),

        meta: {
          ...(ridePayload?.meta && typeof ridePayload.meta === "object" ? ridePayload.meta : {}),
          ...(d.driver_to_pickup_distance_m != null
            ? { driver_to_pickup_distance_m: d.driver_to_pickup_distance_m }
            : {}),
          ...(d.driver_to_pickup_distance_km != null
            ? { driver_to_pickup_distance_km: d.driver_to_pickup_distance_km }
            : {}),
          ...(d.driver_to_pickup_duration_s != null
            ? { driver_to_pickup_duration_s: d.driver_to_pickup_duration_s }
            : {}),
          ...(d.driver_to_pickup_duration_min != null
            ? {
                driver_to_pickup_duration_min: d.driver_to_pickup_duration_min,
                estimated_arrival_min: d.driver_to_pickup_duration_min,
              }
            : {}),
        },
      },
      ridePayload?.user_details ?? userDetails ?? null
    );

    inboxUpsert(d.driver_id, rideId, ridePayloadForDriver);
    emitDriverPatch(io, d.driver_id, [{ op: "upsert", ride: ridePayloadForDriver }]);

    console.log(`   -> Sent bidRequest + patch(upsert) to driver ${d.driver_id} (room:${room})`);
  });

  if (candidates.length > 0) {
    console.log(`✅ Finished dispatching ride ${rideId} — notified ${candidates.length} driver(s)`);
  }
  emitRideCandidatesSummary(io, rideId);

  return true;
}

const isCandidateDriver = (rideId, driverId) => {
  const set = rideCandidates.get(rideId);
  if (!set) {
    const inInbox = driverRideInbox.get(driverId)?.has(rideId);
    if (!inInbox) {
      console.log(`🚫 Driver ${driverId} tried to bid on ride ${rideId} but ride is not tracked`);
      return false;
    }
    return true;
  }
  const ok = set.has(driverId);
  if (!ok) {
    console.log(`🚫 Driver ${driverId} tried to bid on ride ${rideId} but is not in candidates`);
  }
  return ok;
};

// ─────────────────────────────
// Socket handler
// ─────────────────────────────

async function fetchRouteAndEmit(io, rideId, driverId, rideSnapshot = null) {
  if (!rideId || !driverId) return null;

  const snapshot = rideSnapshot ?? getFullRideSnapshot(rideId, driverId) ?? null;
  const driverLive = driverLocationService.getDriver(driverId) ?? null;
  const startLongitude = toNumber(driverLive?.long);
  const startLatitude = toNumber(driverLive?.lat);
  const endLongitude = toNumber(snapshot?.pickup_long);
  const endLatitude = toNumber(snapshot?.pickup_lat);
  const requested_at = new Date().toISOString();

  if (
    startLongitude === null ||
    startLatitude === null ||
    endLongitude === null ||
    endLatitude === null
  ) {
    const errorPayload = {
      ride_id: rideId,
      driver_id: driverId,
      status: 0,
      message: "Missing coordinates for route API",
      params: {
        startLongitude,
        startLatitude,
        endLongitude,
        endLatitude,
        requested_at,
      },
      at: Date.now(),
    };

    io.to(rideRoom(rideId)).emit("ride:routeDataError", errorPayload);
    io.to(driverRoom(driverId)).emit("ride:routeDataError", errorPayload);

    console.log("[ride:routeDataError] missing coordinates", errorPayload);
    return null;
  }

  try {
    const response = await axios.get(
      `${LARAVEL_BASE_URL}${LARAVEL_GET_ROUTE_PATH}`,
      {
        params: {
          startLongitude,
          startLatitude,
          endLongitude,
          endLatitude,
          requested_at,
        },
        timeout: 10000,
      }
    );

    const routeApiData = response?.data ?? null;
    const normalizedDuration = normalizeDuration(routeApiData?.duration);

    if (normalizedDuration === null) {
      const errorPayload = {
        ride_id: rideId,
        driver_id: driverId,
        status: 0,
        message: "Route API returned invalid duration",
        api_duration: routeApiData?.duration ?? null,
        at: Date.now(),
      };

      io.to(rideRoom(rideId)).emit("ride:routeDataError", errorPayload);
      io.to(driverRoom(driverId)).emit("ride:routeDataError", errorPayload);

      console.error("[ride:routeDataError] invalid API duration:", errorPayload);
      return routeApiData;
    }

    const successPayload = {
      ride_id: rideId,
      driver_id: driverId,
      status: 1,
      duration: normalizedDuration,
      at: Date.now(),
    };
    // 🔎 DEBUG
    console.log("[ride:routeData] api raw duration =", routeApiData?.duration);
    console.log("[ride:routeData] success payload =", successPayload);
    console.log("[ride:routeData][source=getRoute-api]", {
      ride_id: rideId,
      driver_id: driverId,
      duration: successPayload.duration,
    });

    io.to(rideRoom(rideId)).emit("ride:routeData", successPayload);
    io.to(driverRoom(driverId)).emit("ride:routeData", successPayload);

    console.log("[ride:routeData] emitted successfully", successPayload);

    return routeApiData;
  } catch (error) {
    const errorPayload = {
      ride_id: rideId,
      driver_id: driverId,
      status: 0,
      message: "Route API request failed",
      error: error?.response?.data || error?.message || "Unknown error",
      at: Date.now(),
    };

    io.to(rideRoom(rideId)).emit("ride:routeDataError", errorPayload);
    io.to(driverRoom(driverId)).emit("ride:routeDataError", errorPayload);

    console.error("[ride:routeDataError] request failed:", error?.response?.data || error?.message);
    return null;
  }
}

async function fetchRouteDataByCoords({
  startLongitude,
  startLatitude,
  endLongitude,
  endLatitude,
  requested_at = null,
}) {
  const safeStartLongitude = toNumber(startLongitude);
  const safeStartLatitude = toNumber(startLatitude);
  const safeEndLongitude = toNumber(endLongitude);
  const safeEndLatitude = toNumber(endLatitude);
  const safeRequestedAt = requested_at || new Date().toISOString();

  if (
    safeStartLongitude === null ||
    safeStartLatitude === null ||
    safeEndLongitude === null ||
    safeEndLatitude === null
  ) {
    return null;
  }

  try {
    const response = await axios.get(`${LARAVEL_BASE_URL}${LARAVEL_GET_ROUTE_PATH}`, {
      params: {
        startLongitude: safeStartLongitude,
        startLatitude: safeStartLatitude,
        endLongitude: safeEndLongitude,
        endLatitude: safeEndLatitude,
        requested_at: safeRequestedAt,
      },
      timeout: 10000,
    });

    return response?.data ?? null;
  } catch (error) {
    console.error(
      "[fetchRouteDataByCoords] failed:",
      error?.response?.data || error?.message || error
    );
    return null;
  }
}
const extractRoadDistanceMeters = (payload) => {
  if (!payload || typeof payload !== "object") return null;

  const directM = toNumber(
    payload?.distance_m ??
      payload?.distanceMeters ??
      payload?.distance_meters ??
      null
  );
  if (directM !== null) return directM;

  const km = toRouteMetricNumber(
    payload?.route ??
      payload?.distance_km ??
      payload?.total_distance ??
      null
  );
  if (km !== null) return Math.round(km * 1000);

  const nestedM = toNumber(
    payload?.routes?.[0]?.distanceMeters ??
      payload?.routes?.[0]?.legs?.[0]?.distance?.value ??
      null
  );
  if (nestedM !== null) return nestedM;

  return null;
};

async function fetchDriverToPickupRoadMetrics(driverLat, driverLong, pickupLat, pickupLong) {
  const la1 = toNumber(driverLat);
  const lo1 = toNumber(driverLong);
  const la2 = toNumber(pickupLat);
  const lo2 = toNumber(pickupLong);
  const airDistanceM =
    la1 !== null && lo1 !== null && la2 !== null && lo2 !== null
      ? Math.round(getDistanceMeters(la1, lo1, la2, lo2))
      : null;
  const airDurationS =
    airDistanceM !== null
      ? Math.max(60, Math.round((airDistanceM * 3.6) / DRIVER_TO_PICKUP_SPEED_KMPH))
      : null;

  if (la1 === null || lo1 === null || la2 === null || lo2 === null) {
    return {
      road_distance_m: airDistanceM,
      road_duration_s: airDurationS,
      road_duration_min: airDurationS !== null ? round2(airDurationS / 60) : null,
      raw: null,
      source: airDistanceM !== null ? "air-fallback-missing-coords" : "missing-coords",
    };
  }

  try {
    const res = await axios.get(`${LARAVEL_BASE_URL}${LARAVEL_GET_ROUTE_PATH}`, {
      params: {
        startLongitude: lo1,
        startLatitude: la1,
        endLongitude: lo2,
        endLatitude: la2,
        requested_at: new Date().toISOString(),
      },
      timeout: 10000,
    });

    const data = res?.data ?? null;
    const roadDistanceM = extractRoadDistanceMeters(data);
    const durationMin = toRouteMetricNumber(data?.duration ?? null);
    const durationS = durationMin !== null ? Math.round(durationMin * 60) : null;

    return {
      road_distance_m: roadDistanceM,
      road_duration_s: durationS,
      road_duration_min: durationMin !== null ? round2(durationMin) : null,
      raw: data,
      source: roadDistanceM !== null ? "road-api" : "road-api-empty",
    };
  } catch (error) {
    console.error(
      "[dispatch][driverToPickupRoadMetrics] failed; using air fallback:",
      {
        error: error?.response?.data || error?.message || error,
        air_distance_m: airDistanceM,
      }
    );
    return {
      road_distance_m: airDistanceM,
      road_duration_s: airDurationS,
      road_duration_min: airDurationS !== null ? round2(airDurationS / 60) : null,
      raw: null,
      source: airDistanceM !== null ? "air-fallback-error" : "error",
    };
  }
}

async function filterDriversByRoadRadius(drivers, pickupLat, pickupLong, roadRadiusM) {
  const list = Array.isArray(drivers) ? drivers.slice(0, MAX_ROAD_FILTER_CANDIDATES) : [];

  const results = await Promise.all(
    list.map(async (d) => {
      const driverId = toNumber(d?.driver_id);
      const driverLat = toNumber(d?.lat);
      const driverLong = toNumber(d?.long);

      if (!driverId || driverLat === null || driverLong === null) return null;

      const metrics = await fetchDriverToPickupRoadMetrics(
        driverLat,
        driverLong,
        pickupLat,
        pickupLong
      );

      if (metrics.road_distance_m === null) return null;
      if (metrics.road_distance_m > roadRadiusM) return null;

      return {
        ...d,
        driver_to_pickup_distance_m: metrics.road_distance_m,
        driver_to_pickup_distance_km: round2(metrics.road_distance_m / 1000),
        driver_to_pickup_duration_s: metrics.road_duration_s,
        driver_to_pickup_duration_min: metrics.road_duration_min,
      };
    })
  );

  return results.filter(Boolean);
}

const getAcceptedDriverToPickupMinutes = (payload = {}, rideSnapshot = null) => {
  return (
    pickFirstValue(
      payload?.driver_to_pickup_duration_min,
      payload?.driverToPickupDurationMin,
      payload?.duration,
      payload?.ride_details?.duration,
      payload?.meta?.duration
    ) ?? getRideDurationRaw(rideSnapshot) ?? null
  );
};

const getAcceptedRouteKm = (payload = {}, rideSnapshot = null) => {
  const routeCandidate = pickFirstValue(
    payload?.route,
    payload?.route_km,
    payload?.distance,
    payload?.route_api_distance_km,
    payload?.driver_to_pickup_distance_km,
    payload?.ride_details?.route_api_distance_km,
    payload?.meta?.route_api_distance_km,
    rideSnapshot?.route,
    rideSnapshot?.distance,
    rideSnapshot?.route_api_distance_km,
    rideSnapshot?.driver_to_pickup_distance_km,
    rideSnapshot?.ride_details?.route_api_distance_km,
    rideSnapshot?.meta?.route_api_distance_km
  );
  return toRouteMetricNumber(routeCandidate);
};

const getAcceptedEtaMin = (payload = {}, rideSnapshot = null) => {
  const etaCandidate = pickFirstValue(
    payload?.eta,
    payload?.eta_min,
    payload?.estimated_time,
    payload?.duration,
    payload?.route_api_duration_min,
    payload?.driver_to_pickup_duration_min,
    payload?.driverToPickupDurationMin,
    payload?.ride_details?.duration,
    payload?.meta?.duration,
    rideSnapshot?.eta,
    rideSnapshot?.eta_min,
    rideSnapshot?.duration,
    rideSnapshot?.route_api_duration_min,
    rideSnapshot?.driver_to_pickup_duration_min,
    rideSnapshot?.ride_details?.duration,
    rideSnapshot?.meta?.duration
  );
  return toRouteMetricNumber(etaCandidate);
};

const getFrontendRouteOverrideFromPayload = (payload = {}) => {
  const durationMin = pickFirstValue(
    payload?.duration,
    payload?.driver_to_pickup_duration_min,
    payload?.driverToPickupDurationMin,
    payload?.ride_details?.duration,
    payload?.meta?.duration
  );
  const distanceKm = pickFirstValue(
    payload?.distance,
    payload?.route_api_distance_km,
    payload?.driver_to_pickup_distance_km,
    payload?.ride_details?.route_api_distance_km,
    payload?.meta?.route_api_distance_km
  );
  const distanceM = pickFirstValue(
    payload?.driver_to_pickup_distance_m,
    payload?.meta?.driver_to_pickup_distance_m
  );

  if (durationMin === null && distanceKm === null && distanceM === null) return null;
  return { durationMin, distanceKm, distanceM };
};

const applyRouteOverrideToRidePayload = (ride = {}, routeOverride = null) => {
  if (!ride || typeof ride !== "object" || !routeOverride) return ride;

  const { durationMin, distanceKm, distanceM } = routeOverride;
  const rideDetails =
    ride.ride_details && typeof ride.ride_details === "object" ? { ...ride.ride_details } : {};
  const meta = ride.meta && typeof ride.meta === "object" ? { ...ride.meta } : {};

  if (durationMin !== null) {
    rideDetails.duration = durationMin;
    meta.duration = durationMin;
  }
  if (distanceKm !== null) {
    rideDetails.route_api_distance_km = distanceKm;
    meta.route_api_distance_km = distanceKm;
  }
  if (distanceM !== null) {
    meta.driver_to_pickup_distance_m = distanceM;
  }

  return {
    ...ride,
    ...(durationMin !== null
      ? {
          duration: durationMin,
          eta_min: durationMin,
          route_api_duration_min: durationMin,
          driver_to_pickup_duration_min: durationMin,
        }
      : {}),
    ...(distanceKm !== null
      ? {
          distance: distanceKm,
          route_api_distance_km: distanceKm,
          driver_to_pickup_distance_km: distanceKm,
        }
      : {}),
    ...(distanceM !== null ? { driver_to_pickup_distance_m: distanceM } : {}),
    ride_details: {
      ...(rideDetails || {}),
    },
    meta: {
      ...(meta || {}),
    },
  };
};

function applyRouteOverrideToTrackedRide(io, rideId, routeOverride, options = {}) {
  const { emit_bid_request = false } = options || {};
  if (!rideId || !routeOverride) return;

  const snapshot = getRideDetails(rideId);
  if (snapshot && typeof snapshot === "object") {
    const updatedSnapshot = applyRouteOverrideToRidePayload(snapshot, routeOverride);
    saveRideDetails(
      rideId,
      attachCustomerFields(updatedSnapshot, updatedSnapshot?.user_details ?? snapshot?.user_details ?? null)
    );
  }

  for (const [driverId, box] of driverRideInbox.entries()) {
    const ride = box.get(rideId);
    if (!ride) continue;

    const updatedBase = applyRouteOverrideToRidePayload(ride, routeOverride);
    const updated = attachCustomerFields(
      {
        ...updatedBase,
        _ts: Date.now(),
      },
      updatedBase?.user_details ?? ride?.user_details ?? null
    );

    box.set(rideId, updated);
    emitDriverPatch(io, driverId, [{ op: "upsert", ride: updated }]);

    if (emit_bid_request) {
      io.to(driverRoom(driverId)).emit("ride:bidRequest", sanitizeRidePayloadForClient(updated));
    }
  }
}

function upsertRideRouteMetrics(io, rideId, routeOverride = null, options = {}) {
  const { emit_bid_request = false } = options || {};
  const safeRideId = toNumber(rideId);
  if (!safeRideId || !routeOverride || typeof routeOverride !== "object") return false;

  const baseSnapshot =
    getRideDetails(safeRideId) ??
    getRideSnapshotForRedispatch(safeRideId) ?? {
      ride_id: safeRideId,
    };

  const seededSnapshot = attachCustomerFields(
    applyRouteOverrideToRidePayload(baseSnapshot, routeOverride),
    baseSnapshot?.user_details ?? null
  );
  saveRideDetails(safeRideId, seededSnapshot);

  applyRouteOverrideToTrackedRide(io, safeRideId, routeOverride, {
    emit_bid_request,
  });
  return true;
}

function emitRouteDataFromAcceptedOffer(io, rideId, driverId, driverToPickupMinutes) {
  const successPayload = {
    ride_id: rideId,
    driver_id: driverId,
    status: 1,
    duration: driverToPickupMinutes,
    at: Date.now(),
  };
  console.log("[ride:routeData][source=accepted-offer]", {
    ride_id: rideId,
    driver_id: driverId,
    duration: successPayload.duration,
  });

  io.to(rideRoom(rideId)).emit("ride:routeData", successPayload);
  io.to(driverRoom(driverId)).emit("ride:routeData", successPayload);

console.log("[ride:routeData] emitted from accepted offer", successPayload);
}
function emitCandidatesSummaryForDriverStateChange(io, driverId) {
  const safeDriverId = toNumber(driverId);
  if (!safeDriverId) return;

  for (const [rideId, candidateSet] of rideCandidates.entries()) {
    if (!candidateSet || !candidateSet.has(safeDriverId)) continue;

    emitRideCandidatesSummary(io, rideId);
  }
}

module.exports = (io, socket) => {
  socket.on("driver:getRidesList", ({ driver_id }) => {
    debugLog("driver:getRidesList", { driver_id }, socket.id);
    const driverId = toNumber(driver_id) ?? toNumber(socket.driverId);
    if (!driverId) return;

    socket.driverId = socket.driverId ?? driverId;
    socket.join(driverRoom(driverId));
    driverLocationService.updateMeta(driverId, {
      is_online: true,
      updatedAt: Date.now(),
    });

    emitDriverInbox(io, driverId, "driver:rides:list");
  });

  socket.on("user:joinRideRoom", ({ user_id, ride_id }) => {
    debugLog("user:joinRideRoom", { user_id, ride_id }, socket.id);
    const rideId = toNumber(ride_id);
    if (!rideId) {
      console.log(`undefined ride`);
      return;
    }
    socket.isUser = true;
    socket.userId = toNumber(user_id) ?? null;

    socket.join(rideRoom(rideId));
    socket.currentRideId = rideId;

    if (socket.userId) {
      setUserActiveRide(socket.userId, rideId);
    }
    socket.emit("ride:joined", { ride_id: rideId });
const vehicleTypes = buildRideCandidatesSummary(rideId);

socket.emit("ride:candidatesSummary", {
  ride_id: rideId,
  vehicle_types: vehicleTypes,
  total_vehicle_types: vehicleTypes.length,
  total_drivers: vehicleTypes.reduce(
    (sum, item) => sum + (toNumber(item?.drivers_count) ?? 0),
    0
  ),
  at: Date.now(),
});
    console.log(
      `👤 User ${socket.userId || "unknown"} joined ride room ${rideRoom(rideId)} (socket:${socket.id})`
    );

    const details = socket.userId ? getUserDetails(socket.userId) : null;
    if (details) {
      updateRideUserDetailsInInbox(io, rideId, details);

      const snapshot = getRideDetails(rideId);
      if (snapshot) {
        const route = toNumber(details?.route ?? null);
        const etaMin = toNumber(details?.eta_min ?? null);
        if (route !== null || etaMin !== null) {
          saveRideDetails(rideId, {
            ...snapshot,
            ...(route !== null ? { route } : {}),
            ...(etaMin !== null ? { eta_min: etaMin } : {}),
            meta: {
              ...(snapshot.meta ?? {}),
              ...(route !== null ? { route } : {}),
              ...(etaMin !== null ? { eta_min: etaMin } : {}),
            },
          });
        }
      }
    }
  });

  socket.on("ride:dispatchToNearbyDrivers", async (data = {}) => {
    debugLog("ride:dispatchToNearbyDrivers", data, socket.id);

    const safeData = data && typeof data === "object" ? data : {};
    const fallbackDistanceKm = toNumber(socket.nearbyRouteDistanceKm ?? null);
    const fallbackDurationMin = toNumber(socket.nearbyRouteDurationMin ?? null);
    const mergedData = { ...safeData };

    const hasIncomingDistance =
      pickFirstValue(
        safeData?.route_api_distance_km,
        safeData?.distance,
        safeData?.meta?.route_api_distance_km
      ) !== null;
    const hasIncomingDuration =
      pickFirstValue(
        safeData?.duration,
        safeData?.route_api_duration_min,
        safeData?.meta?.duration,
        safeData?.meta?.route_api_duration_min,
        safeData?.eta_min,
        safeData?.meta?.eta_min
      ) !== null;

    let appliedFrontendMetrics = false;
    if (!hasIncomingDistance && fallbackDistanceKm !== null) {
      mergedData.distance = fallbackDistanceKm;
      mergedData.route_api_distance_km = fallbackDistanceKm;
      appliedFrontendMetrics = true;
    }
    if (!hasIncomingDuration && fallbackDurationMin !== null) {
      mergedData.duration = fallbackDurationMin;
      mergedData.route_api_duration_min = fallbackDurationMin;
      mergedData.eta_min = mergedData.eta_min ?? fallbackDurationMin;
      appliedFrontendMetrics = true;
    }

    if (appliedFrontendMetrics) {
      mergedData.route_metrics_source = "frontend";
      mergedData.prefer_frontend_route_metrics = 1;
    }

    console.log("-> Received internal dispatch event:", mergedData);
    await dispatchToNearbyDrivers(io, mergedData);
  });

  // ✅ Frontend can push duration/distance before accept to sync bidRequest + rides list
  socket.on("ride:updateRouteMetrics", (payload = {}) => {
    debugLog("ride:updateRouteMetrics", payload, socket.id);

    const rideId = toNumber(payload?.ride_id);
    if (!rideId) {
      console.log("⚠️ ride:updateRouteMetrics ignored: missing ride_id");
      return;
    }

    const routeOverride = getFrontendRouteOverrideFromPayload(payload);
    if (!routeOverride) {
      console.log(`⚠️ ride:updateRouteMetrics ignored: missing duration/distance (ride ${rideId})`);
      return;
    }

    applyRouteOverrideToTrackedRide(io, rideId, routeOverride, {
      emit_bid_request: true,
    });

    const updatedSnapshot = getRideDetails(rideId) ?? getRideSnapshotForRedispatch(rideId);
    const duration = getRideDurationRaw(updatedSnapshot);
    const distanceKm = getRideDistanceKm(updatedSnapshot);

    socket.emit("ride:routeMetricsUpdated", {
      ride_id: rideId,
      duration: duration ?? null,
      distance: distanceKm ?? null,
      at: Date.now(),
    });

    console.log("[ride:updateRouteMetrics] applied", {
      ride_id: rideId,
      duration: duration ?? routeOverride?.durationMin ?? null,
      distance: distanceKm ?? routeOverride?.distanceKm ?? null,
    });
  });

  socket.on("ride:cancel", (payload = {}) => {
    const { ride_id, user_id, reason } = payload;
    debugLog("ride:cancel", { ride_id, user_id, reason }, socket.id);
    const rideId = toNumber(ride_id);
    if (!rideId) return;

    cancelledRides.add(rideId);

    setTimeout(() => {
      cancelledRides.delete(rideId);
    }, CANCELLED_RIDE_TTL_MS);

    removeRideFromAllInboxes(io, rideId);
    rideCandidates.delete(rideId);
    clearActiveRideByRideId(rideId);

    io.to(rideRoom(rideId)).emit("ride:cancelled", {
      ride_id: rideId,
      user_id: toNumber(user_id) ?? null,
      reason: reason ?? null,
      at: Date.now(),
    });

    console.log(`🧨 Ride ${rideId} CANCELLED by user ${user_id ?? "unknown"} | reason=${reason ?? "-"}`);

    const storedUser = user_id ? getUserDetails(toNumber(user_id)) : null;
    const accessToken =
      payload?.access_token ??
      payload?.token ??
      storedUser?.user_token ??
      storedUser?.token ??
      storedUser?.access_token ??
      null;

    const cancelPayload = {
      ride_id: rideId,
      user_id: user_id,
      reason_id: reason,
      sub_ride_id: 0,
      access_token: accessToken || "user-access-token",
    };

    axios
      .post(`${LARAVEL_BASE_URL}/api/customer/transport/cancel-ride`, cancelPayload, {
        timeout: 7000,
      })
      .then((response) => {
        console.log("API Response: Ride Cancelled", response.data);
      })
      .catch((error) => {
        console.error("Error while calling cancel ride API:", error?.response?.data || error.message);
      });
  });

  // ✅ إذا السائق قبل العرض
  socket.on("driver:acceptOffer", (payload) => {
    console.log("[accept][driver:acceptOffer] incoming", {
      ride_id: payload?.ride_id ?? null,
      driver_id: payload?.driver_id ?? socket.driverId ?? null,
      offered_price: payload?.offered_price ?? null,
    });

    const driverId = toNumber(socket.driverId) ?? toNumber(payload?.driver_id);
    const rideId = toNumber(payload?.ride_id);
    const offeredPrice = toNumber(payload?.offered_price);

    if (!driverId || !rideId || offeredPrice === null) return;

    const activeRide = getActiveRideByDriver(driverId);
    if (activeRide && activeRide !== rideId) {
      console.log(
        `⚠️ driver:acceptOffer ignored: driver ${driverId} already active on ride ${activeRide}`
      );
      return;
    }

    const rideSnapshot = driverRideInbox.get(driverId)?.get(rideId) ?? getRideDetails(rideId) ?? null;
    const ridePriceBounds = getRidePriceBounds(rideSnapshot ?? {});
    if (!isPriceWithinBounds(offeredPrice, ridePriceBounds)) {
      emitPriceValidationError(io, driverRoom(driverId), {
        ride_id: rideId,
        attempted_price: offeredPrice,
        min_price: ridePriceBounds.min_price,
        max_price: ridePriceBounds.max_price,
        actor: "driver",
        message: "Accepted user price is outside allowed range",
      });
      return;
    }

    const removed = inboxRemove(driverId, rideId);
    if (removed) {
      clearDriverBidStatus(driverId, rideId);

      const set = rideCandidates.get(rideId);
      if (set) {
        set.delete(driverId);
        if (set.size === 0) rideCandidates.delete(rideId);
      }

      emitDriverPatch(io, driverId, [{ op: "remove", ride_id: rideId }]);

      console.log(`🧹 Removed ride ${rideId} from driver ${driverId} inbox after acceptOffer`);
    }

    const rideDetails = getRideDetails(rideId);
    const userId =
      toNumber(payload?.user_id) ??
      toNumber(rideSnapshot?.user_id) ??
      toNumber(rideDetails?.user_id) ??
      toNumber(rideOwnerByRide.get(rideId));
    const acceptedRouteKm = getAcceptedRouteKm(payload, rideSnapshot ?? rideDetails);
    const acceptedEtaMin = getAcceptedEtaMin(payload, rideSnapshot ?? rideDetails);

    // ✅ accept token fallback (retry-safe)
    const tokenTmp =
      payload?.access_token ??
      payload?.token ??
      rideSnapshot?.token ??
      rideSnapshot?.user_details?.user_token ??
      rideSnapshot?.user_details?.token ??
      rideDetails?.token ??
      rideDetails?.user_details?.user_token ??
      rideDetails?.user_details?.token ??
      null;

    const accessToken = tokenTmp;

    io.to(driverRoom(driverId)).emit("ride:driverAccepted", {
      ride_id: rideId,
      driver_id: driverId,
      offered_price: offeredPrice,
      message: "Offer accepted by driver",
      ride_details: rideSnapshot,
      at: Date.now(),
    });

    io.to(rideRoom(rideId)).emit("ride:acceptedByDriver", {
      ride_id: rideId,
      driver_id: driverId,
      offered_price: offeredPrice,
      message: "Offer accepted by driver",
      at: Date.now(),
    });

    const finalize = () =>
      finalizeAcceptedRide(io, rideId, driverId, offeredPrice, {
        message: "Driver accepted the offer",
        rideDetails: rideSnapshot,
        userId,
      });

    if (!userId || !accessToken) {
      console.log(`WARN driver:acceptOffer API skipped: missing user_id/access_token (ride ${rideId})`);
      finalize();
    } else {
      const acceptPayload = {
        user_id: userId,
        access_token: accessToken,
        driver_id: driverId,
        ride_id: rideId,
        offered_price: offeredPrice,
        ...(acceptedRouteKm !== null
          ? {
              route: acceptedRouteKm,
              total_distance: acceptedRouteKm,
            }
          : {}),
        ...(acceptedEtaMin !== null
          ? {
              eta: acceptedEtaMin,
              estimated_time: acceptedEtaMin,
            }
          : {}),
      };

      axios
        .post(`${LARAVEL_BASE_URL}${LARAVEL_ACCEPT_BID_PATH}`, acceptPayload, {
          timeout: LARAVEL_ACCEPT_BID_TIMEOUT_MS,
        })
        .then((response) => {
          console.log("API Response: Accept Bid (driver)", response.data);
          finalize();
        })
        .catch((error) => {
          console.error(
            "Error while calling accept bid API (driver):",
            error?.response?.data || error.message
          );
          // حتى لو فشل API — خلّي الflow يكمل محلياً
          finalize();
        });
    }

    console.log(`✅ Driver ${driverId} ACCEPTED offer ${offeredPrice} for ride ${rideId}`);
  });

  socket.on("driver:submitBid", async (payload) => {
    debugLog("driver:submitBid", payload, socket.id);
    console.log("[bid][driver:submitBid] incoming", {
      ride_id: payload?.ride_id ?? null,
      driver_id: socket.driverId ?? payload?.driver_id ?? null,
      offered_price: payload?.offered_price ?? null,
    });

    const rideId = toNumber(payload?.ride_id);
    const driverId = toNumber(socket.driverId);
    const offeredPrice = toNumber(payload?.offered_price);

    if (!driverId || !rideId || offeredPrice === null) {
      console.log(
        `⚠️ Invalid bid attempt - missing ride_id or driver_id or offered_price (socket: ${socket.id})`
      );
      return;
    }

    const activeRide = getActiveRideByDriver(driverId);
    if (activeRide && activeRide !== rideId) {
      console.log(`⚠️ Driver ${driverId} already active on ride ${activeRide}; bid blocked for ride ${rideId}`);
      return;
    }

    const currencyRaw = toNumber(payload?.currency);
    const currency = currencyRaw !== null && currencyRaw > 0 ? currencyRaw : 1;
    const finalPrice = round2(offeredPrice * currency);
    const rideSnapshot =
      driverRideInbox.get(driverId)?.get(rideId) ??
      getRideDetails(rideId) ??
      null;

    const ridePriceBounds = getRidePriceBounds(rideSnapshot ?? {});
    if (!isPriceWithinBounds(offeredPrice, ridePriceBounds)) {
      emitPriceValidationError(io, driverRoom(driverId), {
        ride_id: rideId,
        attempted_price: offeredPrice,
        min_price: ridePriceBounds.min_price,
        max_price: ridePriceBounds.max_price,
        actor: "driver",
        message: "Driver offered price is outside allowed range",
      });
      return;
    }

    const payloadMeta =
      payload?.meta && typeof payload.meta === "object" && !Array.isArray(payload.meta)
        ? payload.meta
        : {};
    const snapshotMeta =
      rideSnapshot?.meta && typeof rideSnapshot.meta === "object" && !Array.isArray(rideSnapshot.meta)
        ? rideSnapshot.meta
        : {};

    const payloadDriverToPickupDistanceM = toNumber(
      payload?.driver_to_pickup_distance_m ?? payloadMeta?.driver_to_pickup_distance_m ?? null
    );
    const payloadDriverToPickupDurationS = toNumber(
      payload?.driver_to_pickup_duration_s ?? payloadMeta?.driver_to_pickup_duration_s ?? null
    );
    const payloadDriverToPickupDurationMin = toNumber(
      payload?.driver_to_pickup_duration_min ?? payloadMeta?.driver_to_pickup_duration_min ?? null
    );

    const snapshotDriverToPickupDistanceM = toNumber(
      rideSnapshot?.driver_to_pickup_distance_m ?? snapshotMeta?.driver_to_pickup_distance_m ?? null
    );
    const snapshotDriverToPickupDurationS = toNumber(
      rideSnapshot?.driver_to_pickup_duration_s ?? snapshotMeta?.driver_to_pickup_duration_s ?? null
    );
    const snapshotDriverToPickupDurationMin = toNumber(
      rideSnapshot?.driver_to_pickup_duration_min ?? snapshotMeta?.driver_to_pickup_duration_min ?? null
    );

    const driverPos = driverLocationService.getDriver(driverId) || null;
    const driverLat = toNumber(payload?.driver_lat ?? payload?.lat ?? driverPos?.lat ?? null);
    const driverLong = toNumber(payload?.driver_long ?? payload?.long ?? driverPos?.long ?? null);
    const pickupLatForEta = toNumber(payload?.pickup_lat ?? rideSnapshot?.pickup_lat ?? null);
    const pickupLongForEta = toNumber(payload?.pickup_long ?? rideSnapshot?.pickup_long ?? null);

    const driverToPickupDistanceM =
      payloadDriverToPickupDistanceM ??
      snapshotDriverToPickupDistanceM ??
      (driverLat !== null &&
      driverLong !== null &&
      pickupLatForEta !== null &&
      pickupLongForEta !== null
        ? Math.round(getDistanceMeters(driverLat, driverLong, pickupLatForEta, pickupLongForEta))
        : null);

    const payloadDurationFromMin =
      payloadDriverToPickupDurationMin !== null
        ? payloadDriverToPickupDurationMin * 60
        : null;
    const snapshotDurationFromMin =
      snapshotDriverToPickupDurationMin !== null
        ? snapshotDriverToPickupDurationMin * 60
        : null;

    const driverToPickupDurationS =
      payloadDriverToPickupDurationS ??
      payloadDurationFromMin ??
      snapshotDriverToPickupDurationS ??
      snapshotDurationFromMin ??
      (driverToPickupDistanceM !== null
        ? Math.max(60, Math.round((driverToPickupDistanceM * 3.6) / DRIVER_TO_PICKUP_SPEED_KMPH))
        : null);

    const driverToPickupDistanceKm =
      driverToPickupDistanceM !== null ? round2(driverToPickupDistanceM / 1000) : null;
    const driverToPickupDurationMin =
      driverToPickupDurationS !== null ? driverToPickupDurationS / 60 : null;

    // Keep frontend/snapshot values authoritative. Route API is fallback only when missing.
    const shouldFetchRouteApi =
      (driverToPickupDurationMin === null || driverToPickupDistanceKm === null) &&
      driverLong !== null &&
      driverLat !== null &&
      pickupLongForEta !== null &&
      pickupLatForEta !== null;

    let routeApiData = null;
    if (shouldFetchRouteApi) {
      routeApiData = await fetchRouteDataByCoords({
        startLongitude: driverLong,
        startLatitude: driverLat,
        endLongitude: pickupLongForEta,
        endLatitude: pickupLatForEta,
        requested_at: new Date().toISOString(),
      });
    }

    // حاول التقاط الوقت/المسافة من API بأي اسم شائع (fallback only)
    const apiDurationMin = toNumber(routeApiData?.duration);
    const apiEtaMin = apiDurationMin !== null ? apiDurationMin : null;
    const apiDurationS = apiEtaMin !== null ? Math.round(apiEtaMin * 60) : null;
    const apiDistanceKm =
      toNumber(routeApiData?.route) ??
      toNumber(routeApiData?.distance_km) ??
      toNumber(routeApiData?.total_distance) ??
      null;

    const finalDriverToPickupDistanceKm =
      driverToPickupDistanceKm !== null ? driverToPickupDistanceKm : apiDistanceKm;
    const finalDriverToPickupDurationS =
      driverToPickupDurationS !== null ? driverToPickupDurationS : apiDurationS;
    const finalDriverToPickupDurationMin =
      driverToPickupDurationMin !== null ? driverToPickupDurationMin : apiEtaMin;

    const currentSnapshot = getRideDetails(rideId) ?? {};
    const updatedSnapshot = attachCustomerFields(
      {
        ...currentSnapshot,
        ...(driverToPickupDistanceM !== null ? { driver_to_pickup_distance_m: driverToPickupDistanceM } : {}),
        ...(finalDriverToPickupDistanceKm !== null
          ? {
              driver_to_pickup_distance_km: finalDriverToPickupDistanceKm,
              route_api_distance_km: finalDriverToPickupDistanceKm,
              distance: finalDriverToPickupDistanceKm,
            }
          : {}),
        ...(finalDriverToPickupDurationS !== null ? { driver_to_pickup_duration_s: finalDriverToPickupDurationS } : {}),
        ...(finalDriverToPickupDurationMin !== null
          ? {
              driver_to_pickup_duration_min: finalDriverToPickupDurationMin,
              estimated_arrival_min: finalDriverToPickupDurationMin,
              duration: finalDriverToPickupDurationMin,
              eta_min: finalDriverToPickupDurationMin,
              route_api_duration_min: finalDriverToPickupDurationMin,
            }
          : {}),
        ride_details: {
          ...(currentSnapshot?.ride_details && typeof currentSnapshot.ride_details === "object"
            ? currentSnapshot.ride_details
            : {}),
          ...(finalDriverToPickupDurationMin !== null
            ? { duration: finalDriverToPickupDurationMin }
            : {}),
          ...(finalDriverToPickupDistanceKm !== null
            ? { route_api_distance_km: finalDriverToPickupDistanceKm }
            : {}),
        },
        meta: {
          ...(currentSnapshot?.meta && typeof currentSnapshot.meta === "object"
            ? currentSnapshot.meta
            : {}),
          ...(driverToPickupDistanceM !== null
            ? { driver_to_pickup_distance_m: driverToPickupDistanceM }
            : {}),
          ...(finalDriverToPickupDistanceKm !== null
            ? {
                driver_to_pickup_distance_km: finalDriverToPickupDistanceKm,
                route_api_distance_km: finalDriverToPickupDistanceKm,
              }
            : {}),
          ...(finalDriverToPickupDurationS !== null
            ? { driver_to_pickup_duration_s: finalDriverToPickupDurationS }
            : {}),
          ...(finalDriverToPickupDurationMin !== null
            ? {
                driver_to_pickup_duration_min: finalDriverToPickupDurationMin,
                estimated_arrival_min: finalDriverToPickupDurationMin,
                duration: finalDriverToPickupDurationMin,
              }
            : {}),
          ...(routeApiData && typeof routeApiData === "object"
            ? { route_api_data: routeApiData }
            : {}),
        },
      },
      currentSnapshot?.user_details ?? null
    );
    saveRideDetails(rideId, updatedSnapshot);

    const bidMeta = {
      ...payloadMeta,

      ...(apiEtaMin !== null
        ? { driver_to_pickup_api_duration_min: apiEtaMin }
        : {}),

      ...(driverToPickupDistanceM !== null || finalDriverToPickupDistanceKm !== null
        ? {
            ...(driverToPickupDistanceM !== null
              ? { driver_to_pickup_distance_m: driverToPickupDistanceM }
              : {}),
            ...(finalDriverToPickupDistanceKm !== null
              ? { driver_to_pickup_distance_km: finalDriverToPickupDistanceKm }
              : {}),
          }
        : {}),

      ...(finalDriverToPickupDurationMin !== null || finalDriverToPickupDurationS !== null
        ? {
            ...(finalDriverToPickupDurationS !== null
              ? { driver_to_pickup_duration_s: finalDriverToPickupDurationS }
              : {}),
            ...(finalDriverToPickupDurationMin !== null
              ? {
                  driver_to_pickup_duration_min: finalDriverToPickupDurationMin,
                  estimated_arrival_min: finalDriverToPickupDurationMin,
                }
              : {}),
          }
        : {}),

      ...(routeApiData && typeof routeApiData === "object"
        ? { route_api_data: routeApiData }
        : {}),
    };

    const driverMeta = driverLocationService.getMeta(driverId) || {};
    const driverServiceId =
      toNumber(payload?.driver_service_id) ??
      toNumber(payload?.driver_service) ??
      toNumber(socket.driverServiceId) ??
      toNumber(driverMeta?.driver_service_id);

    const accessToken =
      payload?.access_token ?? socket.driverAccessToken ?? driverMeta?.access_token ?? null;

    let driverDetails =
      payload.driver_details ?? {
        driver_name: driverMeta?.driver_name ?? null,
        vehicle_type: driverMeta?.vehicle_type_name ?? null,
        vehicle_number: driverMeta?.plat_no ?? null,
        rating: driverMeta?.rating ?? null,
        driver_image: driverMeta?.driver_image ?? null,
      };

    if (isDriverDetailsEmpty(driverDetails) && driverServiceId && accessToken) {
      const fetched = await fetchDriverMetaFromApi(driverId, accessToken, driverServiceId);
      if (fetched) {
        driverDetails = {
          driver_name: driverDetails?.driver_name ?? fetched.driver_name ?? null,
          vehicle_type: driverDetails?.vehicle_type ?? fetched.vehicle_type ?? null,
          vehicle_number: driverDetails?.vehicle_number ?? fetched.vehicle_number ?? null,
          rating: driverDetails?.rating ?? fetched.rating ?? null,
          driver_image: driverDetails?.driver_image ?? fetched.driver_image ?? null,
        };
      }
    }

    if (cancelledRides.has(rideId)) {
      console.log(`⚠️ Bid ignored: ride ${rideId} is cancelled`);
      return;
    }

    const activeDriverId = getActiveDriverByRide(rideId);
    if (activeDriverId) {
      console.log(`⚠️ Bid ignored: ride ${rideId} already accepted by driver ${activeDriverId}`);
      return;
    }

    if (!isCandidateDriver(rideId, driverId)) return;

    const lastBid = driverLastBidStatus.get(driverId);
    if (lastBid && lastBid.rideId === rideId && !lastBid.responded) {
      console.log(`⚠️ Driver ${driverId} cannot submit a new bid until user responds to the previous bid.`);
      return;
    }

    driverLastBidStatus.set(driverId, { rideId, responded: false });

    if (!driverServiceId || !accessToken) {
      console.log(`⚠️ driver:submitBid API skipped: missing driver_service_id/access_token (driver ${driverId})`);
    } else {
      const bidPayload = {
        driver_id: driverId,
        access_token: accessToken,
        driver_service_id: driverServiceId,
        ride_id: rideId,
        offered_price: offeredPrice,
      };

      axios
        .post(`${LARAVEL_BASE_URL}${LARAVEL_DRIVER_BID_PATH}`, bidPayload, { timeout: 7000 })
        .then((response) => {
          console.log("API Response: Driver Bid Offer", response.data);
        })
        .catch((error) => {
          console.error("Error while calling driver bid API:", error?.response?.data || error.message);
        });
    }

    // ✅ TIMER refresh: every new bid resets timer (90s seconds)
    const timer = refreshRideTimer(io, rideId, {
      update_snapshot: true,
      patch_inboxes: true,
    });

    const ridePayload = {
      ride_id: rideId,
      driver_id: driverId,
      offered_price: offeredPrice,
      bidding_time: Date.now(),

      pickup_lat: payload.pickup_lat ?? null,
      pickup_long: payload.pickup_long ?? null,
      pickup_address: payload.pickup_address ?? null,
      destination_lat: toNumber(payload.destination_lat) ?? null,
      destination_long: toNumber(payload.destination_long) ?? null,
      destination_address: payload.destination_address ?? null,

      radius: payload.radius ?? 0,
      user_bid_price: offeredPrice,
      user_bid_price_final: finalPrice,
      min_fare_amount:
        toNumber(payload?.min_fare_amount) ??
        toNumber(rideSnapshot?.min_fare_amount) ??
        null,
      base_fare:
        toNumber(payload?.base_fare) ??
        toNumber(rideSnapshot?.base_fare) ??
        ridePriceBounds.base_fare,
      min_price:
        toNumber(payload?.min_price) ??
        toNumber(rideSnapshot?.min_price) ??
        ridePriceBounds.min_price,
      max_price:
        toNumber(payload?.max_price) ??
        toNumber(rideSnapshot?.max_price) ??
        ridePriceBounds.max_price,
      service_type_id: toNumber(payload.service_type_id) ?? null,
      service_category_id: toNumber(payload.service_category_id) ?? null,
      created_at: payload.created_at ?? null,

      ...(apiEtaMin !== null
        ? { driver_to_pickup_api_duration_min: apiEtaMin }
        : {}),

      ...(driverToPickupDistanceM !== null || finalDriverToPickupDistanceKm !== null
        ? {
            ...(driverToPickupDistanceM !== null
              ? { driver_to_pickup_distance_m: driverToPickupDistanceM }
              : {}),
            ...(finalDriverToPickupDistanceKm !== null
              ? { driver_to_pickup_distance_km: finalDriverToPickupDistanceKm }
              : {}),
          }
        : {}),

      ...(finalDriverToPickupDurationS !== null || finalDriverToPickupDurationMin !== null
        ? {
            ...(finalDriverToPickupDurationS !== null
              ? { driver_to_pickup_duration_s: finalDriverToPickupDurationS }
              : {}),
            ...(finalDriverToPickupDurationMin !== null
              ? {
                  driver_to_pickup_duration_min: finalDriverToPickupDurationMin,
                  estimated_arrival_min: finalDriverToPickupDurationMin,
                }
              : {}),
          }
        : {}),

      meta: {
        ...bidMeta,
        ...(apiEtaMin !== null
          ? { driver_to_pickup_api_duration_min: apiEtaMin }
          : {}),
      },

      driver_details: driverDetails,
      address_list: payload.address_list ?? [],
      user_timeout: payload.user_timeout ?? 60,
      driver_algo: payload.driver_algo ?? "default_algorithm",
      bid_limit: payload.bid_limit ?? 5,
      can_bid_more: payload.can_bid_more ?? true,
      remain_bid: (payload.bid_limit ?? 5) - (payload.user_bid_count ?? 0),

      // ✅ timer fields (SECONDS)
      ...(timer ? timer : {}),
    };
    io.to(rideRoom(rideId)).emit("ride:newBid", ridePayload);

    console.log(`💰 Driver ${driverId} submitted bid ${offeredPrice} for ride ${rideId}`);

    const removed = inboxRemove(driverId, rideId);
    if (removed) {
      emitDriverPatch(io, driverId, [{ op: "remove", ride_id: rideId }]);
      console.log(`🧹 [inbox] removed ride ${rideId} from driver ${driverId} after submitBid`);
    }
  });

  // ✅ باقي الملف (user:respondToDriver / user:acceptOffer / ride:close / disconnect)
  // ما تم تغييره نهائياً — فقط سيستمر باستعمال refreshRideTimer الذي صار 90s بالثواني
  // -------------------------------------------------

  socket.on("user:respondToDriver", (payload) => {
    console.log("[bid][user:respondToDriver] incoming", {
      ride_id: payload?.ride_id ?? null,
      driver_id: payload?.driver_id ?? null,
      price: payload?.price ?? payload?.user_bid_price ?? null,
    });

    const rideId = toNumber(payload?.ride_id);
    if (!rideId) {
      console.log("[bid][user:respondToDriver] missing ride_id");
      return;
    }

    if (cancelledRides.has(rideId)) {
      console.log(`⚠️ user:respondToDriver ignored: ride ${rideId} is cancelled`);
      return;
    }

    const activeDriverId = getActiveDriverByRide(rideId);
    if (activeDriverId) {
      console.log(`⚠️ user:respondToDriver ignored: ride ${rideId} already accepted by driver ${activeDriverId}`);
      return;
    }

    const newPrice = toNumber(payload?.price) ?? toNumber(payload?.user_bid_price);

    const set = rideCandidates.get(rideId);
    const candidateDrivers = set ? Array.from(set.values()) : [];

    if (newPrice === null) {
      console.log("⚠️ user:respondToDriver ignored: missing price");
      return;
    }

    const snapshot = getRideSnapshotForRedispatch(rideId);
    const ridePriceBounds = getRidePriceBounds(snapshot ?? {});
    if (!isPriceWithinBounds(newPrice, ridePriceBounds)) {
      emitPriceValidationError(io, rideRoom(rideId), {
        ride_id: rideId,
        attempted_price: newPrice,
        min_price: ridePriceBounds.min_price,
        max_price: ridePriceBounds.max_price,
        actor: "user",
        message: "User price is outside allowed range",
      });
      return;
    }

    const payloadToken = payload?.token ?? payload?.access_token ?? payload?.user_token ?? null;
    const fallbackUserId =
      toNumber(payload?.user_id) ?? toNumber(snapshot?.user_id) ?? toNumber(getUserIdForRide(rideId));

    const stored = fallbackUserId ? getUserDetails(fallbackUserId) : null;
    const storedByToken =
      !stored && (payloadToken || snapshot?.token)
        ? getUserDetailsByToken(payloadToken ?? snapshot?.token)
        : null;

    const incomingUserDetails = buildUserDetails(payload);

    const mergedUserDetails = buildUserDetails({
      ...payload,
      user_id: fallbackUserId ?? payload?.user_id,
      token: payloadToken ?? snapshot?.token,
      access_token: payloadToken ?? snapshot?.token,
      user_token: payloadToken ?? snapshot?.token,
      user_details: incomingUserDetails ?? stored ?? storedByToken ?? snapshot?.user_details ?? null,
    });

    if (mergedUserDetails) {
      updateRideUserDetailsInInbox(io, rideId, mergedUserDetails);
    }

    let snapshotDetails = mergedUserDetails ?? snapshot?.user_details ?? null;
    if (!snapshotDetails) {
      const uid = snapshot?.user_id ?? null;
      const details = uid ? getUserDetails(uid) : null;
      if (details) snapshotDetails = details;
    }

    const snapshotBase = snapshot
      ? attachCustomerFields(
          {
            ...snapshot,
            user_details: snapshotDetails ?? snapshot.user_details ?? null,
            user_id: snapshot.user_id ?? snapshotDetails?.user_id ?? null,
            user_name: snapshot.user_name ?? snapshotDetails?.user_name ?? null,
            user_gender: snapshot.user_gender ?? snapshotDetails?.user_gender ?? null,
            user_image: snapshot.user_image ?? snapshotDetails?.user_image ?? null,
            user_phone: snapshot.user_phone ?? snapshotDetails?.user_phone ?? null,
            user_country_code: snapshot.user_country_code ?? snapshotDetails?.user_country_code ?? null,
            user_phone_full: snapshot.user_phone_full ?? snapshotDetails?.user_phone_full ?? null,
            token:
              snapshot?.token ??
              snapshotDetails?.user_token ??
              snapshotDetails?.token ??
              payloadToken ??
              null,
          },
          snapshotDetails
        )
      : null;

    // ✅ TIMER refresh: every user response resets timer (90s seconds)
    const timer = refreshRideTimer(io, rideId, {
      update_snapshot: true,
      patch_inboxes: true,
    });

    for (const dId of candidateDrivers) {
      const rideFull = driverRideInbox.get(dId)?.get(rideId);
      const baseRide = rideFull ?? snapshotBase;

      if (baseRide) {
        const mergedUD = incomingUserDetails ?? baseRide.user_details ?? snapshotDetails ?? null;

        const updatedRide = attachCustomerFields(
          {
            ...baseRide,
            user_details: mergedUD ?? baseRide.user_details ?? null,
            user_id: mergedUD?.user_id ?? baseRide.user_id ?? null,
            user_name: mergedUD?.user_name ?? baseRide.user_name ?? null,
            user_gender: mergedUD?.user_gender ?? baseRide.user_gender ?? null,
            user_image: mergedUD?.user_image ?? baseRide.user_image ?? null,
            user_phone: mergedUD?.user_phone ?? baseRide.user_phone ?? null,
            user_country_code: mergedUD?.user_country_code ?? baseRide.user_country_code ?? null,
            user_phone_full: mergedUD?.user_phone_full ?? baseRide.user_phone_full ?? null,
            token:
              baseRide?.token ??
              mergedUD?.user_token ??
              mergedUD?.token ??
              payloadToken ??
              null,
            user_bid_price: newPrice,
            isPriceUpdated: true,
            updatedPrice: newPrice,
            updatedAt: Date.now(),

            // ✅ timer fields (SECONDS)
            ...(timer ? timer : {}),
          },
          mergedUD
        );

        inboxUpsert(dId, rideId, updatedRide);
        emitDriverPatch(io, dId, [{ op: "upsert", ride: updatedRide }]);
      } else {
        console.log(`ℹ️ ride ${rideId} not found in inbox for driver ${dId} (skip update)`);
      }

      io.to(driverRoom(dId)).emit("ride:userResponse", {
        ride_id: rideId,
        price: newPrice,

        // ✅ timer fields (SECONDS)
        ...(timer ? timer : {}),
        at: Date.now(),
      });

      driverLastBidStatus.set(dId, { rideId, responded: true });
    }

    if (snapshotBase) {
      const redispatchData = {
        ...snapshotBase,
        ride_id: rideId,
        user_bid_price: newPrice,
        isPriceUpdated: true,
        updatedPrice: newPrice,
        updatedAt: Date.now(),

        // ✅ carry same timer forward (SECONDS)
        server_time: timer?.server_time ?? null,
        expires_at: timer?.expires_at ?? null,
        timeout_ms: timer?.timeout_ms ?? null,
      };
      void dispatchToNearbyDrivers(io, redispatchData);
    } else {
      console.log(`⚠️ redispatch skipped: no ride snapshot for ride ${rideId}`);
    }

    io.to(rideRoom(rideId)).emit("ride:priceUpdated", {
      ride_id: rideId,
      user_bid_price: newPrice,

      // ✅ timer fields (SECONDS)
      ...(timer ? timer : {}),
      at: Date.now(),
    });

    console.log(
      `✅ user response(broadcast) -> ride ${rideId} newPrice ${newPrice} sent to ${candidateDrivers.length} drivers`
    );
  });

  socket.on("user:acceptOffer", async (payload) => {
    console.log("[user:acceptOffer] incoming payload:", payload, "socket:", socket.id);
    debugLog("user:acceptOffer", payload, socket.id);

    const rideId = toNumber(payload?.ride_id);
    const driverId = toNumber(payload?.driver_id);

    if (!rideId || !driverId) {
      console.log("⚠️ user:acceptOffer ignored: missing ride_id/driver_id");
      return;
    }

    const rideSnapshot = getFullRideSnapshot(rideId, driverId);
    let acceptedRideSnapshot = rideSnapshot;

    if (cancelledRides.has(rideId)) {
      console.log(`⚠️ user:acceptOffer ignored: ride ${rideId} is cancelled`);
      return;
    }

    const activeDriverId = getActiveDriverByRide(rideId);
    if (activeDriverId && activeDriverId !== driverId) {
      console.log(`⚠️ user:acceptOffer ignored: ride ${rideId} already accepted by driver ${activeDriverId}`);
      return;
    }

    let finalPrice = toNumber(payload?.offered_price ?? payload?.price);
    const rideFull = driverRideInbox.get(driverId)?.get(rideId);
    if (finalPrice === null && rideFull) {
      finalPrice = toNumber(
        rideFull.user_bid_price_final ??
          rideFull.updatedPrice ??
          rideFull.user_bid_price ??
          rideFull.min_fare_amount
      );
    }

    if (finalPrice === null) {
      console.log("⚠️ user:acceptOffer ignored: missing offered_price");
      return;
    }

    const rideDetails = getRideDetails(rideId);
    const ridePriceBounds = getRidePriceBounds(rideSnapshot ?? rideDetails ?? {});
    if (!isPriceWithinBounds(finalPrice, ridePriceBounds)) {
      emitPriceValidationError(io, rideRoom(rideId), {
        ride_id: rideId,
        attempted_price: finalPrice,
        min_price: ridePriceBounds.min_price,
        max_price: ridePriceBounds.max_price,
        actor: "user",
        message: "Accepted offer price is outside allowed range",
      });
      return;
    }

    const payloadToken =
      payload?.access_token ?? payload?.token ?? payload?.user_token ?? null;

    const tokenTmp =
      payloadToken ??
      rideSnapshot?.token ??
      rideSnapshot?.user_details?.user_token ??
      rideSnapshot?.user_details?.token ??
      rideDetails?.token ??
      rideDetails?.user_details?.user_token ??
      rideDetails?.user_details?.token ??
      null;

    const userFromToken = tokenTmp ? getUserDetailsByToken(tokenTmp) : null;

    const userId =
      toNumber(payload?.user_id) ??
      toNumber(socket.userId) ??
      toNumber(rideOwnerByRide.get(rideId)) ??
      toNumber(rideDetails?.user_id) ??
      toNumber(userFromToken?.user_id);

    const storedUser = userId ? getUserDetails(userId) : null;
    const accessToken =
      tokenTmp ??
      storedUser?.user_token ??
      storedUser?.token ??
      userFromToken?.user_token ??
      null;
    const acceptedRouteKmForApi = getAcceptedRouteKm(payload, rideSnapshot ?? rideDetails);
    const acceptedEtaMinForApi = getAcceptedEtaMin(payload, rideSnapshot ?? rideDetails);

    if (!userId || !accessToken) {
      console.log(`⚠️ user:acceptOffer API skipped: missing user_id/access_token (ride ${rideId})`);
    } else {
      const acceptPayload = {
        user_id: userId,
        access_token: accessToken,
        driver_id: driverId,
        ride_id: rideId,
        offered_price: finalPrice,
        ...(acceptedRouteKmForApi !== null
          ? {
              route: acceptedRouteKmForApi,
              total_distance: acceptedRouteKmForApi,
            }
          : {}),
        ...(acceptedEtaMinForApi !== null
          ? {
              eta: acceptedEtaMinForApi,
              estimated_time: acceptedEtaMinForApi,
            }
          : {}),
      };

      axios
        .post(`${LARAVEL_BASE_URL}${LARAVEL_ACCEPT_BID_PATH}`, acceptPayload, {
          timeout: LARAVEL_ACCEPT_BID_TIMEOUT_MS,
        })
        .then((response) => {
          console.log("API Response: Accept Bid", response.data);
        })
        .catch((error) => {
          console.error("Error while calling accept bid API:", error?.response?.data || error.message);
        });
    }
    // ✅ trust frontend values on accept (no recalculation)
    const frontendRouteOverride = getFrontendRouteOverrideFromPayload(payload);
    if (frontendRouteOverride) {
      console.log("[user:acceptOffer][routeData-source=frontend-payload]", {
        ride_id: rideId,
        driver_id: driverId,
        duration: frontendRouteOverride.durationMin,
        distance: frontendRouteOverride.distanceKm,
      });

      applyRouteOverrideToTrackedRide(io, rideId, frontendRouteOverride, {
        emit_bid_request: true,
      });
      acceptedRideSnapshot = getFullRideSnapshot(rideId, driverId) ?? acceptedRideSnapshot;
    }

    const acceptedMinutes = getAcceptedDriverToPickupMinutes(payload, acceptedRideSnapshot);

    if (acceptedMinutes !== null) {
      console.log("[user:acceptOffer][routeData-source=accepted-offer]", {
        ride_id: rideId,
        driver_id: driverId,
        duration: acceptedMinutes,
      });
      emitRouteDataFromAcceptedOffer(io, rideId, driverId, acceptedMinutes);
    } else {
      console.log("[user:acceptOffer][routeData-source=none] missing duration in frontend payload", {
        ride_id: rideId,
        driver_id: driverId,
      });
    }

    finalizeAcceptedRide(io, rideId, driverId, finalPrice, {
      message: "User accepted the offer",
      rideDetails: acceptedRideSnapshot,
      userId,
    });

    console.log(`✅ user:acceptOffer -> ride ${rideId} driver ${driverId} price ${finalPrice}`);
  });

  socket.on("ride:close", ({ ride_id }) => {
    debugLog("ride:close", { ride_id }, socket.id);
    const rideId = toNumber(ride_id);
    if (!rideId) return;

    removeRideFromAllInboxes(io, rideId);
    clearActiveRideByRideId(rideId);
    io.to(rideRoom(rideId)).emit("ride:closed", { ride_id: rideId });

    console.log(`🔒 Ride ${rideId} CLOSED`);
  });

  socket.on("disconnect", () => {
    debugLog("disconnect", {}, socket.id);
    const driverId = toNumber(socket.driverId);
    if (driverId) {
      driverPatchSeq.delete(driverId);
      // Keep inbox + bid status across transient disconnects.
      // Stale data is cleaned by inbox TTL/prune logic.
    }
  });

  socket.on("driver:declineRide", (payload = {}) => {
    const driverId = toNumber(socket.driverId) ?? toNumber(payload?.driver_id);
    const rideId = toNumber(payload?.ride_id);

    if (!driverId || !rideId) return;

    const activeDriverId = getActiveDriverByRide(rideId);
    if (activeDriverId) {
      console.log(
        `⚠️ driver:declineRide ignored: ride ${rideId} already accepted by driver ${activeDriverId}`
      );
      return;
    }

    const set = rideCandidates.get(rideId);
    if (set) {
      set.delete(driverId);
      if (set.size === 0) rideCandidates.delete(rideId);
    }

    const existed = inboxRemove(driverId, rideId);
    clearDriverBidStatus(driverId, rideId);

    if (existed) {
      emitDriverPatch(io, driverId, [{ op: "remove", ride_id: rideId }]);
    }

    io.to(driverRoom(driverId)).emit("ride:declined", {
      ride_id: rideId,
      driver_id: driverId,
      at: Date.now(),
    });

    emitRideCandidatesSummary(io, rideId);

    console.log(`❌ Driver ${driverId} declined ride ${rideId}`);
  });

  socket.on("ride:getCandidatesSummary", ({ ride_id }) => {
    const rideId = toNumber(ride_id);
    if (!rideId) return;

    const vehicleTypes = buildRideCandidatesSummary(rideId);

    socket.emit("ride:candidatesSummary", {
      ride_id: rideId,
      vehicle_types: vehicleTypes,
      total_vehicle_types: vehicleTypes.length,
      total_drivers: vehicleTypes.reduce(
        (sum, item) => sum + (toNumber(item?.drivers_count) ?? 0),
        0
      ),
      at: Date.now(),
    });
  });
};

module.exports.dispatchToNearbyDrivers = dispatchToNearbyDrivers;
module.exports.closeRideBidding = closeRideBidding;
module.exports.refreshUserDetailsForUserId = refreshUserDetailsForUserId;
module.exports.saveRideDetails = saveRideDetails;
module.exports.getRideDetails = getRideDetails;
module.exports.getUserIdForRide = getUserIdForRide;
module.exports.getActiveRideIdForUser = getActiveRideIdForUser;
module.exports.touchUserActiveRide = touchUserActiveRide;
module.exports.finalizeAcceptedRide = finalizeAcceptedRide;
module.exports.removeRideFromAllInboxes = removeRideFromAllInboxes;
module.exports.upsertRideRouteMetrics = upsertRideRouteMetrics;
module.exports.emitCandidatesSummaryForDriverStateChange = emitCandidatesSummaryForDriverStateChange;
