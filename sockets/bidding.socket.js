// sockets/bidding.socket.js
const driverLocationService = require("../services/driverLocation.service");
const axios = require("axios"); // لضمان استدعاء Laravel API عند قبول العرض
const { getDistanceMeters } = require("../utils/geo.util");
const routeCacheL2 = require("../services/routeCacheL2.service");
const {
  getDriverAdminProfile,
} = require("../services/adminDriverProfile.service");
const { normalizePublicAssetUrl: normalizeAssetUrl } = require("../utils/imageUrl.util");
const {
  getUserDetails,
  getUserDetailsByToken,
  setUserDetails,
} = require("../store/users.store");
const {
  clearActiveRideByRideId,
  getActiveRideByDriver,
  getActiveDriverByRide,
  setActiveRide,
  clearActiveRideByDriver,
} = require("../store/activeRides.store");
const { getRideStatusSnapshot } = require("../store/rideStatusSnapshots.store");
const LARAVEL_GET_ROUTE_PATH =
  process.env.LARAVEL_GET_ROUTE_PATH || "/api/getRoute";

const DEBUG_EVENTS = process.env.DEBUG_SOCKET_EVENTS === "1";
const debugLog = (event, payload, socketId) => {
  if (!DEBUG_EVENTS) return;
  console.log("[bidding.socket]", event, "socket:", socketId, "payload:", payload);
};
const LOG_ROUTE_FAILURES = String(process.env.LOG_ROUTE_FAILURES || "1") === "1";
const ROUTE_FAILURE_LOG_THROTTLE_MS = Number.isFinite(
  Number(process.env.ROUTE_FAILURE_LOG_THROTTLE_MS)
)
  ? Math.max(1000, Number(process.env.ROUTE_FAILURE_LOG_THROTTLE_MS))
  : 60_000;
const throttledWarnAt = new Map();
const warnThrottled = (key, ...args) => {
  if (!LOG_ROUTE_FAILURES) return;
  const now = Date.now();
  const last = throttledWarnAt.get(key) ?? 0;
  if (now - last < ROUTE_FAILURE_LOG_THROTTLE_MS) return;
  throttledWarnAt.set(key, now);
  console.error(...args);
};

// ✅ In-memory Maps (no Redis)
const rideCandidates = new Map(); // rideId -> Set(driverId)
const driverRideInbox = new Map(); // driverId -> Map(rideId -> ridePayload)
const driverLastBidStatus = new Map(); // driverId -> { rideId, responded }
const acceptLocks = new Map(); // rideId -> timestamp (prevent double-accept race)
const driverAcceptLocks = new Map(); // driverId -> timestamp (prevent concurrent accepts for same driver)
const driverQueuedRide = new Map(); // driverId -> { ride_id, offered_price, ride_snapshot, reserved_at }
const rideDriverStates = new Map(); // rideId -> Map(driverId -> { status, notified_at, updated_at })
const dispatchInFlightByRide = new Map(); // rideId -> token

// ✅ NEW: per-driver patch sequence (ordering)
const driverPatchSeq = new Map(); // driverId -> number
const driverRidesListEmptyLoggedAt = new Map(); // driverId -> timestamp
const driverRecoveryNoopLoggedAt = new Map(); // `${source}:${driverId}` -> timestamp

const DRIVER_RIDES_LIST_EMPTY_LOG_THROTTLE_MS = Number.isFinite(
  Number(process.env.DRIVER_RIDES_LIST_EMPTY_LOG_THROTTLE_MS)
)
  ? Math.max(1000, Number(process.env.DRIVER_RIDES_LIST_EMPTY_LOG_THROTTLE_MS))
  : 30_000;
const DRIVER_RECOVERY_NOOP_LOG_THROTTLE_MS = Number.isFinite(
  Number(process.env.DRIVER_RECOVERY_NOOP_LOG_THROTTLE_MS)
)
  ? Math.max(1000, Number(process.env.DRIVER_RECOVERY_NOOP_LOG_THROTTLE_MS))
  : 30_000;
const DISPATCH_EXPAND_STOP_AFTER_NO_NEW_STAGES = Number.isFinite(
  Number(process.env.DISPATCH_EXPAND_STOP_AFTER_NO_NEW_STAGES)
)
  ? Math.max(1, Math.floor(Number(process.env.DISPATCH_EXPAND_STOP_AFTER_NO_NEW_STAGES)))
  : 2;

const RIDE_STATE_STALE_TTL_MS = Number.isFinite(Number(process.env.RIDE_STATE_STALE_TTL_MS))
  ? Math.max(60_000, Number(process.env.RIDE_STATE_STALE_TTL_MS))
  : 20 * 60 * 1000;
const RIDE_STATE_SWEEP_EVERY_MS = Number.isFinite(Number(process.env.RIDE_STATE_SWEEP_EVERY_MS))
  ? Math.max(30_000, Number(process.env.RIDE_STATE_SWEEP_EVERY_MS))
  : 60_000;
const rideStateActivityAt = new Map(); // rideId -> last activity timestamp
const touchRideState = (rideId) => {
  const safeRideId = toNumber(rideId);
  if (!safeRideId) return;
  rideStateActivityAt.set(safeRideId, Date.now());
};
const clearRideStateTouch = (rideId) => {
  const safeRideId = toNumber(rideId);
  if (!safeRideId) return;
  rideStateActivityAt.delete(safeRideId);
};

const ACCEPT_LOCK_TTL_MS = Number.isFinite(Number(process.env.ACCEPT_LOCK_TTL_MS))
  ? Number(process.env.ACCEPT_LOCK_TTL_MS)
  : 5000;

  const DRIVER_ACCEPT_LOCK_TTL_MS = Number.isFinite(Number(process.env.DRIVER_ACCEPT_LOCK_TTL_MS))
  ? Number(process.env.DRIVER_ACCEPT_LOCK_TTL_MS)
  : 4000;
const rideRoom = (rideId) => `ride:${rideId}`;
const userRoom = (userId) => `user:${userId}`;
const driverRoom = (driverId) => `driver:${driverId}`;

const toNumber = (v) => {
  if (v === null || v === undefined || v === "") return null;
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
};
const toBinaryFlag = (v) => {
  if (v === null || v === undefined || v === "") return null;
  if (typeof v === "boolean") return v ? 1 : 0;
  if (typeof v === "number") return v === 1 ? 1 : v === 0 ? 0 : null;
  if (typeof v === "string") {
    const s = v.trim().toLowerCase();
    if (["1", "true", "yes", "on"].includes(s)) return 1;
    if (["0", "false", "no", "off"].includes(s)) return 0;
    const n = Number(s);
    if (Number.isFinite(n)) return n === 1 ? 1 : n === 0 ? 0 : null;
    return null;
  }
  const n = Number(v);
  return Number.isFinite(n) ? (n === 1 ? 1 : n === 0 ? 0 : null) : null;
};
const toGenderFilter = (v) => {
  if (v === null || v === undefined || v === "") return null;
  if (typeof v === "number") return v === 1 ? 1 : v === 2 ? 2 : v === 0 ? 0 : null;

  const n = Number(v);
  if (Number.isFinite(n)) return n === 1 ? 1 : n === 2 ? 2 : n === 0 ? 0 : null;

  if (typeof v === "string") {
    const token = v.trim().toLowerCase();
    if (["male", "man", "m", "ذكر"].includes(token)) return 1;
    if (["female", "woman", "f", "انثى", "أنثى"].includes(token)) return 2;
    if (["0", "all", "any", "both", "none"].includes(token)) return 0;
  }

  return null;
};
const toTrimmedText = (v) => {
  if (v === null || v === undefined) return null;
  const s = String(v).trim();
  return s.length ? s : null;
};
const normalizeLanguageCode = (value) => {
  const raw = toTrimmedText(value);
  if (!raw) return null;
  const normalized = raw.toLowerCase().replace(/_/g, "-");
  if (normalized.startsWith("ar")) return "ar";
  if (normalized.startsWith("en")) return "en";
  return normalized;
};
const pickLocalizedText = (language, englishText, arabicText, fallbackText = null) => {
  const lang = normalizeLanguageCode(language);
  const en = toTrimmedText(englishText);
  const ar = toTrimmedText(arabicText);
  const fallback = toTrimmedText(fallbackText);

  if (lang === "ar") return ar ?? en ?? fallback;
  return en ?? ar ?? fallback;
};
const resolveLocalizedFieldVariants = (
  language,
  englishText = null,
  arabicText = null,
  fallbackText = null
) => {
  const en = toTrimmedText(englishText);
  const ar = toTrimmedText(arabicText);
  const fallback = toTrimmedText(fallbackText);
  return {
    localized: pickLocalizedText(language, en, ar, fallback),
    en: en ?? ar ?? fallback ?? null,
    ar: ar ?? en ?? fallback ?? null,
  };
};
const normalizeToken = (value) => {
  if (value === null || value === undefined) return null;
  const token = String(value).trim();
  return token.length ? token : null;
};
const isUnsafeNumericToken = (value) =>
  typeof value === "number" && Number.isFinite(value) && !Number.isSafeInteger(value);
const getSocketUserToken = (sock) => {
  return normalizeToken(
    sock?.userToken ??
      sock?.handshake?.auth?.user_token ??
      sock?.handshake?.auth?.token ??
      sock?.handshake?.query?.user_token ??
      sock?.handshake?.query?.token ??
      null
  );
};
const getLiveUserTokenFromRoom = (io, userId) => {
  const safeUserId = toNumber(userId);
  if (!io || !safeUserId) return null;

  const roomSockets = io?.sockets?.adapter?.rooms?.get(userRoom(safeUserId));
  if (!roomSockets || roomSockets.size === 0) return null;

  for (const socketId of roomSockets) {
    const roomSocket = io?.sockets?.sockets?.get(socketId);
    const token = getSocketUserToken(roomSocket);
    if (token) return token;
  }

  return null;
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
const toPositiveRouteDistanceKm = (v) => {
  const parsed = toRouteMetricNumber(v);
  return parsed !== null && parsed > 0 ? parsed : null;
};
const normalizeCoordForRouteKey = (value) => {
  const safe = toNumber(value);
  if (safe === null) return null;
  return safe.toFixed(5);
};
const buildRouteMetricsCacheKey = (startLat, startLong, endLat, endLong) => {
  const aLat = normalizeCoordForRouteKey(startLat);
  const aLong = normalizeCoordForRouteKey(startLong);
  const bLat = normalizeCoordForRouteKey(endLat);
  const bLong = normalizeCoordForRouteKey(endLong);
  if (aLat === null || aLong === null || bLat === null || bLong === null) return null;
  return `${aLat},${aLong}->${bLat},${bLong}`;
};
const buildRouteL2CacheKey = (cacheKey) =>
  cacheKey ? `route:v1:${cacheKey}` : null;
const buildRouteL2LockKey = (cacheKey) =>
  cacheKey ? `route:lock:v1:${cacheKey}` : null;
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, Math.max(0, ms)));
const getRandomLockRecheckDelayMs = () => 100 + Math.floor(Math.random() * 101);
const getCachedRouteMetrics = (cacheKey) => {
  if (!cacheKey || ROUTE_API_CACHE_TTL_MS <= 0) return null;
  const cached = routeMetricsCache.get(cacheKey);
  if (!cached) return null;
  if (Date.now() - cached.at > ROUTE_API_CACHE_TTL_MS) {
    routeMetricsCache.delete(cacheKey);
    return null;
  }
  return cached.value ? { ...cached.value } : null;
};
const setCachedRouteMetrics = (cacheKey, value) => {
  if (!cacheKey || ROUTE_API_CACHE_TTL_MS <= 0 || !value || typeof value !== "object") return;
  routeMetricsCache.set(cacheKey, {
    at: Date.now(),
    value: { ...value },
  });
};
const mapWithConcurrency = async (items = [], concurrency = 4, mapper = async () => null) => {
  const list = Array.isArray(items) ? items : [];
  if (list.length === 0) return [];
  const safeConcurrency = Math.max(1, Math.floor(toNumber(concurrency) ?? 1));
  const results = new Array(list.length);
  let index = 0;
  let active = 0;
  return new Promise((resolve) => {
    const launch = () => {
      if (index >= list.length && active === 0) {
        resolve(results);
        return;
      }
      while (active < safeConcurrency && index < list.length) {
        const current = index++;
        active += 1;
        Promise.resolve(mapper(list[current], current))
          .then((result) => {
            results[current] = result;
          })
          .catch((error) => {
            console.warn("[mapWithConcurrency] worker failed:", error?.message || error);
            results[current] = null;
          })
          .finally(() => {
            active -= 1;
            launch();
          });
      }
    };
    launch();
  });
};
const pickFirstValue = (...values) => {
  for (const v of values) {
    if (v !== undefined && v !== null && v !== "") return v;
  }
  return null;
};
const pickFirstPresentValueWithSource = (candidates = []) => {
  for (const candidate of candidates) {
    if (!candidate || typeof candidate !== "object") continue;
    const value = candidate.value;
    if (value === undefined || value === null || value === "") continue;
    return {
      value,
      source: candidate.source ?? null,
    };
  }
  return {
    value: null,
    source: null,
  };
};
const parseMaybeJsonObject = (value) => {
  if (!value) return null;
  if (typeof value === "object" && !Array.isArray(value)) return value;
  if (typeof value !== "string") return null;
  try {
    const parsed = JSON.parse(value);
    return parsed && typeof parsed === "object" && !Array.isArray(parsed) ? parsed : null;
  } catch (_) {
    return null;
  }
};
const getNestedRidePayload = (payload = {}) => {
  if (!payload || typeof payload !== "object") return null;
  return (
    parseMaybeJsonObject(payload?.ride) ??
    parseMaybeJsonObject(payload?.ride_data) ??
    parseMaybeJsonObject(payload?.ride_model) ??
    null
  );
};
const toObjectPayload = (value) => {
  if (!value) return null;
  if (typeof value === "object" && !Array.isArray(value)) return value;
  return parseMaybeJsonObject(value);
};
const resolveAdditionalRemarks = (...sources) => {
  for (const source of sources) {
    if (!source || typeof source !== "object") continue;

    const dispatchPayload = toObjectPayload(
      source?.dispatch_payload ?? source?.dispatchPayload ?? null
    );
    const rideModel = toObjectPayload(source?.ride_model ?? source?.rideModel ?? null);
    const nestedPayload = toObjectPayload(source?.payload ?? null);
    const nestedRide = getNestedRidePayload(source);

    const scopes = [
      source,
      source?.ride_details,
      source?.meta,
      dispatchPayload,
      dispatchPayload?.ride_details,
      dispatchPayload?.meta,
      rideModel,
      rideModel?.ride_details,
      rideModel?.meta,
      nestedPayload,
      nestedPayload?.ride_details,
      nestedPayload?.meta,
      nestedRide,
      nestedRide?.ride_details,
      nestedRide?.meta,
    ];

    for (const scope of scopes) {
      if (!scope || typeof scope !== "object") continue;
      const resolved = toTrimmedText(
        pickFirstValue(
          scope?.additional_remarks,
          scope?.additional_remark,
          scope?.additional_request,
          scope?.pickup_note,
          scope?.additionalRemarks,
          scope?.additionalRemark,
          scope?.additionalRequest,
          scope?.pickupNote
        )
      );
      if (resolved !== null) return resolved;
    }
  }

  return null;
};
const resolveDispatchPreferenceInfo = (payload = {}, keys = []) => {
  if (!payload || typeof payload !== "object" || !Array.isArray(keys) || keys.length === 0) {
    return { provided: false, value: null, key: null };
  }

  const dispatchPayload = toObjectPayload(
    payload?.dispatch_payload ?? payload?.dispatchPayload ?? null
  );
  const rideModel = toObjectPayload(payload?.ride_model ?? payload?.rideModel ?? null);
  const nestedPayload = toObjectPayload(payload?.payload ?? null);
  const nestedRide = getNestedRidePayload(payload);

  const scopes = [
    payload,
    payload?.ride_details,
    payload?.meta,
    dispatchPayload,
    dispatchPayload?.ride_details,
    dispatchPayload?.meta,
    rideModel,
    rideModel?.ride_details,
    rideModel?.meta,
    nestedPayload,
    nestedPayload?.ride_details,
    nestedPayload?.meta,
    nestedRide,
    nestedRide?.ride_details,
    nestedRide?.meta,
  ];

  for (const scope of scopes) {
    if (!scope || typeof scope !== "object") continue;
    for (const key of keys) {
      if (!Object.prototype.hasOwnProperty.call(scope, key)) continue;
      return {
        provided: true,
        value: scope[key],
        key,
      };
    }
  }

  return { provided: false, value: null, key: null };
};
const getRideIdFromDriverPayload = (payload = {}) => {
  const nestedRide = getNestedRidePayload(payload);
  return pickFirstValue(
    toNumber(payload?.ride_id),
    toNumber(payload?.request_id),
    toNumber(payload?.rideId),
    toNumber(nestedRide?.ride_id),
    toNumber(nestedRide?.request_id),
    toNumber(payload?.ride_details?.ride_id),
    toNumber(payload?.meta?.ride_id)
  );
};
const getOfferedPriceFromDriverPayload = (payload = {}) => {
  const nestedRide = getNestedRidePayload(payload);
  return pickFirstValue(
    toNumber(payload?.offered_price),
    toNumber(payload?.price),
    toNumber(payload?.user_bid_price),
    toNumber(payload?.offer_price),
    toNumber(payload?.bid_price),
    toNumber(payload?.amount),
    toNumber(nestedRide?.offered_price),
    toNumber(nestedRide?.price),
    toNumber(nestedRide?.user_bid_price),
    toNumber(payload?.ride_details?.offered_price),
    toNumber(payload?.ride_details?.price),
    toNumber(payload?.ride_details?.user_bid_price),
    toNumber(payload?.meta?.offered_price),
    toNumber(payload?.meta?.price),
    toNumber(payload?.meta?.user_bid_price)
  );
};
const extractDriverIdentity = (...sources) => {
  let providerId = null;
  let driverServiceId = null;
  let driverDetailId = null;

  for (const source of sources) {
    if (!source || typeof source !== "object") continue;

    if (providerId === null) {
      providerId = toNumber(
        source?.provider_id ??
          source?.providerId ??
          source?.driver_provider_id ??
          null
      );
    }
    if (driverServiceId === null) {
      driverServiceId = toNumber(
        source?.driver_service_id ??
          source?.driverServiceId ??
          source?.driver_service ??
          null
      );
    }
    if (driverDetailId === null) {
      driverDetailId = toNumber(
        source?.driver_detail_id ??
          source?.driverDetailId ??
          source?.driver_details_id ??
          null
      );
    }
  }

  return {
    provider_id: providerId,
    driver_service_id: driverServiceId,
    driver_detail_id: driverDetailId,
  };
};

const normalizeDriverDetailsPayload = (details = null, fallbackMeta = null) => {
  const src = details && typeof details === "object" ? details : {};
  const meta = fallbackMeta && typeof fallbackMeta === "object" ? fallbackMeta : {};

  const driverName = toTrimmedText(
    pickFirstValue(src.driver_name, src.driverName, src.name, meta.driver_name, meta.name)
  );
  const vehicleType = toTrimmedText(
    pickFirstValue(
      src.vehicle_type,
      src.vehicle_type_name,
      src.vehicleType,
      meta.vehicle_type,
      meta.vehicle_type_name
    )
  );
  const vehicleNumber = toTrimmedText(
    pickFirstValue(
      src.vehicle_number,
      src.plat_no,
      src.plate_no,
      src.vehicle_no,
      src.vehicle_plate,
      meta.vehicle_number,
      meta.plat_no,
      meta.plate_no,
      meta.vehicle_no,
      meta.vehicle_plate
    )
  );
  const vehicleCompany = toTrimmedText(
    pickFirstValue(
      src.vehicle_company,
      src.vehicle_manufacture_name,
      src.company,
      src.brand,
      src.make,
      src.manufacturer_name,
      src.vehicle_company_en,
      src.vehicle_company_ar,
      src.vehicle_manufacture_name_en,
      src.vehicle_manufacture_name_ar,
      src.manufacturer_name_en,
      src.manufacturer_name_ar,
      meta.vehicle_company,
      meta.vehicle_manufacture_name,
      meta.company,
      meta.brand,
      meta.make,
      meta.manufacturer_name,
      meta.vehicle_company_en,
      meta.vehicle_company_ar,
      meta.vehicle_manufacture_name_en,
      meta.vehicle_manufacture_name_ar,
      meta.manufacturer_name_en,
      meta.manufacturer_name_ar
    )
  );
  const vehicleCompanyEn = toTrimmedText(
    pickFirstValue(
      src.vehicle_company_en,
      src.vehicle_manufacture_name_en,
      src.manufacturer_name_en,
      meta.vehicle_company_en,
      meta.vehicle_manufacture_name_en,
      meta.manufacturer_name_en
    )
  );
  const vehicleCompanyAr = toTrimmedText(
    pickFirstValue(
      src.vehicle_company_ar,
      src.vehicle_manufacture_name_ar,
      src.manufacturer_name_ar,
      meta.vehicle_company_ar,
      meta.vehicle_manufacture_name_ar,
      meta.manufacturer_name_ar
    )
  );
  const modelName = toTrimmedText(
    pickFirstValue(
      src.model_name,
      src.model,
      src.vehicle_model,
      src.vehicle_model_name,
      src.model_name_en,
      src.model_name_ar,
      src.vehicle_model_name_en,
      src.vehicle_model_name_ar,
      meta.model_name,
      meta.model,
      meta.vehicle_model,
      meta.vehicle_model_name,
      meta.model_name_en,
      meta.model_name_ar,
      meta.vehicle_model_name_en,
      meta.vehicle_model_name_ar
    )
  );
  const modelNameEn = toTrimmedText(
    pickFirstValue(
      src.model_name_en,
      src.vehicle_model_name_en,
      meta.model_name_en,
      meta.vehicle_model_name_en
    )
  );
  const modelNameAr = toTrimmedText(
    pickFirstValue(
      src.model_name_ar,
      src.vehicle_model_name_ar,
      meta.model_name_ar,
      meta.vehicle_model_name_ar
    )
  );
  const vehicleColor = toTrimmedText(
    pickFirstValue(
      src.vehicle_color,
      src.color,
      src.vehicle_color_en,
      src.vehicle_color_ar,
      src.color_en,
      src.color_ar,
      meta.vehicle_color,
      meta.color,
      meta.vehicle_color_en,
      meta.vehicle_color_ar,
      meta.color_en,
      meta.color_ar
    )
  );
  const vehicleColorEn = toTrimmedText(
    pickFirstValue(src.vehicle_color_en, src.color_en, meta.vehicle_color_en, meta.color_en)
  );
  const vehicleColorAr = toTrimmedText(
    pickFirstValue(src.vehicle_color_ar, src.color_ar, meta.vehicle_color_ar, meta.color_ar)
  );
  const vehicleManufacturer = toTrimmedText(
    pickFirstValue(
      src.vehicle_manufacturer,
      src.manufacturer,
      src.manufacturer_name,
      src.vehicle_manufacture_name,
      src.vehicle_manufacturer_en,
      src.vehicle_manufacturer_ar,
      src.manufacturer_name_en,
      src.manufacturer_name_ar,
      src.vehicle_manufacture_name_en,
      src.vehicle_manufacture_name_ar,
      src.make,
      src.brand,
      meta.vehicle_manufacturer,
      meta.manufacturer,
      meta.manufacturer_name,
      meta.vehicle_manufacture_name,
      meta.vehicle_manufacturer_en,
      meta.vehicle_manufacturer_ar,
      meta.manufacturer_name_en,
      meta.manufacturer_name_ar,
      meta.vehicle_manufacture_name_en,
      meta.vehicle_manufacture_name_ar,
      meta.make,
      meta.brand
    )
  );
  const vehicleManufacturerEn = toTrimmedText(
    pickFirstValue(
      src.vehicle_manufacturer_en,
      src.manufacturer_name_en,
      src.vehicle_manufacture_name_en,
      meta.vehicle_manufacturer_en,
      meta.manufacturer_name_en,
      meta.vehicle_manufacture_name_en
    )
  );
  const vehicleManufacturerAr = toTrimmedText(
    pickFirstValue(
      src.vehicle_manufacturer_ar,
      src.manufacturer_name_ar,
      src.vehicle_manufacture_name_ar,
      meta.vehicle_manufacturer_ar,
      meta.manufacturer_name_ar,
      meta.vehicle_manufacture_name_ar
    )
  );
  const resolvedLanguage = normalizeLanguageCode(
    pickFirstValue(src.user_language, src.language, meta.user_language, meta.language)
  );
  const localizedVehicleCompany = pickLocalizedText(
    resolvedLanguage,
    vehicleCompanyEn,
    vehicleCompanyAr,
    vehicleCompany
  );
  const localizedModelName = pickLocalizedText(
    resolvedLanguage,
    modelNameEn,
    modelNameAr,
    modelName
  );
  const localizedVehicleColor = pickLocalizedText(
    resolvedLanguage,
    vehicleColorEn,
    vehicleColorAr,
    vehicleColor
  );
  const localizedVehicleManufacturer = pickLocalizedText(
    resolvedLanguage,
    vehicleManufacturerEn,
    vehicleManufacturerAr,
    vehicleManufacturer ?? localizedVehicleCompany
  );

  const modelYearRaw = pickFirstValue(
    src.model_year,
    src.manufacture_year,
    src.vehicle_year,
    src.year,
    meta.model_year,
    meta.manufacture_year,
    meta.vehicle_year,
    meta.year
  );
  const modelYearNumeric = toNumber(modelYearRaw);
  const modelYear = modelYearNumeric ?? toTrimmedText(modelYearRaw);

  const ratingRaw = pickFirstValue(src.rating, src.driver_rating, meta.rating, meta.driver_rating);
  const rating = toNumber(ratingRaw) ?? ratingRaw ?? null;
  const driverImage = normalizeDriverImageUrl(
    toTrimmedText(
      pickFirstValue(
        src.driver_image,
        src.driver_image_url,
        src.driver_profile_image,
        src.provider_image,
        src.profile_image,
        src.avatar,
        src.image,
        meta.driver_image,
        meta.driver_image_url,
        meta.driver_profile_image,
        meta.provider_image,
        meta.profile_image,
        meta.avatar,
        meta.image
      )
    )
  );

  return {
    driver_name: driverName,
    vehicle_type: vehicleType,
    vehicle_number: vehicleNumber,
    vehicle_company: localizedVehicleCompany ?? vehicleCompany,
    vehicle_company_en: vehicleCompanyEn ?? vehicleCompany ?? null,
    vehicle_company_ar: vehicleCompanyAr ?? vehicleCompany ?? null,
    model_name: localizedModelName ?? modelName,
    model_name_en: modelNameEn ?? modelName ?? null,
    model_name_ar: modelNameAr ?? modelName ?? null,
    model_year: modelYear,
    vehicle_color: localizedVehicleColor ?? vehicleColor,
    vehicle_color_en: vehicleColorEn ?? vehicleColor ?? null,
    vehicle_color_ar: vehicleColorAr ?? vehicleColor ?? null,
    vehicle_manufacturer: localizedVehicleManufacturer ?? vehicleManufacturer,
    vehicle_manufacturer_en:
      vehicleManufacturerEn ?? vehicleCompanyEn ?? vehicleManufacturer ?? null,
    vehicle_manufacturer_ar:
      vehicleManufacturerAr ?? vehicleCompanyAr ?? vehicleManufacturer ?? null,
    rating,
    driver_image: driverImage,

    // aliases for frontend flexibility
    vehicle_type_name: vehicleType,
    plat_no: vehicleNumber,
    plate_no: vehicleNumber,
    manufacturer_name: localizedVehicleManufacturer ?? vehicleManufacturer,
    manufacturer_name_en:
      vehicleManufacturerEn ?? vehicleCompanyEn ?? vehicleManufacturer ?? null,
    manufacturer_name_ar:
      vehicleManufacturerAr ?? vehicleCompanyAr ?? vehicleManufacturer ?? null,
    vehicle_manufacture_name: localizedVehicleManufacturer ?? vehicleManufacturer,
    vehicle_manufacture_name_en:
      vehicleManufacturerEn ?? vehicleCompanyEn ?? vehicleManufacturer ?? null,
    vehicle_manufacture_name_ar:
      vehicleManufacturerAr ?? vehicleCompanyAr ?? vehicleManufacturer ?? null,
    vehicle_model_name: localizedModelName ?? modelName,
    vehicle_model_name_en: modelNameEn ?? modelName ?? null,
    vehicle_model_name_ar: modelNameAr ?? modelName ?? null,
    vehicle_make: localizedVehicleCompany ?? vehicleCompany,
  };
};
const resolveDriverIdFromPayload = (payload = {}, explicitDriverId = null) => {
  const p = payload && typeof payload === "object" ? payload : {};
  const details =
    p?.driver_details && typeof p.driver_details === "object" ? p.driver_details : {};
  const rideDetails =
    p?.ride_details && typeof p.ride_details === "object" ? p.ride_details : {};
  const rideDriverDetails =
    rideDetails?.driver_details && typeof rideDetails.driver_details === "object"
      ? rideDetails.driver_details
      : {};
  const meta = p?.meta && typeof p.meta === "object" ? p.meta : {};

  const identity = extractDriverIdentity(p, details, rideDetails, rideDriverDetails, meta);
  let resolvedDriverId =
    toNumber(explicitDriverId) ??
    toNumber(p?.driver_id) ??
    toNumber(details?.driver_id) ??
    toNumber(rideDetails?.driver_id) ??
    toNumber(rideDriverDetails?.driver_id) ??
    identity?.provider_id ??
    null;

  const rideId = toNumber(p?.ride_id ?? rideDetails?.ride_id ?? null);
  if (!resolvedDriverId && rideId) {
    resolvedDriverId = toNumber(getActiveDriverByRide(rideId));
  }

  return resolvedDriverId ?? null;
};
const resolveDriverImageFromPayloadWithSource = (payload = {}, explicitDriverId = null) => {
  const p = payload && typeof payload === "object" ? payload : {};
  const details =
    p?.driver_details && typeof p.driver_details === "object" ? p.driver_details : {};
  const rideDetails =
    p?.ride_details && typeof p.ride_details === "object" ? p.ride_details : {};
  const rideDriverDetails =
    rideDetails?.driver_details && typeof rideDetails.driver_details === "object"
      ? rideDetails.driver_details
      : {};
  const meta = p?.meta && typeof p.meta === "object" ? p.meta : {};

  const driverId = resolveDriverIdFromPayload(p, explicitDriverId);
  const memoryMeta = driverId ? driverLocationService.getMeta(driverId) || {} : {};

  const rideId = toNumber(p?.ride_id ?? rideDetails?.ride_id ?? null);
  const snapshot = rideId ? getFullRideSnapshot(rideId, driverId) : null;
  const snapshotDetails =
    snapshot?.driver_details && typeof snapshot.driver_details === "object"
      ? snapshot.driver_details
      : {};
  const snapshotMeta =
    snapshot?.meta && typeof snapshot.meta === "object" ? snapshot.meta : {};

  const imagePick = pickFirstPresentValueWithSource([
    { source: "payload.driver_image", value: p?.driver_image },
    { source: "payload.driver_image_url", value: p?.driver_image_url },
    { source: "payload.driver_profile_image", value: p?.driver_profile_image },
    { source: "payload.provider_image", value: p?.provider_image },
    { source: "payload.profile_image", value: p?.profile_image },
    { source: "payload.avatar", value: p?.avatar },
    { source: "payload.image", value: p?.image },
    { source: "driver_details.driver_image", value: details?.driver_image },
    { source: "driver_details.driver_image_url", value: details?.driver_image_url },
    { source: "driver_details.driver_profile_image", value: details?.driver_profile_image },
    { source: "driver_details.provider_image", value: details?.provider_image },
    { source: "driver_details.profile_image", value: details?.profile_image },
    { source: "driver_details.avatar", value: details?.avatar },
    { source: "driver_details.image", value: details?.image },
    { source: "ride_details.driver_image", value: rideDetails?.driver_image },
    { source: "ride_details.driver_image_url", value: rideDetails?.driver_image_url },
    { source: "ride_details.driver_profile_image", value: rideDetails?.driver_profile_image },
    { source: "ride_details.provider_image", value: rideDetails?.provider_image },
    { source: "ride_details.driver_details.driver_image", value: rideDriverDetails?.driver_image },
    { source: "ride_details.driver_details.driver_image_url", value: rideDriverDetails?.driver_image_url },
    { source: "ride_details.driver_details.driver_profile_image", value: rideDriverDetails?.driver_profile_image },
    { source: "ride_details.driver_details.provider_image", value: rideDriverDetails?.provider_image },
    { source: "ride_details.driver_details.profile_image", value: rideDriverDetails?.profile_image },
    { source: "ride_details.driver_details.avatar", value: rideDriverDetails?.avatar },
    { source: "ride_details.driver_details.image", value: rideDriverDetails?.image },
    { source: "meta.driver_image", value: meta?.driver_image },
    { source: "meta.driver_image_url", value: meta?.driver_image_url },
    { source: "meta.driver_profile_image", value: meta?.driver_profile_image },
    { source: "meta.provider_image", value: meta?.provider_image },
    { source: "meta.profile_image", value: meta?.profile_image },
    { source: "meta.avatar", value: meta?.avatar },
    { source: "meta.image", value: meta?.image },
    { source: "snapshot.driver_image", value: snapshot?.driver_image },
    { source: "snapshot.driver_image_url", value: snapshot?.driver_image_url },
    { source: "snapshot.driver_profile_image", value: snapshot?.driver_profile_image },
    { source: "snapshot.provider_image", value: snapshot?.provider_image },
    { source: "snapshot.driver_details.driver_image", value: snapshotDetails?.driver_image },
    { source: "snapshot.driver_details.driver_image_url", value: snapshotDetails?.driver_image_url },
    { source: "snapshot.driver_details.driver_profile_image", value: snapshotDetails?.driver_profile_image },
    { source: "snapshot.driver_details.provider_image", value: snapshotDetails?.provider_image },
    { source: "snapshot.driver_details.profile_image", value: snapshotDetails?.profile_image },
    { source: "snapshot.driver_details.avatar", value: snapshotDetails?.avatar },
    { source: "snapshot.driver_details.image", value: snapshotDetails?.image },
    { source: "snapshot.meta.driver_image", value: snapshotMeta?.driver_image },
    { source: "snapshot.meta.driver_image_url", value: snapshotMeta?.driver_image_url },
    { source: "snapshot.meta.driver_profile_image", value: snapshotMeta?.driver_profile_image },
    { source: "snapshot.meta.provider_image", value: snapshotMeta?.provider_image },
    { source: "snapshot.meta.profile_image", value: snapshotMeta?.profile_image },
    { source: "snapshot.meta.avatar", value: snapshotMeta?.avatar },
    { source: "snapshot.meta.image", value: snapshotMeta?.image },
    { source: "memory.meta.driver_image", value: memoryMeta?.driver_image },
    { source: "memory.meta.driver_image_url", value: memoryMeta?.driver_image_url },
    { source: "memory.meta.driver_profile_image", value: memoryMeta?.driver_profile_image },
    { source: "memory.meta.provider_image", value: memoryMeta?.provider_image },
    { source: "memory.meta.profile_image", value: memoryMeta?.profile_image },
    { source: "memory.meta.avatar", value: memoryMeta?.avatar },
    { source: "memory.meta.image", value: memoryMeta?.image },
  ]);
  const normalizedImage = normalizeDriverImageUrl(toTrimmedText(imagePick.value));
  return {
    image: normalizedImage,
    source: normalizedImage ? imagePick.source : null,
  };
};
const resolveDriverImageFromPayload = (payload = {}, explicitDriverId = null) =>
  resolveDriverImageFromPayloadWithSource(payload, explicitDriverId)?.image ?? null;
const withDriverImage = (payload = {}, explicitDriverId = null) => {
  if (!payload || typeof payload !== "object") return payload;

  const resolvedDriverImage = resolveDriverImageFromPayloadWithSource(
    payload,
    explicitDriverId
  );
  const resolvedImage = resolvedDriverImage?.image ?? null;
  const resolvedSource = resolvedDriverImage?.source ?? null;
  if (!resolvedImage) return payload;

  let patched = payload;
  let changed = false;

  if (!toTrimmedText(patched?.driver_image)) {
    patched = { ...patched, driver_image: resolvedImage };
    changed = true;
  }
  if (!toTrimmedText(patched?.driver_image_url)) {
    patched = { ...patched, driver_image_url: resolvedImage };
    changed = true;
  }
  if (!toTrimmedText(patched?.profile_image)) {
    patched = { ...patched, profile_image: resolvedImage };
    changed = true;
  }
  if (!toTrimmedText(patched?.driver_profile_image)) {
    patched = { ...patched, driver_profile_image: resolvedImage };
    changed = true;
  }
  if (!toTrimmedText(patched?.provider_image)) {
    patched = { ...patched, provider_image: resolvedImage };
    changed = true;
  }
  if (!toTrimmedText(patched?.driver_image_source) && resolvedSource) {
    patched = { ...patched, driver_image_source: resolvedSource };
    changed = true;
  }

  if (patched?.driver_details && typeof patched.driver_details === "object") {
    const existingDetails = patched.driver_details;
    const nextDetails = {
      ...existingDetails,
      ...(toTrimmedText(existingDetails?.driver_image)
        ? {}
        : { driver_image: resolvedImage }),
      ...(toTrimmedText(existingDetails?.driver_image_url)
        ? {}
        : { driver_image_url: resolvedImage }),
      ...(toTrimmedText(existingDetails?.profile_image)
        ? {}
        : { profile_image: resolvedImage }),
      ...(toTrimmedText(existingDetails?.driver_profile_image)
        ? {}
        : { driver_profile_image: resolvedImage }),
      ...(toTrimmedText(existingDetails?.provider_image)
        ? {}
        : { provider_image: resolvedImage }),
      ...(toTrimmedText(existingDetails?.driver_image_source) || !resolvedSource
        ? {}
        : { driver_image_source: resolvedSource }),
    };
    patched = { ...patched, driver_details: nextDetails };
    changed = true;
  }

  if (patched?.ride_details && typeof patched.ride_details === "object") {
    const existingRideDetails = patched.ride_details;
    const nextRideDetails = {
      ...existingRideDetails,
      ...(toTrimmedText(existingRideDetails?.driver_image)
        ? {}
        : { driver_image: resolvedImage }),
      ...(toTrimmedText(existingRideDetails?.driver_image_url)
        ? {}
        : { driver_image_url: resolvedImage }),
      ...(toTrimmedText(existingRideDetails?.profile_image)
        ? {}
        : { profile_image: resolvedImage }),
      ...(toTrimmedText(existingRideDetails?.driver_profile_image)
        ? {}
        : { driver_profile_image: resolvedImage }),
      ...(toTrimmedText(existingRideDetails?.provider_image)
        ? {}
        : { provider_image: resolvedImage }),
      ...(toTrimmedText(existingRideDetails?.driver_image_source) || !resolvedSource
        ? {}
        : { driver_image_source: resolvedSource }),
    };
    patched = { ...patched, ride_details: nextRideDetails };
    changed = true;
  }

  return changed ? patched : payload;
};
const getRoomSocketCount = (io, roomName) => {
  if (!io || !roomName) return 0;
  const roomSet = io?.sockets?.adapter?.rooms?.get(roomName);
  return roomSet ? roomSet.size : 0;
};
const DISPATCH_EMIT_RETRY_MAX_ATTEMPTS = Number.isFinite(
  Number(process.env.DISPATCH_EMIT_RETRY_MAX_ATTEMPTS)
)
  ? Math.max(0, Number(process.env.DISPATCH_EMIT_RETRY_MAX_ATTEMPTS))
  : 3;
const DISPATCH_EMIT_RETRY_BASE_DELAY_MS = Number.isFinite(
  Number(process.env.DISPATCH_EMIT_RETRY_BASE_DELAY_MS)
)
  ? Math.max(200, Number(process.env.DISPATCH_EMIT_RETRY_BASE_DELAY_MS))
  : 1200;
const DEBUG_REMOVE_CANDIDATE_STACK =
  String(process.env.DEBUG_REMOVE_CANDIDATE_STACK || "0") === "1";
const pendingBidEmitRetryTimers = new Map(); // `${rideId}:${driverId}` -> timeout
const pendingBidEmitRetryKey = (rideId, driverId) => `${rideId}:${driverId}`;

const clearPendingBidEmitRetry = (rideId, driverId) => {
  const safeRideId = toNumber(rideId);
  const safeDriverId = toNumber(driverId);
  if (!safeRideId || !safeDriverId) return;

  const key = pendingBidEmitRetryKey(safeRideId, safeDriverId);
  const timer = pendingBidEmitRetryTimers.get(key);
  if (timer) {
    clearTimeout(timer);
  }
  pendingBidEmitRetryTimers.delete(key);
};

const clearPendingBidEmitRetriesForRide = (rideId) => {
  const safeRideId = toNumber(rideId);
  if (!safeRideId) return;

  const prefix = `${safeRideId}:`;
  for (const [key, timer] of pendingBidEmitRetryTimers.entries()) {
    if (!key.startsWith(prefix)) continue;
    clearTimeout(timer);
    pendingBidEmitRetryTimers.delete(key);
  }
};
const buildNewBidEmitDebugSnapshot = (payload = {}) => {
  const p = payload && typeof payload === "object" ? payload : {};
  const details =
    p?.driver_details && typeof p.driver_details === "object"
      ? p.driver_details
      : {};

  return {
    ride_id: toNumber(p?.ride_id),
    user_id: toNumber(p?.user_id ?? p?.user_details?.user_id ?? null),
    driver_id:
      toNumber(
        p?.driver_id ??
          p?.driver_detail_id ??
          p?.driver_details_id ??
          details?.driver_id ??
          details?.provider_id ??
          details?.driver_detail_id
      ) ?? null,
    driver_name: toTrimmedText(p?.driver_name ?? details?.driver_name ?? null),
    vehicle_type: toTrimmedText(
      p?.vehicle_type ??
        p?.vehicle_type_name ??
        details?.vehicle_type ??
        details?.vehicle_type_name ??
        null
    ),
    vehicle_company: toTrimmedText(
      p?.vehicle_company ?? details?.vehicle_company ?? details?.company ?? null
    ),
    model_name: toTrimmedText(
      p?.model_name ?? details?.model_name ?? details?.model ?? null
    ),
    model_year: p?.model_year ?? details?.model_year ?? null,
    vehicle_color: toTrimmedText(
      p?.vehicle_color ?? details?.vehicle_color ?? details?.color ?? null
    ),
    vehicle_number: toTrimmedText(
      p?.vehicle_number ??
        p?.plat_no ??
        details?.vehicle_number ??
        details?.plat_no ??
        null
    ),
    additional_remarks: resolveAdditionalRemarks(p),
  };
};
const buildDriverIdentityPayload = (identity = {}, legacyDriverId = null) => {
  const payload = {};
  const safeLegacyDriverId = toNumber(legacyDriverId);
  const safeProviderId = toNumber(identity?.provider_id);
  const safeDriverServiceId = toNumber(identity?.driver_service_id);
  const safeDriverDetailId = toNumber(identity?.driver_detail_id);

  if (safeLegacyDriverId) payload.driver_id = safeLegacyDriverId;
  if (safeProviderId) payload.provider_id = safeProviderId;
  if (safeDriverServiceId) payload.driver_service_id = safeDriverServiceId;
  if (safeDriverDetailId) {
    payload.driver_detail_id = safeDriverDetailId;
    payload.driver_details_id = safeDriverDetailId;
  }

  return payload;
};
function resolveRideDriverIdentity(rideId, payload = {}, options = {}) {
  const legacyDriverId = toNumber(payload?.driver_id);
  const requestedIdentity = extractDriverIdentity(payload);
  const states = getRideDriverStatesMap(rideId);
  let matchedState = options?.state ?? null;

  if (!matchedState && states) {
    for (const state of states.values()) {
      if (!state || typeof state !== "object") continue;

      const stateProviderId = toNumber(state?.provider_id ?? state?.driver_id);
      const stateDriverServiceId = toNumber(state?.driver_service_id);
      const stateDriverDetailId = toNumber(state?.driver_detail_id);

      if (
        (requestedIdentity.provider_id && stateProviderId === requestedIdentity.provider_id) ||
        (requestedIdentity.driver_service_id &&
          stateDriverServiceId === requestedIdentity.driver_service_id) ||
        (requestedIdentity.driver_detail_id &&
          stateDriverDetailId === requestedIdentity.driver_detail_id) ||
        (legacyDriverId &&
          (stateProviderId === legacyDriverId ||
            stateDriverServiceId === legacyDriverId ||
            stateDriverDetailId === legacyDriverId))
      ) {
        matchedState = state;
        break;
      }
    }
  }

  const fallbackIdentity = extractDriverIdentity(
    matchedState,
    options?.meta,
    options?.snapshot,
    options?.rideDetails
  );

  const providerId =
    requestedIdentity.provider_id ??
    fallbackIdentity.provider_id ??
    (matchedState ? toNumber(matchedState?.driver_id) : null) ??
    (requestedIdentity.driver_detail_id === null &&
    requestedIdentity.driver_service_id === null
      ? legacyDriverId
      : null);

  let driverDetailId =
    requestedIdentity.driver_detail_id ??
    fallbackIdentity.driver_detail_id;
  if (
    driverDetailId === null &&
    legacyDriverId &&
    providerId !== null &&
    legacyDriverId !== providerId
  ) {
    driverDetailId = legacyDriverId;
  }

  return {
    provider_id: providerId,
    driver_service_id:
      requestedIdentity.driver_service_id ?? fallbackIdentity.driver_service_id,
    driver_detail_id: driverDetailId,
  };
}

const round2 = (v) => (Number.isFinite(v) ? Math.round(v * 100) / 100 : null);
const buildPriceBounds = (baseFare, distanceKm = null) => {
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
    // Business rule: min is always 70% of computed trip price.
    min_price: round2(base * 0.7),
    max_price: round2(base * 2),
  };
};

const normalizePriceBoundsPair = (minRaw, maxRaw) => {
  const min = toNumber(minRaw);
  const max = toNumber(maxRaw);

  if (min !== null && max !== null && min > max) {
    return { min_price: round2(max), max_price: round2(min), swapped: true };
  }

  return {
    min_price: min !== null ? round2(min) : null,
    max_price: max !== null ? round2(max) : null,
    swapped: false,
  };
};

const getPayloadDistanceKm = (payload = {}) => {
  const distance = pickFirstValue(
    toRouteMetricNumber(payload?.distance_km),
    toRouteMetricNumber(payload?.route_api_distance_km),
    toRouteMetricNumber(payload?.ride_details?.route_api_distance_km),
    toRouteMetricNumber(payload?.meta?.route_api_distance_km),
    toRouteMetricNumber(payload?.meta?.route_api_data?.distance_km),
    toRouteMetricNumber(payload?.meta?.route_api_data?.total_distance),
    toRouteMetricNumber(payload?.distance),
    toRouteMetricNumber(payload?.route),
    toRouteMetricNumber(payload?.total_distance),
    toRouteMetricNumber(payload?.meta?.distance),
    toRouteMetricNumber(payload?.meta?.route),
    toRouteMetricNumber(payload?.meta?.total_distance)
  );

  return distance !== null && distance >= 0 ? distance : null;
};

const getEstimatedPriceFromPayload = (payload = {}) =>
  pickFirstValue(
    toNumber(payload?.estimated_price),
    toNumber(payload?.ride_details?.estimated_price),
    toNumber(payload?.meta?.estimated_price),
    toNumber(payload?.estimatedPrice),
    toNumber(payload?.ride_details?.estimatedPrice),
    toNumber(payload?.meta?.estimatedPrice),
    toNumber(payload?.estimated_fare),
    toNumber(payload?.ride_details?.estimated_fare),
    toNumber(payload?.meta?.estimated_fare)
  );

const getBaseFareFromPayload = (payload = {}) =>
  pickFirstValue(
    toNumber(payload?.base_fare),
    toNumber(payload?.ride_details?.base_fare),
    toNumber(payload?.meta?.base_fare)
  );

const getRidePriceBounds = (payload = {}) => {
  if (!payload || typeof payload !== "object") {
    return { base_fare: null, min_price: null, max_price: null };
  }

  const explicitMin = pickFirstValue(
    toNumber(payload?.min_price),
    toNumber(payload?.min_fare),
    toNumber(payload?.MIN_PRICE),
    toNumber(payload?.ride_details?.min_price),
    toNumber(payload?.ride_details?.min_fare),
    toNumber(payload?.ride_details?.MIN_PRICE),
    toNumber(payload?.meta?.min_price),
    toNumber(payload?.meta?.min_fare),
    toNumber(payload?.meta?.MIN_PRICE)
  );
  const explicitMax = pickFirstValue(
    toNumber(payload?.max_price),
    toNumber(payload?.max_fare),
    toNumber(payload?.MAX_PRICE),
    toNumber(payload?.ride_details?.max_price),
    toNumber(payload?.ride_details?.max_fare),
    toNumber(payload?.ride_details?.MAX_PRICE),
    toNumber(payload?.meta?.max_price),
    toNumber(payload?.meta?.max_fare),
    toNumber(payload?.meta?.MAX_PRICE)
  );
  const explicitBase = pickFirstValue(
    getEstimatedPriceFromPayload(payload),
    getBaseFareFromPayload(payload)
  );
  if (explicitMin !== null && explicitMax !== null) {
    const normalized = normalizePriceBoundsPair(explicitMin, explicitMax);
    return {
      base_fare: explicitBase !== null ? round2(explicitBase) : null,
      min_price: normalized.min_price,
      max_price: normalized.max_price,
    };
  }

  const distanceKm = getPayloadDistanceKm(payload);
  const computedBase = pickFirstValue(
    getEstimatedPriceFromPayload(payload),
    getBaseFareFromPayload(payload)
  );

  if (computedBase !== null) {
    return buildPriceBounds(computedBase, distanceKm);
  }

  const normalized = normalizePriceBoundsPair(explicitMin, explicitMax);

  return {
    base_fare: explicitBase !== null ? round2(explicitBase) : null,
    min_price: normalized.min_price,
    max_price: normalized.max_price,
  };
};

const extractRidePriceAnchor = (payload = {}) => {
  if (!payload || typeof payload !== "object") return null;

  const anchorBase = pickFirstValue(
    toNumber(payload?.price_anchor_base_fare),
    toNumber(payload?.ride_details?.price_anchor_base_fare),
    toNumber(payload?.meta?.price_anchor_base_fare),
    toNumber(payload?.price_anchor_estimated_price),
    toNumber(payload?.ride_details?.price_anchor_estimated_price),
    toNumber(payload?.meta?.price_anchor_estimated_price),
    getEstimatedPriceFromPayload(payload),
    getBaseFareFromPayload(payload)
  );

  const anchorMin = pickFirstValue(
    toNumber(payload?.price_anchor_min_price),
    toNumber(payload?.ride_details?.price_anchor_min_price),
    toNumber(payload?.meta?.price_anchor_min_price),
    toNumber(payload?.min_price),
    toNumber(payload?.min_fare),
    toNumber(payload?.MIN_PRICE),
    toNumber(payload?.ride_details?.min_price),
    toNumber(payload?.ride_details?.min_fare),
    toNumber(payload?.ride_details?.MIN_PRICE),
    toNumber(payload?.meta?.min_price),
    toNumber(payload?.meta?.min_fare),
    toNumber(payload?.meta?.MIN_PRICE)
  );

  const anchorMax = pickFirstValue(
    toNumber(payload?.price_anchor_max_price),
    toNumber(payload?.ride_details?.price_anchor_max_price),
    toNumber(payload?.meta?.price_anchor_max_price),
    toNumber(payload?.max_price),
    toNumber(payload?.max_fare),
    toNumber(payload?.MAX_PRICE),
    toNumber(payload?.ride_details?.max_price),
    toNumber(payload?.ride_details?.max_fare),
    toNumber(payload?.ride_details?.MAX_PRICE),
    toNumber(payload?.meta?.max_price),
    toNumber(payload?.meta?.max_fare),
    toNumber(payload?.meta?.MAX_PRICE)
  );

  const normalized = normalizePriceBoundsPair(anchorMin, anchorMax);
  if (normalized.min_price === null || normalized.max_price === null) return null;

  const lockedAt =
    pickFirstValue(
      toNumber(payload?.price_anchor_locked_at),
      toNumber(payload?.ride_details?.price_anchor_locked_at),
      toNumber(payload?.meta?.price_anchor_locked_at)
    ) ?? null;

  return {
    base_fare: anchorBase !== null ? round2(anchorBase) : null,
    min_price: normalized.min_price,
    max_price: normalized.max_price,
    locked_at: lockedAt,
  };
};

const applyRidePriceAnchor = (payload = {}, anchor = null) => {
  if (!payload || typeof payload !== "object" || !anchor) return payload;

  const minPrice = toNumber(anchor?.min_price);
  const maxPrice = toNumber(anchor?.max_price);
  if (minPrice === null || maxPrice === null) return payload;

  const baseFare = toNumber(anchor?.base_fare);
  const lockedAt = toNumber(anchor?.locked_at) ?? Date.now();
  const rideDetails =
    payload?.ride_details && typeof payload.ride_details === "object"
      ? { ...payload.ride_details }
      : {};
  const meta =
    payload?.meta && typeof payload.meta === "object" ? { ...payload.meta } : {};

  if (baseFare !== null) {
    rideDetails.base_fare = baseFare;
    rideDetails.estimated_price = baseFare;
    rideDetails.estimated_fare = baseFare;
    meta.base_fare = baseFare;
    meta.estimated_price = baseFare;
    meta.estimated_fare = baseFare;
  }

  rideDetails.min_price = minPrice;
  rideDetails.max_price = maxPrice;
  rideDetails.min_fare = minPrice;
  rideDetails.max_fare = maxPrice;
  rideDetails.MIN_PRICE = minPrice;
  rideDetails.MAX_PRICE = maxPrice;
  rideDetails.price_anchor_min_price = minPrice;
  rideDetails.price_anchor_max_price = maxPrice;
  rideDetails.price_anchor_locked_at = lockedAt;
  if (baseFare !== null) {
    rideDetails.price_anchor_base_fare = baseFare;
  }

  meta.min_price = minPrice;
  meta.max_price = maxPrice;
  meta.min_fare = minPrice;
  meta.max_fare = maxPrice;
  meta.MIN_PRICE = minPrice;
  meta.MAX_PRICE = maxPrice;
  meta.price_anchor_min_price = minPrice;
  meta.price_anchor_max_price = maxPrice;
  meta.price_anchor_locked_at = lockedAt;
  if (baseFare !== null) {
    meta.price_anchor_base_fare = baseFare;
  }

  return {
    ...payload,
    ...(baseFare !== null
      ? {
          base_fare: baseFare,
          estimated_price: baseFare,
          estimated_fare: baseFare,
          price_anchor_base_fare: baseFare,
        }
      : {}),
    min_price: minPrice,
    max_price: maxPrice,
    min_fare: minPrice,
    max_fare: maxPrice,
    MIN_PRICE: minPrice,
    MAX_PRICE: maxPrice,
    price_anchor_min_price: minPrice,
    price_anchor_max_price: maxPrice,
    price_anchor_locked: 1,
    price_anchor_locked_at: lockedAt,
    ride_details: rideDetails,
    meta,
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

const USER_ACCEPT_OFFER_STATUS = Object.freeze({
  SUCCESS: 1,
  DRIVER_NOT_ELIGIBLE_FOR_QUEUED_RIDE: 2,
  MISSING_AUTH_DATA: 3,
  ACCEPT_API_REJECTED: 4,
  ACCEPT_API_FAILED: 5,
  ACCEPT_FAILED: 6,
});

function emitUserAcceptOfferResult(io, userId, payload = {}, fallbackSocket = null) {
  const safeUserId = toNumber(userId);
  const resultPayload = {
    success: payload.success === true,
    status:
      toNumber(payload.status) ??
      (payload.success === true
        ? USER_ACCEPT_OFFER_STATUS.SUCCESS
        : USER_ACCEPT_OFFER_STATUS.ACCEPT_FAILED),
    ride_id: payload.ride_id ?? null,
    driver_id: payload.driver_id ?? null,
    message: payload.message ?? null,
    reason: payload.reason ?? null,
    details: payload.details ?? null,
    at: Date.now(),
  };

  let delivered = false;
  const roomName = safeUserId ? userRoom(safeUserId) : null;
  const roomSocketIds = roomName ? io?.sockets?.adapter?.rooms?.get(roomName) : null;
  const roomHasSockets = !!(roomSocketIds && roomSocketIds.size > 0);

  if (safeUserId && roomHasSockets) {
    io.to(roomName).emit("ride:acceptOfferResult", resultPayload);
    // Backward-compatible alias for clients listening with lowercase "o".
    io.to(roomName).emit("ride:acceptofferResult", resultPayload);
    delivered = true;
  }

  const shouldEmitToFallbackSocket =
    fallbackSocket &&
    typeof fallbackSocket.emit === "function" &&
    (!roomHasSockets ||
      !roomSocketIds ||
      !roomSocketIds.has(fallbackSocket.id));

  if (shouldEmitToFallbackSocket) {
    fallbackSocket.emit("ride:acceptOfferResult", resultPayload);
    fallbackSocket.emit("ride:acceptofferResult", resultPayload);
    delivered = true;
  }

  return delivered;
}
const emitRideUnavailable = (io, driverId, rideId) => {
  const safeDriverId = toNumber(driverId);
  const safeRideId = toNumber(rideId);
  if (!io || !safeDriverId || !safeRideId) return false;

  io.to(driverRoom(safeDriverId)).emit("ride:unavailable", {
    ride_id: safeRideId,
    message: "هذه الرحلة لم تعد متاحة",
    at: Date.now(),
  });

  return true;
};

const normalizeDuration = (v) => toNumber(v);
const getRideDurationRaw = (payload = null) => {
  if (!payload || typeof payload !== "object") return null;
  return pickFirstValue(
    payload?.driver_to_pickup_distance_m,
    payload?.meta?.driver_to_pickup_distance_m,
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
  const computedBounds = getRidePriceBounds({
    ...payload,
    ride_details: rideDetails,
    meta,
  });
  const estimatedPrice = getEstimatedPriceFromPayload({
    ...payload,
    ride_details: rideDetails,
    meta,
  });
  const baseFare = pickFirstValue(
    estimatedPrice,
    getBaseFareFromPayload({ ...payload, ride_details: rideDetails, meta }),
    toNumber(computedBounds?.base_fare)
  );
  const minPrice = pickFirstValue(
    toNumber(payload?.min_price),
    toNumber(payload?.min_fare),
    toNumber(rideDetails?.min_price),
    toNumber(rideDetails?.min_fare),
    toNumber(meta?.min_price),
    toNumber(meta?.min_fare),
    toNumber(computedBounds?.min_price)
  );
  const maxPrice = pickFirstValue(
    toNumber(payload?.max_price),
    toNumber(payload?.max_fare),
    toNumber(rideDetails?.max_price),
    toNumber(rideDetails?.max_fare),
    toNumber(meta?.max_price),
    toNumber(meta?.max_fare),
    toNumber(computedBounds?.max_price)
  );
  const normalizedPriceBounds = normalizePriceBoundsPair(minPrice, maxPrice);
  const resolvedMinPrice = normalizedPriceBounds.min_price;
  const resolvedMaxPrice = normalizedPriceBounds.max_price;

  if (duration !== null) {
    rideDetails.duration = duration;
    meta.duration = duration;
  }
  if (distanceKm !== null) {
    rideDetails.route_api_distance_km = distanceKm;
    meta.route_api_distance_km = distanceKm;
  }
  if (baseFare !== null) {
    rideDetails.base_fare = baseFare;
    rideDetails.estimated_price = baseFare;
    rideDetails.estimated_fare = baseFare;
    meta.base_fare = baseFare;
    meta.estimated_price = baseFare;
    meta.estimated_fare = baseFare;
  }
  if (resolvedMinPrice !== null) {
    rideDetails.min_price = resolvedMinPrice;
    rideDetails.min_fare = resolvedMinPrice;
    meta.min_price = resolvedMinPrice;
    meta.min_fare = resolvedMinPrice;
  }
  if (resolvedMaxPrice !== null) {
    rideDetails.max_price = resolvedMaxPrice;
    rideDetails.max_fare = resolvedMaxPrice;
    meta.max_price = resolvedMaxPrice;
    meta.max_fare = resolvedMaxPrice;
  }

  return {
    ...payload,
...(duration !== null
  ? {
      duration,
      eta_min: duration,
      route_api_duration_min: duration,
      driver_to_pickup_duration_min: duration,
    }
  : {}),    ...(distanceKm !== null
      ? { distance: distanceKm, route_api_distance_km: distanceKm }
      : {}),
    ...(baseFare !== null
      ? { base_fare: baseFare, estimated_price: baseFare, estimated_fare: baseFare }
      : {}),
    ...(resolvedMinPrice !== null
      ? { min_price: resolvedMinPrice, min_fare: resolvedMinPrice }
      : {}),
    ...(resolvedMaxPrice !== null
      ? { max_price: resolvedMaxPrice, max_fare: resolvedMaxPrice }
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
  : 120; // ✅ fixed 90 seconds
const CUSTOMER_SEARCH_TIMEOUT_S = 90;

const CANCELLED_RIDE_TTL_MS = 10 * 60 * 1000;

// ✅ NEW: TTL لتنظيف الانبوكس من الرحلات القديمة (حتى لو ما وصل cancel/accept)
const INBOX_ENTRY_TTL_MS = Number.isFinite(Number(process.env.INBOX_ENTRY_TTL_MS))
  ? Number(process.env.INBOX_ENTRY_TTL_MS)
  : 10 * 60 * 1000; // default 10 minutes (survive short network drops)
const RETRY_STATE_TTL_MS = Number.isFinite(Number(process.env.RETRY_STATE_TTL_MS))
  ? Math.max(30 * 1000, Number(process.env.RETRY_STATE_TTL_MS))
  : 5 * 60 * 1000;

const MIN_DISPATCH_RADIUS_METERS = 200;
const DISPATCH_RADIUS_STEPS_KM = Object.freeze([1, 2, 3, 5, 7, 10, 15, 20]);
const ROAD_RADIUS_METERS = Number.isFinite(Number(process.env.ROAD_RADIUS_METERS))
  ? Math.max(100, Number(process.env.ROAD_RADIUS_METERS))
  : 5000;
const DEFAULT_MAX_DISPATCH_RADIUS_METERS =
  Math.max(...DISPATCH_RADIUS_STEPS_KM) * 1000;

const AIR_CANDIDATE_RADIUS_METERS = Number.isFinite(Number(process.env.AIR_CANDIDATE_RADIUS_METERS))
  ? Math.max(ROAD_RADIUS_METERS, Number(process.env.AIR_CANDIDATE_RADIUS_METERS))
  : 8000;

const MAX_ROAD_FILTER_CANDIDATES = Number.isFinite(Number(process.env.MAX_ROAD_FILTER_CANDIDATES))
  ? Math.max(0, Number(process.env.MAX_ROAD_FILTER_CANDIDATES))
  : 0;
const MAX_DISPATCH_RADIUS_METERS = Number.isFinite(Number(process.env.MAX_DISPATCH_RADIUS_METERS))
  ? Math.max(MIN_DISPATCH_RADIUS_METERS, Number(process.env.MAX_DISPATCH_RADIUS_METERS))
  : DEFAULT_MAX_DISPATCH_RADIUS_METERS;

const MAX_DISPATCH_CANDIDATES = Number.isFinite(Number(process.env.MAX_DISPATCH_CANDIDATES))
  ? Math.max(0, Number(process.env.MAX_DISPATCH_CANDIDATES))
  : 0;
const DISPATCH_WAVE_SIZE = Number.isFinite(Number(process.env.DISPATCH_WAVE_SIZE))
  ? Math.max(0, Number(process.env.DISPATCH_WAVE_SIZE))
  : 0;
const DISPATCH_WAVE_INTERVAL_MS = Number.isFinite(Number(process.env.DISPATCH_WAVE_INTERVAL_MS))
  ? Math.max(0, Number(process.env.DISPATCH_WAVE_INTERVAL_MS))
  : 0;

const MAX_DRIVER_LOCATION_AGE_MS = Number.isFinite(Number(process.env.MAX_DRIVER_LOCATION_AGE_MS))
  ? Number(process.env.MAX_DRIVER_LOCATION_AGE_MS)
  : 2 * 60 * 1000;
const ALLOW_BUSY_DRIVER_NEAR_FINISH =
  String(process.env.ALLOW_BUSY_DRIVER_NEAR_FINISH || "1") === "1";
const BUSY_DRIVER_FINISH_RADIUS_M = Number.isFinite(Number(process.env.BUSY_DRIVER_FINISH_RADIUS_M))
  ? Math.max(100, Number(process.env.BUSY_DRIVER_FINISH_RADIUS_M))
  : 500;
const MAX_QUEUED_RIDES_PER_DRIVER = Number.isFinite(Number(process.env.MAX_QUEUED_RIDES_PER_DRIVER))
  ? Math.max(0, Number(process.env.MAX_QUEUED_RIDES_PER_DRIVER))
  : 1;

const LARAVEL_BASE_URL =
  process.env.LARAVEL_BASE_URL ||
  process.env.LARAVEL_URL ||
  "https://api.catch-syria.com";
const NORMALIZED_LARAVEL_BASE_URL = String(LARAVEL_BASE_URL || "").trim().replace(/\/+$/, "");
const DEFAULT_CUSTOMER_IMAGE_URL = `${NORMALIZED_LARAVEL_BASE_URL}/assets/images/user.svg`;
const DRIVER_IMAGE_RELATIVE_DIR = "assets/images/profile-images/provider";
const CUSTOMER_IMAGE_RELATIVE_DIR = "assets/images/profile-images/customer";

const LARAVEL_ACCEPT_BID_PATH = "/api/customer/transport/accept-bid";
const LARAVEL_DRIVER_BID_PATH = "/api/driver/bid-offer";
const LARAVEL_DRIVER_REJECT_REQUEST_PATH = "/api/driver/reject-request";
const LARAVEL_DRIVER_REJECT_NOTIFICATION_PATH = "/api/driver/driver-reject-notification";
const LARAVEL_DRIVER_UPDATE_LIST_NOTIFICATION_PATH =
  "/api/internal/driver-update-list-notification";
const LARAVEL_INTERNAL_SECRET = String(
  process.env.SOCKET_INTERNAL_SECRET || process.env.LARAVEL_INTERNAL_SECRET || ""
).trim();
const LARAVEL_TIMEOUT_MS = Number.isFinite(Number(process.env.LARAVEL_TIMEOUT_MS))
  ? Math.max(1000, Number(process.env.LARAVEL_TIMEOUT_MS))
  : 7000;
const LARAVEL_ACCEPT_BID_TIMEOUT_MS = Number.isFinite(
  Number(process.env.LARAVEL_ACCEPT_BID_TIMEOUT_MS)
)
  ? Number(process.env.LARAVEL_ACCEPT_BID_TIMEOUT_MS)
  : LARAVEL_TIMEOUT_MS;
const LARAVEL_ROUTE_TIMEOUT_MS = Number.isFinite(Number(process.env.LARAVEL_ROUTE_TIMEOUT_MS))
  ? Math.max(1000, Number(process.env.LARAVEL_ROUTE_TIMEOUT_MS))
  : 10000;
const ROUTE_API_CACHE_TTL_MS = Number.isFinite(Number(process.env.ROUTE_API_CACHE_TTL_MS))
  ? Math.max(0, Number(process.env.ROUTE_API_CACHE_TTL_MS))
  : 15000;
const ROUTE_API_MAX_CONCURRENCY = Number.isFinite(Number(process.env.ROUTE_API_MAX_CONCURRENCY))
  ? Math.max(1, Number(process.env.ROUTE_API_MAX_CONCURRENCY))
  : 4;
const ROUTE_CACHE_L2_TTL_S = Number.isFinite(Number(process.env.ROUTE_CACHE_L2_TTL_S))
  ? Math.max(1, Math.floor(Number(process.env.ROUTE_CACHE_L2_TTL_S)))
  : 15;
const ROUTE_LOCK_TTL_S = Number.isFinite(Number(process.env.ROUTE_LOCK_TTL_S))
  ? Math.max(1, Math.floor(Number(process.env.ROUTE_LOCK_TTL_S)))
  : 3;
const routeMetricsCache = new Map();
const routeMetricsInFlight = new Map();
const DRIVER_WALLET_GUARD_ENABLED =
  String(process.env.DRIVER_WALLET_GUARD_ENABLED || "1") === "1";
const DRIVER_WALLET_GUARD_MAX_AGE_MS = Number.isFinite(
  Number(process.env.DRIVER_WALLET_GUARD_MAX_AGE_MS)
)
  ? Math.max(1000, Number(process.env.DRIVER_WALLET_GUARD_MAX_AGE_MS))
  : 60_000;
const DRIVER_WALLET_GUARD_MAX_CONCURRENCY = Number.isFinite(
  Number(process.env.DRIVER_WALLET_GUARD_MAX_CONCURRENCY)
)
  ? Math.max(1, Number(process.env.DRIVER_WALLET_GUARD_MAX_CONCURRENCY))
  : 2;
const walletGuardInFlightByDriver = new Map(); // driverId -> Promise<boolean>
const DISPATCH_EXPANSION_INTERVAL_S = 5;
const buildInternalLaravelHeaders = () => {
  if (!LARAVEL_INTERNAL_SECRET) return undefined;
  return {
    "X-Socket-Internal-Secret": LARAVEL_INTERNAL_SECRET,
  };
};

const getDriverLocationAgeMs = (driver = null, meta = null) => {
  const ts =
    toNumber(driver?.timestamp) ??
    toNumber(meta?.timestamp) ??
    toNumber(meta?.updatedAt) ??
    null;
  if (ts === null) return null;
  return Date.now() - ts;
};

const isDriverLocationFresh = (driver = null, meta = null) => {
  const ageMs = getDriverLocationAgeMs(driver, meta);
  if (ageMs === null) return false;
  return ageMs <= MAX_DRIVER_LOCATION_AGE_MS;
};

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

async function syncDriverRejectNotification({
  driverId,
  rideId,
  serviceCategoryId,
  accessToken,
  driverServiceId,
}) {
  if (!driverId || !rideId || !serviceCategoryId || !accessToken || !driverServiceId) {
    console.log("[driver:declineRide] Laravel reject-notification sync skipped", {
      driver_id: driverId ?? null,
      ride_id: rideId ?? null,
      service_category_id: serviceCategoryId ?? null,
      has_access_token: !!accessToken,
      driver_service_id: driverServiceId ?? null,
    });
    return false;
  }

  try {
    const rejectPayload = {
      driver_id: driverId,
      access_token: accessToken,
      driver_service_id: driverServiceId,
      request_id: rideId,
      service_category_id: serviceCategoryId,
    };
    const rejectPaths = [
      LARAVEL_DRIVER_REJECT_REQUEST_PATH,
      LARAVEL_DRIVER_REJECT_NOTIFICATION_PATH,
    ];

    for (const path of rejectPaths) {
      try {
        await axios.post(`${LARAVEL_BASE_URL}${path}`, rejectPayload, {
          timeout: LARAVEL_TIMEOUT_MS,
        });

        console.log("[driver:declineRide] Laravel reject sync succeeded", {
          driver_id: driverId,
          ride_id: rideId,
          service_category_id: serviceCategoryId,
          path,
        });
        return true;
      } catch (error) {
        console.error("[driver:declineRide] Laravel reject sync failed", {
          path,
          driver_id: driverId,
          ride_id: rideId,
          service_category_id: serviceCategoryId,
          error: error?.response?.data || error?.message || error,
        });
      }
    }
  } catch (error) {
    console.error(
      "[driver:declineRide] Laravel reject-notification sync failed:",
      error?.response?.data || error?.message || error
    );
    return false;
  }
}

async function syncDriverUpdateListNotification({
  driverId,
  rideId,
  serviceCategoryId,
  triggerEvent,
  dispatchPayload,
  rideModel,
  minPrice,
  maxPrice,
  minFare,
  maxFare,
  routeApiDistanceKm,
  duration,
  etaMin,
  additionalRemarks,
  isPriceUpdated,
  serverTime,
  expiresAt,
}) {
  if (!driverId || !rideId) {
    console.log("[driver:rides:list][push] skipped: missing fields", {
      driver_id: driverId ?? null,
      ride_id: rideId ?? null,
      service_category_id: serviceCategoryId ?? null,
      trigger_event: triggerEvent ?? null,
    });
    return false;
  }

  const resolvedMinPrice =
    toNumber(minPrice) ?? toNumber(minFare) ?? null;
  const resolvedMaxPrice =
    toNumber(maxPrice) ?? toNumber(maxFare) ?? null;
  const resolvedMinFare =
    toNumber(minFare) ?? resolvedMinPrice;
  const resolvedMaxFare =
    toNumber(maxFare) ?? resolvedMaxPrice;
  const normalizedDispatchPayload =
    dispatchPayload && typeof dispatchPayload === "object"
      ? sanitizeRidePayloadForClient(dispatchPayload)
      : null;
  const normalizedRideModel =
    rideModel && typeof rideModel === "object"
      ? sanitizeRidePayloadForClient(rideModel)
      : null;
  const normalizedAdditionalRemarks = toTrimmedText(additionalRemarks);
  const laravelSyncPayload = {
    driver_id: driverId,
    ride_id: rideId,
    service_category_id: serviceCategoryId ?? null,
    min_price: resolvedMinPrice,
    max_price: resolvedMaxPrice,
    MIN_PRICE: resolvedMinPrice,
    MAX_PRICE: resolvedMaxPrice,
    min_fare: resolvedMinFare,
    max_fare: resolvedMaxFare,
    route_api_distance_km: toNumber(routeApiDistanceKm),
    duration: toNumber(duration),
    eta_min: toNumber(etaMin),
    additional_remarks: normalizedAdditionalRemarks,
    additional_remark: normalizedAdditionalRemarks,
    additional_request: normalizedAdditionalRemarks,
    isPriceUpdated: isPriceUpdated ? 1 : 0,
    server_time: toNumber(serverTime),
    expires_at: toNumber(expiresAt),
    trigger_event: triggerEvent ?? "ride:bidRequest",
    paired_event: "driver:rides:list",
    dispatch_payload: normalizedDispatchPayload,
    ride_model: normalizedRideModel,
  };
  console.log("[driver:rides:list][push] Laravel sync request", {
    driver_id: driverId,
    ride_id: rideId,
    payload_bytes: Buffer.byteLength(JSON.stringify(laravelSyncPayload), "utf8"),
    payload: laravelSyncPayload,
  });

  try {
    await axios.post(
      `${LARAVEL_BASE_URL}${LARAVEL_DRIVER_UPDATE_LIST_NOTIFICATION_PATH}`,
      laravelSyncPayload,
      {
        timeout: LARAVEL_TIMEOUT_MS,
        headers: buildInternalLaravelHeaders(),
      }
    );

    console.log("[driver:rides:list][push] Laravel sync succeeded", {
      driver_id: driverId,
      ride_id: rideId,
      service_category_id: serviceCategoryId ?? null,
      min_price: resolvedMinPrice,
      max_price: resolvedMaxPrice,
      min_fare: resolvedMinFare,
      max_fare: resolvedMaxFare,
      route_api_distance_km: toNumber(routeApiDistanceKm),
      duration: toNumber(duration),
      eta_min: toNumber(etaMin),
      server_time: toNumber(serverTime),
      expires_at: toNumber(expiresAt),
      trigger_event: triggerEvent ?? null,
      has_dispatch_payload: !!normalizedDispatchPayload,
      dispatch_payload_keys: normalizedDispatchPayload
        ? Object.keys(normalizedDispatchPayload).length
        : 0,
      has_ride_model: !!normalizedRideModel,
      ride_model_keys: normalizedRideModel
        ? Object.keys(normalizedRideModel).length
        : 0,
    });
    return true;
  } catch (error) {
    console.error("[driver:rides:list][push] Laravel sync failed", {
      driver_id: driverId,
      ride_id: rideId,
      service_category_id: serviceCategoryId ?? null,
      min_price: resolvedMinPrice,
      max_price: resolvedMaxPrice,
      min_fare: resolvedMinFare,
      max_fare: resolvedMaxFare,
      route_api_distance_km: toNumber(routeApiDistanceKm),
      duration: toNumber(duration),
      eta_min: toNumber(etaMin),
      server_time: toNumber(serverTime),
      expires_at: toNumber(expiresAt),
      trigger_event: triggerEvent ?? null,
      has_dispatch_payload: !!normalizedDispatchPayload,
      dispatch_payload_keys: normalizedDispatchPayload
        ? Object.keys(normalizedDispatchPayload).length
        : 0,
      has_ride_model: !!normalizedRideModel,
      ride_model_keys: normalizedRideModel
        ? Object.keys(normalizedRideModel).length
        : 0,
      error: error?.response?.data || error?.message || error,
    });
    return false;
  }
}

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
    event_type: "driver_bid_list_patch",
    ui_action: "show_bid_list",
    auto_open_running: false,
    ops: safeOps,
    seq: nextDriverSeq(driverId),
    at: Date.now(),
  });
  return true;
}

function isRideOfferExpired(ride) {
  const expiresAt = toNumber(ride?.expires_at);
  if (expiresAt === null) return false;
  return expiresAt <= nowSec();
}

function isDriverOfferStillActive(driverId, rideId) {
  const safeDriverId = toNumber(driverId);
  const safeRideId = toNumber(rideId);
  if (!safeDriverId || !safeRideId) return false;

  const state = getRideDriverState(safeRideId, safeDriverId);
  if (isTerminalDriverRideState(state?.status)) return false;

  const box = driverRideInbox.get(safeDriverId);
  const ride = box?.get(safeRideId);

  if (ride && typeof ride === "object") {
    if (isRideOfferExpired(ride)) return false;
    return true;
  }

  const candidateSet = rideCandidates.get(safeRideId);

  if (!candidateSet?.has(safeDriverId)) return false;

  return state?.status === "notified" || state?.status === "bid_submitted";
}

function getRideDriverStatesMap(rideId, { create = false } = {}) {
  const safeRideId = toNumber(rideId);
  if (!safeRideId) return null;

  const existing = rideDriverStates.get(safeRideId);
  if (existing || !create) return existing ?? null;

  const next = new Map();
  rideDriverStates.set(safeRideId, next);
  return next;
}

function getRideDriverState(rideId, driverId) {
  const states = getRideDriverStatesMap(rideId);
  if (!states) return null;
  return states.get(toNumber(driverId)) ?? null;
}

function hasRideDriverBeenNotified(rideId, driverId) {
  const state = getRideDriverState(rideId, driverId);
  return !!(state && state.notified_at);
}

function hasRideDriverPushBeenNotified(rideId, driverId) {
  const state = getRideDriverState(rideId, driverId);
  return !!(state && state.push_notified_at);
}

function markRideDriverPushNotified(rideId, driverId, extra = {}) {
  const safeRideId = toNumber(rideId);
  const safeDriverId = toNumber(driverId);
  if (!safeRideId || !safeDriverId) return null;

  const currentState = getRideDriverState(safeRideId, safeDriverId);
  const currentStatus = currentState?.status || "pending_emit";

  return markRideDriverState(safeRideId, safeDriverId, currentStatus, {
    ...extra,
    push_notified_at: Date.now(),
  });
}

function markRideDriverState(rideId, driverId, status, extra = {}) {
  const safeRideId = toNumber(rideId);
  const safeDriverId = toNumber(driverId);
  if (!safeRideId || !safeDriverId || !status) return null;

  const states = getRideDriverStatesMap(safeRideId, { create: true });
  const previous = states.get(safeDriverId) ?? null;
  const now = Date.now();
  const next = {
    ...(previous ?? {}),
    ...extra,
    driver_id: safeDriverId,
    status,
    updated_at: now,
    notified_at: previous?.notified_at ?? null,
  };

  if (status === "notified" && !next.notified_at) {
    next.notified_at = now;
  }

  states.set(safeDriverId, next);
  if (isTerminalDriverRideState(status)) {
    clearPendingBidEmitRetry(safeRideId, safeDriverId);
  }
  return next;
}

function clearRideDriverStates(rideId) {
  const safeRideId = toNumber(rideId);
  if (!safeRideId) return;
  clearPendingBidEmitRetriesForRide(safeRideId);
  rideDriverStates.delete(safeRideId);
}

function emitDispatchDeliverySummary(io, driverId, ridePayloadForDriver = null) {
  const safeDriverId = toNumber(driverId);
  if (!safeDriverId || !ridePayloadForDriver) return;
  emitDriverPatch(io, safeDriverId, [{ op: "upsert", ride: ridePayloadForDriver }]);
  emitDriverInbox(io, safeDriverId, "driver:rides:list");
}

async function emitDispatchNotificationSync(payload = {}) {
  const ridePayloadForDriver = payload?.ridePayloadForDriver;
  const bidRequestPayload =
    payload?.bidRequestPayload && typeof payload?.bidRequestPayload === "object"
      ? payload.bidRequestPayload
      : null;
  const safeDriverId = toNumber(payload?.driverId);
  const safeRideId = toNumber(payload?.rideId);
  if (!safeDriverId || !safeRideId || !ridePayloadForDriver) return;

  const isPriceUpdatedFlag =
    ridePayloadForDriver?.isPriceUpdated === true ||
    ridePayloadForDriver?.isPriceUpdated === 1 ||
    ridePayloadForDriver?.isPriceUpdated === "1" ||
    toNumber(ridePayloadForDriver?.updatedPrice) !== null;

  const alreadyNotified =
    payload?.alreadyNotified === true
      ? true
      : payload?.alreadyNotified === false
      ? false
      : hasRideDriverBeenNotified(safeRideId, safeDriverId);
  const alreadyPushNotified =
    payload?.alreadyPushNotified === true
      ? true
      : payload?.alreadyPushNotified === false
      ? false
      : hasRideDriverPushBeenNotified(safeRideId, safeDriverId);
  if ((alreadyNotified || alreadyPushNotified) && !isPriceUpdatedFlag) {
    console.log("[driver:rides:list][push] skipped: already-notified", {
      driver_id: safeDriverId,
      ride_id: safeRideId,
      already_notified: alreadyNotified,
      already_push_notified: alreadyPushNotified,
      is_price_updated: isPriceUpdatedFlag,
      push_source: payload?.pushSource ?? null,
    });
    return;
  }

  const synced = await syncDriverUpdateListNotification({
    driverId: safeDriverId,
    rideId: safeRideId,
    serviceCategoryId: toNumber(ridePayloadForDriver?.service_category_id),
    rideModel: bidRequestPayload ?? ridePayloadForDriver ?? null,
    minPrice:
      toNumber(ridePayloadForDriver?.min_price) ??
      toNumber(ridePayloadForDriver?.ride_details?.min_price) ??
      toNumber(ridePayloadForDriver?.min_fare) ??
      toNumber(ridePayloadForDriver?.ride_details?.min_fare) ??
      toNumber(ridePayloadForDriver?.min_fare_amount),
    maxPrice:
      toNumber(ridePayloadForDriver?.max_price) ??
      toNumber(ridePayloadForDriver?.ride_details?.max_price) ??
      toNumber(ridePayloadForDriver?.max_fare) ??
      toNumber(ridePayloadForDriver?.ride_details?.max_fare) ??
      toNumber(ridePayloadForDriver?.max_fare_amount),
    minFare:
      toNumber(ridePayloadForDriver?.min_fare) ??
      toNumber(ridePayloadForDriver?.ride_details?.min_fare) ??
      toNumber(ridePayloadForDriver?.min_price) ??
      toNumber(ridePayloadForDriver?.ride_details?.min_price) ??
      toNumber(ridePayloadForDriver?.min_fare_amount),
    maxFare:
      toNumber(ridePayloadForDriver?.max_fare) ??
      toNumber(ridePayloadForDriver?.ride_details?.max_fare) ??
      toNumber(ridePayloadForDriver?.max_price) ??
      toNumber(ridePayloadForDriver?.ride_details?.max_price) ??
      toNumber(ridePayloadForDriver?.max_fare_amount),
    routeApiDistanceKm:
      toNumber(ridePayloadForDriver?.route_api_distance_km) ??
      toNumber(ridePayloadForDriver?.ride_details?.route_api_distance_km),
    duration:
      toNumber(ridePayloadForDriver?.duration) ??
      toNumber(ridePayloadForDriver?.ride_details?.duration),
    etaMin:
      toNumber(ridePayloadForDriver?.eta_min) ??
      toNumber(ridePayloadForDriver?.ride_details?.eta_min),
    additionalRemarks: resolveAdditionalRemarks(
      ridePayloadForDriver,
      bidRequestPayload
    ),
    isPriceUpdated:
      ridePayloadForDriver?.isPriceUpdated === true ||
      ridePayloadForDriver?.isPriceUpdated === 1 ||
      ridePayloadForDriver?.isPriceUpdated === "1",
    serverTime:
      toNumber(ridePayloadForDriver?.server_time) ??
      toNumber(ridePayloadForDriver?.ride_details?.server_time),
    expiresAt:
      toNumber(ridePayloadForDriver?.expires_at) ??
      toNumber(ridePayloadForDriver?.ride_details?.expires_at),
    triggerEvent: "ride:bidRequest",
    dispatchPayload: bidRequestPayload ?? ridePayloadForDriver ?? null,
  });

  if (synced) {
    markRideDriverPushNotified(safeRideId, safeDriverId, {
      last_push_source: payload?.pushSource ?? "dispatch",
      last_push_synced_at: Date.now(),
      last_push_price_updated: isPriceUpdatedFlag ? 1 : 0,
    });
  }
}

function tryEmitBidRequestToDriver(
  io,
  {
    rideId,
    driverId,
    bidRequestPayload,
    ridePayloadForDriver,
    dispatchStageIndex = null,
    dispatchRadiusMeters = null,
    source = "dispatch",
    attempt = 1,
  } = {}
) {
  const safeRideId = toNumber(rideId);
  const safeDriverId = toNumber(driverId);
  if (!safeRideId || !safeDriverId || !bidRequestPayload) {
    return { delivered: false, room_sockets: 0, reason: "invalid-input" };
  }

  const room = driverRoom(safeDriverId);
  const roomSocketCount = getRoomSocketCount(io, room);
  const wasNotifiedBefore = hasRideDriverBeenNotified(safeRideId, safeDriverId);
  const now = Date.now();
  const stateMeta = {
    ...(dispatchStageIndex != null
      ? { last_dispatch_stage_index: dispatchStageIndex }
      : {}),
    ...(dispatchRadiusMeters != null
      ? { last_dispatch_radius_m: dispatchRadiusMeters }
      : {}),
    last_emit_attempt: attempt,
    last_emit_source: source,
    last_emit_room_sockets: roomSocketCount,
    last_emit_attempt_at: now,
  };

  if (roomSocketCount <= 0) {
    markRideDriverState(safeRideId, safeDriverId, "pending_emit", {
      ...stateMeta,
      last_emit_reason: "room_empty",
    });
    void emitDispatchNotificationSync({
      rideId: safeRideId,
      driverId: safeDriverId,
      bidRequestPayload,
      ridePayloadForDriver,
      alreadyNotified: wasNotifiedBefore,
      alreadyPushNotified: hasRideDriverPushBeenNotified(safeRideId, safeDriverId),
      pushSource: "dispatch:room_empty",
    });
    console.log("[dispatch][emit-miss][room-empty]", {
      ride_id: safeRideId,
      driver_id: safeDriverId,
      room,
      room_sockets: roomSocketCount,
      source,
      attempt,
    });
    return { delivered: false, room_sockets: roomSocketCount, reason: "room_empty" };
  }

  io.to(room).emit("ride:bidRequest", bidRequestPayload);
  markRideDriverState(safeRideId, safeDriverId, "notified", {
    ...stateMeta,
    last_emit_reason: "emitted_ok",
  });
  clearPendingBidEmitRetry(safeRideId, safeDriverId);
  void emitDispatchNotificationSync({
    rideId: safeRideId,
    driverId: safeDriverId,
    bidRequestPayload,
    ridePayloadForDriver,
    alreadyNotified: wasNotifiedBefore,
    alreadyPushNotified: hasRideDriverPushBeenNotified(safeRideId, safeDriverId),
    pushSource: "dispatch:emitted_ok",
  });

  console.log("[dispatch][emit-ok]", {
    ride_id: safeRideId,
    driver_id: safeDriverId,
    room,
    room_sockets: roomSocketCount,
    source,
    attempt,
  });
  return { delivered: true, room_sockets: roomSocketCount, reason: "emitted_ok" };
}

function scheduleBidRequestRetry(
  io,
  {
    rideId,
    driverId,
    bidRequestPayload,
    ridePayloadForDriver,
    dispatchStageIndex = null,
    dispatchRadiusMeters = null,
    source = "dispatch",
    attempt = 1,
  } = {}
) {
  const safeRideId = toNumber(rideId);
  const safeDriverId = toNumber(driverId);
  if (!safeRideId || !safeDriverId || !bidRequestPayload) return false;

  if (attempt > DISPATCH_EMIT_RETRY_MAX_ATTEMPTS) {
    markRideDriverState(safeRideId, safeDriverId, "pending_emit", {
      ...(dispatchStageIndex != null
        ? { last_dispatch_stage_index: dispatchStageIndex }
        : {}),
      ...(dispatchRadiusMeters != null
        ? { last_dispatch_radius_m: dispatchRadiusMeters }
        : {}),
      last_emit_source: `${source}:retry-exhausted`,
      last_emit_attempt: attempt - 1,
      last_emit_reason: "retry_exhausted",
      last_emit_attempt_at: Date.now(),
    });
    console.log("[dispatch][retry-exhausted]", {
      ride_id: safeRideId,
      driver_id: safeDriverId,
      max_attempts: DISPATCH_EMIT_RETRY_MAX_ATTEMPTS,
      source,
    });
    return false;
  }

  const key = pendingBidEmitRetryKey(safeRideId, safeDriverId);
  if (pendingBidEmitRetryTimers.has(key)) {
    return false;
  }

  const delayMs = DISPATCH_EMIT_RETRY_BASE_DELAY_MS * attempt;
  const timer = setTimeout(() => {
    pendingBidEmitRetryTimers.delete(key);

    const stillQueuedInInbox = !!driverRideInbox.get(safeDriverId)?.has(safeRideId);
    if (!stillQueuedInInbox) {
      return;
    }

    const emitResult = tryEmitBidRequestToDriver(io, {
      rideId: safeRideId,
      driverId: safeDriverId,
      bidRequestPayload,
      ridePayloadForDriver,
      dispatchStageIndex,
      dispatchRadiusMeters,
      source: `${source}:retry`,
      attempt,
    });

    if (emitResult.delivered) {
      emitDispatchDeliverySummary(io, safeDriverId, ridePayloadForDriver);
      return;
    }

    scheduleBidRequestRetry(io, {
      rideId: safeRideId,
      driverId: safeDriverId,
      bidRequestPayload,
      ridePayloadForDriver,
      dispatchStageIndex,
      dispatchRadiusMeters,
      source,
      attempt: attempt + 1,
    });
  }, delayMs);

  pendingBidEmitRetryTimers.set(key, timer);
  console.log("[dispatch][retry-scheduled]", {
    ride_id: safeRideId,
    driver_id: safeDriverId,
    attempt,
    delay_ms: delayMs,
    source,
  });
  return true;
}

function isTerminalDriverRideState(status) {
  return status === "accepted" || status === "declined" || status === "expired";
}

function isTerminalRideStatus(status) {
  return status === 4 || status === 6 || status === 7 || status === 8 || status === 9 || status === 10 || status === 11;
}

function removeDriverFromRideCandidates(io, rideId, driverId, options = {}) {
  const tracePayload = {
    rideId,
    driverId,
  };
  if (DEBUG_REMOVE_CANDIDATE_STACK) {
    tracePayload.stack = new Error().stack;
  }
  console.log("[removeDriverFromRideCandidates]", tracePayload);

  const { emitSummary = true } = options || {};
  const set = rideCandidates.get(rideId);
  if (!set) return false;

  const existed = set.delete(driverId);
  if (!existed) return false;

  if (set.size === 0) {
    rideCandidates.delete(rideId);
  }

  if (emitSummary && io) {
    emitRideCandidatesSummary(io, rideId);
  }

  return true;
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

    if (!isDriverLocationFresh(driver, meta)) continue;

    const isOnline = Number(driver?.is_online ?? meta?.is_online ?? 1) === 1;
    if (!isOnline) continue;
    const walletBlocked = Number(meta?.not_valid_wallet_balance ?? 0) === 1;
    if (walletBlocked) continue;

    const activeRide = getActiveRideByDriver(driverId);
    if (activeRide && activeRide !== rideId && !canDriverReceiveNewRideRequests(driverId)) continue;
    if (!isDriverOfferStillActive(driverId, rideId)) continue;

    const typeId = toNumber(driver?.service_type_id ?? meta?.service_type_id);
    if (!typeId) continue;

    if (requestedServiceTypeId && typeId !== requestedServiceTypeId) continue;

    const serviceCategoryId = toNumber(
      driver?.service_category_id ?? meta?.service_category_id
    );

    const vehicleTypeName = driver?.vehicle_type_name ?? meta?.vehicle_type_name ?? "";
    const vehicleTypeIcon = driver?.vehicle_type_icon ?? meta?.vehicle_type_icon ?? null;
    const driverImage = normalizeDriverImageUrl(
      pickFirstValue(
        driver?.driver_image,
        driver?.driver_image_url,
        driver?.driver_profile_image,
        driver?.provider_image,
        driver?.profile_image,
        driver?.avatar,
        meta?.driver_image,
        meta?.driver_image_url,
        meta?.driver_profile_image,
        meta?.provider_image,
        meta?.profile_image,
        meta?.avatar
      )
    );
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
      { timeout: LARAVEL_TIMEOUT_MS }
    );

    let d = res?.data || {};
    if (typeof d === "string") {
      try {
        d = JSON.parse(d);
      } catch (_) {}
    }

    const normalizedDetails = normalizeDriverDetailsPayload(d, d);
    const driverName = normalizedDetails.driver_name;
    const vehicleType = normalizedDetails.vehicle_type;
    const vehicleNumber = normalizedDetails.vehicle_number;
    const vehicleCompany = normalizedDetails.vehicle_company;
    const vehicleCompanyEn = normalizedDetails.vehicle_company_en;
    const vehicleCompanyAr = normalizedDetails.vehicle_company_ar;
    const modelName = normalizedDetails.model_name;
    const modelNameEn = normalizedDetails.model_name_en;
    const modelNameAr = normalizedDetails.model_name_ar;
    const modelYear = normalizedDetails.model_year;
    const vehicleColor = normalizedDetails.vehicle_color;
    const vehicleColorEn = normalizedDetails.vehicle_color_en;
    const vehicleColorAr = normalizedDetails.vehicle_color_ar;
    const vehicleManufacturer = normalizedDetails.vehicle_manufacturer;
    const vehicleManufacturerEn = normalizedDetails.vehicle_manufacturer_en;
    const vehicleManufacturerAr = normalizedDetails.vehicle_manufacturer_ar;
    const rating = normalizedDetails.rating;
    const driverImage = normalizedDetails.driver_image;
    const childSeatFromApi = toBinaryFlag(
      d?.child_seat_accessibility ?? d?.child_seat ?? d?.smoking ?? d?.smoking_value ?? null
    );
    const handicapFromApi = toBinaryFlag(
      d?.handicap_accessibility ?? d?.handicap ?? null
    );
    const driverGenderFromApi = toGenderFilter(d?.driver_gender ?? d?.gender ?? null);
    const providerId = toNumber(d.provider_id ?? d.driver_id ?? driverId);
    const resolvedDriverServiceId = toNumber(d.driver_service_id ?? driverServiceId);
    const driverDetailId = toNumber(d.driver_detail_id ?? d.driver_details_id ?? null);
    const metaUpdate = {
      ...(providerId ? { provider_id: providerId } : {}),
      ...(resolvedDriverServiceId ? { driver_service_id: resolvedDriverServiceId } : {}),
      ...(driverDetailId
        ? { driver_detail_id: driverDetailId, driver_details_id: driverDetailId }
        : {}),
      ...(driverName ? { driver_name: driverName } : {}),
      ...(vehicleType ? { vehicle_type_name: vehicleType } : {}),
      ...(vehicleNumber ? { plat_no: vehicleNumber } : {}),
      ...(vehicleCompany ? { vehicle_company: vehicleCompany } : {}),
      ...(vehicleCompanyEn ? { vehicle_company_en: vehicleCompanyEn } : {}),
      ...(vehicleCompanyAr ? { vehicle_company_ar: vehicleCompanyAr } : {}),
      ...(vehicleCompany ? { vehicle_manufacture_name: vehicleCompany } : {}),
      ...(vehicleCompanyEn ? { vehicle_manufacture_name_en: vehicleCompanyEn } : {}),
      ...(vehicleCompanyAr ? { vehicle_manufacture_name_ar: vehicleCompanyAr } : {}),
      ...(modelName ? { model_name: modelName } : {}),
      ...(modelNameEn ? { model_name_en: modelNameEn } : {}),
      ...(modelNameAr ? { model_name_ar: modelNameAr } : {}),
      ...(modelName ? { vehicle_model_name: modelName } : {}),
      ...(modelNameEn ? { vehicle_model_name_en: modelNameEn } : {}),
      ...(modelNameAr ? { vehicle_model_name_ar: modelNameAr } : {}),
      ...(modelYear !== null && modelYear !== undefined ? { model_year: modelYear } : {}),
      ...(vehicleColor ? { vehicle_color: vehicleColor } : {}),
      ...(vehicleColorEn ? { vehicle_color_en: vehicleColorEn } : {}),
      ...(vehicleColorAr ? { vehicle_color_ar: vehicleColorAr } : {}),
      ...(vehicleManufacturer ? { vehicle_manufacturer: vehicleManufacturer } : {}),
      ...(vehicleManufacturerEn ? { vehicle_manufacturer_en: vehicleManufacturerEn } : {}),
      ...(vehicleManufacturerAr ? { vehicle_manufacturer_ar: vehicleManufacturerAr } : {}),
      ...(vehicleManufacturer ? { manufacturer_name: vehicleManufacturer } : {}),
      ...(vehicleManufacturerEn ? { manufacturer_name_en: vehicleManufacturerEn } : {}),
      ...(vehicleManufacturerAr ? { manufacturer_name_ar: vehicleManufacturerAr } : {}),
      ...(rating != null ? { rating } : {}),
      ...(driverImage ? { driver_image: driverImage } : {}),
      ...(childSeatFromApi === 0 || childSeatFromApi === 1
        ? { child_seat: childSeatFromApi }
        : {}),
      ...(handicapFromApi === 0 || handicapFromApi === 1
        ? { handicap: handicapFromApi }
        : {}),
      ...(driverGenderFromApi === 1 || driverGenderFromApi === 2
        ? { driver_gender: driverGenderFromApi }
        : {}),
    };

    if (Object.keys(metaUpdate).length > 0) {
      driverLocationService.updateMeta(driverId, metaUpdate);
    }

    return {
      provider_id: providerId,
      driver_service_id: resolvedDriverServiceId,
      driver_detail_id: driverDetailId,
      driver_name: driverName,
      vehicle_type: vehicleType,
      vehicle_number: vehicleNumber,
      vehicle_company: vehicleCompany,
      vehicle_company_en: vehicleCompanyEn,
      vehicle_company_ar: vehicleCompanyAr,
      vehicle_manufacture_name: vehicleCompany,
      vehicle_manufacture_name_en: vehicleCompanyEn,
      vehicle_manufacture_name_ar: vehicleCompanyAr,
      model_name: modelName,
      model_name_en: modelNameEn,
      model_name_ar: modelNameAr,
      vehicle_model_name: modelName,
      vehicle_model_name_en: modelNameEn,
      vehicle_model_name_ar: modelNameAr,
      model_year: modelYear,
      vehicle_color: vehicleColor,
      vehicle_color_en: vehicleColorEn,
      vehicle_color_ar: vehicleColorAr,
      vehicle_manufacturer: vehicleManufacturer,
      vehicle_manufacturer_en: vehicleManufacturerEn,
      vehicle_manufacturer_ar: vehicleManufacturerAr,
      manufacturer_name: vehicleManufacturer,
      manufacturer_name_en: vehicleManufacturerEn,
      manufacturer_name_ar: vehicleManufacturerAr,
      rating,
      driver_image: driverImage,
      child_seat: childSeatFromApi,
      handicap: handicapFromApi,
      driver_gender: driverGenderFromApi,
    };
  } catch (e) {
    return null;
  }
};

const isDriverDetailsEmpty = (d) => {
  if (!d) return true;
  return (
    toTrimmedText(d.driver_name) == null &&
    toTrimmedText(d.vehicle_type) == null &&
    toTrimmedText(d.vehicle_number) == null &&
    toTrimmedText(d.vehicle_company) == null &&
    toTrimmedText(d.model_name) == null &&
    toTrimmedText(d.vehicle_color) == null &&
    toTrimmedText(d.vehicle_manufacturer) == null &&
    d.model_year == null &&
    d.rating == null &&
    toTrimmedText(d.driver_image) == null
  );
};

// User -> ride owner map (rideId -> userId)
const rideOwnerByRide = new Map(); // rideId -> userId
const retryStateCleanupTimers = new Map(); // rideId -> timeout

const getUserIdFromDispatch = (data, userDetails) => {
  const uid = toNumber(
    data?.user_id ?? data?.customer_id ?? data?.passenger_id ?? userDetails?.user_id
  );
  return uid || null;
};

const setUserActiveRide = (userId, rideId) => {
  if (!userId || !rideId) return;
  cancelRetryStateCleanup(rideId);
  rideOwnerByRide.set(rideId, userId);
  touchRideState(rideId);
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

const normalizeCustomerImageUrl = (value) => {
  const normalized = normalizeAssetUrl(value, {
    baseUrl: NORMALIZED_LARAVEL_BASE_URL,
    defaultRelativeDir: CUSTOMER_IMAGE_RELATIVE_DIR,
    emptyValue: "",
    upgradeSameHostToHttps: true,
  });
  return normalized || DEFAULT_CUSTOMER_IMAGE_URL;
};

const normalizeCustomerImageUrlOrNull = (value) => {
  const normalized = normalizeAssetUrl(value, {
    baseUrl: NORMALIZED_LARAVEL_BASE_URL,
    defaultRelativeDir: CUSTOMER_IMAGE_RELATIVE_DIR,
    emptyValue: null,
    upgradeSameHostToHttps: true,
  });
  return normalized || null;
};

function normalizeDriverImageUrl(value) {
  const normalized = normalizeAssetUrl(value, {
    baseUrl: NORMALIZED_LARAVEL_BASE_URL,
    defaultRelativeDir: DRIVER_IMAGE_RELATIVE_DIR,
    emptyValue: null,
    upgradeSameHostToHttps: true,
  });
  return normalized || null;
}

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

  const userImagePick = pickFirstPresentValueWithSource([
    { source: "src.profile_image", value: src?.profile_image },
    { source: "src.user_profile_image", value: src?.user_profile_image },
    { source: "src.user_profile", value: src?.user_profile },
    { source: "src.user_image", value: src?.user_image },
    { source: "src.image", value: src?.image },
    { source: "src.avatar", value: src?.avatar },
    { source: "payload.profile_image", value: data?.profile_image },
    { source: "payload.user_profile_image", value: data?.user_profile_image },
    { source: "payload.user_profile", value: data?.user_profile },
    { source: "payload.user_image", value: data?.user_image },
    { source: "payload.customer_image", value: data?.customer_image },
  ]);

  const stored = userId ? getUserDetails(userId) : null;
  const storedByToken = !stored && token ? getUserDetailsByToken(token) : null;
  const userLanguage = normalizeLanguageCode(
    src?.user_language ??
      src?.language ??
      data?.user_language ??
      data?.language ??
      stored?.user_language ??
      stored?.language ??
      storedByToken?.user_language ??
      storedByToken?.language ??
      null
  );

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
    user_image: normalizeCustomerImageUrlOrNull(
      userImagePick.value ?? stored?.user_image ?? storedByToken?.user_image ?? null
    ),
    user_image_source:
      userImagePick.value != null && userImagePick.value !== ""
        ? userImagePick.source
        : stored?.user_image
        ? stored?.user_image_source ?? "stored.user_image"
        : storedByToken?.user_image
        ? storedByToken?.user_image_source ?? "stored_by_token.user_image"
        : null,

    // ✅ NEW: keep token in user_details snapshot (helps later merges on retry)
    user_token: token ?? stored?.user_token ?? stored?.token ?? storedByToken?.user_token ?? null,
    token: token ?? stored?.user_token ?? stored?.token ?? storedByToken?.user_token ?? null,
    user_language: userLanguage,
    language: userLanguage,
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

  const customerImagePick = pickFirstPresentValueWithSource([
    { source: "details.user_image", value: details?.user_image },
    { source: "details.user_profile_image", value: details?.user_profile_image },
    { source: "details.user_profile", value: details?.user_profile },
    { source: "details.profile_image", value: details?.profile_image },
    { source: "details.avatar", value: details?.avatar },
    { source: "payload.user_image", value: payload?.user_image },
    { source: "payload.customer_image", value: payload?.customer_image },
    { source: "payload.profile_image", value: payload?.profile_image },
    { source: "payload.user_profile_image", value: payload?.user_profile_image },
    { source: "payload.user_profile", value: payload?.user_profile },
    { source: "payload.avatar", value: payload?.avatar },
  ]);
  const customerImage = customerImagePick.value;

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
    user_image: normalizeCustomerImageUrlOrNull(customerImage ?? null),
    user_image_source:
      details?.user_image_source ??
      payload?.user_image_source ??
      payload?.customer_image_source ??
      customerImagePick.source ??
      null,
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

const sanitizeCustomerForClient = (customer = null) => {
  if (!customer || typeof customer !== "object") return null;

  return {
    user_id: toNumber(customer?.user_id ?? null),
    user_name: customer?.user_name ?? null,
    user_gender: toNumber(customer?.user_gender ?? null),
    user_country_code: customer?.user_country_code ?? null,
    user_phone: customer?.user_phone ?? null,
    user_phone_full: customer?.user_phone_full ?? null,
    user_image: normalizeCustomerImageUrl(customer?.user_image ?? null),
    user_image_source: customer?.user_image_source ?? null,
  };
};

const stripTokenFields = (value) => {
  if (!value || typeof value !== "object" || Array.isArray(value)) return value;
  const {
    token,
    access_token,
    user_token,
    driver_token,
    driver_access_token,
    ...rest
  } = value;
  return rest;
};

const sanitizeRidePayloadForClient = (payload = {}) => {
  if (!payload || typeof payload !== "object") return payload;
  const {
    user_details,
    token,
    access_token,
    user_token,
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

  const builtCustomer = buildCustomerPayload(payload, user_details ?? customer_details ?? customer ?? null);
  const safeCustomer = sanitizeCustomerForClient(builtCustomer);

  const sanitized = {
    ...stripTokenFields(rest),
  };

  if (rest?.meta && typeof rest.meta === "object" && !Array.isArray(rest.meta)) {
    sanitized.meta = stripTokenFields(rest.meta);
  }

  if (safeCustomer) {
    const resolvedUserId = toNumber(sanitized.user_id) ?? safeCustomer.user_id ?? null;
    const resolvedUserName = toTrimmedText(sanitized.user_name) ?? safeCustomer.user_name ?? null;
    const resolvedUserGenderRaw = sanitized.user_gender ?? safeCustomer.user_gender ?? null;
    const resolvedUserGender =
      resolvedUserGenderRaw === "" || resolvedUserGenderRaw == null
        ? null
        : Number.isFinite(Number(resolvedUserGenderRaw))
        ? Number(resolvedUserGenderRaw)
        : resolvedUserGenderRaw;
    const resolvedUserCountryCode =
      toTrimmedText(sanitized.user_country_code) ?? safeCustomer.user_country_code ?? null;
    const resolvedUserPhone = toTrimmedText(sanitized.user_phone) ?? safeCustomer.user_phone ?? null;
    const resolvedUserPhoneFull =
      toTrimmedText(sanitized.user_phone_full) ??
      safeCustomer.user_phone_full ??
      (resolvedUserCountryCode && resolvedUserPhone
        ? `${resolvedUserCountryCode}${resolvedUserPhone}`
        : null);
    const resolvedUserImage = normalizeCustomerImageUrl(
      toTrimmedText(sanitized.user_image) ?? safeCustomer.user_image ?? null
    );

    sanitized.user_id = resolvedUserId;
    sanitized.user_name = resolvedUserName;
    sanitized.user_gender = resolvedUserGender;
    sanitized.user_country_code = resolvedUserCountryCode;
    sanitized.user_phone = resolvedUserPhone;
    sanitized.user_phone_full = resolvedUserPhoneFull;
    sanitized.user_image = resolvedUserImage;
    sanitized.user_image_source =
      safeCustomer.user_image_source ??
      sanitized.user_image_source ??
      null;
    sanitized.user_profile = resolvedUserImage;
    sanitized.user_details = safeCustomer;
    sanitized.customer = safeCustomer;
    sanitized.customer_details = safeCustomer;
    sanitized.customer_id = resolvedUserId;
    sanitized.customer_name = resolvedUserName;
    sanitized.customer_gender = resolvedUserGender;
    sanitized.customer_country_code = resolvedUserCountryCode;
    sanitized.customer_phone = resolvedUserPhone;
    sanitized.customer_phone_full = resolvedUserPhoneFull;
    sanitized.customer_image = resolvedUserImage;
    sanitized.customer_image_source =
      safeCustomer.user_image_source ??
      sanitized.customer_image_source ??
      null;
  } else {
    sanitized.user_id = null;
    sanitized.user_name = null;
    sanitized.user_gender = null;
    sanitized.user_country_code = null;
    sanitized.user_phone = null;
    sanitized.user_phone_full = null;
    sanitized.user_image = null;
    sanitized.user_image_source = null;
    sanitized.user_profile = null;
    sanitized.user_details = null;
    sanitized.customer = null;
    sanitized.customer_details = null;
    sanitized.customer_id = null;
    sanitized.customer_name = null;
    sanitized.customer_gender = null;
    sanitized.customer_country_code = null;
    sanitized.customer_phone = null;
    sanitized.customer_phone_full = null;
    sanitized.customer_image = null;
    sanitized.customer_image_source = null;
  }

  return withDriverImage(sanitized);
};

// ✅ rides cancelled (block dispatch)
const cancelledRides = new Set(); // rideId
const cancelledRideCleanupTimers = new Map(); // rideId -> timeout

// ─────────────────────────────
// Inbox helpers
// ─────────────────────────────
const rideTimers = new Map(); // rideId -> setTimeout ID

// الخريطة لحفظ تفاصيل الرحلات في الذاكرة (إذا احتجتها لاحقاً)
const rideDetailsMap = new Map();
function saveRideDetails(rideId, rideDetails) {
  cancelRetryStateCleanup(rideId);
  const previous = rideDetailsMap.get(rideId);
  const previousAnchor = extractRidePriceAnchor(previous ?? {});
  const incomingAnchor = extractRidePriceAnchor(rideDetails ?? {});
  const resolvedAnchor = previousAnchor ?? incomingAnchor ?? null;

  const anchoredRideDetails =
    resolvedAnchor && rideDetails && typeof rideDetails === "object"
      ? applyRidePriceAnchor(rideDetails, {
          ...resolvedAnchor,
          locked_at:
            resolvedAnchor?.locked_at ??
            incomingAnchor?.locked_at ??
            Date.now(),
        })
      : rideDetails;

  rideDetailsMap.set(rideId, anchoredRideDetails);
  touchRideState(rideId);
  debugLog("ride:details:saved", { ride_id: rideId }, null);
}
function getRideDetails(rideId) {
  return rideDetailsMap.get(rideId);
}

function getDriverQueuedRide(driverId) {
  return driverQueuedRide.get(driverId) ?? null;
}

function setDriverQueuedRide(driverId, payload) {
  if (!driverId || !payload?.ride_id || MAX_QUEUED_RIDES_PER_DRIVER <= 0) return false;

  const existing = driverQueuedRide.get(driverId);
  if (existing && toNumber(existing.ride_id) !== toNumber(payload.ride_id)) {
    return false;
  }

  driverQueuedRide.set(driverId, {
    ...payload,
    reserved_at: Date.now(),
    queued_at: Date.now(),
  });
  touchRideState(payload?.ride_id);
  return true;
}

function clearDriverQueuedRide(driverId) {
  if (!driverId) return;
  driverQueuedRide.delete(driverId);
}

function markRideCancelled(io, rideId, options = {}) {
  const safeRideId = toNumber(rideId);
  if (!safeRideId) return false;

  const { activateQueued = true } = options || {};
  const activeDriverIdBeforeCancel = getActiveDriverByRide(safeRideId);

  cancelledRides.add(safeRideId);
  const existingCleanupTimer = cancelledRideCleanupTimers.get(safeRideId);
  if (existingCleanupTimer) {
    clearTimeout(existingCleanupTimer);
  }
  const cleanupTimer = setTimeout(() => {
    cancelledRides.delete(safeRideId);
    cancelledRideCleanupTimers.delete(safeRideId);
  }, CANCELLED_RIDE_TTL_MS);
  cancelledRideCleanupTimers.set(safeRideId, cleanupTimer);

  removeRideFromAllInboxes(io, safeRideId);
  rideCandidates.delete(safeRideId);
  clearRideStateTouch(safeRideId);
  clearActiveRideByRideId(safeRideId);

  if (activateQueued && activeDriverIdBeforeCancel) {
    activateQueuedRideForDriver(io, activeDriverIdBeforeCancel);
  }

  return true;
}

function getActiveRideSnapshotByDriver(driverId) {
  const activeRideId = getActiveRideByDriver(driverId);
  if (!activeRideId) return null;
  return getRideDetails(activeRideId) ?? findRideInInboxes(activeRideId) ?? null;
}

function isDriverNearActiveRideDestination(driverId) {
  if (!driverId) return false;

  const activeRideId = getActiveRideByDriver(driverId);
  if (!activeRideId) return true;

  const driver = driverLocationService.getDriver(driverId);
  const activeRide = getActiveRideSnapshotByDriver(driverId);

  if (!driver || !activeRide) return false;

  const driverLat = toNumber(driver?.lat);
  const driverLong = toNumber(driver?.long);
  const destLat = toNumber(activeRide?.destination_lat);
  const destLong = toNumber(activeRide?.destination_long);

  if (driverLat === null || driverLong === null || destLat === null || destLong === null) {
    return false;
  }

  const remainingMeters = getDistanceMeters(driverLat, driverLong, destLat, destLong);
  return remainingMeters <= BUSY_DRIVER_FINISH_RADIUS_M;
}

function canDriverReceiveNewRideRequests(driverId) {
  if (!driverId) return false;
  const meta = driverLocationService.getMeta(driverId) || {};
  const walletBlocked = Number(meta?.not_valid_wallet_balance ?? 0) === 1;
  if (walletBlocked) return false;

  const activeRideId = getActiveRideByDriver(driverId);
  if (!activeRideId) return true;

  const activeRideSnapshot =
    getRideDetails(activeRideId) ??
    getRideSnapshotForRedispatch(activeRideId) ??
    findRideInInboxes(activeRideId) ??
    null;

  const activeRideStatus =
    toNumber(activeRideSnapshot?.ride_status) ??
    toNumber(activeRideSnapshot?.status) ??
    toNumber(getRideStatusSnapshot(activeRideId)?.ride_status) ??
    null;

  // إذا الرحلة الحالية لساتها accepted / arrived / started
  // لا تبعتلو أي رحلة ثانية نهائياً
  if ([1, 2, 3].includes(activeRideStatus)) {
    return false;
  }

  // إذا عنده queued ride أصلاً، لا تبعتلو كمان وحدة
  const queued = getDriverQueuedRide(driverId);
  if (queued) return false;

  // فقط بالحالات غير 1/2/3، فيك تترك منطق "قرب النهاية" يشتغل
  if (!ALLOW_BUSY_DRIVER_NEAR_FINISH) return false;

  return isDriverNearActiveRideDestination(driverId);
}

const isWalletMetaFreshEnough = (meta = null) => {
  const walletCheckedAt = toNumber(meta?.wallet_checked_at ?? null);
  if (!Number.isFinite(walletCheckedAt)) return false;
  return Date.now() - walletCheckedAt <= DRIVER_WALLET_GUARD_MAX_AGE_MS;
};

const mergeWalletMetaFromProfile = (currentMeta = {}, profile = null) => {
  const now = Date.now();
  if (!profile || typeof profile !== "object") {
    return {
      ...currentMeta,
      wallet_checked_at: now,
      updatedAt: now,
    };
  }

  const profileWalletBlocked = Number(
    profile.not_valid_wallet_balance ?? currentMeta.not_valid_wallet_balance ?? 0
  ) === 1;
  return {
    ...currentMeta,
    ...profile,
    not_valid_wallet_balance: profileWalletBlocked ? 1 : 0,
    can_receive_new_requests: profileWalletBlocked ? 0 : 1,
    wallet_checked_at: now,
    updatedAt: now,
  };
};

async function ensureFreshDriverWalletEligibility(driverId) {
  if (!DRIVER_WALLET_GUARD_ENABLED) {
    return canDriverReceiveNewRideRequests(driverId);
  }

  const safeDriverId = toNumber(driverId);
  if (!safeDriverId) return false;

  const currentMeta = driverLocationService.getMeta(safeDriverId) || {};
  const walletBlockedNow = Number(currentMeta?.not_valid_wallet_balance ?? 0) === 1;
  const walletFlagKnown =
    currentMeta?.not_valid_wallet_balance !== undefined &&
    currentMeta?.not_valid_wallet_balance !== null;

  if (walletFlagKnown && isWalletMetaFreshEnough(currentMeta)) {
    return canDriverReceiveNewRideRequests(safeDriverId);
  }

  if (walletGuardInFlightByDriver.has(safeDriverId)) {
    return walletGuardInFlightByDriver.get(safeDriverId);
  }

  const refreshPromise = (async () => {
    try {
      const refreshedProfile = await getDriverAdminProfile({
        driverId: safeDriverId,
        driverServiceId: toNumber(currentMeta?.driver_service_id ?? null),
        forceRefresh: true,
      });
      const mergedMeta = mergeWalletMetaFromProfile(currentMeta, refreshedProfile);
      driverLocationService.updateMeta(safeDriverId, mergedMeta);
      return canDriverReceiveNewRideRequests(safeDriverId);
    } catch (error) {
      warnThrottled(
        `dispatch-wallet-guard-refresh-failed:${safeDriverId}`,
        "[dispatch][wallet-guard] refresh failed; keeping current eligibility:",
        {
          driver_id: safeDriverId,
          error: error?.message || error,
        }
      );

      if (walletFlagKnown) {
        return !walletBlockedNow && canDriverReceiveNewRideRequests(safeDriverId);
      }

      // Fail-open only when wallet flag has never been resolved yet.
      return canDriverReceiveNewRideRequests(safeDriverId);
    } finally {
      walletGuardInFlightByDriver.delete(safeDriverId);
    }
  })();

  walletGuardInFlightByDriver.set(safeDriverId, refreshPromise);
  return refreshPromise;
}

function canDriverSubmitBidForRide(driverId, rideId) {
  if (!driverId || !rideId) return false;

  const activeRideId = getActiveRideByDriver(driverId);
  if (!activeRideId) return true;
  if (activeRideId === rideId) return true;

  const queued = getDriverQueuedRide(driverId);
  if (queued && toNumber(queued.ride_id) !== rideId) return false;

  return canDriverReceiveNewRideRequests(driverId);
}

function cancelRetryStateCleanup(rideId) {
  const timer = retryStateCleanupTimers.get(rideId);
  if (!timer) return;
  clearTimeout(timer);
  retryStateCleanupTimers.delete(rideId);
}

function scheduleRetryStateCleanup(
  rideId,
  { preserveSnapshot = false, preserveUser = false, ttlMs = RETRY_STATE_TTL_MS } = {}
) {
  if (!rideId || (!preserveSnapshot && !preserveUser)) {
    cancelRetryStateCleanup(rideId);
    return;
  }

  cancelRetryStateCleanup(rideId);

  const timer = setTimeout(() => {
    if (preserveSnapshot) {
      rideDetailsMap.delete(rideId);
    }
    if (preserveUser) {
      clearUserRideByRideId(rideId);
    }
    if (preserveSnapshot || preserveUser) {
      clearRideStateTouch(rideId);
    }
    retryStateCleanupTimers.delete(rideId);
    console.log(`[retry-state] cleared preserved state for ride ${rideId}`);
  }, ttlMs);

  retryStateCleanupTimers.set(rideId, timer);
}

const uniqueSortedNumbers = (values = []) =>
  Array.from(
    new Set(values.filter((value) => typeof value === "number" && Number.isFinite(value)))
  ).sort((a, b) => a - b);

const normalizeDispatchRadiusMeters = (radiusMeters) => {
  const parsed = toNumber(radiusMeters);
  if (parsed === null) return ROAD_RADIUS_METERS;

  return Math.max(
    MIN_DISPATCH_RADIUS_METERS,
    Math.min(parsed, MAX_DISPATCH_RADIUS_METERS)
  );
};

const buildDispatchRadiusStagesMeters = (initialRadiusMeters) => {
  const initialMeters = normalizeDispatchRadiusMeters(initialRadiusMeters);
  // Dashboard-first behavior: when dispatch stages are missing from payload,
  // keep a single stage instead of expanding through hardcoded km steps.
  return [initialMeters];
};

const normalizeDispatchStageIndex = (stagesMeters, stageIndex, currentRadiusMeters) => {
  if (!Array.isArray(stagesMeters) || stagesMeters.length === 0) return 0;

  const parsedStageIndex = toNumber(stageIndex);
  if (parsedStageIndex !== null) {
    return Math.max(0, Math.min(Math.floor(parsedStageIndex), stagesMeters.length - 1));
  }

  const normalizedCurrentRadius = normalizeDispatchRadiusMeters(currentRadiusMeters);
  const exactIndex = stagesMeters.findIndex((meters) => meters === normalizedCurrentRadius);
  if (exactIndex >= 0) return exactIndex;

  const nearestHigherIndex = stagesMeters.findIndex((meters) => meters >= normalizedCurrentRadius);
  return nearestHigherIndex >= 0 ? nearestHigherIndex : stagesMeters.length - 1;
};

const resolveDispatchRadiusPlan = (payload = null) => {
  const initialRadiusMeters = normalizeDispatchRadiusMeters(
    payload?.initial_dispatch_radius ?? payload?.radius ?? ROAD_RADIUS_METERS
  );

  const snapshotStages = Array.isArray(payload?.dispatch_radius_stages_m)
    ? uniqueSortedNumbers(
        payload.dispatch_radius_stages_m
          .map((value) => normalizeDispatchRadiusMeters(value))
          .filter((value) => value >= initialRadiusMeters)
      )
    : [];

  const stagesMeters =
    snapshotStages.length > 0
      ? uniqueSortedNumbers([initialRadiusMeters, ...snapshotStages])
      : buildDispatchRadiusStagesMeters(initialRadiusMeters);

  const currentRadiusMeters = normalizeDispatchRadiusMeters(payload?.radius ?? initialRadiusMeters);
  const currentStageIndex = normalizeDispatchStageIndex(
    stagesMeters,
    payload?.dispatch_stage_index,
    currentRadiusMeters
  );
  const normalizedCurrentRadiusMeters = stagesMeters[currentStageIndex] ?? currentRadiusMeters;
  const nextRadiusMeters = stagesMeters[currentStageIndex + 1] ?? null;

  return {
    initialRadiusMeters,
    currentRadiusMeters: normalizedCurrentRadiusMeters,
    currentStageIndex,
    stagesMeters,
    nextRadiusMeters,
    hasNextStage: nextRadiusMeters !== null,
  };
};

const resolveDispatchTimeoutSeconds = (payload = null) => {
  const parsed = toNumber(
    payload?.dispatch_timeout_s ??
      payload?.provider_accept_timeout ??
      payload?.timeout_s ??
      payload?.timeout_seconds ??
      null
  );

  return Math.max(1, Math.floor(parsed !== null ? parsed : RIDE_TIMEOUT_S));
};

const resolveCustomerOfferTimeoutSeconds = (payload = null) => {
  return CUSTOMER_SEARCH_TIMEOUT_S;
};

const resolveRideSearchLifetimeSeconds = (payload = null) => {
  const forceNewSearchWindow =
    toNumber(payload?.force_new_search_window ?? payload?.reset_search_window ?? null) === 1;
  const expiresAt = toNumber(payload?.expires_at);
  if (!forceNewSearchWindow && expiresAt !== null) {
    return Math.max(0, Math.floor(expiresAt - nowSec()));
  }

  return resolveCustomerOfferTimeoutSeconds(payload);
};

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

function isDriverAcceptLocked(driverId) {
  if (!driverId) return false;

  const now = Date.now();
  const last = driverAcceptLocks.get(driverId);

  if (last && now - last < DRIVER_ACCEPT_LOCK_TTL_MS) {
    return true;
  }

  driverAcceptLocks.set(driverId, now);

  setTimeout(() => {
    if (driverAcceptLocks.get(driverId) === now) {
      driverAcceptLocks.delete(driverId);
    }
  }, DRIVER_ACCEPT_LOCK_TTL_MS);

  return false;
}

function releaseDriverAcceptLock(driverId) {
  if (!driverId) return;
  driverAcceptLocks.delete(driverId);
}

function finalizeAcceptedRide(io, rideId, driverId, finalPrice, options = {}) {
  const {
    message = "User accepted the offer",
    rideDetails = null,
    userId = null,
    driverIdentity = null,
  } = options || {};

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

  const snapshot =
    rideDetails ?? (typeof getRideDetails === "function" ? getRideDetails(rideId) : null);
  const resolvedDriverIdentity = extractDriverIdentity(
    driverIdentity,
    snapshot,
    driverLocationService.getMeta(driverId) ?? null,
    { provider_id: driverId }
  );
  const currentActiveRideId = getActiveRideByDriver(driverId);
  const hasAnotherActiveRide = currentActiveRideId && currentActiveRideId !== rideId;
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

  if (hasAnotherActiveRide) {
    const alreadyQueued = getDriverQueuedRide(driverId);
    if (alreadyQueued && toNumber(alreadyQueued.ride_id) !== rideId) {
      console.log(
        `⚠️ driver ${driverId} already has queued ride ${alreadyQueued.ride_id}; cannot queue ride ${rideId}`
      );
      return;
    }

const queuedOk = setDriverQueuedRide(driverId, {
  ride_id: rideId,
  offered_price: finalPrice,
  ride_snapshot:
    rideDetailsPayload && typeof rideDetailsPayload === "object"
      ? {
          ...(snapshot && typeof snapshot === "object" ? snapshot : {}),
          ...rideDetailsPayload,
        }
      : snapshot ?? null,
  message,
});

    if (!queuedOk) {
      console.log(`⚠️ failed to queue ride ${rideId} for driver ${driverId}`);
      return;
    }

const queuedPayload = withDriverImage({
  ...(rideDetailsPayload && typeof rideDetailsPayload === "object"
    ? rideDetailsPayload
    : {}),
  ride_id: rideId,
  driver_id: driverId,
  ...buildDriverIdentityPayload(resolvedDriverIdentity, driverId),
  active_ride_id: currentActiveRideId,
  offered_price: finalPrice,
  message: "Ride accepted and queued until current ride ends",
  at: Date.now(),
}, driverId);

if (rideDetailsPayload) {
  queuedPayload.ride_details = rideDetailsPayload;
}

    io.to(driverRoom(driverId)).emit("ride:queued", queuedPayload);
    emitToRideAudience(io, rideId, "ride:queued", queuedPayload, userId);
    closeRideBidding(io, rideId, {
      clearUser: false,
      preserveQueued: true,
      preserveSnapshot: true,
    });

    console.log(
      `🟡 ride queued -> ride ${rideId} reserved for driver ${driverId} until active ride ${currentActiveRideId} ends`
    );
    return;
  }

  clearActiveRideByDriver(driverId);
  setActiveRide(driverId, rideId);

  const acceptedPayload = withDriverImage({
    ride_id: rideId,
    driver_id: driverId,
    ...buildDriverIdentityPayload(resolvedDriverIdentity, driverId),
    offered_price: finalPrice,
    message,
    at: Date.now(),
  }, driverId);
  if (rideDetailsPayload) acceptedPayload.ride_details = rideDetailsPayload;

  io.to(driverRoom(driverId)).emit("ride:userAccepted", acceptedPayload);
  emitToRideAudience(io, rideId, "ride:userAccepted", acceptedPayload, userId);
  emitToRideAudience(
    io,
    rideId,
    "ride:trackingStarted",
    withDriverImage({
      ride_id: rideId,
      driver_id: driverId,
      at: Date.now(),
    }, driverId),
    userId
  );
  closeRideBidding(io, rideId, { clearUser: false, preserveSnapshot: true });

  const d = driverLocationService.getDriver(driverId);
  if (d?.lat != null && d?.long != null) {
    emitToRideAudience(
      io,
      rideId,
      "ride:locationUpdate",
      {
        ride_id: rideId,
        driver_id: driverId,
        lat: d.lat,
        long: d.long,
        at: Date.now(),
      },
      userId
    );
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

function startRideTimeoutWithExpansion(io, rideId, remainingLifetimeSec = RIDE_TIMEOUT_S) {
  cancelRideTimeout(rideId);

  const snapshot = getRideSnapshotForRedispatch(rideId);
  const resolvedRemainingLifetimeSec = Math.max(
    0,
    Math.floor(
      toNumber(remainingLifetimeSec) ?? resolveRideSearchLifetimeSeconds(snapshot ?? null)
    )
  );
  if (resolvedRemainingLifetimeSec <= 0) {
    console.log(`Ride ${rideId} has timed out. Removing from memory.`);
    removeRideFromAllInboxes(io, rideId, {
      preserveSnapshot: true,
      preserveUser: true,
    });
    rideCandidates.delete(rideId);
    return;
  }

  const radiusPlan = snapshot ? resolveDispatchRadiusPlan(snapshot) : null;
  const waitSeconds = radiusPlan?.hasNextStage
    ? Math.min(DISPATCH_EXPANSION_INTERVAL_S, resolvedRemainingLifetimeSec)
    : resolvedRemainingLifetimeSec;
  const ms = waitSeconds * 1000;

  const timer = setTimeout(async () => {
    rideTimers.delete(rideId);

    if (cancelledRides.has(rideId)) {
      console.log(`Ride ${rideId} timeout ignored because ride is cancelled`);
      return;
    }

    const activeDriverId = getActiveDriverByRide(rideId);
    if (activeDriverId) {
      console.log(
        `Ride ${rideId} timeout ignored because ride already accepted by driver ${activeDriverId}`
      );
      return;
    }

    const latestSnapshot = getRideSnapshotForRedispatch(rideId);
    const latestRemainingLifetimeSec = resolveRideSearchLifetimeSeconds(latestSnapshot ?? null);
    if (latestRemainingLifetimeSec <= 0) {
      console.log(`Ride ${rideId} has timed out. Removing from memory.`);
      removeRideFromAllInboxes(io, rideId, {
        preserveSnapshot: true,
        preserveUser: true,
      });
      rideCandidates.delete(rideId);
      return;
    }

    const latestRadiusPlan = latestSnapshot ? resolveDispatchRadiusPlan(latestSnapshot) : null;
    if (latestRadiusPlan?.hasNextStage) {
      try {
        const expanded = await expandRideDispatchRadius(io, rideId, "interval");
        if (expanded) return;
      } catch (error) {
        console.log(
          `[dispatch][expand] interval expansion failed for ride ${rideId}:`,
          error?.message || error
        );
      }
    }

    startRideTimeoutWithExpansion(io, rideId, latestRemainingLifetimeSec);
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
  startRideTimeoutWithExpansion(io, rideId, Math.max(0, timer.expires_at - nowSec()));

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
    const affectedRideIds = new Set();

    for (const [driverId, box] of driverRideInbox.entries()) {
      if (!box?.has?.(rideId)) continue;
      const current = box.get(rideId);
      if (isRideOfferExpired(current)) {
        box.delete(rideId);
        clearDriverBidStatus(driverId, rideId);
        markRideDriverState(rideId, driverId, "expired");
        removeDriverFromRideCandidates(io, rideId, driverId, { emitSummary: false });
        emitDriverPatch(io, driverId, [{ op: "remove", ride_id: rideId }]);
        affectedRideIds.add(rideId);
        if (box.size === 0) driverRideInbox.delete(driverId);
        continue;
      }
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

    for (const affectedRideId of affectedRideIds) {
      emitRideCandidatesSummary(io, affectedRideId);
    }
  }

  return timer;
}

function refreshRideTimerWithDispatchTimeout(io, rideId, options = {}) {
  const { update_snapshot = true, patch_inboxes = true } = options || {};
  if (!rideId) return null;

  const rideSnapshot = getRideDetails(rideId);
  const customerOfferTimeoutSeconds = resolveCustomerOfferTimeoutSeconds(rideSnapshot);
  const timer = makeTimer(customerOfferTimeoutSeconds);

  startRideTimeoutWithExpansion(io, rideId, customerOfferTimeoutSeconds);

  if (update_snapshot) {
    const snap = getRideDetails(rideId);
    if (snap && typeof snap === "object") {
      saveRideDetails(rideId, {
        ...snap,
        customer_offer_timeout_s: customerOfferTimeoutSeconds,
        user_timeout: customerOfferTimeoutSeconds,
        ...timer,
      });
    }
  }

  if (patch_inboxes) {
    const affectedRideIds = new Set();

    for (const [driverId, box] of driverRideInbox.entries()) {
      if (!box?.has?.(rideId)) continue;
      const current = box.get(rideId);
      if (isRideOfferExpired(current)) {
        box.delete(rideId);
        clearDriverBidStatus(driverId, rideId);
        markRideDriverState(rideId, driverId, "expired");
        removeDriverFromRideCandidates(io, rideId, driverId, { emitSummary: false });
        emitDriverPatch(io, driverId, [{ op: "remove", ride_id: rideId }]);
        affectedRideIds.add(rideId);
        if (box.size === 0) driverRideInbox.delete(driverId);
        continue;
      }
      if (!current || typeof current !== "object") continue;

      const updated = {
        ...current,
        ...timer,
        _ts: Date.now(),
      };
      const updatedWithCustomer = attachCustomerFields(updated, current?.user_details ?? null);

      box.set(rideId, updatedWithCustomer);
      emitDriverPatch(io, driverId, [{ op: "upsert", ride: updatedWithCustomer }]);
    }

    for (const affectedRideId of affectedRideIds) {
      emitRideCandidatesSummary(io, affectedRideId);
    }
  }

  return timer;
}

function inboxUpsert(driverId, rideId, payload) {
  const currentState = getRideDriverState(rideId, driverId);
  if (isTerminalDriverRideState(currentState?.status)) {
    console.log("[inboxUpsert] skipped terminal driver ride state", {
      driver_id: driverId,
      ride_id: rideId,
      status: currentState.status,
    });
    return false;
  }

  if (!driverRideInbox.has(driverId)) driverRideInbox.set(driverId, new Map());
  const prepared = attachCustomerFields(payload, payload?.user_details ?? null);
  driverRideInbox.get(driverId).set(rideId, {
    ...prepared,
    ride_id: rideId,
    _ts: Date.now(), // ✅ مهم للتنظيف
  });
  touchRideState(rideId);
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
  const affectedRideIds = new Set();

  for (const [rideId, ride] of box.entries()) {
    const ts = toNumber(ride?._ts) ?? 0;
    if (isRideOfferExpired(ride) || (ts && now - ts > INBOX_ENTRY_TTL_MS)) {
      box.delete(rideId);
      clearDriverBidStatus(driverId, rideId);
      markRideDriverState(rideId, driverId, "expired");
      removeDriverFromRideCandidates(io, rideId, driverId, { emitSummary: false });
      removedOps.push({ op: "remove", ride_id: rideId });
      affectedRideIds.add(rideId);
      console.log(`🧹 [inbox prune] removed old ride ${rideId} from driver ${driverId}`);
    }
  }

  if (box.size === 0) driverRideInbox.delete(driverId);

  if (removedOps.length > 0) {
    emitDriverPatch(io, driverId, removedOps);
    for (const rideId of affectedRideIds) {
      emitRideCandidatesSummary(io, rideId);
    }
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
        if (isRideOfferExpired(ride) || (ts && now - ts > INBOX_ENTRY_TTL_MS)) {
          box.delete(rideId);
          clearDriverBidStatus(driverId, rideId);
          markRideDriverState(rideId, driverId, "expired");
          removeDriverFromRideCandidates(null, rideId, driverId, { emitSummary: false });
        }
      }
      if (box.size === 0) driverRideInbox.delete(driverId);
    }
  } catch (e) {
    console.log("⚠️ [inbox prune] interval error:", e?.message || e);
  }
}, 30 * 1000);

// ✅ periodic stale-ride state sweep (covers maps that can survive missed close/cancel flows)
setInterval(() => {
  try {
    const now = Date.now();
    let removed = 0;

    for (const [rideIdRaw, lastActivityAtRaw] of rideStateActivityAt.entries()) {
      const rideId = toNumber(rideIdRaw);
      const lastActivityAt = toNumber(lastActivityAtRaw) ?? 0;
      if (!rideId || !lastActivityAt) {
        rideStateActivityAt.delete(rideIdRaw);
        continue;
      }

      if (now - lastActivityAt < RIDE_STATE_STALE_TTL_MS) continue;
      if (rideTimers.has(rideId)) continue;
      if (getActiveDriverByRide(rideId)) continue;
      if (cancelledRides.has(rideId)) continue;

      rideDetailsMap.delete(rideId);
      rideCandidates.delete(rideId);
      clearRideDriverStates(rideId);
      clearUserRideByRideId(rideId);
      cancelRetryStateCleanup(rideId);
      dispatchInFlightByRide.delete(rideId);

      for (const [driverId, box] of driverRideInbox.entries()) {
        if (!box || !box.has(rideId)) continue;
        box.delete(rideId);
        clearDriverBidStatus(driverId, rideId);
        if (box.size === 0) driverRideInbox.delete(driverId);
      }

      for (const [driverId, queued] of driverQueuedRide.entries()) {
        if (toNumber(queued?.ride_id) !== rideId) continue;
        driverQueuedRide.delete(driverId);
      }

      clearRideStateTouch(rideId);
      removed += 1;
    }

    if (removed > 0) {
      console.log("[memory-sweep] removed stale ride state entries", {
        removed,
        ttl_ms: RIDE_STATE_STALE_TTL_MS,
      });
    }
  } catch (e) {
    console.log("[memory-sweep] interval error:", e?.message || e);
  }
}, RIDE_STATE_SWEEP_EVERY_MS);

function inboxList(driverId, limit = 30) {
  const box = driverRideInbox.get(driverId);
  if (!box) return [];

  const visibleRides = [];
  for (const [rideId, ride] of box.entries()) {
    const currentState = getRideDriverState(rideId, driverId);
    const statusSnapshot = getRideStatusSnapshot(rideId);
    const rideStatus =
      toNumber(statusSnapshot?.ride_status) ??
      toNumber(ride?.ride_status) ??
      toNumber(ride?.status) ??
      null;

    if (
      isTerminalDriverRideState(currentState?.status) ||
      cancelledRides.has(toNumber(rideId)) ||
      isTerminalRideStatus(rideStatus)
    ) {
      box.delete(rideId);
      continue;
    }
    visibleRides.push(ride);
  }

  if (box.size === 0) driverRideInbox.delete(driverId);

  return visibleRides
    .sort((a, b) => (b._ts || 0) - (a._ts || 0))
    .slice(0, limit);
}

function getDriverInboxStats(driverId) {
  const safeDriverId = toNumber(driverId);
  if (!safeDriverId) {
    return { driver_id: null, has_data: false, total: 0, ride_ids: [] };
  }

  const box = driverRideInbox.get(safeDriverId);
  if (!box || box.size === 0) {
    return { driver_id: safeDriverId, has_data: false, total: 0, ride_ids: [] };
  }

  const now = Date.now();
  let total = 0;
  const rideIds = [];

  for (const [rideId, ride] of box.entries()) {
    if (!ride || typeof ride !== "object") continue;
    if (isRideOfferExpired(ride)) continue;

    const ts = toNumber(ride?._ts);
    if (ts && now - ts > INBOX_ENTRY_TTL_MS) continue;

    total += 1;
    const safeRideId = toNumber(rideId);
    if (safeRideId) {
      rideIds.push(safeRideId);
    }
  }

  return {
    driver_id: safeDriverId,
    has_data: total > 0,
    total,
    ride_ids: rideIds,
  };
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
  if (list.length > 0) {
    driverRidesListEmptyLoggedAt.delete(driverId);
    console.log("[driver:rides:list] payload", {
      driver_id: driverId,
      rides: list.map((ride) => ({
        ride_id: ride?.ride_id ?? null,
        duration: getRideRouteApiDurationRaw(ride) ?? getRideDurationRaw(ride),
        route_api_distance_km: getRideRouteApiDistanceKmRaw(ride) ?? getRideDistanceKm(ride),
        min_price: toNumber(ride?.min_price ?? ride?.ride_details?.min_price ?? null),
        max_price: toNumber(ride?.max_price ?? ride?.ride_details?.max_price ?? null),
        min_fare: toNumber(ride?.min_fare ?? ride?.ride_details?.min_fare ?? null),
        max_fare: toNumber(ride?.max_fare ?? ride?.ride_details?.max_fare ?? null),
      })),
    });
  } else {
    const now = Date.now();
    const lastLoggedAt = driverRidesListEmptyLoggedAt.get(driverId) ?? 0;
    if (now - lastLoggedAt >= DRIVER_RIDES_LIST_EMPTY_LOG_THROTTLE_MS) {
      driverRidesListEmptyLoggedAt.set(driverId, now);
      console.log("[driver:rides:list] payload", {
        driver_id: driverId,
        rides: [],
      });
    }
  }
  io.to(driverRoom(driverId)).emit(eventName, {
    driver_id: driverId,
    event_type: "driver_bid_list",
    ui_action: "show_bid_list",
    auto_open_running: false,
    rides: list,
    total: list.length,
    at: Date.now(),
  });
}

function emitPendingBidRequestsForDriver(io, driverId, source = "driver:getRidesList") {
  const safeDriverId = toNumber(driverId);
  if (!safeDriverId) return { attempted: 0, delivered: 0, pending: 0 };

  pruneDriverInbox(io, safeDriverId);
  const box = driverRideInbox.get(safeDriverId);
  if (!box || box.size === 0) return { attempted: 0, delivered: 0, pending: 0 };

  let attempted = 0;
  let delivered = 0;
  let pending = 0;

  for (const [rideId, ride] of box.entries()) {
    const safeRideId = toNumber(rideId);
    if (!safeRideId || !ride || typeof ride !== "object") continue;
    if (isRideOfferExpired(ride)) continue;

    const state = getRideDriverState(safeRideId, safeDriverId);
    const rideStatusSnapshot = getRideStatusSnapshot(safeRideId);
    const rideStatus =
      toNumber(rideStatusSnapshot?.ride_status) ??
      toNumber(ride?.ride_status) ??
      toNumber(ride?.status) ??
      null;

    if (
      isTerminalDriverRideState(state?.status) ||
      cancelledRides.has(safeRideId) ||
      isTerminalRideStatus(rideStatus)
    ) {
      continue;
    }

    if (state?.status === "notified" && state?.notified_at) {
      continue;
    }

    const bidRequestPayload = sanitizeRidePayloadForClient({
      ...ride,
      event_type: "driver_new_bid_request",
      ui_action: "show_bid_request",
      auto_open_running: false,
      is_running_ride: false,
    });

    const emitResult = tryEmitBidRequestToDriver(io, {
      rideId: safeRideId,
      driverId: safeDriverId,
      bidRequestPayload,
      ridePayloadForDriver: ride,
      dispatchStageIndex: state?.last_dispatch_stage_index ?? null,
      dispatchRadiusMeters: state?.last_dispatch_radius_m ?? null,
      source,
      attempt: 1,
    });

    attempted += 1;
    if (emitResult.delivered) {
      delivered += 1;
      continue;
    }

    pending += 1;
    scheduleBidRequestRetry(io, {
      rideId: safeRideId,
      driverId: safeDriverId,
      bidRequestPayload,
      ridePayloadForDriver: ride,
      dispatchStageIndex: state?.last_dispatch_stage_index ?? null,
      dispatchRadiusMeters: state?.last_dispatch_radius_m ?? null,
      source,
      attempt: 1,
    });
  }

  if (attempted > 0 || pending > 0) {
    console.log("[dispatch][recovery-report]", {
      driver_id: safeDriverId,
      source,
      attempted,
      delivered,
      pending,
    });
  }

  return { attempted, delivered, pending };
}

function recoverDriverPendingDispatch(io, driverId, source = "driver:recovery", options = {}) {
  const safeDriverId = toNumber(driverId);
  if (!safeDriverId) return { attempted: 0, delivered: 0, pending: 0 };
  const emitInbox = options?.emitInbox !== false;

  if (emitInbox) {
    emitDriverInbox(io, safeDriverId, "driver:rides:list");
  }
  const recoveryReport = emitPendingBidRequestsForDriver(io, safeDriverId, source);
  const hasRecoveryWork =
    recoveryReport.attempted > 0 || recoveryReport.delivered > 0 || recoveryReport.pending > 0;
  if (hasRecoveryWork || source !== "driver:getRidesList") {
    console.log("[dispatch][driver-recovery]", {
      driver_id: safeDriverId,
      source,
      attempted: recoveryReport.attempted,
      delivered: recoveryReport.delivered,
      pending: recoveryReport.pending,
    });
  } else {
    const key = `${source}:${safeDriverId}`;
    const now = Date.now();
    const lastLoggedAt = driverRecoveryNoopLoggedAt.get(key) ?? 0;
    if (now - lastLoggedAt >= DRIVER_RECOVERY_NOOP_LOG_THROTTLE_MS) {
      driverRecoveryNoopLoggedAt.set(key, now);
      console.log("[dispatch][driver-recovery]", {
        driver_id: safeDriverId,
        source,
        attempted: 0,
        delivered: 0,
        pending: 0,
      });
    }
  }
  return recoveryReport;
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

function resolveRideAudienceUserId(rideId, explicitUserId = null) {
  const safeExplicitUserId = toNumber(explicitUserId);
  if (safeExplicitUserId) return safeExplicitUserId;

  const mappedUserId = toNumber(getUserIdForRide(rideId));
  if (mappedUserId) return mappedUserId;

  const snapshot = getRideSnapshotForRedispatch(rideId);
  return toNumber(snapshot?.user_id ?? snapshot?.user_details?.user_id ?? null);
}

function emitToRideAudience(io, rideId, eventName, payload, explicitUserId = null) {
  const safeRideId = toNumber(rideId);
  if (!safeRideId) return;
  const safePayload = withDriverImage(payload);

  const audienceUserId = resolveRideAudienceUserId(safeRideId, explicitUserId);
  const rideRoomName = rideRoom(safeRideId);
  const userRoomName = audienceUserId ? userRoom(audienceUserId) : null;

  let emitter = io.to(rideRoomName);

  if (audienceUserId) {
    emitter = emitter.to(userRoomName);
  }

  if (eventName === "ride:newBid") {
    const snapshot = buildNewBidEmitDebugSnapshot(safePayload);
    console.log("[emit][ride:newBid]", {
      ...snapshot,
      audience_user_id: audienceUserId ?? null,
      ride_room: rideRoomName,
      ride_room_sockets: getRoomSocketCount(io, rideRoomName),
      user_room: userRoomName,
      user_room_sockets: userRoomName ? getRoomSocketCount(io, userRoomName) : 0,
      at: Date.now(),
    });
  }

  emitter.emit(eventName, safePayload);
}

async function expandRideDispatchRadius(io, rideId, reason = "timeout") {
  const safeRideId = toNumber(rideId);
  if (!safeRideId || cancelledRides.has(safeRideId)) return false;

  const activeDriverId = getActiveDriverByRide(safeRideId);
  if (activeDriverId) return false;

  const snapshot = getRideSnapshotForRedispatch(safeRideId);
  if (!snapshot || typeof snapshot !== "object") return false;

  const radiusPlan = resolveDispatchRadiusPlan(snapshot);
  if (!radiusPlan.hasNextStage || radiusPlan.nextRadiusMeters === null) return false;

  const nextStageIndex = Math.min(
    radiusPlan.currentStageIndex + 1,
    radiusPlan.stagesMeters.length - 1
  );
  const nextRadiusMeters = radiusPlan.stagesMeters[nextStageIndex];

  console.log("[dispatch][expand]", {
    ride_id: safeRideId,
    reason,
    from_radius_m: radiusPlan.currentRadiusMeters,
    to_radius_m: nextRadiusMeters,
    stage_number: nextStageIndex + 1,
    total_stages: radiusPlan.stagesMeters.length,
  });

  const redispatchPayload = {
    ...snapshot,
    ride_id: safeRideId,
    radius: nextRadiusMeters,
    initial_dispatch_radius: radiusPlan.initialRadiusMeters,
    dispatch_stage_index: nextStageIndex,
    dispatch_radius_stages_m: radiusPlan.stagesMeters,
    dispatch_expand_reason: reason,
    dispatch_incremental_only: 1,
    dispatch_expanded_at: Date.now(),
    ...(snapshot?.duration != null || snapshot?.route_api_distance_km != null
      ? {
          prefer_frontend_route_metrics: 1,
          route_metrics_source: snapshot?.route_metrics_source ?? "snapshot",
        }
      : {}),
  };

  return !!(await dispatchToNearbyDrivers(io, redispatchPayload));
}

async function restartRideDispatch(io, payload = {}) {
  const safeRideId = toNumber(payload?.ride_id ?? payload?.id);
  if (!safeRideId) return false;
  if (cancelledRides.has(safeRideId)) return false;

  const activeDriverId = getActiveDriverByRide(safeRideId);
  if (activeDriverId) {
    console.log(
      `[dispatch][retry] blocked: ride ${safeRideId} already accepted by driver ${activeDriverId}`
    );
    return false;
  }

  const snapshot = getRideSnapshotForRedispatch(safeRideId);
  const initialRadiusMeters =
    normalizeDispatchRadiusMeters(
      payload?.initial_dispatch_radius ??
        snapshot?.initial_dispatch_radius ??
        payload?.radius ??
        snapshot?.radius ??
        null
    ) ?? ROAD_RADIUS_METERS;
  const shouldHardReset =
    toBinaryFlag(payload?.hard_reset ?? payload?.reset_candidates ?? null) === 1;
  if (shouldHardReset) {
    removeRideFromAllInboxes(io, safeRideId, {
      preserveSnapshot: true,
      preserveUser: true,
      emitUnavailable: false,
    });
  }

  const restartPayload = {
    ...(snapshot && typeof snapshot === "object" ? snapshot : {}),
    ...(payload && typeof payload === "object" ? payload : {}),
    ride_id: safeRideId,
    radius: initialRadiusMeters,
    initial_dispatch_radius: initialRadiusMeters,
    dispatch_stage_index: 0,
    dispatch_incremental_only: 0,
    dispatch_expand_reason: "retry",
    force_new_search_window: 1,
    server_time: null,
    expires_at: null,
    timeout_ms: null,
  };

  return !!(await dispatchToNearbyDrivers(io, restartPayload));
}

// ✅ remove ride from all drivers inboxes (safe global scan) + PATCH remove
function removeRideFromAllInboxes(io, rideId, options = {}) {
  const {
    preserveSnapshot = false,
    preserveUser = false,
    emitUnavailable = true,
    retryStateTtlMs = RETRY_STATE_TTL_MS,
  } = options || {};
  cancelRideTimeout(rideId);

  if (!preserveUser) {
    clearUserRideByRideId(rideId);
  }
  if (!preserveSnapshot) {
    rideDetailsMap.delete(rideId);
  }
  if (preserveSnapshot || preserveUser) {
    scheduleRetryStateCleanup(rideId, {
      preserveSnapshot,
      preserveUser,
      ttlMs: retryStateTtlMs,
    });
    console.log("[retry-state] preserved ride state for retry", {
      ride_id: rideId,
      preserve_snapshot: preserveSnapshot,
      preserve_user: preserveUser,
      ttl_ms: retryStateTtlMs,
    });
  } else {
    cancelRetryStateCleanup(rideId);
  }
  acceptLocks.delete(rideId);

  for (const [driverId, box] of driverRideInbox.entries()) {
    if (box.has(rideId)) {
      box.delete(rideId);
      clearDriverBidStatus(driverId, rideId);

      if (box.size === 0) driverRideInbox.delete(driverId);

      emitDriverPatch(io, driverId, [{ op: "remove", ride_id: rideId }]);
      if (emitUnavailable) {
        emitRideUnavailable(io, driverId, rideId);
      }
    }
  }

  for (const [driverId, queued] of driverQueuedRide.entries()) {
    if (toNumber(queued?.ride_id) !== rideId) continue;

    driverQueuedRide.delete(driverId);
    io.to(driverRoom(driverId)).emit("ride:queueRemoved", {
      ride_id: rideId,
      driver_id: driverId,
      at: Date.now(),
    });
    if (emitUnavailable) {
      emitRideUnavailable(io, driverId, rideId);
    }
  }

  rideCandidates.delete(rideId);
  clearRideDriverStates(rideId);
  clearRideStateTouch(rideId);
}

// ✅ Close bidding for a ride (remove from all inboxes) + PATCH remove
function closeRideBidding(io, rideId, opts = {}) {
  cancelRideTimeout(rideId);
  cancelRetryStateCleanup(rideId);

  const clearUser = opts.clearUser !== false;
  const preserveQueued = opts.preserveQueued === true;
  const preserveSnapshot = opts.preserveSnapshot === true;
  if (clearUser) clearUserRideByRideId(rideId);
  if (!preserveSnapshot) {
    rideDetailsMap.delete(rideId);
  }
  acceptLocks.delete(rideId);

  for (const [driverId, box] of driverRideInbox.entries()) {
    if (box.has(rideId)) {
      box.delete(rideId);
      clearDriverBidStatus(driverId, rideId);
      if (box.size === 0) driverRideInbox.delete(driverId);

      emitDriverPatch(io, driverId, [{ op: "remove", ride_id: rideId }]);
      emitRideUnavailable(io, driverId, rideId);
    }
  }

  if (!preserveQueued) {
    for (const [driverId, queued] of driverQueuedRide.entries()) {
      if (toNumber(queued?.ride_id) !== rideId) continue;

      driverQueuedRide.delete(driverId);
      io.to(driverRoom(driverId)).emit("ride:queueRemoved", {
        ride_id: rideId,
        driver_id: driverId,
        at: Date.now(),
      });
      emitRideUnavailable(io, driverId, rideId);
    }
  }

  rideCandidates.delete(rideId);
  clearRideDriverStates(rideId);
  clearRideStateTouch(rideId);
}

// ─────────────────────────────
// Dispatch
// ─────────────────────────────
function syncRideCandidates(io, rideId, nextDriverIds = [], options = {}) {
  const { preserveExisting = false } = options || {};
  const normalizedDriverIds = nextDriverIds
    .map((driverId) => toNumber(driverId))
    .filter((driverId) => !!driverId);
  const nextSet = new Set(normalizedDriverIds);
  const prevSet = rideCandidates.get(rideId) ?? new Set();

  if (preserveExisting) {
    const mergedSet = new Set(prevSet);
    const newlyAddedIds = [];

    for (const driverId of nextSet.values()) {
      if (mergedSet.has(driverId)) continue;
      mergedSet.add(driverId);
      newlyAddedIds.push(driverId);
    }

    rideCandidates.set(rideId, mergedSet);
    touchRideState(rideId);

    return {
      candidateSet: mergedSet,
      notifyDriverIds: newlyAddedIds,
      newlyAddedIds,
      removedIds: [],
    };
  }

  const removedIds = [];
  for (const driverId of prevSet.values()) {
    if (nextSet.has(driverId)) continue;

    const existed = inboxRemove(driverId, rideId);
    clearDriverBidStatus(driverId, rideId);
    removedIds.push(driverId);
    if (existed) {
      emitDriverPatch(io, driverId, [{ op: "remove", ride_id: rideId }]);
    }
  }

  rideCandidates.set(rideId, nextSet);
  touchRideState(rideId);

  return {
    candidateSet: nextSet,
    notifyDriverIds: normalizedDriverIds,
    newlyAddedIds: normalizedDriverIds.filter((driverId) => !prevSet.has(driverId)),
    removedIds,
  };
}

function shouldKeepExistingCandidateForRide(rideId, driverId) {
  const safeDriverId = toNumber(driverId);
  if (!safeDriverId) return false;

  const driver = driverLocationService.getDriver(safeDriverId);
  const meta = driverLocationService.getMeta(safeDriverId) || {};

  if (!driver || !isDriverLocationFresh(driver, meta)) return false;

  const isOnline = Number(driver?.is_online ?? meta?.is_online ?? 1) === 1;
  if (!isOnline) return false;
  const walletBlocked = Number(meta?.not_valid_wallet_balance ?? 0) === 1;
  if (walletBlocked) return false;

  // إذا عنده active ride ثانية غير هالرحلة -> لا تحتفظ فيه
  const activeRideId = getActiveRideByDriver(safeDriverId);
  if (activeRideId && activeRideId !== rideId) return false;

  // إذا عنده queued ride ثانية غير هالرحلة -> لا تحتفظ فيه
  const queued = getDriverQueuedRide(safeDriverId);
  if (queued && toNumber(queued.ride_id) !== rideId) return false;

  // لا تعتمد على inbox هنا
  // لأننا عم نحذف الرحلة من inbox بعد submit/accept
  const state = getRideDriverState(rideId, safeDriverId);
  if (isTerminalDriverRideState(state?.status)) return false;

  return true;
}

async function dispatchToNearbyDrivers(io, data) {
  const inputPayload = data && typeof data === "object" ? data : {};
  const rideId = toNumber(inputPayload?.ride_id ?? inputPayload?.id);
  if (!rideId) return false;
  const existingInFlightToken = dispatchInFlightByRide.get(rideId);
  if (existingInFlightToken) {
    console.log(`[dispatch][dedup] ride ${rideId} skipped duplicate in-flight dispatch`);
    return true;
  }
  const inFlightToken = `${Date.now()}:${Math.random().toString(36).slice(2)}`;
  dispatchInFlightByRide.set(rideId, inFlightToken);

  try {
    data = inputPayload;

  // Re-broadcast decision must rely on explicit input only, not merged snapshot fields.
  const forcedRebroadcastFromInput =
    toBinaryFlag(inputPayload?.force_rebroadcast ?? inputPayload?.rebroadcast_all ?? null) === 1;

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
  const radiusPlan = resolveDispatchRadiusPlan(data);
  const roadRadius = radiusPlan.currentRadiusMeters;
  const dispatchTimeoutSeconds = resolveDispatchTimeoutSeconds(data);
  const customerOfferTimeoutSeconds = resolveCustomerOfferTimeoutSeconds(data);
  const searchTimeoutSeconds = resolveRideSearchLifetimeSeconds(data);
  if (searchTimeoutSeconds <= 0) {
    console.log(`[dispatch] blocked: ride ${rideId} search window already expired`);
    removeRideFromAllInboxes(io, rideId, {
      preserveSnapshot: true,
      preserveUser: true,
    });
    rideCandidates.delete(rideId);
    return false;
  }

  const airCandidateRadius = Math.max(roadRadius, AIR_CANDIDATE_RADIUS_METERS);

  if (rawRadius !== roadRadius) {
    console.log(`[dispatch] road radius clamped for ride ${rideId}: ${rawRadius} -> ${roadRadius}`);
  }

  const lat = toNumber(data?.pickup_lat);
  const long = toNumber(data?.pickup_long);

  const serviceTypeId = toNumber(data?.service_type_id) ?? null;
  const previousRideSnapshot =
    getRideDetails(rideId) ??
    getRideSnapshotForRedispatch(rideId) ??
    null;
  const persistedBaseFare = pickFirstValue(
    toNumber(previousRideSnapshot?.base_fare),
    toNumber(previousRideSnapshot?.ride_details?.base_fare),
    toNumber(previousRideSnapshot?.meta?.base_fare)
  );
  const persistedMinPrice = pickFirstValue(
    toNumber(previousRideSnapshot?.min_price),
    toNumber(previousRideSnapshot?.ride_details?.min_price),
    toNumber(previousRideSnapshot?.ride_details?.min_fare),
    toNumber(previousRideSnapshot?.meta?.min_price),
    toNumber(previousRideSnapshot?.meta?.min_fare)
  );
  const persistedMaxPrice = pickFirstValue(
    toNumber(previousRideSnapshot?.max_price),
    toNumber(previousRideSnapshot?.ride_details?.max_price),
    toNumber(previousRideSnapshot?.ride_details?.max_fare),
    toNumber(previousRideSnapshot?.meta?.max_price),
    toNumber(previousRideSnapshot?.meta?.max_fare)
  );
  const snapshotBounds = normalizePriceBoundsPair(persistedMinPrice, persistedMaxPrice);
  const incomingEstimatedBaseFare = getEstimatedPriceFromPayload(data);
  const incomingSystemBaseFare = pickFirstValue(
    incomingEstimatedBaseFare,
    getBaseFareFromPayload(data)
  );
  const resolvedBaseFare = pickFirstValue(persistedBaseFare, incomingSystemBaseFare);
  const base =
    toNumber(data?.user_bid_price) ??
    toNumber(data?.price) ??
    toNumber(data?.offered_price) ??
    null;
  const incomingExplicitMin = pickFirstValue(
    toNumber(data?.min_price),
    toNumber(data?.min_fare),
    toNumber(data?.MIN_PRICE),
    toNumber(data?.min_fare_amount),
    toNumber(data?.ride_details?.min_price),
    toNumber(data?.ride_details?.min_fare),
    toNumber(data?.ride_details?.MIN_PRICE),
    toNumber(data?.ride_details?.min_fare_amount),
    toNumber(data?.meta?.min_price),
    toNumber(data?.meta?.min_fare),
    toNumber(data?.meta?.MIN_PRICE),
    toNumber(data?.meta?.min_fare_amount)
  );
  const incomingExplicitMax = pickFirstValue(
    toNumber(data?.max_price),
    toNumber(data?.max_fare),
    toNumber(data?.MAX_PRICE),
    toNumber(data?.max_fare_amount),
    toNumber(data?.ride_details?.max_price),
    toNumber(data?.ride_details?.max_fare),
    toNumber(data?.ride_details?.MAX_PRICE),
    toNumber(data?.ride_details?.max_fare_amount),
    toNumber(data?.meta?.max_price),
    toNumber(data?.meta?.max_fare),
    toNumber(data?.meta?.MAX_PRICE),
    toNumber(data?.meta?.max_fare_amount)
  );
  let priceBounds = null;
  if (snapshotBounds.min_price !== null && snapshotBounds.max_price !== null) {
    priceBounds = {
      base_fare: resolvedBaseFare !== null ? round2(resolvedBaseFare) : null,
      min_price: snapshotBounds.min_price,
      max_price: snapshotBounds.max_price,
    };
  } else if (incomingEstimatedBaseFare !== null) {
    priceBounds = buildPriceBounds(incomingEstimatedBaseFare);
  } else if (incomingExplicitMin !== null && incomingExplicitMax !== null) {
    const normalizedIncoming = normalizePriceBoundsPair(
      incomingExplicitMin,
      incomingExplicitMax
    );
    priceBounds = {
      base_fare: resolvedBaseFare !== null ? round2(resolvedBaseFare) : null,
      min_price: normalizedIncoming.min_price,
      max_price: normalizedIncoming.max_price,
    };
  } else if (resolvedBaseFare !== null) {
    priceBounds = buildPriceBounds(resolvedBaseFare);
  } else {
    priceBounds = getRidePriceBounds(data);
  }
  const minBound = toNumber(priceBounds?.min_price);
  const maxBound = toNumber(priceBounds?.max_price);
  let dispatchBidPrice = base;
  if (dispatchBidPrice === null) {
    dispatchBidPrice = toNumber(priceBounds?.base_fare);
  }
  if (dispatchBidPrice !== null && minBound !== null && dispatchBidPrice < minBound) {
    dispatchBidPrice = minBound;
  }
  if (dispatchBidPrice !== null && maxBound !== null && dispatchBidPrice > maxBound) {
    dispatchBidPrice = maxBound;
  }
  dispatchBidPrice = dispatchBidPrice !== null ? round2(dispatchBidPrice) : null;
  const legacyMinFareAmount =
    toNumber(priceBounds?.min_price) ??
    (incomingExplicitMin !== null && incomingExplicitMin > 0 ? incomingExplicitMin : 0);
  const legacyMaxFareAmount =
    toNumber(priceBounds?.max_price) ??
    (base !== null && base > 0 ? round2(base * 2) : 0);

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
  const additionalRemarks = resolveAdditionalRemarks(data);

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
  const requiredGenderInfo = resolveDispatchPreferenceInfo(data, [
    "required_driver_gender",
    "required_gender",
    "driver_gender",
    "gender",
    "requiredDriverGender",
    "driverGender",
  ]);
  const needChildSeatInfo = resolveDispatchPreferenceInfo(data, [
    "need_child_seat",
    "child_seat",
    "require_child_seat",
    "smoking",
    "need_smoking",
    "smoking_value",
    "child_seat_accessibility",
  ]);
  const needHandicapInfo = resolveDispatchPreferenceInfo(data, [
    "need_handicap",
    "handicap",
    "require_handicap",
    "need_special_needs",
    "special_needs",
    "handicap_accessibility",
    "can_receive_special_needs",
  ]);
  const requiredGenderRaw = requiredGenderInfo.value;
  const needChildSeatRaw = needChildSeatInfo.value;
  const needHandicapRaw = needHandicapInfo.value;
  const requiredGenderProvided = requiredGenderInfo.provided;
  const needChildSeatProvided = needChildSeatInfo.provided;
  const needHandicapProvided = needHandicapInfo.provided;
  const requiredGender = toGenderFilter(requiredGenderRaw);
  const needChildSeat = toBinaryFlag(needChildSeatRaw);
  const needHandicap = toBinaryFlag(needHandicapRaw);
  const applyRequiredGenderFilter =
    requiredGenderProvided && (requiredGender === 1 || requiredGender === 2);
  const applyNeedChildSeatFilter =
    needChildSeatProvided && needChildSeat === 1;
  const applyNeedHandicapFilter =
    needHandicapProvided && needHandicap === 1;
  const dispatchPreferencePayload = {
    ...(applyRequiredGenderFilter
      ? {
          required_driver_gender: requiredGender,
          required_gender: requiredGender,
          driver_gender: requiredGender,
          gender: requiredGender,
        }
      : {}),
    ...(applyNeedChildSeatFilter
      ? {
          need_child_seat: needChildSeat,
          child_seat: needChildSeat,
          require_child_seat: needChildSeat,
          smoking: needChildSeat,
          need_smoking: needChildSeat,
          smoking_value: needChildSeat,
          child_seat_accessibility: needChildSeat,
        }
      : {}),
    ...(applyNeedHandicapFilter
      ? {
          need_handicap: needHandicap,
          handicap: needHandicap,
          require_handicap: needHandicap,
          need_special_needs: needHandicap,
          special_needs: needHandicap,
          handicap_accessibility: needHandicap,
          can_receive_special_needs: needHandicap,
        }
      : {}),
  };

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
  const frontendRouteDistanceRaw = pickFirstValue(
    data?.route_api_distance_km,
    data?.distance,
    data?.meta?.route_api_distance_km
  );
  let routeApiDistanceKm = pickFirstValue(
    toPositiveRouteDistanceKm(data?.route_api_distance_km),
    toPositiveRouteDistanceKm(data?.distance),
    toPositiveRouteDistanceKm(data?.meta?.route_api_distance_km)
  );
  if (routeApiDistanceKm === null && toRouteMetricNumber(frontendRouteDistanceRaw) === 0) {
    console.log("[dispatch][routeApi] ignored frontend zero distance", {
      ride_id: rideId,
      route_api_distance_km: frontendRouteDistanceRaw,
    });
  }

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
      toPositiveRouteDistanceKm(routeApiData?.route) ??
      toPositiveRouteDistanceKm(routeApiData?.distance_km) ??
      toPositiveRouteDistanceKm(routeApiData?.total_distance) ??
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
    required_gender: applyRequiredGenderFilter ? requiredGender : null,
    need_child_seat: applyNeedChildSeatFilter ? needChildSeat : null,
    need_handicap: applyNeedHandicapFilter ? needHandicap : null,
  });

  const availableAirResults = await mapWithConcurrency(
    nearbyAir,
    DRIVER_WALLET_GUARD_MAX_CONCURRENCY,
    async (driver) => {
      const dId = toNumber(driver?.driver_id);
      if (!dId) return null;
      const eligible = await ensureFreshDriverWalletEligibility(dId);
      return eligible ? driver : null;
    }
  );
  const availableAir = availableAirResults.filter(Boolean);

  const targetDriverIdSet = Array.isArray(data?.driver_ids)
    ? new Set(
        data.driver_ids
          .map((value) => toNumber(value))
          .filter((value) => !!value)
      )
    : null;
  const strictTargetDispatch =
    toBinaryFlag(data?.restrict_to_driver_ids ?? data?.target_only ?? null) === 1;

  const roadFilteredRaw = await filterDriversByRoadRadius(
    availableAir,
    lat,
    long,
    roadRadius
  );

  const roadFiltered =
    strictTargetDispatch && targetDriverIdSet && targetDriverIdSet.size > 0
      ? roadFilteredRaw.filter((driver) => {
          const driverId = toNumber(driver?.driver_id);
          return !!driverId && targetDriverIdSet.has(driverId);
        })
      : roadFilteredRaw;

// Keep old candidates only during incremental expansion stages.
// For a fresh/initial dispatch window, start from current filtered drivers only.
const incrementalExpansion =
  data?.dispatch_expand_reason === "timeout" ||
  toNumber(data?.dispatch_incremental_only) === 1;
const forceNewSearchWindow =
  toNumber(data?.force_new_search_window ?? data?.reset_search_window ?? null) === 1;
const allowDeclinedDriverReoffer =
  forcedRebroadcastFromInput || forceNewSearchWindow;
const shouldResetCandidateHistory =
  forceNewSearchWindow &&
  !incrementalExpansion &&
  radiusPlan.currentStageIndex === 0;
if (shouldResetCandidateHistory) {
  rideCandidates.delete(rideId);
  clearRideDriverStates(rideId);
}
const shouldRetainExistingCandidates = incrementalExpansion;
const existingCandidateSet = shouldRetainExistingCandidates
  ? (rideCandidates.get(rideId) ?? new Set())
  : new Set();

const eligibleForDispatch = roadFiltered.filter((driver) => {
  const driverId = toNumber(driver?.driver_id);
  if (!driverId) return false;
  const state = getRideDriverState(rideId, driverId);
  const driverStatus = state?.status ?? null;

  // Drivers that already accepted/declined/expired this ride must not be re-dispatched.
  if (
    isTerminalDriverRideState(driverStatus) &&
    !(driverStatus === "declined" && allowDeclinedDriverReoffer)
  ) {
    return false;
  }

  // إذا كان مرشحًا أصلًا، خليه eligible دائمًا
  if (existingCandidateSet.has(driverId)) return true;

  // Allow previously declined drivers only when rebroadcast/reset is explicitly requested.
  // Reset from terminal declined -> pending_emit so inbox upsert/retry flow can run normally.
  if (driverStatus === "declined" && allowDeclinedDriverReoffer) {
    markRideDriverState(rideId, driverId, "pending_emit", {
      reoffer_reset_at: Date.now(),
      last_emit_source: toTrimmedText(data?.dispatch_expand_reason) ?? "dispatch",
      last_emit_reason: "declined_reoffer_reset",
    });
    return true;
  }

  // السائق الجديد فقط: لا تعيده إذا سبق وتم إشعاره قبل
  return !hasRideDriverBeenNotified(rideId, driverId);
});

const nearbySmokingReady = nearbyAir.filter(
  (driver) => toBinaryFlag(driver?.child_seat) === 1
).length;
const availableSmokingReady = availableAir.filter(
  (driver) => toBinaryFlag(driver?.child_seat) === 1
).length;
const roadSmokingReady = roadFiltered.filter(
  (driver) => toBinaryFlag(driver?.child_seat) === 1
).length;
const nearbyHandicapReady = nearbyAir.filter(
  (driver) => toBinaryFlag(driver?.handicap) === 1
).length;
const availableHandicapReady = availableAir.filter(
  (driver) => toBinaryFlag(driver?.handicap) === 1
).length;
const roadHandicapReady = roadFiltered.filter(
  (driver) => toBinaryFlag(driver?.handicap) === 1
).length;

const prioritizedEligibleForDispatch =
  !strictTargetDispatch && targetDriverIdSet && targetDriverIdSet.size > 0
    ? [...eligibleForDispatch].sort((a, b) => {
        const aPriority = targetDriverIdSet.has(toNumber(a?.driver_id)) ? 1 : 0;
        const bPriority = targetDriverIdSet.has(toNumber(b?.driver_id)) ? 1 : 0;
        if (aPriority !== bPriority) return bPriority - aPriority;
        return 0;
      })
    : eligibleForDispatch;

const candidateDriversRaw =
  MAX_DISPATCH_CANDIDATES > 0
    ? prioritizedEligibleForDispatch.slice(0, MAX_DISPATCH_CANDIDATES)
    : prioritizedEligibleForDispatch;

// احتفظ بالمرشحين القدامى طالما:
// - online
// - ما عندهم رحلة ثانية
// - ما عندهم queued ride ثانية
// - العرض لسا صالح
const retainedExistingIds = shouldRetainExistingCandidates
  ? Array.from(existingCandidateSet).filter((driverId) =>
      shouldKeepExistingCandidateForRide(rideId, driverId)
    )
  : [];

// السائقين الجدد من الفلترة الحالية
const newCandidateIds = candidateDriversRaw
  .map((d) => toNumber(d?.driver_id))
  .filter((driverId) => !!driverId);

// الدمج بين القدامى المحتفظ فيهم + الجدد
const nextCandidateIds = Array.from(new Set([
  ...retainedExistingIds,
  ...newCandidateIds,
]));

// هذا المتغير منخليه فقط للـ log والتوافق مع بقية المنطق

const remainingSearchStageCount = Math.max(
  1,
  radiusPlan.stagesMeters.length - radiusPlan.currentStageIndex
);

const driverOfferTimer = makeTimer(searchTimeoutSeconds);
const searchTimer = makeTimer(searchTimeoutSeconds);

console.log("[dispatch][dispatchToNearbyDrivers]", {
  ride_id: rideId,
  dispatch_base_fare: priceBounds.base_fare ?? null,
  dispatch_estimated_price: priceBounds.base_fare ?? null,
  dispatch_min_price: priceBounds.min_price ?? null,
  dispatch_max_price: priceBounds.max_price ?? null,
  price_anchor_min_price:
    toNumber(previousRideSnapshot?.price_anchor_min_price) ??
    toNumber(previousRideSnapshot?.ride_details?.price_anchor_min_price) ??
    toNumber(previousRideSnapshot?.meta?.price_anchor_min_price) ??
    null,
  price_anchor_max_price:
    toNumber(previousRideSnapshot?.price_anchor_max_price) ??
    toNumber(previousRideSnapshot?.ride_details?.price_anchor_max_price) ??
    toNumber(previousRideSnapshot?.meta?.price_anchor_max_price) ??
    null,
  price_anchor_base_fare:
    toNumber(previousRideSnapshot?.price_anchor_base_fare) ??
    toNumber(previousRideSnapshot?.ride_details?.price_anchor_base_fare) ??
    toNumber(previousRideSnapshot?.meta?.price_anchor_base_fare) ??
    null,
  road_radius_m: roadRadius,
  air_candidate_radius_m: airCandidateRadius,
  dispatch_timeout_s: dispatchTimeoutSeconds,
  customer_offer_timeout_s: customerOfferTimeoutSeconds,
  search_timeout_s: searchTimeoutSeconds,
  dispatch_remaining_stages: remainingSearchStageCount,
  dispatch_expand_every_s: DISPATCH_EXPANSION_INTERVAL_S,
  incremental_expansion: incrementalExpansion,
  initial_radius_m: radiusPlan.initialRadiusMeters,
  dispatch_stage_number: radiusPlan.currentStageIndex + 1,
  dispatch_total_stages: radiusPlan.stagesMeters.length,
  next_radius_m: radiusPlan.nextRadiusMeters ?? null,
  service_type_id: serviceTypeId ?? null,
  target_driver_filter_applied:
    !!(strictTargetDispatch && targetDriverIdSet && targetDriverIdSet.size > 0),
  target_driver_prioritized:
    !!(!strictTargetDispatch && targetDriverIdSet && targetDriverIdSet.size > 0),
  target_driver_strict_mode: strictTargetDispatch,
  target_driver_ids_count: targetDriverIdSet ? targetDriverIdSet.size : 0,
  nearby_air: nearbyAir.length,
  available_air: availableAir.length,
  road_filtered_raw: roadFilteredRaw.length,
  road_filtered: roadFiltered.length,
  dispatch_eligible: eligibleForDispatch.length,
  nearby_smoking_ready: nearbySmokingReady,
  available_smoking_ready: availableSmokingReady,
  road_smoking_ready: roadSmokingReady,
  nearby_handicap_ready: nearbyHandicapReady,
  available_handicap_ready: availableHandicapReady,
  road_handicap_ready: roadHandicapReady,
  retained_existing_candidates: retainedExistingIds.length,
  new_candidates: newCandidateIds.length,
  final_candidates: nextCandidateIds.length,
  dispatch_wave_size: DISPATCH_WAVE_SIZE > 0 ? DISPATCH_WAVE_SIZE : null,
  dispatch_wave_interval_ms: DISPATCH_WAVE_INTERVAL_MS > 0 ? DISPATCH_WAVE_INTERVAL_MS : null,
  dispatch_wave_enabled: DISPATCH_WAVE_SIZE > 0 && DISPATCH_WAVE_INTERVAL_MS > 0,
  required_gender: applyRequiredGenderFilter ? requiredGender : null,
  required_gender_filter_applied: applyRequiredGenderFilter,
  need_child_seat: applyNeedChildSeatFilter ? needChildSeat : null,
  need_child_seat_filter_applied: applyNeedChildSeatFilter,
  raw_required_gender: requiredGenderRaw ?? null,
  required_gender_provided: requiredGenderProvided,
  raw_smoking: needChildSeatRaw ?? null,
  raw_child_seat: needChildSeatRaw ?? null,
  raw_need_child_seat: needChildSeatRaw ?? null,
  need_child_seat_provided: needChildSeatProvided,
  need_handicap: applyNeedHandicapFilter ? needHandicap : null,
  need_handicap_filter_applied: applyNeedHandicapFilter,
  raw_handicap: needHandicapRaw ?? null,
  raw_need_handicap: needHandicapRaw ?? null,
  raw_require_handicap: needHandicapRaw ?? null,
  raw_special_needs: needHandicapRaw ?? null,
  raw_need_special_needs: needHandicapRaw ?? null,
  need_handicap_provided: needHandicapProvided,
  has_user_details: !!userDetails,
  token_present: !!tokenTmp,
  additional_remarks: additionalRemarks ?? null,
  has_additional_remarks:
    typeof additionalRemarks === "string"
      ? additionalRemarks.trim().length > 0
      : !!additionalRemarks,
});

// نمرر القائمة النهائية المدموجة
const candidateSync = syncRideCandidates(
  io,
  rideId,
  nextCandidateIds,
  { preserveExisting: false }
);

// افتراضياً: dispatch يكون فقط للجدد حتى لا نكرر bidRequest على نفس السائق.
// إعادة البث للجميع تكون فقط بطلب صريح (force_rebroadcast / rebroadcast_all),
// ونستخدمها في retry.
const shouldRebroadcastBidRequest = forcedRebroadcastFromInput;

const notifyDriverIdSet = new Set(
  shouldRebroadcastBidRequest
    ? nextCandidateIds
    : candidateSync.newlyAddedIds
);

const driverLookup = new Map(
  roadFiltered.map((d) => [toNumber(d.driver_id), d])
);

const candidatesToNotify = Array.from(notifyDriverIdSet)
  .map((driverId) => {
    const roadMatch = driverLookup.get(driverId);
    if (roadMatch) return roadMatch;

    const live = driverLocationService.getDriver(driverId);
    if (!live) return null;

    return {
      driver_id: driverId,
      lat: toNumber(live.lat),
      long: toNumber(live.long),
    };
  })
  .filter(Boolean);
  const previousNoNewConsecutiveStages = Math.max(
    0,
    Math.floor(toNumber(data?.dispatch_no_new_stage_count) ?? 0)
  );
  const noNewConsecutiveStages = incrementalExpansion
    ? candidatesToNotify.length === 0
      ? previousNoNewConsecutiveStages + 1
      : 0
    : 0;
  const shouldHaltFurtherExpansion =
    incrementalExpansion &&
    candidatesToNotify.length === 0 &&
    DISPATCH_EXPAND_STOP_AFTER_NO_NEW_STAGES > 0 &&
    noNewConsecutiveStages >= DISPATCH_EXPAND_STOP_AFTER_NO_NEW_STAGES &&
    radiusPlan.nextRadiusMeters !== null;
  const effectiveDispatchStagesMeters = shouldHaltFurtherExpansion
    ? radiusPlan.stagesMeters.slice(0, radiusPlan.currentStageIndex + 1)
    : radiusPlan.stagesMeters;
  const effectiveNextRadiusMeters = shouldHaltFurtherExpansion
    ? null
    : radiusPlan.nextRadiusMeters;
  const effectiveRemainingSearchStageCount = Math.max(
    1,
    effectiveDispatchStagesMeters.length - radiusPlan.currentStageIndex
  );
  if (shouldHaltFurtherExpansion) {
    console.log("[dispatch][expand][auto-stop]", {
      ride_id: rideId,
      stage_number: radiusPlan.currentStageIndex + 1,
      stage_total: radiusPlan.stagesMeters.length,
      no_new_stage_count: noNewConsecutiveStages,
      stop_after: DISPATCH_EXPAND_STOP_AFTER_NO_NEW_STAGES,
      reason: "no-newly-notified-drivers",
    });
  }
 if (userId) setUserActiveRide(userId, rideId);

  const baseMeta =
    data?.meta && typeof data.meta === "object" && !Array.isArray(data.meta) ? data.meta : {};
  const dispatchStagePayload = {
    dispatch_timeout_s: dispatchTimeoutSeconds,
    customer_offer_timeout_s: customerOfferTimeoutSeconds,
    user_timeout: customerOfferTimeoutSeconds,
    search_timeout_s: searchTimeoutSeconds,
    dispatch_remaining_stages: effectiveRemainingSearchStageCount,
    dispatch_expand_every_s: DISPATCH_EXPANSION_INTERVAL_S,
    initial_dispatch_radius: radiusPlan.initialRadiusMeters,
    dispatch_stage_index: radiusPlan.currentStageIndex,
    dispatch_stage_number: radiusPlan.currentStageIndex + 1,
    dispatch_stage_total: effectiveDispatchStagesMeters.length,
    dispatch_radius_stages_m: effectiveDispatchStagesMeters,
    dispatch_no_new_stage_count: noNewConsecutiveStages,
    dispatch_current_radius_m: roadRadius,
    dispatch_current_radius_km: round2(roadRadius / 1000),
    ...(effectiveNextRadiusMeters !== null
      ? {
          dispatch_next_radius_m: effectiveNextRadiusMeters,
          dispatch_next_radius_km: round2(effectiveNextRadiusMeters / 1000),
        }
      : {}),
  };

  const dispatchUserLanguage = normalizeLanguageCode(
    pickFirstValue(
      data?.user_language,
      data?.language,
      data?.user_details?.user_language,
      data?.user_details?.language,
      userDetails?.user_language,
      userDetails?.language,
      storedUser?.user_language,
      storedUser?.language,
      storedByToken?.user_language,
      storedByToken?.language,
      previousRideSnapshot?.user_language,
      previousRideSnapshot?.language,
      previousRideSnapshot?.user_details?.user_language,
      previousRideSnapshot?.user_details?.language
    )
  );
  const serviceTypeNameEn = toTrimmedText(
    pickFirstValue(
      data?.service_type_name_en,
      data?.vehicle_type_name_en,
      previousRideSnapshot?.service_type_name_en,
      previousRideSnapshot?.vehicle_type_name_en,
      previousRideSnapshot?.ride_details?.service_type_name_en,
      previousRideSnapshot?.ride_details?.vehicle_type_name_en,
      previousRideSnapshot?.meta?.service_type_name_en,
      previousRideSnapshot?.meta?.vehicle_type_name_en
    )
  );
  const serviceTypeNameAr = toTrimmedText(
    pickFirstValue(
      data?.service_type_name_ar,
      data?.vehicle_type_name_ar,
      previousRideSnapshot?.service_type_name_ar,
      previousRideSnapshot?.vehicle_type_name_ar,
      previousRideSnapshot?.ride_details?.service_type_name_ar,
      previousRideSnapshot?.ride_details?.vehicle_type_name_ar,
      previousRideSnapshot?.meta?.service_type_name_ar,
      previousRideSnapshot?.meta?.vehicle_type_name_ar
    )
  );
  const serviceTypeNameRaw = toTrimmedText(
    pickFirstValue(
      data?.service_type_name,
      data?.vehicle_type_name,
      previousRideSnapshot?.service_type_name,
      previousRideSnapshot?.vehicle_type_name,
      previousRideSnapshot?.ride_details?.service_type_name,
      previousRideSnapshot?.ride_details?.vehicle_type_name,
      previousRideSnapshot?.meta?.service_type_name,
      previousRideSnapshot?.meta?.vehicle_type_name
    )
  );
  const localizedServiceTypeName = pickLocalizedText(
    dispatchUserLanguage,
    serviceTypeNameEn,
    serviceTypeNameAr,
    serviceTypeNameRaw
  );
  if (userDetails && dispatchUserLanguage) {
    userDetails = {
      ...userDetails,
      user_language: userDetails?.user_language ?? dispatchUserLanguage,
      language: userDetails?.language ?? dispatchUserLanguage,
    };
  }

  const ridePayloadBase = {
    ride_id: rideId,
    event_type: "driver_bid_list_item",
    ui_action: "show_bid_list",
    auto_open_running: false,
    is_running_ride: false,

    // ✅ FLAT user fields (important for retry + merges)
    user_id: userId ?? userDetails?.user_id ?? null,
    user_name: userDetails?.user_name ?? null,
    user_gender: userDetails?.user_gender ?? null,
    user_image: userDetails?.user_image ?? null,
    user_phone: userDetails?.user_phone ?? null,
    user_country_code: userDetails?.user_country_code ?? null,
    user_phone_full: userDetails?.user_phone_full ?? null,
    user_language: dispatchUserLanguage ?? null,
    language: dispatchUserLanguage ?? null,

    // ✅ keep token for later accept/merge paths
    token: tokenTmp ?? null,
    ...dispatchPreferencePayload,

    pickup_lat: lat,
    pickup_long: long,
    pickup_address: data.pickup_address ?? null,

    destination_lat: toNumber(data.destination_lat),
    destination_long: toNumber(data.destination_long),
    destination_address: data.destination_address ?? null,
      additional_remarks: additionalRemarks,
      additional_remark: additionalRemarks,
      additional_request: additionalRemarks,

    radius: roadRadius,
    ...dispatchStagePayload,
    user_bid_price: dispatchBidPrice,
    min_fare_amount: legacyMinFareAmount,
    max_fare_amount: legacyMaxFareAmount,
    base_fare: priceBounds.base_fare,
    estimated_price: priceBounds.base_fare,
    estimated_fare: priceBounds.base_fare,
    min_price: priceBounds.min_price,
    max_price: priceBounds.max_price,
    MIN_PRICE: priceBounds.min_price,
    MAX_PRICE: priceBounds.max_price,
    min_fare: priceBounds.min_price,
    max_fare: priceBounds.max_price,

    service_type_id: toNumber(data.service_type_id) ?? null,
    service_category_id: toNumber(data.service_category_id) ?? null,
    service_type_name: localizedServiceTypeName ?? null,
    vehicle_type_name: localizedServiceTypeName ?? null,
    service_type_name_en: serviceTypeNameEn ?? localizedServiceTypeName ?? null,
    service_type_name_ar: serviceTypeNameAr ?? localizedServiceTypeName ?? null,
    vehicle_type_name_en: serviceTypeNameEn ?? localizedServiceTypeName ?? null,
    vehicle_type_name_ar: serviceTypeNameAr ?? localizedServiceTypeName ?? null,
    created_at: data.created_at ?? null,

    ...(routeKm !== null ? { route: routeKm } : {}),
    ...(finalEtaMin !== null ? { eta_min: finalEtaMin } : {}),
    duration: finalRouteApiDurationMin,
    route_api_distance_km: finalRouteApiDistanceKm,
    ride_details: {
      ride_id: rideId,
      pickup_lat: lat,
      pickup_long: long,
      pickup_address: data.pickup_address ?? null,
      destination_lat: toNumber(data.destination_lat),
      destination_long: toNumber(data.destination_long),
      destination_address: data.destination_address ?? null,
      additional_remarks: additionalRemarks,
      additional_remark: additionalRemarks,
      additional_request: additionalRemarks,
      user_bid_price: dispatchBidPrice,
      min_fare_amount: legacyMinFareAmount,
      max_fare_amount: legacyMaxFareAmount,
      base_fare: priceBounds.base_fare,
      estimated_price: priceBounds.base_fare,
      estimated_fare: priceBounds.base_fare,
      min_price: priceBounds.min_price,
      max_price: priceBounds.max_price,
      MIN_PRICE: priceBounds.min_price,
      MAX_PRICE: priceBounds.max_price,
      min_fare: priceBounds.min_price,
      max_fare: priceBounds.max_price,
      min_fare: priceBounds.min_price,
      max_fare: priceBounds.max_price,
      service_type_id: toNumber(data.service_type_id) ?? null,
      service_category_id: toNumber(data.service_category_id) ?? null,
      service_type_name: localizedServiceTypeName ?? null,
      vehicle_type_name: localizedServiceTypeName ?? null,
      service_type_name_en: serviceTypeNameEn ?? localizedServiceTypeName ?? null,
      service_type_name_ar: serviceTypeNameAr ?? localizedServiceTypeName ?? null,
      vehicle_type_name_en: serviceTypeNameEn ?? localizedServiceTypeName ?? null,
      vehicle_type_name_ar: serviceTypeNameAr ?? localizedServiceTypeName ?? null,
      user_language: dispatchUserLanguage ?? null,
      language: dispatchUserLanguage ?? null,
      duration: finalRouteApiDurationMin,
      route_api_distance_km: finalRouteApiDistanceKm,
      ...dispatchPreferencePayload,
      ...(routeKm !== null ? { route: routeKm } : {}),
      ...(finalEtaMin !== null ? { eta_min: finalEtaMin } : {}),
    },

    meta: {
      ...baseMeta,
      ...(routeKm !== null ? { route: routeKm } : {}),
      ...(finalEtaMin !== null ? { eta_min: finalEtaMin } : {}),
      duration: finalRouteApiDurationMin,
      route_api_distance_km: finalRouteApiDistanceKm,
      ...(priceBounds.base_fare !== null ? { base_fare: priceBounds.base_fare } : {}),
      ...(priceBounds.base_fare !== null ? { estimated_price: priceBounds.base_fare } : {}),
      ...(priceBounds.base_fare !== null ? { estimated_fare: priceBounds.base_fare } : {}),
      ...(priceBounds.min_price !== null ? { min_price: priceBounds.min_price } : {}),
      ...(priceBounds.max_price !== null ? { max_price: priceBounds.max_price } : {}),
      ...(localizedServiceTypeName ? { service_type_name: localizedServiceTypeName } : {}),
      ...(localizedServiceTypeName ? { vehicle_type_name: localizedServiceTypeName } : {}),
      ...(serviceTypeNameEn ? { service_type_name_en: serviceTypeNameEn, vehicle_type_name_en: serviceTypeNameEn } : {}),
      ...(serviceTypeNameAr ? { service_type_name_ar: serviceTypeNameAr, vehicle_type_name_ar: serviceTypeNameAr } : {}),
      ...(dispatchUserLanguage ? { user_language: dispatchUserLanguage, language: dispatchUserLanguage } : {}),
      ...dispatchPreferencePayload,
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

    // ✅ timer fields for drivers (remaining search lifetime)
    ...driverOfferTimer,
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
              additional_remarks: additionalRemarks,
              additional_remark: additionalRemarks,
              additional_request: additionalRemarks,

        radius: roadRadius,
        ...dispatchStagePayload,
        user_bid_price: dispatchBidPrice,
        min_fare_amount: legacyMinFareAmount,
        max_fare_amount: legacyMaxFareAmount,
        base_fare: priceBounds.base_fare,
        min_price: priceBounds.min_price,
        max_price: priceBounds.max_price,
        MIN_PRICE: priceBounds.min_price,
        MAX_PRICE: priceBounds.max_price,
        min_fare: priceBounds.min_price,
        max_fare: priceBounds.max_price,
        service_type_id: serviceTypeId,
        service_category_id: toNumber(data.service_category_id) ?? null,
        service_type_name: localizedServiceTypeName ?? null,
        vehicle_type_name: localizedServiceTypeName ?? null,
        service_type_name_en: serviceTypeNameEn ?? localizedServiceTypeName ?? null,
        service_type_name_ar: serviceTypeNameAr ?? localizedServiceTypeName ?? null,
        vehicle_type_name_en: serviceTypeNameEn ?? localizedServiceTypeName ?? null,
        vehicle_type_name_ar: serviceTypeNameAr ?? localizedServiceTypeName ?? null,
        user_language: dispatchUserLanguage ?? null,
        language: dispatchUserLanguage ?? null,
        created_at: data.created_at ?? null,

        ...(routeKm !== null ? { route: routeKm } : {}),
        ...(finalEtaMin !== null ? { eta_min: finalEtaMin } : {}),
        duration: finalRouteApiDurationMin,
        route_api_distance_km: finalRouteApiDistanceKm,
        ...dispatchPreferencePayload,
        ride_details: {
          ride_id: rideId,
          pickup_lat: lat,
          pickup_long: long,
          pickup_address: data.pickup_address ?? null,
          destination_lat: toNumber(data.destination_lat),
          destination_long: toNumber(data.destination_long),
          destination_address: data.destination_address ?? null,
          additional_remarks: additionalRemarks,
          additional_remark: additionalRemarks,
          additional_request: additionalRemarks,
          user_bid_price: dispatchBidPrice,
          min_fare_amount: legacyMinFareAmount,
          max_fare_amount: legacyMaxFareAmount,
          base_fare: priceBounds.base_fare,
          min_price: priceBounds.min_price,
          max_price: priceBounds.max_price,
          MIN_PRICE: priceBounds.min_price,
          MAX_PRICE: priceBounds.max_price,
          min_fare: priceBounds.min_price,
          max_fare: priceBounds.max_price,
          service_type_id: serviceTypeId,
          service_category_id: toNumber(data.service_category_id) ?? null,
          service_type_name: localizedServiceTypeName ?? null,
          vehicle_type_name: localizedServiceTypeName ?? null,
          service_type_name_en: serviceTypeNameEn ?? localizedServiceTypeName ?? null,
          service_type_name_ar: serviceTypeNameAr ?? localizedServiceTypeName ?? null,
          vehicle_type_name_en: serviceTypeNameEn ?? localizedServiceTypeName ?? null,
          vehicle_type_name_ar: serviceTypeNameAr ?? localizedServiceTypeName ?? null,
          user_language: dispatchUserLanguage ?? null,
          language: dispatchUserLanguage ?? null,
          duration: finalRouteApiDurationMin,
          route_api_distance_km: finalRouteApiDistanceKm,
          ...dispatchPreferencePayload,
          ...(routeKm !== null ? { route: routeKm } : {}),
          ...(finalEtaMin !== null ? { eta_min: finalEtaMin } : {}),
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
          ...(localizedServiceTypeName ? { service_type_name: localizedServiceTypeName } : {}),
          ...(localizedServiceTypeName ? { vehicle_type_name: localizedServiceTypeName } : {}),
          ...(serviceTypeNameEn
            ? { service_type_name_en: serviceTypeNameEn, vehicle_type_name_en: serviceTypeNameEn }
            : {}),
          ...(serviceTypeNameAr
            ? { service_type_name_ar: serviceTypeNameAr, vehicle_type_name_ar: serviceTypeNameAr }
            : {}),
          ...(dispatchUserLanguage
            ? { user_language: dispatchUserLanguage, language: dispatchUserLanguage }
            : {}),
          ...dispatchPreferencePayload,
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
        user_language: dispatchUserLanguage ?? null,
        language: dispatchUserLanguage ?? null,

        token: tokenTmp ?? null,
        ...dispatchPreferencePayload,
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

        // ✅ timer fields for customer search phase (remaining search lifetime)
        ...searchTimer,
      },
      userDetails
    )
  );

  // ✅ expansion cadence is fixed; overall lifetime still follows the ride timer
  startRideTimeoutWithExpansion(io, rideId, searchTimeoutSeconds);

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

  const deliveredDriverIds = [];
  const pendingDriverIds = [];
  const effectiveWaveSize = Math.max(
    1,
    DISPATCH_WAVE_SIZE > 0 ? DISPATCH_WAVE_SIZE : candidatesToNotify.length
  );
  const waveDispatchEnabled =
    DISPATCH_WAVE_SIZE > 0 &&
    DISPATCH_WAVE_INTERVAL_MS > 0 &&
    candidatesToNotify.length > effectiveWaveSize;

  if (waveDispatchEnabled) {
    console.log("[dispatch][wave][start]", {
      ride_id: rideId,
      total_candidates: candidatesToNotify.length,
      wave_size: effectiveWaveSize,
      wave_interval_ms: DISPATCH_WAVE_INTERVAL_MS,
    });
  }

  const notifyDriverCandidate = (d) => {
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
        ...driverOfferTimer,
      },
      ridePayload?.user_details ?? userDetails ?? null
    );
    const bidRequestPayload = sanitizeRidePayloadForClient({
      ...ridePayloadForDriver,
      event_type: "driver_new_bid_request",
      ui_action: "show_bid_request",
      auto_open_running: false,
      is_running_ride: false,
    });
    console.log("[ride:bidRequest] payload", {
      driver_id: d.driver_id,
      ride_id: bidRequestPayload?.ride_id ?? null,
      user_image: bidRequestPayload?.user_image ?? null,
      customer_image: bidRequestPayload?.customer_image ?? null,
      duration:
        bidRequestPayload?.ride_details?.duration ?? bidRequestPayload?.duration ?? null,
      route_api_distance_km: bidRequestPayload?.route_api_distance_km ?? null,
      min_price:
        bidRequestPayload?.min_price ?? bidRequestPayload?.ride_details?.min_price ?? null,
      max_price:
        bidRequestPayload?.max_price ?? bidRequestPayload?.ride_details?.max_price ?? null,
      min_fare:
        bidRequestPayload?.min_fare ?? bidRequestPayload?.ride_details?.min_fare ?? null,
      max_fare:
        bidRequestPayload?.max_fare ?? bidRequestPayload?.ride_details?.max_fare ?? null,
      additional_remarks: resolveAdditionalRemarks(bidRequestPayload),
    });

    inboxUpsert(d.driver_id, rideId, ridePayloadForDriver);
    emitDispatchDeliverySummary(io, d.driver_id, ridePayloadForDriver);

    const emitResult = tryEmitBidRequestToDriver(io, {
      rideId,
      driverId: d.driver_id,
      bidRequestPayload,
      ridePayloadForDriver,
      dispatchStageIndex: radiusPlan.currentStageIndex,
      dispatchRadiusMeters: roadRadius,
      source: "dispatch-initial",
      attempt: 1,
    });

    if (emitResult.delivered) {
      deliveredDriverIds.push(toNumber(d.driver_id));
      console.log(
        `[dispatch][delivery] ride ${rideId} -> driver ${d.driver_id} delivered (room_sockets:${emitResult.room_sockets})`
      );
      return;
    }

    pendingDriverIds.push(toNumber(d.driver_id));
    scheduleBidRequestRetry(io, {
      rideId,
      driverId: d.driver_id,
      bidRequestPayload,
      ridePayloadForDriver,
      dispatchStageIndex: radiusPlan.currentStageIndex,
      dispatchRadiusMeters: roadRadius,
      source: "dispatch-initial",
      attempt: 1,
    });
    console.log(
      `[dispatch][delivery] ride ${rideId} -> driver ${d.driver_id} pending (${emitResult.reason})`
    );
  };

  for (let offset = 0; offset < candidatesToNotify.length; offset += effectiveWaveSize) {
    const waveCandidates = candidatesToNotify.slice(offset, offset + effectiveWaveSize);
    waveCandidates.forEach((d) => notifyDriverCandidate(d));

    if (!waveDispatchEnabled) continue;
    const remainingCandidates = Math.max(
      0,
      candidatesToNotify.length - (offset + waveCandidates.length)
    );
    const waveNumber = Math.floor(offset / effectiveWaveSize) + 1;
    console.log("[dispatch][wave]", {
      ride_id: rideId,
      wave_number: waveNumber,
      wave_size: waveCandidates.length,
      remaining_candidates: remainingCandidates,
      sleep_ms: remainingCandidates > 0 ? DISPATCH_WAVE_INTERVAL_MS : 0,
    });
    if (remainingCandidates > 0) {
      await sleep(DISPATCH_WAVE_INTERVAL_MS);
    }
  }

  if (waveDispatchEnabled) {
    console.log("[dispatch][wave][done]", {
      ride_id: rideId,
      total_candidates: candidatesToNotify.length,
      delivered_count: deliveredDriverIds.length,
      pending_count: pendingDriverIds.length,
    });
  }

if (incrementalExpansion) {
  console.log("[dispatch][expand][notify]", {
    ride_id: rideId,
    total_candidates_in_radius: nextCandidateIds.length,
    newly_notified: candidatesToNotify.length,
    already_notified: nextCandidateIds.length - candidatesToNotify.length,
    no_new_stage_count: noNewConsecutiveStages,
    stop_after_no_new_stages: DISPATCH_EXPAND_STOP_AFTER_NO_NEW_STAGES,
  });

  }

  if (candidatesToNotify.length > 0) {
    console.log(`✅ Finished dispatching ride ${rideId} — notified ${candidatesToNotify.length} driver(s)`);
  } else {
    console.log(`ℹ️ Dispatch for ride ${rideId} completed with no newly notified drivers`);
  }
  console.log("[dispatch][delivery-report]", {
    ride_id: rideId,
    delivered_driver_ids: deliveredDriverIds.filter((id) => !!id),
    pending_driver_ids: pendingDriverIds.filter((id) => !!id),
    delivered_count: deliveredDriverIds.filter((id) => !!id).length,
    pending_count: pendingDriverIds.filter((id) => !!id).length,
  });
  emitRideCandidatesSummary(io, rideId);

  return true;
  } finally {
    if (dispatchInFlightByRide.get(rideId) === inFlightToken) {
      dispatchInFlightByRide.delete(rideId);
    }
  }
}

const isCandidateDriver = (rideId, driverId) => {
  const set = rideCandidates.get(rideId);
  if (!set) {
    const inInbox = driverRideInbox.get(driverId)?.has(rideId);
    if (!inInbox) {
      console.log(`🚫 Driver ${driverId} tried to bid on ride ${rideId} but ride is not tracked`);
      return false;
    }
    if (!isDriverOfferStillActive(driverId, rideId)) {
      console.log(`🚫 Driver ${driverId} tried to bid on ride ${rideId} but offer already expired`);
      return false;
    }
    return true;
  }
  const ok = set.has(driverId);
  if (!ok) {
    console.log(`🚫 Driver ${driverId} tried to bid on ride ${rideId} but is not in candidates`);
    return false;
  }
  if (!isDriverOfferStillActive(driverId, rideId)) {
    console.log(`🚫 Driver ${driverId} tried to bid on ride ${rideId} but offer already expired`);
    return false;
  }
  return true;
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
        timeout: LARAVEL_ROUTE_TIMEOUT_MS,
      }
    );

    const routeApiData = response?.data ?? null;
    const normalizedDuration = normalizeDuration(
      pickFirstValue(
        routeApiData?.duration,
        routeApiData?.eta_min,
        routeApiData?.route_api_duration_min
      )
    );
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
      timeout: LARAVEL_ROUTE_TIMEOUT_MS,
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

  const routeKey = buildRouteMetricsCacheKey(la1, lo1, la2, lo2);
  const cached = getCachedRouteMetrics(routeKey);
  if (cached) {
    return {
      ...cached,
      source: cached?.source ? `${cached.source}-cache` : "cache",
    };
  }

  if (routeKey && routeMetricsInFlight.has(routeKey)) {
    return routeMetricsInFlight.get(routeKey);
  }

  const inFlightPromise = (async () => {
    let routeLockToken = null;
    const routeL2CacheKey = buildRouteL2CacheKey(routeKey);
    const routeL2LockKey = buildRouteL2LockKey(routeKey);

    try {
      if (routeL2CacheKey && routeCacheL2.isEnabled()) {
        const cachedL2 = await routeCacheL2.getJson(routeL2CacheKey);
        if (cachedL2 && typeof cachedL2 === "object") {
          setCachedRouteMetrics(routeKey, cachedL2);
          return {
            ...cachedL2,
            source: cachedL2?.source ? `${cachedL2.source}-l2-cache` : "l2-cache",
          };
        }

        routeLockToken = await routeCacheL2.tryAcquireLock(routeL2LockKey, ROUTE_LOCK_TTL_S);
        if (!routeLockToken) {
          await sleep(getRandomLockRecheckDelayMs());
          const delayedL2 = await routeCacheL2.getJson(routeL2CacheKey);
          if (delayedL2 && typeof delayedL2 === "object") {
            setCachedRouteMetrics(routeKey, delayedL2);
            return {
              ...delayedL2,
              source: delayedL2?.source
                ? `${delayedL2.source}-l2-cache-after-wait`
                : "l2-cache-after-wait",
            };
          }
        }
      }

      const res = await axios.get(`${LARAVEL_BASE_URL}${LARAVEL_GET_ROUTE_PATH}`, {
        params: {
          startLongitude: lo1,
          startLatitude: la1,
          endLongitude: lo2,
          endLatitude: la2,
          requested_at: new Date().toISOString(),
        },
        timeout: LARAVEL_ROUTE_TIMEOUT_MS,
      });

      const data = res?.data ?? null;
      const roadDistanceM = extractRoadDistanceMeters(data);
      const durationMin = toRouteMetricNumber(data?.duration ?? null);
      const durationS = durationMin !== null ? Math.round(durationMin * 60) : null;
      const resolvedRoadDistanceM = roadDistanceM ?? airDistanceM;
      const resolvedDurationS = durationS ?? airDurationS;

      const result = {
        road_distance_m: resolvedRoadDistanceM,
        road_duration_s: resolvedDurationS,
        road_duration_min:
          resolvedDurationS !== null
            ? round2(resolvedDurationS / 60)
            : durationMin !== null
            ? round2(durationMin)
            : null,
        raw: data,
        source: roadDistanceM !== null ? "road-api" : "air-fallback-road-api-empty",
      };
      setCachedRouteMetrics(routeKey, result);
      if (routeL2CacheKey && routeCacheL2.isEnabled()) {
        await routeCacheL2.setJson(routeL2CacheKey, result, ROUTE_CACHE_L2_TTL_S);
      }
      return result;
    } catch (error) {
      warnThrottled(
        "dispatch-driver-to-pickup-road-metrics-failed",
        "[dispatch][driverToPickupRoadMetrics] failed; using air fallback:",
        {
          error: error?.response?.data || error?.message || error,
          air_distance_m: airDistanceM,
        }
      );
      const fallback = {
        road_distance_m: airDistanceM,
        road_duration_s: airDurationS,
        road_duration_min: airDurationS !== null ? round2(airDurationS / 60) : null,
        raw: null,
        source: airDistanceM !== null ? "air-fallback-error" : "error",
      };
      setCachedRouteMetrics(routeKey, fallback);
      if (routeL2CacheKey && routeCacheL2.isEnabled()) {
        await routeCacheL2.setJson(routeL2CacheKey, fallback, ROUTE_CACHE_L2_TTL_S);
      }
      return fallback;
    } finally {
      if (routeL2LockKey && routeLockToken) {
        await routeCacheL2.releaseLock(routeL2LockKey, routeLockToken);
      }
      if (routeKey) routeMetricsInFlight.delete(routeKey);
    }
  })();

  if (routeKey) routeMetricsInFlight.set(routeKey, inFlightPromise);
  return inFlightPromise;
}

async function filterDriversByRoadRadius(drivers, pickupLat, pickupLong, roadRadiusM) {
  const list = Array.isArray(drivers)
    ? MAX_ROAD_FILTER_CANDIDATES > 0
      ? drivers.slice(0, MAX_ROAD_FILTER_CANDIDATES)
      : drivers
    : [];

  const results = await mapWithConcurrency(list, ROUTE_API_MAX_CONCURRENCY, async (d) => {
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
    });

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
    payload?.driver_to_pickup_distance_m,
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

function emitRouteDataFromAcceptedOffer(io, rideId, driverId, durationValue) {
  const successPayload = {
    ride_id: rideId,
    driver_id: driverId,
    status: 1,
    duration: durationValue,
    at: Date.now(),
  };

  console.log("[ride:routeData][source=accepted-offer]", {
    ride_id: rideId,
    driver_id: driverId,
    duration: successPayload.duration,
  });

  emitToRideAudience(io, rideId, "ride:routeData", successPayload);
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

function activateQueuedRideForDriver(io, driverId) {
  console.log("[activateQueuedRideForDriver] start", {
    driverId,
    activeRideIdBefore: getActiveRideByDriver(driverId),
    queued: getDriverQueuedRide(driverId),
  });

  if (!driverId) {
    console.log("[activateQueuedRideForDriver] abort: missing driverId");
    return false;
  }

  const queued = getDriverQueuedRide(driverId);
  if (!queued?.ride_id) {
    console.log("[activateQueuedRideForDriver] abort: no queued ride");
    return false;
  }

  const rideId = toNumber(queued.ride_id);
  if (!rideId) {
    console.log("[activateQueuedRideForDriver] abort: invalid queued ride_id", {
      queued,
    });
    clearDriverQueuedRide(driverId);
    return false;
  }

  const snapshot =
    queued.ride_snapshot ??
    getRideDetails(rideId) ??
    getRideSnapshotForRedispatch(rideId) ??
    null;

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
          ...(routeApiDistanceKm !== null
            ? { route_api_distance_km: routeApiDistanceKm }
            : {}),
          ...(cleanedRideDetails || duration !== null || routeApiDistanceKm !== null
            ? {
                ride_details: {
                  ...(cleanedRideDetails ?? {}),
                  ...(duration !== null ? { duration } : {}),
                  ...(routeApiDistanceKm !== null
                    ? { route_api_distance_km: routeApiDistanceKm }
                    : {}),
                },
              }
            : {}),
          ...(cleanedMeta ? { meta: cleanedMeta } : {}),
        };
      })()
    : null;

  clearActiveRideByDriver(driverId);
  setActiveRide(driverId, rideId);
  clearDriverQueuedRide(driverId);

  if (snapshot && typeof snapshot === "object") {
    saveRideDetails(
      rideId,
      attachCustomerFields(
        {
          ...snapshot,
          queued_offer_price: toNumber(queued.offered_price),
          activated_from_queue: 1,
          queue_activated_at: Date.now(),
          queue_activation_recovered: 0,
        },
        snapshot?.user_details ?? null
      )
    );
  }

  const offeredPrice = toNumber(queued.offered_price);
  const userId = resolveRideAudienceUserId(rideId);

  const payload = withDriverImage({
    ...(rideDetailsPayload && typeof rideDetailsPayload === "object"
      ? rideDetailsPayload
      : {}),
    ride_id: rideId,
    driver_id: driverId,
    offered_price: offeredPrice,
    message: "Queued ride is now active",
    at: Date.now(),
  }, driverId);

  if (rideDetailsPayload) {
    payload.ride_details = rideDetailsPayload;
  }

  io.to(driverRoom(driverId)).emit("ride:queueActivated", payload);
  emitToRideAudience(io, rideId, "ride:queueActivated", payload, userId);

  io.to(driverRoom(driverId)).emit("ride:userAccepted", payload);
  emitToRideAudience(io, rideId, "ride:userAccepted", payload, userId);

  const trackingPayload = withDriverImage({
    ...(rideDetailsPayload && typeof rideDetailsPayload === "object"
      ? rideDetailsPayload
      : {}),
    ride_id: rideId,
    driver_id: driverId,
    offered_price: offeredPrice,
    message: "Queued ride tracking started",
    at: Date.now(),
  }, driverId);

  if (rideDetailsPayload) {
    trackingPayload.ride_details = rideDetailsPayload;
  }

  io.to(driverRoom(driverId)).emit("ride:trackingStarted", trackingPayload);
  emitToRideAudience(io, rideId, "ride:trackingStarted", trackingPayload, userId);

  const d = driverLocationService.getDriver(driverId);
  if (d?.lat != null && d?.long != null) {
    const locationPayload = {
      ride_id: rideId,
      driver_id: driverId,
      lat: d.lat,
      long: d.long,
      at: Date.now(),
    };

    io.to(driverRoom(driverId)).emit("ride:locationUpdate", locationPayload);
    emitToRideAudience(io, rideId, "ride:locationUpdate", locationPayload, userId);
  }

  console.log(`🟢 queued ride activated -> ride ${rideId} for driver ${driverId}`);
  return true;
}

function emitDriverRideRecovery(io, driverId) {
  if (!driverId) return false;

  // 1) إذا لسا الرحلة queued وما تفعلت، رجّع ابعتها
  const queued = getDriverQueuedRide(driverId);
  if (queued?.ride_id) {
    const queuedRideId = toNumber(queued.ride_id);
    const snapshot =
      queued.ride_snapshot ??
      getRideDetails(queuedRideId) ??
      getRideSnapshotForRedispatch(queuedRideId) ??
      null;

    const payload = withDriverImage({
      ...(snapshot && typeof snapshot === "object" ? snapshot : {}),
      ride_id: queuedRideId,
      driver_id: driverId,
      active_ride_id: getActiveRideByDriver(driverId),
      offered_price: toNumber(queued.offered_price),
      message: queued.message ?? "Ride accepted and queued until current ride ends",
      at: Date.now(),
    }, driverId);

    if (snapshot) {
      payload.ride_details = snapshot;
    }

    io.to(driverRoom(driverId)).emit("ride:queued", payload);
    console.log(`[recovery] re-emitted ride:queued for driver ${driverId} ride ${queuedRideId}`);
    return true;
  }

  // 2) فقط إذا الرحلة الحالية متفعلة من queue ولسا ما عملنا recovery إلها
  const activeRideId = getActiveRideByDriver(driverId);
  if (!activeRideId) return false;

  const snapshot =
    getRideDetails(activeRideId) ??
    getRideSnapshotForRedispatch(activeRideId) ??
    null;

  if (!snapshot || toNumber(snapshot?.activated_from_queue) !== 1) {
    return false;
  }

  // لا تعيد نفس recovery أكثر من مرة
  if (toNumber(snapshot?.queue_activation_recovered) === 1) {
    return false;
  }

  const userId = resolveRideAudienceUserId(activeRideId);

  const payload = withDriverImage({
    ...(snapshot && typeof snapshot === "object" ? snapshot : {}),
    ride_id: activeRideId,
    driver_id: driverId,
    offered_price: toNumber(snapshot?.queued_offer_price ?? snapshot?.offered_price ?? null),
    message: "Queued ride is now active",
    at: Date.now(),
  }, driverId);

  if (snapshot) {
    payload.ride_details = snapshot;
  }

  io.to(driverRoom(driverId)).emit("ride:queueActivated", payload);
  io.to(driverRoom(driverId)).emit("ride:userAccepted", payload);
  emitToRideAudience(io, activeRideId, "ride:queueActivated", payload, userId);
  emitToRideAudience(io, activeRideId, "ride:userAccepted", payload, userId);

  const trackingPayload = withDriverImage({
    ...(snapshot && typeof snapshot === "object" ? snapshot : {}),
    ride_id: activeRideId,
    driver_id: driverId,
    offered_price: toNumber(snapshot?.queued_offer_price ?? snapshot?.offered_price ?? null),
    message: "Queued ride tracking started",
    at: Date.now(),
  }, driverId);

  if (snapshot) {
    trackingPayload.ride_details = snapshot;
  }

  io.to(driverRoom(driverId)).emit("ride:trackingStarted", trackingPayload);
  emitToRideAudience(io, activeRideId, "ride:trackingStarted", trackingPayload, userId);

  const d = driverLocationService.getDriver(driverId);
  if (d?.lat != null && d?.long != null) {
    const locationPayload = {
      ride_id: activeRideId,
      driver_id: driverId,
      lat: d.lat,
      long: d.long,
      at: Date.now(),
    };

    io.to(driverRoom(driverId)).emit("ride:locationUpdate", locationPayload);
    emitToRideAudience(io, activeRideId, "ride:locationUpdate", locationPayload, userId);
  }

  saveRideDetails(activeRideId, {
    ...snapshot,
    queue_activation_recovered: 1,
  });

  console.log(
    `[recovery] re-emitted queue activation flow for driver ${driverId} ride ${activeRideId}`
  );
  return true;
}

module.exports = (io, socket) => {
  socket.on("driver:getRidesList", (payload = {}) => {
    const { driver_id, recover_active_ride } = payload;
    debugLog("driver:getRidesList", { driver_id, recover_active_ride }, socket.id);
    const driverId = toNumber(driver_id) ?? toNumber(socket.driverId);
    if (!driverId) return;

    socket.driverId = socket.driverId ?? driverId;
    socket.join(driverRoom(driverId));
    driverLocationService.updateMeta(driverId, {
      is_online: true,
      updatedAt: Date.now(),
    });

    emitDriverInbox(io, driverId, "driver:rides:list");
    const inbox = driverRideInbox.get(driverId);
    if (inbox && inbox.size > 0) {
      recoverDriverPendingDispatch(io, driverId, "driver:getRidesList", {
        emitInbox: false,
      });
    } else {
      const key = `driver:getRidesList:${driverId}`;
      const now = Date.now();
      const lastLoggedAt = driverRecoveryNoopLoggedAt.get(key) ?? 0;
      if (now - lastLoggedAt >= DRIVER_RECOVERY_NOOP_LOG_THROTTLE_MS) {
        driverRecoveryNoopLoggedAt.set(key, now);
        console.log("[dispatch][driver-recovery]", {
          driver_id: driverId,
          source: "driver:getRidesList",
          attempted: 0,
          delivered: 0,
          pending: 0,
        });
      }
    }

    // Recovery must be explicit to avoid forcing driver UI into running screen.
    const shouldRecoverActiveRide =
      recover_active_ride === true || toNumber(recover_active_ride) === 1;
    if (shouldRecoverActiveRide) {
      emitDriverRideRecovery(io, driverId);
    }

  });

  socket.on("user:joinRideRoom", (payload = {}) => {
    const { user_id, ride_id } = payload;
    debugLog("user:joinRideRoom", { user_id, ride_id }, socket.id);
    const rideId = toNumber(ride_id);
    const joinToken = normalizeToken(
      payload?.access_token ??
        payload?.token ??
        payload?.user_token ??
        null
    );
    if (!rideId) {
      console.log(`undefined ride`);
      return;
    }
    socket.isUser = true;
    socket.userId = toNumber(user_id) ?? null;
    if (joinToken) {
      socket.userToken = joinToken;
    }

    if (socket.userId) {
      socket.join(userRoom(socket.userId));
      if (joinToken) {
        setUserDetails(socket.userId, {
          user_id: socket.userId,
          user_token: joinToken,
          token: joinToken,
          access_token: joinToken,
        });
      }
    }
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

  socket.on("ride:newBid:ack", (payload = {}) => {
    const safePayload = payload && typeof payload === "object" ? payload : {};
    const rideId = toNumber(safePayload?.ride_id ?? socket.currentRideId ?? null);
    const userId = toNumber(
      safePayload?.user_id ?? socket.userId ?? safePayload?.user_details?.user_id ?? null
    );

    console.log("[ack][ride:newBid]", {
      ride_id: rideId,
      user_id: userId,
      socket_id: socket.id,
      driver_id: toNumber(
        safePayload?.driver_id ??
          safePayload?.driver_detail_id ??
          safePayload?.driver_details_id ??
          safePayload?.driver_details?.driver_id ??
          safePayload?.driver_details?.provider_id ??
          null
      ),
      offered_price: toNumber(safePayload?.offered_price ?? null),
      client_ts:
        toNumber(
          safePayload?.client_ts ?? safePayload?.received_at ?? safePayload?.at ?? null
        ) ?? null,
      additional_remarks: resolveAdditionalRemarks(safePayload),
      at: Date.now(),
    });
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
    if (!markRideCancelled(io, rideId, { activateQueued: true })) return;

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
        timeout: LARAVEL_TIMEOUT_MS,
      })
      .then((response) => {
        console.log("API Response: Ride Cancelled", response.data);
      })
      .catch((error) => {
        console.error("Error while calling cancel ride API:", error?.response?.data || error.message);
      });
  });

  // ✅ إذا السائق قبل العرض
  socket.on("driver:acceptOffer", async (payload) => {
    const resolvedRideId = getRideIdFromDriverPayload(payload);
    const resolvedOfferedPrice = getOfferedPriceFromDriverPayload(payload);
    console.log("[accept][driver:acceptOffer] incoming", {
      ride_id: resolvedRideId ?? payload?.ride_id ?? payload?.request_id ?? null,
      driver_id: payload?.driver_id ?? socket.driverId ?? null,
      offered_price: resolvedOfferedPrice ?? payload?.offered_price ?? payload?.price ?? null,
    });

    const driverId = toNumber(socket.driverId) ?? toNumber(payload?.driver_id);
    const rideId = resolvedRideId;
    const offeredPrice = resolvedOfferedPrice;

    if (!driverId || !rideId || offeredPrice === null) return;
    if (!isDriverOfferStillActive(driverId, rideId)) {
      console.log(`⚠️ Driver ${driverId} cannot accept: offer expired for ride ${rideId}`);
      markRideDriverState(rideId, driverId, "expired");
      inboxRemove(driverId, rideId);
      clearDriverBidStatus(driverId, rideId);
      removeDriverFromRideCandidates(io, rideId, driverId, { emitSummary: true });
      emitDriverPatch(io, driverId, [{ op: "remove", ride_id: rideId }]);
      emitRideUnavailable(io, driverId, rideId);
      return;
    }

    const activeRide = getActiveRideByDriver(driverId);
    if (activeRide && activeRide !== rideId && !canDriverSubmitBidForRide(driverId, rideId)) {
      console.log(
        `⚠️ driver:acceptOffer ignored: driver ${driverId} already active on ride ${activeRide} and is not eligible for queued accept`
      );
      return;
    }

        if (cancelledRides.has(rideId)) {
      console.log(`⚠️ Driver ${driverId} cannot accept: ride ${rideId} is cancelled`);
      emitRideUnavailable(io, driverId, rideId);
      return;
    }

    const acceptedByAnotherDriver = getActiveDriverByRide(rideId);
    if (acceptedByAnotherDriver) {
      console.log(
        `⚠️ Driver ${driverId} cannot accept: ride ${rideId} already accepted by driver ${acceptedByAnotherDriver}`
      );
      emitRideUnavailable(io, driverId, rideId);
      return;
    }

    if (!isCandidateDriver(rideId, driverId)) {
      emitRideUnavailable(io, driverId, rideId);
      return;
    }

    const rideSnapshot = driverRideInbox.get(driverId)?.get(rideId) ?? getRideDetails(rideId) ?? null;
    const driverMeta = driverLocationService.getMeta(driverId) || {};
    let driverIdentity = {
      provider_id: driverId,
      ...extractDriverIdentity(payload, driverMeta),
    };
    const driverServiceId =
      toNumber(payload?.driver_service_id) ??
      toNumber(payload?.driver_service) ??
      toNumber(socket.driverServiceId) ??
      toNumber(driverMeta?.driver_service_id);
    const driverAcceptAccessToken =
      payload?.driver_access_token ??
      payload?.access_token ??
      socket.driverAccessToken ??
      driverMeta?.access_token ??
      null;
    if (driverIdentity.driver_detail_id === null && driverServiceId && driverAcceptAccessToken) {
      const fetchedIdentity = await fetchDriverMetaFromApi(
        driverId,
        driverAcceptAccessToken,
        driverServiceId
      );
      if (fetchedIdentity) {
        driverIdentity = {
          provider_id: driverIdentity.provider_id ?? fetchedIdentity.provider_id ?? driverId,
          driver_service_id:
            driverIdentity.driver_service_id ?? fetchedIdentity.driver_service_id ?? driverServiceId,
          driver_detail_id:
            driverIdentity.driver_detail_id ?? fetchedIdentity.driver_detail_id ?? null,
        };
      }
    }
    const customerFacingDriverId =
      toNumber(driverIdentity.driver_detail_id) ?? toNumber(driverIdentity.provider_id) ?? driverId;
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
  removeDriverFromRideCandidates(io, rideId, driverId, { emitSummary: false });
  emitDriverPatch(io, driverId, [{ op: "remove", ride_id: rideId }]);

  console.log(`🧹 Removed ride ${rideId} from driver ${driverId} inbox after acceptOffer`);
}

    markRideDriverState(rideId, driverId, "accepted", {
      last_offered_price: offeredPrice,
      ...buildDriverIdentityPayload(driverIdentity, driverId),
    });

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
      ...buildDriverIdentityPayload(driverIdentity, driverId),
      offered_price: offeredPrice,
      message: "Offer accepted by driver",
      ride_details: rideSnapshot,
      at: Date.now(),
    });

    emitToRideAudience(
      io,
      rideId,
      "ride:acceptedByDriver",
      {
        ride_id: rideId,
        ...buildDriverIdentityPayload(driverIdentity, customerFacingDriverId),
        offered_price: offeredPrice,
        message: "Offer accepted by driver",
        ride_details: rideSnapshot,
        at: Date.now(),
      },
      userId
    );

    const finalize = () =>
      finalizeAcceptedRide(io, rideId, driverId, offeredPrice, {
        message: "Driver accepted the offer",
        rideDetails: rideSnapshot,
        userId,
        driverIdentity,
      });

    if (!userId || !accessToken) {
      console.log(`WARN driver:acceptOffer API skipped: missing user_id/access_token (ride ${rideId})`);
      finalize();
    } else {
      const acceptPayload = {
        user_id: userId,
        access_token: accessToken,
        ...buildDriverIdentityPayload(driverIdentity, customerFacingDriverId),
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
    const resolvedRideId = getRideIdFromDriverPayload(payload);
    const resolvedOfferedPrice = getOfferedPriceFromDriverPayload(payload);
    debugLog("driver:submitBid", payload, socket.id);
    console.log("[bid][driver:submitBid] incoming", {
      ride_id: resolvedRideId ?? payload?.ride_id ?? payload?.request_id ?? null,
      driver_id: socket.driverId ?? payload?.driver_id ?? null,
      offered_price: resolvedOfferedPrice ?? payload?.offered_price ?? payload?.price ?? null,
    });

    const rideId = resolvedRideId;
    const driverId = toNumber(socket.driverId) ?? toNumber(payload?.driver_id);
    const offeredPrice = resolvedOfferedPrice;

    if (!driverId || !rideId || offeredPrice === null) {
      console.log(
        `⚠️ Invalid bid attempt - missing ride_id or driver_id or offered_price (socket: ${socket.id})`
      );
      socket.emit("ride:bidBlocked", {
        ride_id: rideId ?? null,
        reason: "invalid_payload",
        message: "Missing ride_id or offered_price",
        at: Date.now(),
      });
      return;
    }
    if (!isDriverOfferStillActive(driverId, rideId)) {
      console.log(`⚠️ Driver ${driverId} cannot bid: offer expired for ride ${rideId}`);
      markRideDriverState(rideId, driverId, "expired");
      inboxRemove(driverId, rideId);
      clearDriverBidStatus(driverId, rideId);
      removeDriverFromRideCandidates(io, rideId, driverId, { emitSummary: true });
      emitDriverPatch(io, driverId, [{ op: "remove", ride_id: rideId }]);
      emitRideUnavailable(io, driverId, rideId);
      return;
    }

    const activeRide = getActiveRideByDriver(driverId);
    if (activeRide && activeRide !== rideId && !canDriverSubmitBidForRide(driverId, rideId)) {
      console.log(
        `⚠️ Driver ${driverId} has active ride ${activeRide} and is not eligible to bid on ride ${rideId}`
      );
      return;
    }

    const currencyRaw = toNumber(payload?.currency);
    const currency = currencyRaw !== null && currencyRaw > 0 ? currencyRaw : 1;
    const finalPrice = round2(offeredPrice * currency);
    const rideSnapshot =
      driverRideInbox.get(driverId)?.get(rideId) ??
      getRideDetails(rideId) ??
      null;
    const rideOwnerUserId = toNumber(
      rideSnapshot?.user_id ?? rideSnapshot?.user_details?.user_id ?? null
    );
    const rideOwnerUserDetails = rideOwnerUserId ? getUserDetails(rideOwnerUserId) : null;
    const bidUserLanguage = normalizeLanguageCode(
      pickFirstValue(
        payload?.user_language,
        payload?.language,
        rideSnapshot?.user_language,
        rideSnapshot?.language,
        rideSnapshot?.user_details?.user_language,
        rideSnapshot?.user_details?.language,
        rideOwnerUserDetails?.user_language,
        rideOwnerUserDetails?.language
      )
    );
    const rideServiceTypeNameEn = toTrimmedText(
      pickFirstValue(
        payload?.service_type_name_en,
        payload?.vehicle_type_name_en,
        rideSnapshot?.service_type_name_en,
        rideSnapshot?.vehicle_type_name_en,
        rideSnapshot?.ride_details?.service_type_name_en,
        rideSnapshot?.ride_details?.vehicle_type_name_en,
        rideSnapshot?.meta?.service_type_name_en,
        rideSnapshot?.meta?.vehicle_type_name_en
      )
    );
    const rideServiceTypeNameAr = toTrimmedText(
      pickFirstValue(
        payload?.service_type_name_ar,
        payload?.vehicle_type_name_ar,
        rideSnapshot?.service_type_name_ar,
        rideSnapshot?.vehicle_type_name_ar,
        rideSnapshot?.ride_details?.service_type_name_ar,
        rideSnapshot?.ride_details?.vehicle_type_name_ar,
        rideSnapshot?.meta?.service_type_name_ar,
        rideSnapshot?.meta?.vehicle_type_name_ar
      )
    );
    const rideServiceTypeNameRaw = toTrimmedText(
      pickFirstValue(
        payload?.service_type_name,
        payload?.vehicle_type_name,
        rideSnapshot?.service_type_name,
        rideSnapshot?.vehicle_type_name,
        rideSnapshot?.ride_details?.service_type_name,
        rideSnapshot?.ride_details?.vehicle_type_name,
        rideSnapshot?.meta?.service_type_name,
        rideSnapshot?.meta?.vehicle_type_name
      )
    );
    const bidLocalizedServiceTypeName = pickLocalizedText(
      bidUserLanguage,
      rideServiceTypeNameEn,
      rideServiceTypeNameAr,
      rideServiceTypeNameRaw
    );

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
    const payloadDriverDetailsRaw =
      payload?.driver_details && typeof payload.driver_details === "object"
        ? payload.driver_details
        : {};
    const payloadDriverProfileFields = {
      vehicle_company: toTrimmedText(
        pickFirstValue(
          payload?.vehicle_company,
          payloadDriverDetailsRaw?.vehicle_company,
          payloadDriverDetailsRaw?.company
        )
      ),
      plat_no: toTrimmedText(
        pickFirstValue(
          payload?.plat_no,
          payload?.vehicle_number,
          payloadDriverDetailsRaw?.plat_no,
          payloadDriverDetailsRaw?.vehicle_number,
          payloadDriverDetailsRaw?.plate_no
        )
      ),
      model_year: pickFirstValue(
        payload?.model_year,
        payloadDriverDetailsRaw?.model_year,
        payloadDriverDetailsRaw?.vehicle_year,
        payloadDriverDetailsRaw?.manufacture_year
      ),
      model_name: toTrimmedText(
        pickFirstValue(
          payload?.model_name,
          payloadDriverDetailsRaw?.model_name,
          payloadDriverDetailsRaw?.model,
          payloadDriverDetailsRaw?.vehicle_model
        )
      ),
      vehicle_color: toTrimmedText(
        pickFirstValue(
          payload?.vehicle_color,
          payloadDriverDetailsRaw?.vehicle_color,
          payloadDriverDetailsRaw?.color
        )
      ),
      driver_name: toTrimmedText(
        pickFirstValue(
          payload?.driver_name,
          payloadDriverDetailsRaw?.driver_name,
          payloadDriverDetailsRaw?.name
        )
      ),
      rating: pickFirstValue(
        payload?.rating,
        payload?.driver_rating,
        payloadDriverDetailsRaw?.rating,
        payloadDriverDetailsRaw?.driver_rating
      ),
      driver_image: toTrimmedText(
        pickFirstValue(
          payload?.driver_image,
          payload?.driver_image_url,
          payloadDriverDetailsRaw?.driver_image,
          payloadDriverDetailsRaw?.driver_image_url,
          payloadDriverDetailsRaw?.profile_image,
          payloadDriverDetailsRaw?.avatar
        )
      ),
    };
    const memoryDriverMetaFields = {
      vehicle_company: toTrimmedText(driverMeta?.vehicle_company ?? driverMeta?.company ?? null),
      plat_no: toTrimmedText(
        pickFirstValue(
          driverMeta?.plat_no,
          driverMeta?.vehicle_number,
          driverMeta?.plate_no,
          driverMeta?.vehicle_no
        )
      ),
      model_year: pickFirstValue(
        driverMeta?.model_year,
        driverMeta?.vehicle_year,
        driverMeta?.manufacture_year
      ),
      model_name: toTrimmedText(
        pickFirstValue(
          driverMeta?.model_name,
          driverMeta?.model,
          driverMeta?.vehicle_model
        )
      ),
      vehicle_color: toTrimmedText(driverMeta?.vehicle_color ?? driverMeta?.color ?? null),
      driver_name: toTrimmedText(driverMeta?.driver_name ?? driverMeta?.name ?? null),
      rating: pickFirstValue(driverMeta?.rating, driverMeta?.driver_rating),
      driver_image: toTrimmedText(
        pickFirstValue(
          driverMeta?.driver_image,
          driverMeta?.driver_image_url,
          driverMeta?.profile_image,
          driverMeta?.avatar
        )
      ),
    };
    console.log("[driver:submitBid][details-raw]", {
      ride_id: rideId,
      driver_id: driverId,
      payload_fields: payloadDriverProfileFields,
      memory_meta_fields: memoryDriverMetaFields,
    });
    const driverServiceId =
      toNumber(payload?.driver_service_id) ??
      toNumber(payload?.driver_service) ??
      toNumber(socket.driverServiceId) ??
      toNumber(driverMeta?.driver_service_id);
    let driverIdentity = extractDriverIdentity(payload, driverMeta, {
      provider_id: driverId,
      driver_service_id: driverServiceId,
    });

    const accessToken =
      payload?.access_token ?? socket.driverAccessToken ?? driverMeta?.access_token ?? null;

    const payloadDriverDetailsSeed = {
      ...(payload && typeof payload === "object" ? payload : {}),
      ...(payload?.driver_details && typeof payload.driver_details === "object"
        ? payload.driver_details
        : {}),
    };
    let driverDetails = normalizeDriverDetailsPayload(payloadDriverDetailsSeed, {
      ...driverMeta,
      vehicle_type: driverMeta?.vehicle_type_name ?? null,
      vehicle_number: driverMeta?.plat_no ?? null,
    });

    const driverImageMissing = toTrimmedText(driverDetails?.driver_image) == null;
    const shouldFetchDriverMeta =
      (driverIdentity.driver_detail_id === null ||
        isDriverDetailsEmpty(driverDetails) ||
        driverImageMissing) &&
      driverServiceId &&
      accessToken;

    if (shouldFetchDriverMeta) {
      const fetched = await fetchDriverMetaFromApi(driverId, accessToken, driverServiceId);
      if (fetched) {
        driverIdentity = {
          provider_id: driverIdentity.provider_id ?? fetched.provider_id ?? driverId,
          driver_service_id:
            driverIdentity.driver_service_id ?? fetched.driver_service_id ?? driverServiceId,
          driver_detail_id: driverIdentity.driver_detail_id ?? fetched.driver_detail_id ?? null,
        };
        driverDetails = normalizeDriverDetailsPayload(driverDetails, fetched);
      }
    }

    driverIdentity = {
      provider_id: driverIdentity.provider_id ?? driverId,
      driver_service_id: driverIdentity.driver_service_id ?? driverServiceId ?? null,
      driver_detail_id: driverIdentity.driver_detail_id ?? null,
    };
    if (bidLocalizedServiceTypeName) {
      driverDetails = {
        ...(driverDetails && typeof driverDetails === "object" ? driverDetails : {}),
        vehicle_type: bidLocalizedServiceTypeName,
        vehicle_type_name: bidLocalizedServiceTypeName,
      };
    }
    if (rideServiceTypeNameEn || rideServiceTypeNameAr || bidUserLanguage) {
      driverDetails = {
        ...(driverDetails && typeof driverDetails === "object" ? driverDetails : {}),
        ...(rideServiceTypeNameEn ? { vehicle_type_name_en: rideServiceTypeNameEn } : {}),
        ...(rideServiceTypeNameAr ? { vehicle_type_name_ar: rideServiceTypeNameAr } : {}),
        ...(bidUserLanguage ? { user_language: bidUserLanguage, language: bidUserLanguage } : {}),
      };
    }
    console.log("[driver:submitBid][details-normalized]", {
      ride_id: rideId,
      driver_id: driverId,
      driver_identity: driverIdentity,
      driver_details: {
        vehicle_company: driverDetails?.vehicle_company ?? null,
        plat_no:
          driverDetails?.plat_no ??
          driverDetails?.vehicle_number ??
          null,
        model_year: driverDetails?.model_year ?? null,
        model_name: driverDetails?.model_name ?? null,
        vehicle_color: driverDetails?.vehicle_color ?? null,
        driver_name: driverDetails?.driver_name ?? null,
        rating: driverDetails?.rating ?? null,
        driver_image: driverDetails?.driver_image ?? null,
      },
    });
    const customerFacingDriverId =
      toNumber(driverIdentity.driver_detail_id) ?? toNumber(driverIdentity.provider_id) ?? driverId;

    if (cancelledRides.has(rideId)) {
      console.log(`⚠️ Bid ignored: ride ${rideId} is cancelled`);
      emitRideUnavailable(io, driverId, rideId);
      return;
    }

    const activeDriverId = getActiveDriverByRide(rideId);
    if (activeDriverId) {
      console.log(`⚠️ Bid ignored: ride ${rideId} already accepted by driver ${activeDriverId}`);
      emitRideUnavailable(io, driverId, rideId);
      return;
    }

    if (!isCandidateDriver(rideId, driverId)) {
      emitRideUnavailable(io, driverId, rideId);
      return;
    }

    const lastBid = driverLastBidStatus.get(driverId);
    if (lastBid && lastBid.rideId === rideId && !lastBid.responded) {
      console.log(`⚠️ Driver ${driverId} cannot submit a new bid until user responds to the previous bid.`);
      io.to(driverRoom(driverId)).emit("ride:bidBlocked", {
        ride_id: rideId,
        reason: "pending_previous_bid",
        message: "User has not responded to the previous bid yet",
        at: Date.now(),
      });
      return;
    }

driverLastBidStatus.set(driverId, { rideId, responded: false });    markRideDriverState(rideId, driverId, "bid_submitted", {
      last_offered_price: offeredPrice,
      ...buildDriverIdentityPayload(driverIdentity, driverId),
    });

    if (!driverServiceId || !accessToken) {
  console.log(
    `⚠️ driver:submitBid API skipped: missing driver_service_id/access_token (driver ${driverId})`
  );
} else {
  const bidPayload = {
    ...buildDriverIdentityPayload(driverIdentity, driverId),
    access_token: accessToken,
    driver_service_id: driverServiceId,
    ride_id: rideId,
    offered_price: offeredPrice,
  };

  console.log("[driver:submitBid][before-api]", {
    ride_id: rideId,
    driver_id: driverId,
    offered_price: offeredPrice,
    driver_service_id: driverServiceId,
    has_access_token: !!accessToken,
  });

  axios
    .post(`${LARAVEL_BASE_URL}${LARAVEL_DRIVER_BID_PATH}`, bidPayload, {
      timeout: LARAVEL_TIMEOUT_MS,
    })
    .then((response) => {
      console.log("[driver:submitBid][api-ok]", {
        ride_id: rideId,
        driver_id: driverId,
        response: response?.data ?? null,
      });
    })
    .catch((error) => {
      console.error("[driver:submitBid][api-failed]", {
        ride_id: rideId,
        driver_id: driverId,
        error: error?.response?.data || error?.message || error,
      });
    });
}
// console.log("[driver:submitBid][api-response]", {
//   ride_id: rideId,
//   driver_id: driverId,
//   response: res?.data ?? null,
// });
    // ✅ TIMER refresh: every new bid resets timer (90s seconds)
    const timer = refreshRideTimerWithDispatchTimeout(io, rideId, {
      update_snapshot: true,
      patch_inboxes: true,
    });

    const snapshotUserToken = normalizeToken(
      pickFirstValue(
        rideSnapshot?.user_details?.user_token,
        rideSnapshot?.user_details?.token,
        rideSnapshot?.token,
        rideSnapshot?.access_token,
        null
      )
    );
    const snapshotUserIdForBid = toNumber(
      pickFirstValue(
        rideSnapshot?.user_id,
        rideSnapshot?.user_details?.user_id,
        getUserIdForRide(rideId)
      )
    );
    const snapshotUserFromStore = snapshotUserIdForBid
      ? getUserDetails(snapshotUserIdForBid)
      : null;
    const snapshotUserFromToken =
      !snapshotUserFromStore && snapshotUserToken
        ? getUserDetailsByToken(snapshotUserToken)
        : null;
    const snapshotUserSeed = buildUserDetails({
      ...(rideSnapshot && typeof rideSnapshot === "object" ? rideSnapshot : {}),
      ...(snapshotUserIdForBid ? { user_id: snapshotUserIdForBid } : {}),
      ...(snapshotUserToken
        ? {
            user_token: snapshotUserToken,
            token: snapshotUserToken,
            access_token: snapshotUserToken,
          }
        : {}),
      user_details:
        (rideSnapshot?.user_details && typeof rideSnapshot.user_details === "object"
          ? rideSnapshot.user_details
          : null) ??
        snapshotUserFromStore ??
        snapshotUserFromToken ??
        null,
    });
    const bidUserDetails =
      snapshotUserSeed ?? snapshotUserFromStore ?? snapshotUserFromToken ?? null;
    const bidVehicleCompany = resolveLocalizedFieldVariants(
      bidUserLanguage,
      pickFirstValue(
        driverDetails?.vehicle_company_en,
        driverDetails?.vehicle_manufacture_name_en,
        rideSnapshot?.vehicle_company_en,
        rideSnapshot?.vehicle_manufacture_name_en,
        rideSnapshot?.ride_details?.vehicle_company_en,
        rideSnapshot?.ride_details?.vehicle_manufacture_name_en,
        rideSnapshot?.meta?.vehicle_company_en,
        rideSnapshot?.meta?.vehicle_manufacture_name_en
      ),
      pickFirstValue(
        driverDetails?.vehicle_company_ar,
        driverDetails?.vehicle_manufacture_name_ar,
        rideSnapshot?.vehicle_company_ar,
        rideSnapshot?.vehicle_manufacture_name_ar,
        rideSnapshot?.ride_details?.vehicle_company_ar,
        rideSnapshot?.ride_details?.vehicle_manufacture_name_ar,
        rideSnapshot?.meta?.vehicle_company_ar,
        rideSnapshot?.meta?.vehicle_manufacture_name_ar
      ),
      pickFirstValue(
        driverDetails?.vehicle_company,
        driverDetails?.vehicle_manufacture_name,
        rideSnapshot?.vehicle_company,
        rideSnapshot?.vehicle_manufacture_name,
        rideSnapshot?.ride_details?.vehicle_company,
        rideSnapshot?.ride_details?.vehicle_manufacture_name,
        rideSnapshot?.meta?.vehicle_company,
        rideSnapshot?.meta?.vehicle_manufacture_name
      )
    );
    const bidModelName = resolveLocalizedFieldVariants(
      bidUserLanguage,
      pickFirstValue(
        driverDetails?.model_name_en,
        driverDetails?.vehicle_model_name_en,
        rideSnapshot?.model_name_en,
        rideSnapshot?.vehicle_model_name_en,
        rideSnapshot?.ride_details?.model_name_en,
        rideSnapshot?.ride_details?.vehicle_model_name_en,
        rideSnapshot?.meta?.model_name_en,
        rideSnapshot?.meta?.vehicle_model_name_en
      ),
      pickFirstValue(
        driverDetails?.model_name_ar,
        driverDetails?.vehicle_model_name_ar,
        rideSnapshot?.model_name_ar,
        rideSnapshot?.vehicle_model_name_ar,
        rideSnapshot?.ride_details?.model_name_ar,
        rideSnapshot?.ride_details?.vehicle_model_name_ar,
        rideSnapshot?.meta?.model_name_ar,
        rideSnapshot?.meta?.vehicle_model_name_ar
      ),
      pickFirstValue(
        driverDetails?.model_name,
        driverDetails?.vehicle_model_name,
        rideSnapshot?.model_name,
        rideSnapshot?.vehicle_model_name,
        rideSnapshot?.ride_details?.model_name,
        rideSnapshot?.ride_details?.vehicle_model_name,
        rideSnapshot?.meta?.model_name,
        rideSnapshot?.meta?.vehicle_model_name
      )
    );
    const bidVehicleColor = resolveLocalizedFieldVariants(
      bidUserLanguage,
      pickFirstValue(
        driverDetails?.vehicle_color_en,
        rideSnapshot?.vehicle_color_en,
        rideSnapshot?.ride_details?.vehicle_color_en,
        rideSnapshot?.meta?.vehicle_color_en
      ),
      pickFirstValue(
        driverDetails?.vehicle_color_ar,
        rideSnapshot?.vehicle_color_ar,
        rideSnapshot?.ride_details?.vehicle_color_ar,
        rideSnapshot?.meta?.vehicle_color_ar
      ),
      pickFirstValue(
        driverDetails?.vehicle_color,
        rideSnapshot?.vehicle_color,
        rideSnapshot?.ride_details?.vehicle_color,
        rideSnapshot?.meta?.vehicle_color
      )
    );
    const bidVehicleManufacturer = resolveLocalizedFieldVariants(
      bidUserLanguage,
      pickFirstValue(
        driverDetails?.vehicle_manufacturer_en,
        driverDetails?.manufacturer_name_en,
        driverDetails?.vehicle_manufacture_name_en,
        bidVehicleCompany.en
      ),
      pickFirstValue(
        driverDetails?.vehicle_manufacturer_ar,
        driverDetails?.manufacturer_name_ar,
        driverDetails?.vehicle_manufacture_name_ar,
        bidVehicleCompany.ar
      ),
      pickFirstValue(
        driverDetails?.vehicle_manufacturer,
        driverDetails?.manufacturer_name,
        driverDetails?.vehicle_manufacture_name,
        bidVehicleCompany.localized
      )
    );

    let ridePayload = {
      ride_id: rideId,
      ...buildDriverIdentityPayload(driverIdentity, customerFacingDriverId),
      offered_price: offeredPrice,
      bidding_time: Date.now(),
      user_id: bidUserDetails?.user_id ?? snapshotUserIdForBid ?? null,
      user_name: bidUserDetails?.user_name ?? rideSnapshot?.user_name ?? null,
      user_gender: bidUserDetails?.user_gender ?? rideSnapshot?.user_gender ?? null,
      user_image: bidUserDetails?.user_image ?? rideSnapshot?.user_image ?? null,
      user_profile: bidUserDetails?.user_image ?? rideSnapshot?.user_image ?? null,
      customer_image: bidUserDetails?.user_image ?? rideSnapshot?.user_image ?? null,
      user_phone: bidUserDetails?.user_phone ?? rideSnapshot?.user_phone ?? null,
      user_country_code:
        bidUserDetails?.user_country_code ?? rideSnapshot?.user_country_code ?? null,
      user_phone_full: bidUserDetails?.user_phone_full ?? rideSnapshot?.user_phone_full ?? null,

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
        toNumber(rideSnapshot?.min_fare_amount) ??
        toNumber(rideSnapshot?.min_price) ??
        toNumber(rideSnapshot?.ride_details?.min_price) ??
        toNumber(rideSnapshot?.meta?.min_price) ??
        toNumber(payload?.min_fare_amount) ??
        toNumber(payload?.min_price) ??
        null,
      base_fare:
        toNumber(rideSnapshot?.base_fare) ??
        toNumber(rideSnapshot?.ride_details?.base_fare) ??
        toNumber(rideSnapshot?.meta?.base_fare) ??
        toNumber(rideSnapshot?.estimated_price) ??
        toNumber(rideSnapshot?.ride_details?.estimated_price) ??
        toNumber(rideSnapshot?.meta?.estimated_price) ??
        toNumber(payload?.base_fare) ??
        toNumber(payload?.estimated_price) ??
        toNumber(payload?.estimated_fare) ??
        ridePriceBounds.base_fare,
      estimated_price:
        toNumber(rideSnapshot?.estimated_price) ??
        toNumber(rideSnapshot?.ride_details?.estimated_price) ??
        toNumber(rideSnapshot?.meta?.estimated_price) ??
        toNumber(payload?.estimated_price) ??
        toNumber(payload?.estimated_fare) ??
        ridePriceBounds.base_fare,
      estimated_fare:
        toNumber(rideSnapshot?.estimated_fare) ??
        toNumber(rideSnapshot?.ride_details?.estimated_fare) ??
        toNumber(rideSnapshot?.meta?.estimated_fare) ??
        toNumber(payload?.estimated_fare) ??
        toNumber(payload?.estimated_price) ??
        ridePriceBounds.base_fare,
      min_price:
        toNumber(rideSnapshot?.min_price) ??
        toNumber(rideSnapshot?.ride_details?.min_price) ??
        toNumber(rideSnapshot?.meta?.min_price) ??
        toNumber(payload?.min_price) ??
        ridePriceBounds.min_price,
      max_price:
        toNumber(rideSnapshot?.max_price) ??
        toNumber(rideSnapshot?.ride_details?.max_price) ??
        toNumber(rideSnapshot?.meta?.max_price) ??
        toNumber(payload?.max_price) ??
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
        ...(bidVehicleCompany.localized
          ? {
              vehicle_company: bidVehicleCompany.localized,
              vehicle_manufacture_name: bidVehicleCompany.localized,
            }
          : {}),
        ...(bidVehicleCompany.en
          ? {
              vehicle_company_en: bidVehicleCompany.en,
              vehicle_manufacture_name_en: bidVehicleCompany.en,
            }
          : {}),
        ...(bidVehicleCompany.ar
          ? {
              vehicle_company_ar: bidVehicleCompany.ar,
              vehicle_manufacture_name_ar: bidVehicleCompany.ar,
            }
          : {}),
        ...(bidModelName.localized
          ? {
              model_name: bidModelName.localized,
              vehicle_model_name: bidModelName.localized,
            }
          : {}),
        ...(bidModelName.en
          ? { model_name_en: bidModelName.en, vehicle_model_name_en: bidModelName.en }
          : {}),
        ...(bidModelName.ar
          ? { model_name_ar: bidModelName.ar, vehicle_model_name_ar: bidModelName.ar }
          : {}),
        ...(bidVehicleColor.localized ? { vehicle_color: bidVehicleColor.localized } : {}),
        ...(bidVehicleColor.en ? { vehicle_color_en: bidVehicleColor.en } : {}),
        ...(bidVehicleColor.ar ? { vehicle_color_ar: bidVehicleColor.ar } : {}),
        ...(bidVehicleManufacturer.localized
          ? {
              vehicle_manufacturer: bidVehicleManufacturer.localized,
              manufacturer_name: bidVehicleManufacturer.localized,
            }
          : {}),
        ...(bidVehicleManufacturer.en
          ? {
              vehicle_manufacturer_en: bidVehicleManufacturer.en,
              manufacturer_name_en: bidVehicleManufacturer.en,
            }
          : {}),
        ...(bidVehicleManufacturer.ar
          ? {
              vehicle_manufacturer_ar: bidVehicleManufacturer.ar,
              manufacturer_name_ar: bidVehicleManufacturer.ar,
            }
          : {}),
      },

      driver_details: {
        ...(driverDetails && typeof driverDetails === "object" ? driverDetails : {}),
        ...buildDriverIdentityPayload(driverIdentity, customerFacingDriverId),
        ...(rideServiceTypeNameEn ? { vehicle_type_name_en: rideServiceTypeNameEn } : {}),
        ...(rideServiceTypeNameAr ? { vehicle_type_name_ar: rideServiceTypeNameAr } : {}),
        ...(bidVehicleCompany.localized
          ? {
              vehicle_company: bidVehicleCompany.localized,
              vehicle_manufacture_name: bidVehicleCompany.localized,
            }
          : {}),
        ...(bidVehicleCompany.en
          ? {
              vehicle_company_en: bidVehicleCompany.en,
              vehicle_manufacture_name_en: bidVehicleCompany.en,
            }
          : {}),
        ...(bidVehicleCompany.ar
          ? {
              vehicle_company_ar: bidVehicleCompany.ar,
              vehicle_manufacture_name_ar: bidVehicleCompany.ar,
            }
          : {}),
        ...(bidModelName.localized
          ? {
              model_name: bidModelName.localized,
              vehicle_model_name: bidModelName.localized,
            }
          : {}),
        ...(bidModelName.en
          ? { model_name_en: bidModelName.en, vehicle_model_name_en: bidModelName.en }
          : {}),
        ...(bidModelName.ar
          ? { model_name_ar: bidModelName.ar, vehicle_model_name_ar: bidModelName.ar }
          : {}),
        ...(bidVehicleColor.localized ? { vehicle_color: bidVehicleColor.localized } : {}),
        ...(bidVehicleColor.en ? { vehicle_color_en: bidVehicleColor.en } : {}),
        ...(bidVehicleColor.ar ? { vehicle_color_ar: bidVehicleColor.ar } : {}),
        ...(bidVehicleManufacturer.localized
          ? {
              vehicle_manufacturer: bidVehicleManufacturer.localized,
              manufacturer_name: bidVehicleManufacturer.localized,
            }
          : {}),
        ...(bidVehicleManufacturer.en
          ? {
              vehicle_manufacturer_en: bidVehicleManufacturer.en,
              manufacturer_name_en: bidVehicleManufacturer.en,
            }
          : {}),
        ...(bidVehicleManufacturer.ar
          ? {
              vehicle_manufacturer_ar: bidVehicleManufacturer.ar,
              manufacturer_name_ar: bidVehicleManufacturer.ar,
            }
          : {}),
        ...(bidUserLanguage ? { user_language: bidUserLanguage, language: bidUserLanguage } : {}),
      },
      driver_name: driverDetails?.driver_name ?? null,
      driver_image: driverDetails?.driver_image ?? null,
      driver_rating: driverDetails?.rating ?? null,
      vehicle_type: bidLocalizedServiceTypeName ?? driverDetails?.vehicle_type ?? null,
      vehicle_type_name:
        bidLocalizedServiceTypeName ??
        driverDetails?.vehicle_type_name ??
        driverDetails?.vehicle_type ??
        null,
      vehicle_type_name_en:
        rideServiceTypeNameEn ??
        driverDetails?.vehicle_type_name_en ??
        bidLocalizedServiceTypeName ??
        null,
      vehicle_type_name_ar:
        rideServiceTypeNameAr ??
        driverDetails?.vehicle_type_name_ar ??
        bidLocalizedServiceTypeName ??
        null,
      service_type_name:
        bidLocalizedServiceTypeName ??
        driverDetails?.vehicle_type_name ??
        driverDetails?.vehicle_type ??
        null,
      service_type_name_en:
        rideServiceTypeNameEn ??
        driverDetails?.vehicle_type_name_en ??
        bidLocalizedServiceTypeName ??
        null,
      service_type_name_ar:
        rideServiceTypeNameAr ??
        driverDetails?.vehicle_type_name_ar ??
        bidLocalizedServiceTypeName ??
        null,
      user_language: bidUserLanguage ?? null,
      language: bidUserLanguage ?? null,
      vehicle_company: bidVehicleCompany.localized,
      vehicle_company_en: bidVehicleCompany.en,
      vehicle_company_ar: bidVehicleCompany.ar,
      vehicle_manufacture_name: bidVehicleCompany.localized,
      vehicle_manufacture_name_en: bidVehicleCompany.en,
      vehicle_manufacture_name_ar: bidVehicleCompany.ar,
      vehicle_manufacturer: bidVehicleManufacturer.localized,
      vehicle_manufacturer_en: bidVehicleManufacturer.en,
      vehicle_manufacturer_ar: bidVehicleManufacturer.ar,
      manufacturer_name: bidVehicleManufacturer.localized,
      manufacturer_name_en: bidVehicleManufacturer.en,
      manufacturer_name_ar: bidVehicleManufacturer.ar,
      model_name: bidModelName.localized,
      model_name_en: bidModelName.en,
      model_name_ar: bidModelName.ar,
      vehicle_model_name: bidModelName.localized,
      vehicle_model_name_en: bidModelName.en,
      vehicle_model_name_ar: bidModelName.ar,
      model_year: driverDetails?.model_year ?? null,
      vehicle_color: bidVehicleColor.localized,
      vehicle_color_en: bidVehicleColor.en,
      vehicle_color_ar: bidVehicleColor.ar,
      vehicle_number: driverDetails?.vehicle_number ?? null,
      plat_no: driverDetails?.plat_no ?? driverDetails?.vehicle_number ?? null,
      address_list: payload.address_list ?? [],
      user_timeout:
        toNumber(payload?.user_timeout) ??
        toNumber(payload?.customer_offer_timeout_s) ??
        toNumber(rideSnapshot?.user_timeout) ??
        toNumber(rideSnapshot?.customer_offer_timeout_s) ??
        RIDE_TIMEOUT_S,
      driver_algo: payload.driver_algo ?? "default_algorithm",
      bid_limit: payload.bid_limit ?? 5,
      can_bid_more: payload.can_bid_more ?? true,
      remain_bid: (payload.bid_limit ?? 5) - (payload.user_bid_count ?? 0),

      // ✅ timer fields (SECONDS)
      ...(timer ? timer : {}),
    };
    ridePayload = attachCustomerFields(
      {
        ...ridePayload,
        user_details: bidUserDetails
          ? {
              ...bidUserDetails,
              user_token:
                bidUserDetails?.user_token ??
                bidUserDetails?.token ??
                snapshotUserToken ??
                null,
              token:
                bidUserDetails?.user_token ??
                bidUserDetails?.token ??
                snapshotUserToken ??
                null,
            }
          : ridePayload?.user_details ?? null,
      },
      bidUserDetails ?? ridePayload?.user_details ?? null
    );

    const driverDefaultVehicleType = toTrimmedText(
      pickFirstValue(
        driverDetails?.vehicle_type_name,
        driverDetails?.vehicle_type,
        ridePayload?.vehicle_type_name,
        ridePayload?.vehicle_type,
        null
      )
    );
    const driverDefaultVehicleTypeEn = toTrimmedText(
      pickFirstValue(
        driverDetails?.vehicle_type_name_en,
        rideServiceTypeNameEn,
        null
      )
    );
    const driverDefaultVehicleTypeAr = toTrimmedText(
      pickFirstValue(
        driverDetails?.vehicle_type_name_ar,
        rideServiceTypeNameAr,
        null
      )
    );
    const driverDefaultVehicleCompany = toTrimmedText(
      pickFirstValue(
        driverDetails?.vehicle_company,
        driverDetails?.vehicle_manufacture_name,
        ridePayload?.vehicle_company,
        ridePayload?.vehicle_manufacture_name,
        null
      )
    );
    const driverDefaultVehicleCompanyEn = toTrimmedText(
      pickFirstValue(
        driverDetails?.vehicle_company_en,
        driverDetails?.vehicle_manufacture_name_en,
        ridePayload?.vehicle_company_en,
        ridePayload?.vehicle_manufacture_name_en,
        null
      )
    );
    const driverDefaultVehicleCompanyAr = toTrimmedText(
      pickFirstValue(
        driverDetails?.vehicle_company_ar,
        driverDetails?.vehicle_manufacture_name_ar,
        ridePayload?.vehicle_company_ar,
        ridePayload?.vehicle_manufacture_name_ar,
        null
      )
    );
    const driverDefaultModelName = toTrimmedText(
      pickFirstValue(
        driverDetails?.model_name,
        driverDetails?.vehicle_model_name,
        ridePayload?.model_name,
        ridePayload?.vehicle_model_name,
        null
      )
    );
    const driverDefaultModelNameEn = toTrimmedText(
      pickFirstValue(
        driverDetails?.model_name_en,
        driverDetails?.vehicle_model_name_en,
        ridePayload?.model_name_en,
        ridePayload?.vehicle_model_name_en,
        null
      )
    );
    const driverDefaultModelNameAr = toTrimmedText(
      pickFirstValue(
        driverDetails?.model_name_ar,
        driverDetails?.vehicle_model_name_ar,
        ridePayload?.model_name_ar,
        ridePayload?.vehicle_model_name_ar,
        null
      )
    );
    const driverDefaultVehicleColor = toTrimmedText(
      pickFirstValue(driverDetails?.vehicle_color, ridePayload?.vehicle_color, null)
    );
    const driverDefaultVehicleColorEn = toTrimmedText(
      pickFirstValue(driverDetails?.vehicle_color_en, ridePayload?.vehicle_color_en, null)
    );
    const driverDefaultVehicleColorAr = toTrimmedText(
      pickFirstValue(driverDetails?.vehicle_color_ar, ridePayload?.vehicle_color_ar, null)
    );
    const driverRoomPayload = {
      ...ridePayload,
      ...(driverDefaultVehicleType
        ? {
            vehicle_type: driverDefaultVehicleType,
            vehicle_type_name: driverDefaultVehicleType,
            service_type_name: driverDefaultVehicleType,
          }
        : {}),
      ...(driverDefaultVehicleTypeEn
        ? {
            vehicle_type_name_en: driverDefaultVehicleTypeEn,
            service_type_name_en: driverDefaultVehicleTypeEn,
          }
        : {}),
      ...(driverDefaultVehicleTypeAr
        ? {
            vehicle_type_name_ar: driverDefaultVehicleTypeAr,
            service_type_name_ar: driverDefaultVehicleTypeAr,
          }
        : {}),
      ...(driverDefaultVehicleCompany
        ? {
            vehicle_company: driverDefaultVehicleCompany,
            vehicle_manufacture_name: driverDefaultVehicleCompany,
          }
        : {}),
      ...(driverDefaultVehicleCompanyEn
        ? {
            vehicle_company_en: driverDefaultVehicleCompanyEn,
            vehicle_manufacture_name_en: driverDefaultVehicleCompanyEn,
          }
        : {}),
      ...(driverDefaultVehicleCompanyAr
        ? {
            vehicle_company_ar: driverDefaultVehicleCompanyAr,
            vehicle_manufacture_name_ar: driverDefaultVehicleCompanyAr,
          }
        : {}),
      ...(driverDefaultModelName
        ? {
            model_name: driverDefaultModelName,
            vehicle_model_name: driverDefaultModelName,
          }
        : {}),
      ...(driverDefaultModelNameEn
        ? {
            model_name_en: driverDefaultModelNameEn,
            vehicle_model_name_en: driverDefaultModelNameEn,
          }
        : {}),
      ...(driverDefaultModelNameAr
        ? {
            model_name_ar: driverDefaultModelNameAr,
            vehicle_model_name_ar: driverDefaultModelNameAr,
          }
        : {}),
      ...(driverDefaultVehicleColor ? { vehicle_color: driverDefaultVehicleColor } : {}),
      ...(driverDefaultVehicleColorEn
        ? { vehicle_color_en: driverDefaultVehicleColorEn }
        : {}),
      ...(driverDefaultVehicleColorAr
        ? { vehicle_color_ar: driverDefaultVehicleColorAr }
        : {}),
    };

    io.to(driverRoom(driverId)).emit("ride:newBid", driverRoomPayload);
    console.log("[emit][driver][ride:newBid]", {
      ride_id: rideId,
      driver_id: driverId,
      room: driverRoom(driverId),
      vehicle_company: driverRoomPayload?.vehicle_company ?? null,
      plat_no: driverRoomPayload?.plat_no ?? null,
      model_year: driverRoomPayload?.model_year ?? null,
      model_name: driverRoomPayload?.model_name ?? null,
      vehicle_color: driverRoomPayload?.vehicle_color ?? null,
      driver_name: driverRoomPayload?.driver_name ?? null,
      driver_rating: driverRoomPayload?.driver_rating ?? null,
      driver_image: driverRoomPayload?.driver_image ?? null,
      additional_remarks:
        resolveAdditionalRemarks(driverRoomPayload),
      at: Date.now(),
    });

    emitToRideAudience(
      io,
      rideId,
      "ride:newBid",
      ridePayload,
      ridePayload?.user_id ?? ridePayload?.user_details?.user_id ?? null
    );

    console.log(`💰 Driver ${driverId} submitted bid ${offeredPrice} for ride ${rideId}`);
    const removed = inboxRemove(driverId, rideId);
if (removed) {
  emitDriverPatch(io, driverId, [{ op: "remove", ride_id: rideId }]);
  console.log(`🧹 [inbox] removed ride ${rideId} from driver ${driverId} after submitBid`);
}

  //  const removed = inboxRemove(driverId,  rideId);
  //   if (removed) {
  //     removeDriverFromRideCandidates(io, rideId, driverId, { emitSummary: false });
  //     emitDriverPatch(io, driverId, [{ op: "remove", ride_id: rideId }]);
  //     emitRideCandidatesSummary(io, rideId);
  //     console.log(`🧹 [inbox] removed ride ${rideId} from driver ${driverId} after submitBid`);
  //   }
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
    const payloadToken = payload?.token ?? payload?.access_token ?? payload?.user_token ?? null;
    const fallbackUserId =
      toNumber(payload?.user_id) ?? toNumber(snapshot?.user_id) ?? toNumber(getUserIdForRide(rideId));

    const ridePriceBounds = getRidePriceBounds(snapshot ?? {});
    if (!isPriceWithinBounds(newPrice, ridePriceBounds)) {
      const validationPayload = {
        ride_id: rideId,
        attempted_price: newPrice,
        min_price: ridePriceBounds.min_price,
        max_price: ridePriceBounds.max_price,
        actor: "user",
        message: "User price is outside allowed range",
        at: Date.now(),
      };
      console.log("[bid][user:respondToDriver] rejected by bounds", {
        ride_id: rideId,
        user_id: fallbackUserId ?? null,
        attempted_price: newPrice,
        min_price: ridePriceBounds.min_price,
        max_price: ridePriceBounds.max_price,
        candidates_count: candidateDrivers.length,
      });
      emitToRideAudience(
        io,
        rideId,
        "ride:priceValidationError",
        validationPayload,
        fallbackUserId ?? null
      );
      return;
    }

    console.log("[bid][user:respondToDriver] accepted", {
      ride_id: rideId,
      user_id: fallbackUserId ?? null,
      new_price: newPrice,
      min_price: ridePriceBounds.min_price,
      max_price: ridePriceBounds.max_price,
      candidates_count: candidateDrivers.length,
    });

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
              snapshotDetails?.user_token ??
              snapshotDetails?.token ??
              snapshot?.token ??
              payloadToken ??
              null,
          },
          snapshotDetails
        )
      : null;

    // ✅ TIMER refresh: every user response resets timer (90s seconds)
    const timer = refreshRideTimerWithDispatchTimeout(io, rideId, {
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
              mergedUD?.user_token ??
              mergedUD?.token ??
              baseRide?.token ??
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
        base_fare: ridePriceBounds.base_fare ?? null,
        estimated_price: ridePriceBounds.base_fare ?? null,
        estimated_fare: ridePriceBounds.base_fare ?? null,
        min_price: ridePriceBounds.min_price ?? null,
        max_price: ridePriceBounds.max_price ?? null,
        min_fare: ridePriceBounds.min_price ?? null,
        max_fare: ridePriceBounds.max_price ?? null,

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
  base_fare: ridePriceBounds.base_fare ?? snapshotBase?.base_fare ?? null,
  estimated_price: ridePriceBounds.base_fare ?? snapshotBase?.estimated_price ?? null,
  estimated_fare: ridePriceBounds.base_fare ?? snapshotBase?.estimated_fare ?? null,
  min_price: ridePriceBounds.min_price ?? snapshotBase?.min_price ?? null,
  max_price: ridePriceBounds.max_price ?? snapshotBase?.max_price ?? null,
  isPriceUpdated: true,
  updatedPrice: newPrice,
  updatedAt: Date.now(),

  dispatch_incremental_only: 1,
  dispatch_expand_reason: "user_response",
  // Re-open the updated offer for all current candidates, not only newly added drivers.
  force_rebroadcast: 1,

  server_time: timer?.server_time ?? null,
  expires_at: timer?.expires_at ?? null,
  timeout_ms: timer?.timeout_ms ?? null,
};
      void dispatchToNearbyDrivers(io, redispatchData);
    } else {
      console.log(`⚠️ redispatch skipped: no ride snapshot for ride ${rideId}`);
    }

    emitToRideAudience(
      io,
      rideId,
      "ride:priceUpdated",
      {
        ride_id: rideId,
        user_bid_price: newPrice,
        base_fare: ridePriceBounds.base_fare ?? null,
        estimated_price: ridePriceBounds.base_fare ?? null,
        estimated_fare: ridePriceBounds.base_fare ?? null,
        min_price: ridePriceBounds.min_price ?? null,
        max_price: ridePriceBounds.max_price ?? null,
        min_fare: ridePriceBounds.min_price ?? null,
        max_fare: ridePriceBounds.max_price ?? null,

        // ✅ timer fields (SECONDS)
        ...(timer ? timer : {}),
        at: Date.now(),
      },
      fallbackUserId ?? snapshotBase?.user_id ?? null
    );

    console.log(
      `✅ user response(broadcast) -> ride ${rideId} newPrice ${newPrice} sent to ${candidateDrivers.length} drivers`
    );
  });

  socket.on("user:acceptOffer", async (payload) => {
    let driverAcceptLockAcquired = false;

    console.log("[user:acceptOffer] incoming payload:", payload, "socket:", socket.id);
    debugLog("user:acceptOffer", payload, socket.id);

    const rideId = toNumber(payload?.ride_id);
    const requestedDriverId = toNumber(payload?.driver_id);
    let driverIdentity = resolveRideDriverIdentity(rideId, payload);
    let driverId = toNumber(driverIdentity?.provider_id);

    if (!rideId || (!requestedDriverId && !driverId)) {
      console.log("⚠️ user:acceptOffer ignored: missing ride_id/driver_id");
      return;
    }

    const initialLookupDriverId = driverId ?? requestedDriverId;
    const rideSnapshot = getFullRideSnapshot(rideId, initialLookupDriverId);
    const rideDetails = getRideDetails(rideId);

    driverIdentity = resolveRideDriverIdentity(rideId, payload, {
      meta: initialLookupDriverId
        ? driverLocationService.getMeta(initialLookupDriverId) ?? null
        : null,
      snapshot: rideSnapshot,
      rideDetails,
    });

    driverId = toNumber(driverIdentity?.provider_id) ?? initialLookupDriverId;
    const customerFacingDriverId =
      toNumber(driverIdentity?.driver_detail_id) ?? requestedDriverId ?? driverId;

    if (!driverId) {
      console.log("⚠️ user:acceptOffer ignored: unable to resolve provider_id from payload");
      return;
    }

    if (isDriverAcceptLocked(driverId)) {
      console.log(
        `⚠️ user:acceptOffer ignored: driver ${driverId} accept flow already in progress`
      );

      const lockedUserId =
        toNumber(payload?.user_id) ??
        toNumber(socket.userId) ??
        toNumber(rideOwnerByRide.get(rideId)) ??
        toNumber(rideDetails?.user_id) ??
        null;

      const lockedPayload = {
        success: false,
        status: USER_ACCEPT_OFFER_STATUS.ACCEPT_FAILED,
        ride_id: rideId,
        driver_id: customerFacingDriverId ?? driverId,
        message: "يوجد طلب قبول آخر قيد المعالجة لهذا السائق",
        reason: "driver_accept_in_progress",
        details: null,
      };

      emitUserAcceptOfferResult(io, lockedUserId, lockedPayload, socket);

      socket.emit("ride:acceptOfferFailed", {
        ...lockedPayload,
        at: Date.now(),
      });

      return;
    }

    driverAcceptLockAcquired = true;

    try {

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

    //const driverCurrentActiveRide = getActiveRideByDriver(driverId);
    // if (
    //   driverCurrentActiveRide &&
    //   driverCurrentActiveRide !== rideId &&
    //   !canDriverSubmitBidForRide(driverId, rideId)
    // ) {
    //   console.log(
    //     `⚠️ user:acceptOffer ignored: driver ${driverId} is not eligible to accept queued ride ${rideId}`
    //   );

    //   const acceptNotEligiblePayload = {
    //     success: false,
    //     status: USER_ACCEPT_OFFER_STATUS.DRIVER_NOT_ELIGIBLE_FOR_QUEUED_RIDE,
    //     ride_id: rideId,
    //     driver_id: customerFacingDriverId ?? driverId,
    //     message: "السائق غير مؤهل حاليًا لقبول هذه الرحلة في الكيو",
    //     reason: "driver_not_eligible_for_queued_ride",
    //     details: {
    //       active_ride_id: driverCurrentActiveRide,
    //     },
    //   };

    //   emitUserAcceptOfferResult(io, userId, acceptNotEligiblePayload);

    //   socket.emit("ride:acceptOfferFailed", {
    //     ...acceptNotEligiblePayload,
    //     at: Date.now(),
    //   });

    //   return;
    // }

const toValidUserId = (value) => {
  const n = toNumber(value);
  return n && n > 0 ? n : null;
};

const payloadUserId = toValidUserId(payload?.user_id);
const payloadToken = normalizeToken(
  payload?.access_token ?? payload?.token ?? payload?.user_token ?? null
);
if (
  isUnsafeNumericToken(payload?.access_token) ||
  isUnsafeNumericToken(payload?.token) ||
  isUnsafeNumericToken(payload?.user_token)
) {
  console.warn("[user:acceptOffer] payload token sent as unsafe JS number; send token as string", {
    ride_id: rideId,
    user_id: payloadUserId ?? null,
  });
}
const userFromPayloadToken = payloadToken ? getUserDetailsByToken(payloadToken) : null;
const payloadTokenUserId = toValidUserId(userFromPayloadToken?.user_id ?? null);

const rideOwnerUserId =
  toValidUserId(getUserIdForRide(rideId)) ??
  toValidUserId(rideSnapshot?.user_id ?? rideSnapshot?.user_details?.user_id ?? null) ??
  toValidUserId(rideDetails?.user_id ?? rideDetails?.user_details?.user_id ?? null) ??
  toValidUserId(socket.userId) ??
  toValidUserId(userFromPayloadToken?.user_id) ??
  payloadUserId;

if (payloadUserId && rideOwnerUserId && payloadUserId !== rideOwnerUserId) {
  console.warn("[user:acceptOffer] payload user mismatch ride owner (non-blocking)", {
    ride_id: rideId,
    payload_user_id: payloadUserId,
    ride_owner_user_id: rideOwnerUserId,
  });
}

const rideSnapshotToken =
  rideSnapshot?.user_details?.user_token ??
  rideSnapshot?.user_details?.token ??
  rideSnapshot?.token ??
  rideDetails?.user_details?.user_token ??
  rideDetails?.user_details?.token ??
  rideDetails?.token ??
  null;

const storedOwnerUser = rideOwnerUserId ? getUserDetails(rideOwnerUserId) : null;
const storedOwnerToken = normalizeToken(
  storedOwnerUser?.user_token ??
  storedOwnerUser?.token ??
  storedOwnerUser?.access_token ??
  null
);

const socketUserId = toValidUserId(socket.userId) ?? null;
const socketUser = socketUserId ? getUserDetails(socketUserId) : null;
const socketUserToken = normalizeToken(
  socket?.userToken ??
    socketUser?.user_token ??
    socketUser?.token ??
    socketUser?.access_token ??
    null
);
const liveOwnerRoomToken = normalizeToken(getLiveUserTokenFromRoom(io, rideOwnerUserId));
const preferredOwnerToken = normalizeToken(
  storedOwnerToken ?? liveOwnerRoomToken ?? rideSnapshotToken ?? null
);

const hasPayloadOwnerMismatch =
  !!(payloadUserId && rideOwnerUserId && payloadUserId !== rideOwnerUserId);
const hasTokenOwnerMismatch =
  !!(payloadTokenUserId && rideOwnerUserId && payloadTokenUserId !== rideOwnerUserId);

let userId = payloadUserId ?? payloadTokenUserId ?? socketUserId ?? rideOwnerUserId;
let tokenTmp = normalizeToken(
  preferredOwnerToken ?? payloadToken ?? socketUserToken ?? null
);

if (rideOwnerUserId && (hasPayloadOwnerMismatch || hasTokenOwnerMismatch)) {
  // Payload can carry stale identity from a previous login/session on frontend.
  // For accepting a ride, prefer the ride owner identity to avoid auth mismatch.
  userId = rideOwnerUserId;
  tokenTmp = normalizeToken(preferredOwnerToken ?? socketUserToken ?? null);

  console.warn("[user:acceptOffer] mismatch detected, using ride owner auth", {
    ride_id: rideId,
    payload_user_id: payloadUserId,
    payload_token_user_id: payloadTokenUserId,
    socket_user_id: socketUserId,
    ride_owner_user_id: rideOwnerUserId,
    has_owner_token: !!storedOwnerToken,
    has_room_owner_token: !!liveOwnerRoomToken,
  });
} else if (
  payloadTokenUserId &&
  payloadUserId &&
  payloadTokenUserId !== payloadUserId
) {
  // If payload user id and payload token map to different users, trust token mapping.
  userId = payloadTokenUserId;
  const tokenMappedUser = getUserDetails(payloadTokenUserId);
  tokenTmp = normalizeToken(
    payloadToken ??
      tokenMappedUser?.user_token ??
      tokenMappedUser?.token ??
      tokenMappedUser?.access_token ??
      tokenTmp
  );
  console.warn("[user:acceptOffer] payload user/token mismatch, using token user", {
    ride_id: rideId,
    payload_user_id: payloadUserId,
    payload_token_user_id: payloadTokenUserId,
  });
} else if (rideOwnerUserId && userId === rideOwnerUserId) {
  tokenTmp = normalizeToken(preferredOwnerToken ?? socketUserToken ?? tokenTmp);
}

const driverCurrentActiveRide = getActiveRideByDriver(driverId);
if (
  driverCurrentActiveRide &&
  driverCurrentActiveRide !== rideId &&
  !canDriverSubmitBidForRide(driverId, rideId)
) {
  console.log(
    `⚠️ user:acceptOffer ignored: driver ${driverId} is not eligible to accept queued ride ${rideId}`
  );

  const acceptNotEligiblePayload = {
    success: false,
    status: USER_ACCEPT_OFFER_STATUS.DRIVER_NOT_ELIGIBLE_FOR_QUEUED_RIDE,
    ride_id: rideId,
    driver_id: customerFacingDriverId ?? driverId,
    message: "السائق غير مؤهل حاليًا لقبول هذه الرحلة في الكيو",
    reason: "driver_not_eligible_for_queued_ride",
    details: {
      active_ride_id: driverCurrentActiveRide,
    },
  };

  emitUserAcceptOfferResult(io, userId, acceptNotEligiblePayload, socket);

  socket.emit("ride:acceptOfferFailed", {
    ...acceptNotEligiblePayload,
    at: Date.now(),
  });

  return;
}

const accessToken = tokenTmp;
    const acceptedRouteKmForApi = getAcceptedRouteKm(payload, rideSnapshot ?? rideDetails);
    const acceptedEtaMinForApi = getAcceptedEtaMin(payload, rideSnapshot ?? rideDetails);
    

    // if (!userId || !accessToken) {
    //   console.log(`⚠️ user:acceptOffer API skipped: missing user_id/access_token (ride ${rideId})`);
    // } else {
      // const acceptPayload = {
      //   user_id: userId,
      //   access_token: accessToken,
      //   ...buildDriverIdentityPayload(driverIdentity, customerFacingDriverId),
      //   ride_id: rideId,
      //   offered_price: finalPrice,
      //   ...(acceptedRouteKmForApi !== null
      //     ? {
      //         route: acceptedRouteKmForApi,
      //         total_distance: acceptedRouteKmForApi,
      //       }
      //     : {}),
      //   ...(acceptedEtaMinForApi !== null
      //     ? {
      //         eta: acceptedEtaMinForApi,
      //         estimated_time: acceptedEtaMinForApi,
      //       }
      //     : {}),
      // };

      // axios
      //   .post(`${LARAVEL_BASE_URL}${LARAVEL_ACCEPT_BID_PATH}`, acceptPayload, {
      //     timeout: LARAVEL_ACCEPT_BID_TIMEOUT_MS,
      //   })
      //   .then((response) => {
      //     console.log("API Response: Accept Bid", response.data);
      //   })
      //   .catch((error) => {
      //     console.error("Error while calling accept bid API:", error?.response?.data || error.message);
      //   });

          let acceptApiOk = false;
    let acceptApiResponse = null;
    let acceptApiError = null;

    const callAcceptApi = async (candidateToken, attempt = "primary") => {
      const acceptPayload = {
        user_id: userId,
        access_token: candidateToken,
        ...buildDriverIdentityPayload(driverIdentity, customerFacingDriverId),
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

      try {
        const response = await axios.post(
          `${LARAVEL_BASE_URL}${LARAVEL_ACCEPT_BID_PATH}`,
          acceptPayload,
          {
            timeout: LARAVEL_ACCEPT_BID_TIMEOUT_MS,
          }
        );

        const raw = response?.data ?? null;
        let parsed = raw;
        if (typeof parsed === "string") {
          try {
            parsed = JSON.parse(parsed);
          } catch (_) {}
        }

        const ok =
          parsed?.status === 1 ||
          parsed?.success === true ||
          parsed?.result === true;

        console.log(`[user:acceptOffer] accept API response (${attempt}):`, parsed);

        if (ok) {
          return {
            ok: true,
            parsed,
            response: raw,
            error: null,
          };
        }

        return {
          ok: false,
          parsed,
          response: raw,
          error: {
            message:
              parsed?.message ||
              parsed?.error ||
              "Accept bid API returned unsuccessful response",
            reason: "accept_api_rejected",
            details: parsed,
          },
        };
      } catch (error) {
        const details = error?.response?.data || null;
        console.error(
          `[user:acceptOffer] Error while calling accept bid API (${attempt}):`,
          details || error.message
        );
        return {
          ok: false,
          parsed: details,
          response: details,
          error: {
            message:
              details?.message ||
              details?.error ||
              error?.message ||
              "Accept bid API request failed",
            reason: "accept_api_failed",
            details,
          },
        };
      }
    };

    if (!userId || !accessToken) {
      const missingUserId = !userId;
      const missingAccessToken = !accessToken;
      acceptApiError = {
        message: "Missing user_id or access_token",
        reason: "missing_auth_data",
        details: {
          missing_user_id: missingUserId,
          missing_access_token: missingAccessToken,
          resolved_user_id: userId ?? null,
          has_payload_user_id: !!payloadUserId,
          has_payload_token: !!payloadToken,
          has_socket_user_id: !!socketUserId,
          has_socket_user_token: !!socketUserToken,
          has_ride_owner_user_id: !!rideOwnerUserId,
          has_stored_owner_token: !!storedOwnerToken,
          has_live_owner_room_token: !!liveOwnerRoomToken,
          has_snapshot_token: !!rideSnapshotToken,
        },
      };
      console.warn("[user:acceptOffer] blocked: missing user_id/access_token", {
        ride_id: rideId,
        missing_user_id: missingUserId,
        missing_access_token: missingAccessToken,
        resolved_user_id: userId ?? null,
        has_payload_user_id: !!payloadUserId,
        has_payload_token: !!payloadToken,
        has_socket_user_id: !!socketUserId,
        has_socket_user_token: !!socketUserToken,
        has_ride_owner_user_id: !!rideOwnerUserId,
        has_stored_owner_token: !!storedOwnerToken,
        has_live_owner_room_token: !!liveOwnerRoomToken,
        has_snapshot_token: !!rideSnapshotToken,
      });
    } else {
      const primaryResult = await callAcceptApi(accessToken, "primary");
      acceptApiOk = primaryResult.ok;
      acceptApiResponse = primaryResult.response;
      acceptApiError = primaryResult.ok ? null : primaryResult.error;

      const payloadTokenFallback = normalizeToken(payloadToken);
      const primaryMessageCode = toNumber(primaryResult?.parsed?.message_code);
      const primaryMessage = String(
        primaryResult?.parsed?.message ?? primaryResult?.error?.message ?? ""
      );
      const isSessionExpired =
        !acceptApiOk &&
        (primaryMessageCode === 4 ||
          /session\s*expired|login\s*session\s*expired/i.test(primaryMessage));
      const isTokenAuthFailure =
        !acceptApiOk &&
        /invalid\s*token|expired\s*token|unauthori(?:z|s)ed|access[_\s-]*token/i.test(
          primaryMessage
        );

      if (
        (isSessionExpired || isTokenAuthFailure) &&
        payloadTokenFallback &&
        payloadTokenFallback !== accessToken
      ) {
        console.warn(
          "[user:acceptOffer] primary token rejected, retrying once with payload token",
          {
            ride_id: rideId,
            user_id: userId,
            has_payload_token: true,
          }
        );

        const retryResult = await callAcceptApi(
          payloadTokenFallback,
          "payload-token-retry"
        );

        acceptApiOk = retryResult.ok;
        acceptApiResponse = retryResult.response;
        acceptApiError = retryResult.ok ? null : retryResult.error;

        if (retryResult.ok && userId) {
          socket.userToken = payloadTokenFallback;
          setUserDetails(userId, {
            user_id: userId,
            user_token: payloadTokenFallback,
            token: payloadTokenFallback,
            access_token: payloadTokenFallback,
          });
        }
      }
    }

      if (!acceptApiOk) {
      const failureStatus =
        acceptApiError?.reason === "missing_auth_data"
          ? USER_ACCEPT_OFFER_STATUS.MISSING_AUTH_DATA
          : acceptApiError?.reason === "accept_api_rejected"
          ? USER_ACCEPT_OFFER_STATUS.ACCEPT_API_REJECTED
          : acceptApiError?.reason === "accept_api_failed"
          ? USER_ACCEPT_OFFER_STATUS.ACCEPT_API_FAILED
          : USER_ACCEPT_OFFER_STATUS.ACCEPT_FAILED;

      const acceptFailurePayload = {
        success: false,
        status: failureStatus,
        ride_id: rideId,
        driver_id: customerFacingDriverId ?? driverId,
        message: acceptApiError?.message || "تعذر بدء الرحلة",
        reason: acceptApiError?.reason || "accept_failed",
        details: acceptApiError?.details ?? null,
      };

      emitUserAcceptOfferResult(io, userId, acceptFailurePayload, socket);

      socket.emit("ride:acceptOfferFailed", {
        ...acceptFailurePayload,
        at: Date.now(),
      });

      return;
    }

    emitUserAcceptOfferResult(io, userId, {
      success: true,
      status: USER_ACCEPT_OFFER_STATUS.SUCCESS,
      ride_id: rideId,
      driver_id: customerFacingDriverId ?? driverId,
      message: "تم قبول العرض وبدء الرحلة بنجاح",
      reason: null,
      details: acceptApiResponse,
    }, socket);
    
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

const acceptedRouteDataDuration = toNumber(
  pickFirstValue(
    payload?.driver_to_pickup_distance_m,
    payload?.meta?.driver_to_pickup_distance_m,
    acceptedRideSnapshot?.driver_to_pickup_distance_m,
    acceptedRideSnapshot?.meta?.driver_to_pickup_distance_m,
    getAcceptedDriverToPickupMinutes(payload, acceptedRideSnapshot)
  )
);

if (acceptedRouteDataDuration !== null) {
  console.log("[user:acceptOffer][routeData-source=accepted-offer]", {
    ride_id: rideId,
    driver_id: driverId,
    duration: acceptedRouteDataDuration,
  });
  emitRouteDataFromAcceptedOffer(io, rideId, driverId, acceptedRouteDataDuration);
} else {
      console.log("[user:acceptOffer][routeData-source=none] missing duration in frontend payload", {
        ride_id: rideId,
        driver_id: driverId,
      });
    }

        const removedAcceptedDriverRide = inboxRemove(driverId, rideId);
    if (removedAcceptedDriverRide) {
      clearDriverBidStatus(driverId, rideId);
      removeDriverFromRideCandidates(io, rideId, driverId, { emitSummary: false });
      emitDriverPatch(io, driverId, [{ op: "remove", ride_id: rideId }]);
      emitRideCandidatesSummary(io, rideId);
    }

    markRideDriverState(rideId, driverId, "accepted", {
      last_offered_price: finalPrice,
      ...buildDriverIdentityPayload(driverIdentity, driverId),
    });

    finalizeAcceptedRide(io, rideId, driverId, finalPrice, {
      message: "User accepted the offer",
      rideDetails: acceptedRideSnapshot,
      userId,
      driverIdentity,
    });

    console.log(`✅ user:acceptOffer -> ride ${rideId} driver ${driverId} price ${finalPrice}`);
        } finally {
      if (driverAcceptLockAcquired) {
        releaseDriverAcceptLock(driverId);
      }
    }
  });

 socket.on("ride:close", ({ ride_id }) => {
  const rideId = toNumber(ride_id);
  if (!rideId) return;

  const driverId = getActiveDriverByRide(rideId);
  console.log("[ride:close] incoming", { rideId, driverId });

  removeRideFromAllInboxes(io, rideId);
  clearActiveRideByRideId(rideId);
  io.to(rideRoom(rideId)).emit("ride:closed", { ride_id: rideId });

  if (driverId) {
    const activated = activateQueuedRideForDriver(io, driverId);
    console.log("[ride:close] activateQueuedRideForDriver result", {
      rideId,
      driverId,
      activated,
      queued: getDriverQueuedRide(driverId),
    });
  }

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

    const rideSnapshot =
      driverRideInbox.get(driverId)?.get(rideId) ??
      getRideDetails(rideId) ??
      null;
    const driverMeta = driverLocationService.getMeta(driverId) || {};
    const serviceCategoryId = toNumber(
      payload?.service_category_id ??
        rideSnapshot?.service_category_id ??
        rideSnapshot?.meta?.service_category_id ??
        null
    );
    const driverServiceId =
      toNumber(payload?.driver_service_id) ??
      toNumber(socket.driverServiceId) ??
      toNumber(driverMeta?.driver_service_id);
    const accessToken =
      payload?.access_token ??
      socket.driverAccessToken ??
      driverMeta?.access_token ??
      null;

    const activeDriverId = getActiveDriverByRide(rideId);
    if (activeDriverId) {
      console.log(
        `⚠️ driver:declineRide ignored: ride ${rideId} already accepted by driver ${activeDriverId}`
      );
      emitRideUnavailable(io, driverId, rideId);
      return;
    }

    if (cancelledRides.has(rideId)) {
      console.log(`⚠️ driver:declineRide ignored: ride ${rideId} is cancelled`);
      emitRideUnavailable(io, driverId, rideId);
      return;
    }

    removeDriverFromRideCandidates(io, rideId, driverId, { emitSummary: false });
    const existed = inboxRemove(driverId, rideId);
    clearDriverBidStatus(driverId, rideId);
    markRideDriverState(rideId, driverId, "declined");

    if (existed) {
      emitDriverPatch(io, driverId, [{ op: "remove", ride_id: rideId }]);
    }

    io.to(driverRoom(driverId)).emit("ride:declined", {
      ride_id: rideId,
      driver_id: driverId,
      at: Date.now(),
    });

    emitRideCandidatesSummary(io, rideId);
    void syncDriverRejectNotification({
      driverId,
      rideId,
      serviceCategoryId,
      accessToken,
      driverServiceId,
    });

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
module.exports.restartRideDispatch = restartRideDispatch;
module.exports.closeRideBidding = closeRideBidding;
module.exports.refreshUserDetailsForUserId = refreshUserDetailsForUserId;
module.exports.saveRideDetails = saveRideDetails;
module.exports.getRideDetails = getRideDetails;
module.exports.getUserIdForRide = getUserIdForRide;
module.exports.getActiveRideIdForUser = getActiveRideIdForUser;
module.exports.touchUserActiveRide = touchUserActiveRide;
module.exports.finalizeAcceptedRide = finalizeAcceptedRide;
module.exports.markRideCancelled = markRideCancelled;
module.exports.removeRideFromAllInboxes = removeRideFromAllInboxes;
module.exports.upsertRideRouteMetrics = upsertRideRouteMetrics;
module.exports.emitCandidatesSummaryForDriverStateChange = emitCandidatesSummaryForDriverStateChange;
module.exports.canDriverReceiveNewRideRequests = canDriverReceiveNewRideRequests;
module.exports.activateQueuedRideForDriver = activateQueuedRideForDriver;
module.exports.getDriverInboxStats = getDriverInboxStats;
module.exports.recoverDriverPendingDispatch = recoverDriverPendingDispatch;
