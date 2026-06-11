// sockets/user.socket.js
const driverLocationService = require("../services/driverLocation.service");
const axios = require("axios");
const { getDistanceMeters } = require("../utils/geo.util");
const routeCacheL2 = require("../services/routeCacheL2.service");
const {
  setUserDetails,
  getUserDetailsByToken,
  deleteUserDetails,
} = require("../store/users.store");
const { getRideStatusSnapshot } = require("../store/rideStatusSnapshots.store");
const {
  savePricingSnapshot,
  getPricingSnapshot,
} = require("../store/pricingSnapshots.store");

// ? UPDATED: add getActiveRideByDriver so we can exclude busy drivers from nearby/types
const { getActiveDriverByRide, getActiveRideByDriver } = require("../store/activeRides.store");

const biddingSocket = require("./bidding.socket");
const { normalizePublicAssetUrl: normalizeAssetUrl } = require("../utils/imageUrl.util");

const DEBUG_EVENTS = process.env.DEBUG_SOCKET_EVENTS === "1";
const debugLog = (event, payload, socketId) => {
  if (!DEBUG_EVENTS) return;
  console.log("[user.socket]", event, "socket:", socketId, "payload:", payload);
};
const VERBOSE_NEARBY_LOGS = process.env.VERBOSE_NEARBY_LOGS === "1";
const LOG_ROUTE_FAILURES = String(process.env.LOG_ROUTE_FAILURES || "1") === "1";
const ROUTE_FAILURE_LOG_THROTTLE_MS = Number.isFinite(
  Number(process.env.ROUTE_FAILURE_LOG_THROTTLE_MS)
)
  ? Math.max(1000, Number(process.env.ROUTE_FAILURE_LOG_THROTTLE_MS))
  : 60_000;
const throttledWarnAt = new Map();
const nearbyLog = (...args) => {
  if (!VERBOSE_NEARBY_LOGS) return;
  console.log(...args);
};
const warnThrottled = (key, ...args) => {
  if (!LOG_ROUTE_FAILURES) return;
  const now = Date.now();
  const last = throttledWarnAt.get(key) ?? 0;
  if (now - last < ROUTE_FAILURE_LOG_THROTTLE_MS) return;
  throttledWarnAt.set(key, now);
  console.warn(...args);
};
const userRoom = (userId) => `user:${userId}`;

const DEFAULT_NEARBY_RADIUS_METERS = 5000;
const NEARBY_DISPATCH_RADIUS_STEPS_KM = Object.freeze([1, 2, 3, 5, 7, 10, 15, 20]);
const NEARBY_EVERY_MS = Number.isFinite(Number(process.env.NEARBY_EVERY_MS))
  ? Math.max(1000, Number(process.env.NEARBY_EVERY_MS))
  :10000;
const MAX_DRIVER_LOCATION_AGE_MS = 2 * 60 * 1000;
const NEARBY_CENTER_RESET_THRESHOLD_M = Number.isFinite(
  Number(process.env.NEARBY_CENTER_RESET_THRESHOLD_M)
)
  ? Math.max(0, Number(process.env.NEARBY_CENTER_RESET_THRESHOLD_M))
  : 75;
const KNOWN_SERVICE_CATEGORY_IDS = new Set([1, 2, 3, 4, 5, 6, 7, 31, 32]);

// ? NEW: road-based filtering
const DEFAULT_AIR_CANDIDATE_RADIUS_METERS = 8000; // ?????? ?????? ???? ????? ??? ????? ??????
const MAX_ROAD_FILTER_CANDIDATES = Number.isFinite(Number(process.env.MAX_ROAD_FILTER_CANDIDATES))
  ? Math.max(1, Number(process.env.MAX_ROAD_FILTER_CANDIDATES))
  : 25;
const SERVICE_RADIUS_CACHE_TTL_MS = Number.isFinite(
  Number(process.env.SERVICE_RADIUS_CACHE_TTL_MS)
)
  ? Math.max(30 * 1000, Number(process.env.SERVICE_RADIUS_CACHE_TTL_MS))
  : 5 * 60 * 1000;
const NEARBY_FARE_VERSION_CACHE_TTL_MS = Number.isFinite(
  Number(process.env.NEARBY_FARE_VERSION_CACHE_TTL_MS)
)
  ? Math.max(0, Number(process.env.NEARBY_FARE_VERSION_CACHE_TTL_MS))
  : 15000;
const NEARBY_FARE_CACHE_TTL_MS = Number.isFinite(
  Number(process.env.NEARBY_FARE_CACHE_TTL_MS)
)
  ? Math.max(0, Number(process.env.NEARBY_FARE_CACHE_TTL_MS))
  : 15000;
const NEARBY_CANDIDATE_QUERY_CACHE_TTL_MS = Number.isFinite(
  Number(process.env.NEARBY_CANDIDATE_QUERY_CACHE_TTL_MS)
)
  ? Math.max(0, Number(process.env.NEARBY_CANDIDATE_QUERY_CACHE_TTL_MS))
  : 1500;
const serviceSearchRadiusCache = new Map();
const serviceSearchRadiusInFlight = new Map();
const nearbyFareVersionCache = new Map();
const nearbyFareVersionInFlight = new Map();
const nearbyVehicleFaresInFlight = new Map();

const LARAVEL_BASE_URL =
  process.env.LARAVEL_BASE_URL ||
  process.env.LARAVEL_URL ||
  "https://api.catch-syria.com";

const LARAVEL_SERVICE_SEARCH_RADIUS_PATH =
  process.env.LARAVEL_SERVICE_SEARCH_RADIUS_PATH ||
  "/api/customer/transport/search-radius";
const LARAVEL_VEHICLE_FARE_VERSION_PATH =
  process.env.LARAVEL_VEHICLE_FARE_VERSION_PATH ||
  "/api/customer/transport/vehicle-fares-version";
const LARAVEL_GET_ROUTE_PATH =
  process.env.LARAVEL_GET_ROUTE_PATH || "/api/getRoute";

const LARAVEL_TIMEOUT_MS = Number.isFinite(Number(process.env.LARAVEL_TIMEOUT_MS))
  ? Math.max(1000, Number(process.env.LARAVEL_TIMEOUT_MS))
  : 7000;
const LARAVEL_ROUTE_TIMEOUT_MS = Number.isFinite(Number(process.env.LARAVEL_ROUTE_TIMEOUT_MS))
  ? Math.max(1000, Number(process.env.LARAVEL_ROUTE_TIMEOUT_MS))
  : LARAVEL_TIMEOUT_MS;
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
const ROUTE_LOCK_RECHECK_MIN_MS = 100;
const ROUTE_LOCK_RECHECK_MAX_MS = 200;
const routeMetricsCache = new Map();
const routeMetricsInFlight = new Map();
const VEHICLE_ICON_RELATIVE_DIR = "assets/images/service-category/transport-service-type";
const DRIVER_IMAGE_RELATIVE_DIR = "assets/images/profile-images/provider";
const CUSTOMER_IMAGE_RELATIVE_DIR = "assets/images/profile-images/customer";
const VERBOSE_IMAGE_SOURCE_LOGS = process.env.VERBOSE_IMAGE_SOURCE_LOGS === "1";
const lastUserImageSignatureByUserId = new Map();

// ? NEW: keep timer unit aligned with bidding.socket (SECONDS)
const RIDE_TIMEOUT_S = 90;
const NEARBY_DISPATCH_TIMEOUT_S = Number.isFinite(
  Number(process.env.NEARBY_DISPATCH_TIMEOUT_S)
)
  ? Math.max(1, Math.floor(Number(process.env.NEARBY_DISPATCH_TIMEOUT_S)))
  : 5;
const BID_MIN_PRICE_MULTIPLIER = Number.isFinite(
  Number(process.env.BID_MIN_PRICE_MULTIPLIER)
)
  ? Math.max(0.01, Number(process.env.BID_MIN_PRICE_MULTIPLIER))
  : 0.75;
const BID_MAX_PRICE_MULTIPLIER = Number.isFinite(
  Number(process.env.BID_MAX_PRICE_MULTIPLIER)
)
  ? Math.max(0.01, Number(process.env.BID_MAX_PRICE_MULTIPLIER))
  : 2.0;

const toNumber = (v) => {
  if (v === null || v === undefined || v === "") return null;
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
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
const getRandomLockRecheckDelayMs = () =>
  ROUTE_LOCK_RECHECK_MIN_MS +
  Math.floor(Math.random() * (ROUTE_LOCK_RECHECK_MAX_MS - ROUTE_LOCK_RECHECK_MIN_MS + 1));
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
const parseLatLongPair = (value) => {
  if (typeof value !== "string" || !value.includes(",")) return null;
  const [rawLat, rawLong] = value.split(",").map((part) => part.trim());
  const lat = toNumber(rawLat);
  const long = toNumber(rawLong);
  if (lat === null || long === null) return null;
  return { lat, long };
};
const normalizeToken = (value) => {
  if (value === null || value === undefined) return null;
  const token = String(value).trim();
  return token.length ? token : null;
};
const getNearbyCenterShiftMeters = (previousCenter, nextCenter) => {
  const prevLat = toNumber(previousCenter?.lat);
  const prevLong = toNumber(previousCenter?.long);
  const nextLat = toNumber(nextCenter?.lat);
  const nextLong = toNumber(nextCenter?.long);
  if (
    prevLat === null ||
    prevLong === null ||
    nextLat === null ||
    nextLong === null
  ) {
    return null;
  }

  const distance = getDistanceMeters(prevLat, prevLong, nextLat, nextLong);
  return Number.isFinite(distance) ? distance : null;
};

const getFirstNumber = (...values) => {
  for (const value of values) {
    const parsed = toNumber(value);
    if (parsed !== null) return parsed;
  }
  return null;
};
const pickFirstDefined = (...values) => {
  for (const value of values) {
    if (value !== undefined) return value;
  }
  return undefined;
};
const normalizeFilterToken = (value) => {
  if (value === null || value === undefined) return "";
  return String(value)
    .trim()
    .toLowerCase()
    .replace(/[\s_-]+/g, "");
};

const uniqueSortedNumbers = (values = []) =>
  Array.from(
    new Set(values.filter((value) => typeof value === "number" && Number.isFinite(value)))
  ).sort((a, b) => a - b);

const parseNumberList = (value) => {
  if (Array.isArray(value)) return value;
  if (typeof value === "string") {
    return value
      .split(",")
      .map((item) => item.trim())
      .filter((item) => item.length > 0);
  }
  return [];
};

const normalizeNearbyDispatchTimeoutSeconds = (value, fallback = RIDE_TIMEOUT_S) => {
  const parsed = toNumber(value);
  const safeFallback = Math.max(1, Math.floor(toNumber(fallback) ?? RIDE_TIMEOUT_S));
  return parsed === null ? safeFallback : Math.max(1, Math.floor(parsed));
};

const normalizeNearbyDispatchStagesMeters = (
  initialRadiusMeters,
  stagesMeters = []
) => {
  const initial = normalizeNearbyRadiusMeters(initialRadiusMeters);
  const normalizedStages = Array.isArray(stagesMeters)
    ? stagesMeters
        .map((value) => normalizeNearbyRadiusMeters(value, initial))
        .filter((value) => value >= initial)
    : [];

  const stages = uniqueSortedNumbers([initial, ...normalizedStages]);
  return stages.length > 0 ? stages : [initial];
};

const extractDispatchTimeoutSecondsFromPayload = (payload = {}, fallback = null) => {
  if (!payload || typeof payload !== "object") {
    return fallback === null
      ? null
      : normalizeNearbyDispatchTimeoutSeconds(fallback, RIDE_TIMEOUT_S);
  }

  const explicit = getFirstNumber(
    payload?.dispatch_timeout_s,
    payload?.provider_accept_timeout,
    payload?.timeout_s,
    payload?.timeout_seconds
  );

  if (explicit === null) {
    return fallback === null
      ? null
      : normalizeNearbyDispatchTimeoutSeconds(fallback, RIDE_TIMEOUT_S);
  }

  return normalizeNearbyDispatchTimeoutSeconds(explicit, fallback ?? RIDE_TIMEOUT_S);
};

const extractDispatchRadiusStagesMetersFromPayload = (
  payload = {},
  initialRadiusMeters = DEFAULT_NEARBY_RADIUS_METERS
) => {
  if (!payload || typeof payload !== "object") return null;

  const explicitMeters = parseNumberList(payload?.dispatch_radius_stages_m)
    .map((value) => toNumber(value))
    .filter((value) => value !== null);

  if (explicitMeters.length > 0) {
    return normalizeNearbyDispatchStagesMeters(initialRadiusMeters, explicitMeters);
  }

  const explicitKm = parseNumberList(payload?.dispatch_radius_stages_km)
    .map((value) => toNumber(value))
    .filter((value) => value !== null)
    .map((value) => Math.round(value * 1000));

  if (explicitKm.length > 0) {
    return normalizeNearbyDispatchStagesMeters(initialRadiusMeters, explicitKm);
  }

  return null;
};

const normalizeNearbyRadiusMeters = (
  value,
  fallback = DEFAULT_NEARBY_RADIUS_METERS
) => {
  const parsed = toNumber(value);
  if (parsed === null) return fallback;
  return Math.max(200, Math.round(parsed));
};

const buildDefaultNearbyDispatchStagesMeters = (initialRadiusMeters) => {
  const initial = normalizeNearbyRadiusMeters(initialRadiusMeters);
  const initialKm = Math.round((initial / 1000) * 100) / 100;
  const stageKm = uniqueSortedNumbers([
    ...NEARBY_DISPATCH_RADIUS_STEPS_KM,
    initialKm,
  ]).filter((km) => km >= initialKm);
  const stageMeters = uniqueSortedNumbers(
    stageKm.map((km) => normalizeNearbyRadiusMeters(Math.round(km * 1000), initial))
  );

  return stageMeters.length > 0 ? stageMeters : [initial];
};

const resolveNearbyRadiusFromPayload = (payload = {}) => {
  if (!payload || typeof payload !== "object") return null;

  // محاولة لاستخراج الراديوس من الـ payload
  const explicitMeters = getFirstNumber(
    payload?.dispatch_current_radius_m,
    payload?.initial_dispatch_radius,
    payload?.provider_search_radius_m,
    payload?.search_radius_m
  );
  if (explicitMeters !== null) {
    return normalizeNearbyRadiusMeters(explicitMeters);
  }

  const genericRadius = toNumber(payload?.radius);
  if (genericRadius !== null) {
    return genericRadius >= 100
      ? normalizeNearbyRadiusMeters(genericRadius)
      : normalizeNearbyRadiusMeters(Math.round(genericRadius * 1000));
  }

  const explicitKm = getFirstNumber(
    payload?.provider_search_radius_km,
    payload?.provider_search_radius,
    payload?.search_radius_km,
    payload?.radius_km
  );
  if (explicitKm !== null) {
    return normalizeNearbyRadiusMeters(Math.round(explicitKm * 1000));
  }

  // إذا لم يكن الراديوس موجودًا في الـ payload، نقوم باستدعاء API للحصول عليه
  return null;
};

const normalizeServiceCategoryId = (value) => {
  const parsed = toNumber(value);
  if (parsed === null) return null;
  const normalized = Math.trunc(parsed);
  return normalized > 0 ? normalized : null;
};

const extractServiceCategoryIdFromPayload = (payload = {}) => {
  if (!payload || typeof payload !== "object") return null;

  const explicit = getFirstNumber(
    payload?.service_category_id,
    payload?.service_cat_id,
    payload?.sub_service_cat_id,
    payload?.selected_service_category_id,
    payload?.selectedServiceCategoryId,
    payload?.service_category,
    payload?.serviceCategoryId,
    payload?.category_id,
    payload?.categoryId
  );
  const normalizedExplicit = normalizeServiceCategoryId(explicit);
  if (normalizedExplicit !== null) return normalizedExplicit;

  // Some clients send service_id/serviceId for category. Guard with known category ids.
  const ambiguous = normalizeServiceCategoryId(payload?.service_id ?? payload?.serviceId ?? null);
  if (ambiguous !== null && KNOWN_SERVICE_CATEGORY_IDS.has(ambiguous)) {
    return ambiguous;
  }

  return null;
};

const extractServiceTypeIdFromPayload = (payload = {}) => {
  if (!payload || typeof payload !== "object") return null;

  const explicit = getFirstNumber(
    payload?.service_type_id,
    payload?.vehicle_type_id,
    payload?.selected_service_type_id,
    payload?.selectedServiceTypeId,
    payload?.serviceTypeId,
    payload?.selected_vehicle_type_id,
    payload?.selectedVehicleTypeId
  );
  const normalized = normalizeServiceCategoryId(explicit);
  return normalized;
};

const pickDominantServiceCategoryId = (drivers = []) => {
  const counts = new Map();
  for (const item of Array.isArray(drivers) ? drivers : []) {
    const sc = normalizeServiceCategoryId(item?.service_category_id);
    if (sc === null) continue;
    counts.set(sc, (counts.get(sc) ?? 0) + 1);
  }

  let bestId = null;
  let bestCount = -1;
  for (const [id, count] of counts.entries()) {
    if (count > bestCount) {
      bestId = id;
      bestCount = count;
    }
  }
  return bestId;
};

const inferServiceCategoryIdFromNearbyMemory = (
  lat,
  long,
  serviceTypeId = null
) => {
  const la = toNumber(lat);
  const lo = toNumber(long);
  if (la === null || lo === null) return null;

  const st = toNumber(serviceTypeId);
  const nearbyWithType =
    typeof driverLocationService.getNearbyDriversFromMemory === "function"
      ? driverLocationService.getNearbyDriversFromMemory(
          la,
          lo,
          DEFAULT_AIR_CANDIDATE_RADIUS_METERS,
          {
            only_online: true,
            max_age_ms: MAX_DRIVER_LOCATION_AGE_MS,
            service_type_id: st ?? null,
          }
        )
      : [];

  let inferred = pickDominantServiceCategoryId(nearbyWithType);
  if (inferred !== null) {
    return inferred;
  }

  const nearbyAnyType =
    typeof driverLocationService.getNearbyDriversFromMemory === "function"
      ? driverLocationService.getNearbyDriversFromMemory(
          la,
          lo,
          DEFAULT_AIR_CANDIDATE_RADIUS_METERS,
          {
            only_online: true,
            max_age_ms: MAX_DRIVER_LOCATION_AGE_MS,
          }
        )
      : [];

  inferred = pickDominantServiceCategoryId(nearbyAnyType);
  if (inferred !== null) {
    return inferred;
  }

  return null;
};

const getCachedServiceSearchRadius = (serviceCategoryId) => {
  const safeServiceCategoryId = toNumber(serviceCategoryId);
  if (!safeServiceCategoryId) return null;

  const entry = serviceSearchRadiusCache.get(safeServiceCategoryId);
  if (!entry) return null;

  if (Date.now() - entry.cached_at > SERVICE_RADIUS_CACHE_TTL_MS) {
    serviceSearchRadiusCache.delete(safeServiceCategoryId);
    return null;
  }

  const radiusMeters = toNumber(entry.radius_m);
  if (radiusMeters === null) return null;

  const normalizedRadius = normalizeNearbyRadiusMeters(radiusMeters);
  const cachedStages = Array.isArray(entry.dispatch_radius_stages_m)
    ? normalizeNearbyDispatchStagesMeters(normalizedRadius, entry.dispatch_radius_stages_m)
    : [normalizedRadius];
  const cachedTimeout = extractDispatchTimeoutSecondsFromPayload(
    { dispatch_timeout_s: entry.dispatch_timeout_s },
    RIDE_TIMEOUT_S
  );

  return {
    radius_m: normalizedRadius,
    dispatch_radius_stages_m: cachedStages,
    dispatch_timeout_s: cachedTimeout,
  };
};

const setCachedServiceSearchRadius = (serviceCategoryId, config = {}) => {
  const safeServiceCategoryId = toNumber(serviceCategoryId);
  if (!safeServiceCategoryId || !config || typeof config !== "object") return;

  const safeRadiusMeters = toNumber(config.radius_m);
  if (safeRadiusMeters === null) return;

  const normalizedRadius = normalizeNearbyRadiusMeters(safeRadiusMeters);
  const normalizedStages = normalizeNearbyDispatchStagesMeters(
    normalizedRadius,
    config.dispatch_radius_stages_m
  );
  const normalizedTimeout = extractDispatchTimeoutSecondsFromPayload(
    { dispatch_timeout_s: config.dispatch_timeout_s },
    RIDE_TIMEOUT_S
  );

  serviceSearchRadiusCache.set(safeServiceCategoryId, {
    radius_m: normalizedRadius,
    dispatch_radius_stages_m: normalizedStages,
    dispatch_timeout_s: normalizedTimeout,
    cached_at: Date.now(),
  });
};

const invalidateServiceSearchRadiusCache = (serviceCategoryId = null) => {
  if (serviceCategoryId === null || serviceCategoryId === undefined || serviceCategoryId === "") {
    const clearedCount = serviceSearchRadiusCache.size;
    serviceSearchRadiusCache.clear();
    return {
      scope: "all",
      cleared_count: clearedCount,
    };
  }

  const safeServiceCategoryId = toNumber(serviceCategoryId);
  if (!safeServiceCategoryId) {
    return {
      scope: "none",
      cleared_count: 0,
    };
  }

  const existed = serviceSearchRadiusCache.delete(safeServiceCategoryId);
  return {
    scope: "single",
    service_category_id: safeServiceCategoryId,
    cleared_count: existed ? 1 : 0,
  };
};

const getCachedNearbyFareVersion = (serviceCategoryId) => {
  const safeServiceCategoryId = toNumber(serviceCategoryId);
  if (!safeServiceCategoryId) return null;

  const entry = nearbyFareVersionCache.get(safeServiceCategoryId);
  if (!entry) return null;

  if (
    NEARBY_FARE_VERSION_CACHE_TTL_MS === 0 ||
    Date.now() - entry.cachedAt > NEARBY_FARE_VERSION_CACHE_TTL_MS
  ) {
    nearbyFareVersionCache.delete(safeServiceCategoryId);
    return null;
  }

  return entry.version;
};

const setCachedNearbyFareVersion = (serviceCategoryId, version) => {
  const safeServiceCategoryId = toNumber(serviceCategoryId);
  const safeVersion = toNumber(version);
  if (!safeServiceCategoryId || safeVersion === null) return;

  nearbyFareVersionCache.set(safeServiceCategoryId, {
    version: safeVersion,
    cachedAt: Date.now(),
  });
};

const fetchServiceSearchRadiusFromApi = async (serviceCategoryId, serviceTypeId = null) => {
  const safeServiceCategoryId = toNumber(serviceCategoryId);
  const safeServiceTypeId = toNumber(serviceTypeId);
  if (!safeServiceCategoryId && !safeServiceTypeId) return null;

  const cachedConfig = safeServiceCategoryId
    ? getCachedServiceSearchRadius(safeServiceCategoryId)
    : null;
  if (cachedConfig !== null) return cachedConfig;

  const inFlightKey = `category:${safeServiceCategoryId ?? 0}:type:${safeServiceTypeId ?? 0}`;
  const inFlightRequest = serviceSearchRadiusInFlight.get(inFlightKey);
  if (inFlightRequest) return inFlightRequest;

  const requestPromise = (async () => {
    try {
      const requestPayload = {};
      if (safeServiceCategoryId) {
        requestPayload.service_category_id = safeServiceCategoryId;
      }
      if (safeServiceTypeId) {
        requestPayload.service_type_id = safeServiceTypeId;
      }

      const res = await axios.post(
        `${LARAVEL_BASE_URL}${LARAVEL_SERVICE_SEARCH_RADIUS_PATH}`,
        requestPayload,
        { timeout: LARAVEL_TIMEOUT_MS }
      );

      let data = res?.data;
      if (typeof data === "string") {
        try {
          data = JSON.parse(data);
        } catch (_) {}
      }

      if (!data || data.status !== 1) return null;

      const resolvedServiceCategoryId = toNumber(data?.service_category_id) ?? safeServiceCategoryId;
      const radiusMeters = resolveNearbyRadiusFromPayload(data);
      if (radiusMeters === null) return null;

      const explicitDispatchStages =
        extractDispatchRadiusStagesMetersFromPayload(data, radiusMeters) ?? null;
      const dispatchRadiusStagesMeters =
        Array.isArray(explicitDispatchStages) && explicitDispatchStages.length > 0
          ? normalizeNearbyDispatchStagesMeters(radiusMeters, explicitDispatchStages)
          : buildDefaultNearbyDispatchStagesMeters(radiusMeters);
      const dispatchTimeoutSeconds = extractDispatchTimeoutSecondsFromPayload(
        data,
        NEARBY_DISPATCH_TIMEOUT_S
      );
      const normalizedConfig = {
        service_category_id: resolvedServiceCategoryId,
        radius_m: normalizeNearbyRadiusMeters(radiusMeters),
        dispatch_radius_stages_m: dispatchRadiusStagesMeters,
        dispatch_timeout_s: dispatchTimeoutSeconds,
      };

      nearbyLog("[service-search-radius] ok", {
        requested_service_category_id: safeServiceCategoryId,
        requested_service_type_id: safeServiceTypeId,
        resolved_service_category_id: resolvedServiceCategoryId,
        radius_m: normalizedConfig.radius_m,
        dispatch_stages_m: normalizedConfig.dispatch_radius_stages_m,
        dispatch_timeout_s: normalizedConfig.dispatch_timeout_s,
      });

      if (resolvedServiceCategoryId !== null) {
        setCachedServiceSearchRadius(resolvedServiceCategoryId, normalizedConfig);
      }
      return normalizedConfig;
    } catch (e) {
      console.warn(
        "[service-search-radius] failed:",
        e?.response?.data || e?.message || e
      );
      return null;
    } finally {
      serviceSearchRadiusInFlight.delete(inFlightKey);
    }
  })();

  serviceSearchRadiusInFlight.set(inFlightKey, requestPromise);
  return requestPromise;
};

const fetchVehicleFareVersionFromApi = async (serviceCategoryId) => {
  const safeServiceCategoryId = toNumber(serviceCategoryId);
  if (!safeServiceCategoryId) return null;

  const cachedVersion = getCachedNearbyFareVersion(safeServiceCategoryId);
  if (cachedVersion !== null) return cachedVersion;

  const inFlightRequest = nearbyFareVersionInFlight.get(safeServiceCategoryId);
  if (inFlightRequest) return inFlightRequest;

  const requestPromise = (async () => {
    try {
      const res = await axios.post(
        `${LARAVEL_BASE_URL}${LARAVEL_VEHICLE_FARE_VERSION_PATH}`,
        {
          service_category_id: safeServiceCategoryId,
        },
        { timeout: LARAVEL_TIMEOUT_MS }
      );

      let data = res?.data;
      if (typeof data === "string") {
        try {
          data = JSON.parse(data);
        } catch (_) {}
      }

      if (!data || data.status !== 1) return null;

      const version = toNumber(data?.config_version);
      if (version === null) return null;

      setCachedNearbyFareVersion(safeServiceCategoryId, version);
      return version;
    } catch (e) {
      warnThrottled(
        "vehicle-fares-version-failed",
        "[vehicle-fares-version] failed:",
        e?.response?.data || e?.message || e
      );
      return null;
    } finally {
      nearbyFareVersionInFlight.delete(safeServiceCategoryId);
    }
  })();

  nearbyFareVersionInFlight.set(safeServiceCategoryId, requestPromise);
  return requestPromise;
};

const roundMoney = (v) => {
  if (!Number.isFinite(v)) return null;
  return Math.round((v + Number.EPSILON) * 100) / 100;
};

const ECONOMY_SERVICE_TYPE_ID = toNumber(
  process.env.NEARBY_ECONOMY_SERVICE_TYPE_ID ??
    process.env.ECONOMY_SERVICE_TYPE_ID ??
    null
);
const ECONOMY_NAME_PATTERNS = [
  /econom/i,
  /economic/i,
  /eco\b/i,
  /\u0627\u0642\u062A\u0635\u0627\u062F/i,
  /\u0627\u0642\u062A\u0635\u0627\u062F\u064A/i,
  /\u0627\u0642\u062A\u0635\u0627\u062F\u064A\u0629/i,
];

const matchesEconomyVehicleTypeName = (value) => {
  const name = String(value ?? "").trim();
  if (!name) return false;
  return ECONOMY_NAME_PATTERNS.some((pattern) => pattern.test(name));
};

const getVehicleTypeSortPrice = (item) => {
  const candidates = [
    item?.estimated_fare,
    item?.base_fare,
    item?.min_price,
    item?.cost_per_km,
    item?.driver_to_pickup_distance_km,
  ];

  for (const value of candidates) {
    const parsed = toNumber(value);
    if (parsed !== null && parsed >= 0) return parsed;
  }

  const distanceMeters = toNumber(item?.driver_to_pickup_distance_m);
  if (distanceMeters !== null && distanceMeters >= 0) {
    return roundMoney(distanceMeters / 1000) ?? distanceMeters / 1000;
  }

  return Number.POSITIVE_INFINITY;
};

const resolveEconomyVehicleTypeId = (types = []) => {
  if (!Array.isArray(types) || types.length === 0) return null;

  if (
    ECONOMY_SERVICE_TYPE_ID !== null &&
    types.some((item) => toNumber(item?.service_type_id) === ECONOMY_SERVICE_TYPE_ID)
  ) {
    return ECONOMY_SERVICE_TYPE_ID;
  }

  const namedEconomyType = types.find((item) =>
    matchesEconomyVehicleTypeName(item?.vehicle_type_name)
  );
  if (namedEconomyType) {
    return toNumber(namedEconomyType?.service_type_id);
  }

  const cheapestType = [...types]
    .filter((item) => toNumber(item?.service_type_id) !== null)
    .sort((a, b) => {
      const priceDiff = getVehicleTypeSortPrice(a) - getVehicleTypeSortPrice(b);
      if (Number.isFinite(priceDiff) && priceDiff !== 0) return priceDiff;

      const driversDiff =
        (toNumber(b?.drivers_count ?? 0) ?? 0) - (toNumber(a?.drivers_count ?? 0) ?? 0);
      if (driversDiff !== 0) return driversDiff;

      return (toNumber(a?.service_type_id) ?? Number.MAX_SAFE_INTEGER) -
        (toNumber(b?.service_type_id) ?? Number.MAX_SAFE_INTEGER);
    })[0];

  return toNumber(cheapestType?.service_type_id);
};

const decorateNearbyVehicleTypes = (types = [], selectedTypeId = null) => {
  if (!Array.isArray(types) || types.length === 0) return [];

  const availableTypeIds = new Set(
    types.map((item) => toNumber(item?.service_type_id)).filter((item) => item !== null)
  );
  const economyTypeId = resolveEconomyVehicleTypeId(types);
  const fallbackTypeId = economyTypeId ?? toNumber(types[0]?.service_type_id);
  const requestedSelectedTypeId = toNumber(selectedTypeId);
  const effectiveSelectedTypeId =
    requestedSelectedTypeId !== null && availableTypeIds.has(requestedSelectedTypeId)
      ? requestedSelectedTypeId
      : fallbackTypeId;

  return types
    .map((item) => {
      const typeId = toNumber(item?.service_type_id);
      const isEconomy = typeId !== null && economyTypeId !== null && typeId === economyTypeId;
      const isDefault = typeId !== null && fallbackTypeId !== null && typeId === fallbackTypeId;
      const isSelected =
        typeId !== null &&
        effectiveSelectedTypeId !== null &&
        typeId === effectiveSelectedTypeId;

      return {
        ...item,
        is_economy: isEconomy ? 1 : 0,
        is_default: isDefault ? 1 : 0,
        is_selected: isSelected ? 1 : 0,
      };
    })
    .sort((a, b) => {
      const defaultDiff = (toNumber(b?.is_default ?? 0) ?? 0) - (toNumber(a?.is_default ?? 0) ?? 0);
      if (defaultDiff !== 0) return defaultDiff;

      const selectedDiff =
        (toNumber(b?.is_selected ?? 0) ?? 0) - (toNumber(a?.is_selected ?? 0) ?? 0);
      if (selectedDiff !== 0) return selectedDiff;

      const driversDiff =
        (toNumber(b?.drivers_count ?? 0) ?? 0) - (toNumber(a?.drivers_count ?? 0) ?? 0);
      if (driversDiff !== 0) return driversDiff;

      const priceDiff = getVehicleTypeSortPrice(a) - getVehicleTypeSortPrice(b);
      if (Number.isFinite(priceDiff) && priceDiff !== 0) return priceDiff;

      return (toNumber(a?.service_type_id) ?? Number.MAX_SAFE_INTEGER) -
        (toNumber(b?.service_type_id) ?? Number.MAX_SAFE_INTEGER);
    });
};

const buildPriceBounds = (baseFare, estimatedFare = null, distanceKm = null) => {
  const base = toNumber(baseFare);
  const estimated = toNumber(estimatedFare);
  const hasAny = base !== null || estimated !== null;
  if (!hasAny) {
    return {
      base_fare: null,
      estimated_fare: null,
      min_price: null,
      max_price: null,
    };
  }

  const roundedBase = base !== null ? roundMoney(base) : null;
  const roundedEstimated =
    estimated !== null
      ? roundMoney(estimated)
      : roundedBase;
  const anchor = roundedEstimated ?? roundedBase;

  return {
    base_fare: roundedBase,
    estimated_fare: roundedEstimated,
    min_price:
      anchor !== null
        ? roundMoney(anchor * BID_MIN_PRICE_MULTIPLIER)
        : null,
    max_price:
      anchor !== null ? roundMoney(anchor * BID_MAX_PRICE_MULTIPLIER) : null,
  };
};

const normalizePriceBoundsPair = (minRaw, maxRaw) => {
  const min = toNumber(minRaw);
  const max = toNumber(maxRaw);

  if (min !== null && max !== null && min > max) {
    return {
      min_price: roundMoney(max),
      max_price: roundMoney(min),
      swapped: true,
    };
  }

  return {
    min_price: min !== null ? roundMoney(min) : null,
    max_price: max !== null ? roundMoney(max) : null,
    swapped: false,
  };
};

const toBinaryFlag = (v) => {
  if (v === null || v === undefined || v === "") return null;
  if (typeof v === "boolean") return v ? 1 : 0;

  const n = toNumber(v);
  if (n === 0 || n === 1) return n;

  const token = normalizeFilterToken(v);
  if (!token) return null;

  if (
    [
      "1",
      "true",
      "yes",
      "on",
      "enable",
      "enabled",
      "allowed",
      "required",
      "need",
      "smoker",
      "smoking",
      "childseat",
      "مدخن",
      "نعم",
      "اي",
      "ايوه",
      "احتياجاتخاصة",
      "specialneeds",
      "handicap",
    ].includes(token)
  ) {
    return 1;
  }

  if (
    [
      "0",
      "false",
      "no",
      "off",
      "disable",
      "disabled",
      "notallowed",
      "none",
      "nonsmoker",
      "غيرمدخن",
      "لا",
      "مو",
      "لااحتياجات",
      "nospecialneeds",
      "nonhandicap",
    ].includes(token)
  ) {
    return 0;
  }

  return null;
};

// Nearby filtering should be opt-in: only explicit "1" enables it.
// Values 0/null/empty mean "no filter".
const toOptionalBinaryRequirement = (v) => {
  const flag = toBinaryFlag(v);
  return flag === 1 ? 1 : null;
};

const toGenderFilter = (v) => {
  if (v === null || v === undefined || v === "") return null;
  const n = toNumber(v);
  if (n === 1 || n === 2) return n;

  const token = normalizeFilterToken(v);
  if (!token) return null;

  if (
    [
      "male",
      "man",
      "boy",
      "m",
      "ذكر",
      "رجل",
      "شاب",
    ].includes(token)
  ) {
    return 1;
  }

  if (
    [
      "female",
      "woman",
      "girl",
      "f",
      "femaledriver",
      "انثى",
      "أنثى",
      "امراه",
      "امرأة",
      "سيده",
      "سيدة",
    ].includes(token)
  ) {
    return 2;
  }

  if (
    [
      "0",
      "all",
      "any",
      "both",
      "none",
      "anygender",
      "غيرمحدد",
      "الكل",
      "الكلالجنسين",
    ].includes(token)
  ) {
    return null;
  }

  return null;
};

const summarizeVehicleTypesForLog = (types) => {
  if (!Array.isArray(types)) return types;
  return types.map((t) => ({
    service_type_id: t?.service_type_id ?? null,
    service_category_id: t?.service_category_id ?? null,
    vehicle_type_name: t?.vehicle_type_name ?? null,
    is_default: t?.is_default ?? 0,
    is_selected: t?.is_selected ?? 0,
    is_economy: t?.is_economy ?? 0,
    drivers_count: t?.drivers_count ?? null,
    base_fare: t?.base_fare ?? null,
    estimated_fare: t?.estimated_fare ?? null,
    min_price: t?.min_price ?? null,
    max_price: t?.max_price ?? null,
  }));
};

const normalizePublicAssetUrl = (value, defaultRelativeDir = "", emptyValue = "") =>
  normalizeAssetUrl(value, {
    baseUrl: LARAVEL_BASE_URL,
    defaultRelativeDir,
    emptyValue,
    upgradeSameHostToHttps: true,
  });

const normalizeVehicleTypeIconUrl = (value) =>
  normalizePublicAssetUrl(value, VEHICLE_ICON_RELATIVE_DIR);

const normalizeDriverImageUrl = (value) =>
  normalizePublicAssetUrl(value, DRIVER_IMAGE_RELATIVE_DIR);

const normalizeCustomerImageUrl = (value) =>
  normalizePublicAssetUrl(value, CUSTOMER_IMAGE_RELATIVE_DIR);

// ? NEW: build stable signature so we emit only on change
const buildVehicleTypesSignature = (types) => {
  if (!Array.isArray(types) || types.length === 0) return "[]";

  const stable = [...types].sort((a, b) => {
    const aId = Number(a?.service_type_id ?? 0);
    const bId = Number(b?.service_type_id ?? 0);
    return aId - bId;
  });

    const compact = stable.map((t) => ({
      service_type_id: toNumber(t?.service_type_id),
      service_category_id: toNumber(t?.service_category_id),
      is_default: toNumber(t?.is_default ?? 0),
      is_selected: toNumber(t?.is_selected ?? 0),
      is_economy: toNumber(t?.is_economy ?? 0),
      drivers_count: toNumber(t?.drivers_count ?? 0),

      vehicle_type_name: t?.vehicle_type_name ?? "",
      vehicle_type_icon: normalizeVehicleTypeIconUrl(t?.vehicle_type_icon),
      driver_image: normalizeDriverImageUrl(t?.driver_image),

    distance_km: t?.distance_km != null ? roundMoney(toNumber(t?.distance_km)) : null,
    cost_per_km: t?.cost_per_km != null ? roundMoney(toNumber(t?.cost_per_km)) : null,
    base_fare: t?.base_fare != null ? roundMoney(toNumber(t?.base_fare)) : null,
    estimated_fare: t?.estimated_fare != null ? roundMoney(toNumber(t?.estimated_fare)) : null,
    min_price: t?.min_price != null ? roundMoney(toNumber(t?.min_price)) : null,
    max_price: t?.max_price != null ? roundMoney(toNumber(t?.max_price)) : null,
    driver_to_pickup_distance_m:
      t?.driver_to_pickup_distance_m != null ? toNumber(t?.driver_to_pickup_distance_m) : null,
    driver_to_pickup_duration_s:
      t?.driver_to_pickup_duration_s != null ? toNumber(t?.driver_to_pickup_duration_s) : null,
  }));

  return JSON.stringify(compact);
};

const fetchVehicleFaresFromApi = async (
  serviceCategoryId,
  distanceKm,
  vehicleTypeIds = [],
  pickupLat = null,
  pickupLong = null
) => {
  const safeServiceCategoryId = toNumber(serviceCategoryId);
  const safeDistanceKm = toNumber(distanceKm);
  if (!safeServiceCategoryId || safeDistanceKm === null) return new Map();

  const safePickupLat = toNumber(pickupLat);
  const safePickupLong = toNumber(pickupLong);
  const normalizedVehicleTypeIds = (Array.isArray(vehicleTypeIds) ? vehicleTypeIds : [])
    .map((id) => toNumber(id))
    .filter((id) => id !== null)
    .sort((a, b) => a - b);
  const distanceKey = safeDistanceKm.toFixed(3);
  const pickupKey =
    safePickupLat !== null && safePickupLong !== null
      ? `${safePickupLat.toFixed(5)},${safePickupLong.toFixed(5)}`
      : "none";
  const inFlightKey = [
    `category:${safeServiceCategoryId}`,
    `distance:${distanceKey}`,
    `types:${normalizedVehicleTypeIds.join(",")}`,
    `pickup:${pickupKey}`,
  ].join("|");

  const inFlightRequest = nearbyVehicleFaresInFlight.get(inFlightKey);
  if (inFlightRequest) return inFlightRequest;

  const requestPromise = (async () => {
    try {
      const payload = {
        service_category_id: safeServiceCategoryId,
        distance_km: safeDistanceKm,
        vehicle_type_ids: normalizedVehicleTypeIds,
      };
      if (safePickupLat !== null && safePickupLong !== null) {
        payload.pickup_lat = safePickupLat;
        payload.pickup_long = safePickupLong;
      }

      const res = await axios.post(
        `${LARAVEL_BASE_URL}/api/customer/transport/vehicle-fares`,
        payload,
        { timeout: LARAVEL_TIMEOUT_MS }
      );

      let data = res?.data;
      if (typeof data === "string") {
        try {
          data = JSON.parse(data);
        } catch (_) {}
      }
      if (data?.status === 1 && Array.isArray(data.items)) {
        const first = data.items[0] || null;
        nearbyLog("[vehicle-fares-api] ok", {
          count: data.items.length,
          has_driver_distance: !!(first && first.driver_to_pickup_distance_m != null),
        });
      }
      if (!data || data.status !== 1 || !Array.isArray(data.items)) return new Map();

      const map = new Map();
      for (const item of data.items) {
        const id = toNumber(item?.vehicle_type_id);
        if (!id) continue;
        map.set(id, item);
      }
      return map;
    } catch (e) {
      warnThrottled(
        "vehicle-fares-api-failed",
        "[vehicle-fares-api] failed:",
        e?.response?.data || e?.message || e
      );
      return new Map();
    } finally {
      nearbyVehicleFaresInFlight.delete(inFlightKey);
    }
  })();

  nearbyVehicleFaresInFlight.set(inFlightKey, requestPromise);
  return requestPromise;
};

// ? NEW: helper extracts road distance from route API response
const extractRoadDistanceMeters = (payload) => {
  if (!payload || typeof payload !== "object") return null;

  const directM = toNumber(
    payload?.distance_m ??
      payload?.distanceMeters ??
      payload?.distance_meters ??
      null
  );
  if (directM !== null) return directM;

  const km = toNumber(
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

// ? NEW: driver -> pickup road metrics via Laravel route API
const fetchDriverToPickupRoadMetrics = async (driverLat, driverLong, pickupLat, pickupLong) => {
  const la1 = toNumber(driverLat);
  const lo1 = toNumber(driverLong);
  const la2 = toNumber(pickupLat);
  const lo2 = toNumber(pickupLong);
  const airDistanceM = getDistanceMeters(la1, lo1, la2, lo2);
  const safeAirDistanceM =
    Number.isFinite(airDistanceM) && airDistanceM >= 0 ? Math.round(airDistanceM) : null;
  const fallbackSpeedKmph = 28;
  const safeAirDurationS =
    safeAirDistanceM !== null
      ? Math.max(
          1,
          Math.round((safeAirDistanceM / 1000 / Math.max(1, fallbackSpeedKmph)) * 3600)
        )
      : null;

  if (la1 === null || lo1 === null || la2 === null || lo2 === null) {
    return {
      road_distance_m: null,
      road_duration_s: null,
      road_duration_min: null,
      raw: null,
      source: "invalid-input",
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
      const durationMin = toNumber(data?.duration ?? null);
      const durationS = durationMin !== null ? Math.round(durationMin * 60) : null;
      const resolvedDistanceM = roadDistanceM ?? safeAirDistanceM;
      const resolvedDurationS = durationS ?? safeAirDurationS;
      const resolvedDurationMin =
        resolvedDurationS !== null
          ? roundMoney(resolvedDurationS / 60)
          : durationMin !== null
          ? roundMoney(durationMin)
          : null;

      const result = {
        road_distance_m: resolvedDistanceM,
        road_duration_s: resolvedDurationS,
        road_duration_min: resolvedDurationMin,
        raw: data,
        source: roadDistanceM !== null ? "road-api" : "air-fallback-empty-route",
      };
      setCachedRouteMetrics(routeKey, result);
      if (routeL2CacheKey && routeCacheL2.isEnabled()) {
        await routeCacheL2.setJson(routeL2CacheKey, result, ROUTE_CACHE_L2_TTL_S);
      }
      return result;
    } catch (e) {
      warnThrottled(
        "driver-to-pickup-road-metrics-failed",
        "[driverToPickupRoadMetrics] failed:",
        e?.response?.data || e?.message || e
      );
      const fallback = {
        road_distance_m: safeAirDistanceM,
        road_duration_s: safeAirDurationS,
        road_duration_min: safeAirDurationS !== null ? roundMoney(safeAirDurationS / 60) : null,
        raw: null,
        source: safeAirDistanceM !== null ? "air-fallback-error" : "error",
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
};

// ? NEW: final road-based filtering
const filterDriversByRoadRadius = async (drivers, pickupLat, pickupLong, roadRadiusM) => {
  const list = Array.isArray(drivers) ? drivers.slice(0, MAX_ROAD_FILTER_CANDIDATES) : [];
  const results = await mapWithConcurrency(
    list,
    ROUTE_API_MAX_CONCURRENCY,
    async (d) => {
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
        driver_to_pickup_distance_km: roundMoney(metrics.road_distance_m / 1000),
        driver_to_pickup_duration_s: metrics.road_duration_s,
        driver_to_pickup_duration_min: metrics.road_duration_min,
      };
    }
  );

  return results.filter(Boolean);
};

const buildAirFallbackCandidates = (drivers = []) => {
  const fallbackSpeedKmph = 28;
  return (Array.isArray(drivers) ? drivers : []).map((d) => {
    const airDistanceM = toNumber(d?.distance);
    const airDurationS =
      airDistanceM !== null
        ? Math.max(
            1,
            Math.round((airDistanceM / 1000 / Math.max(1, fallbackSpeedKmph)) * 3600)
          )
        : null;

    return {
      ...d,
      driver_to_pickup_distance_m: airDistanceM,
      driver_to_pickup_distance_km:
        airDistanceM !== null ? roundMoney(airDistanceM / 1000) : null,
      driver_to_pickup_duration_s: airDurationS,
      driver_to_pickup_duration_min:
        airDurationS !== null ? roundMoney(airDurationS / 60) : null,
      distance_source: "air-fallback",
    };
  });
};

const extractRouteDistanceKm = (payload) => {
  if (payload == null) return null;
  if (typeof payload === "number") return roundMoney(payload);
  if (typeof payload === "string" && payload.trim() !== "") {
    const n = toNumber(payload);
    if (n !== null) return roundMoney(n);
  }
  if (typeof payload !== "object") return null;

  const route =
    payload.route ??
    payload.route_info ??
    payload.routeInfo ??
    payload.trip ??
    payload.path ??
    payload;

  if (typeof route === "number") return roundMoney(route);
  if (typeof route === "string" && route.trim() !== "") {
    const n = toNumber(route);
    if (n !== null) return roundMoney(n);
  }
  if (!route || typeof route !== "object") return null;

  const directKm = toNumber(
    route.distance_km ??
      route.distanceKm ??
      route.total_distance_km ??
      route.totalDistanceKm ??
      route.distance_in_km ??
      route.total_distance ??
      null
  );
  if (directKm !== null) return roundMoney(directKm);

  let meters = route.distanceMeters ?? route.distance_m ?? route.distance_meters ?? null;
  if (meters === null && route?.routes?.[0]?.distanceMeters != null) {
    meters = route.routes[0].distanceMeters;
  }
  if (meters === null && route?.routes?.[0]?.legs?.[0]?.distance?.value != null) {
    meters = route.routes[0].legs[0].distance.value;
  }

  if (meters !== null) {
    return roundMoney(Number(meters) / 1000);
  }

  const distanceMaybeRaw = toNumber(route.distance ?? null);
  if (distanceMaybeRaw !== null) {
    if (distanceMaybeRaw > 1000) return roundMoney(distanceMaybeRaw / 1000);
    return roundMoney(distanceMaybeRaw);
  }

  return null;
};

const extractRouteDurationMin = (payload) => {
  if (payload == null) return null;
  if (typeof payload === "number") return roundMoney(payload);
  if (typeof payload === "string" && payload.trim() !== "") {
    const n = toNumber(payload);
    if (n !== null) return roundMoney(n);
  }
  if (typeof payload !== "object") return null;

  const route =
    payload.route ??
    payload.route_info ??
    payload.routeInfo ??
    payload.trip ??
    payload.path ??
    payload;

  if (typeof route === "number") return roundMoney(route);
  if (typeof route === "string" && route.trim() !== "") {
    const n = toNumber(route);
    if (n !== null) return roundMoney(n);
  }
  if (!route || typeof route !== "object") return null;

  const directMin = toNumber(
    route.duration_min ??
      route.durationMin ??
      route.route_api_duration_min ??
      route.driver_to_pickup_duration_min ??
      route.eta_min ??
      route.etaMin ??
      route.time_min ??
      route.timeMin ??
      route.duration ??
      null
  );
  if (directMin !== null) return roundMoney(directMin);

  let durationSec = toNumber(
    route.duration_s ??
      route.durationSec ??
      route.durationSeconds ??
      route.driver_to_pickup_duration_s ??
      route.time_s ??
      route.timeSec ??
      null
  );
  if (durationSec === null && route?.routes?.[0]?.durationSeconds != null) {
    durationSec = toNumber(route.routes[0].durationSeconds);
  }
  if (durationSec === null && route?.routes?.[0]?.legs?.[0]?.duration?.value != null) {
    durationSec = toNumber(route.routes[0].legs[0].duration.value);
  }
  if (durationSec !== null) return roundMoney(durationSec / 60);

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

const maybeLogUserImageSource = (userId, imageValue, imageSource, context = "unknown") => {
  if (!VERBOSE_IMAGE_SOURCE_LOGS) return;
  const safeUserId = toNumber(userId);
  if (!safeUserId) return;
  const normalizedImage = typeof imageValue === "string" ? imageValue.trim() : "";
  const signature = `${imageSource ?? "none"}|${normalizedImage}`;
  if (lastUserImageSignatureByUserId.get(safeUserId) === signature) return;
  lastUserImageSignatureByUserId.set(safeUserId, signature);
  console.log("[image-source][user]", {
    user_id: safeUserId,
    source: imageSource ?? null,
    image: normalizedImage || null,
    context,
  });
};

const extractUserDetails = (payload, sourceContext = "extractUserDetails") => {
  if (!payload || typeof payload !== "object") return null;

  const src =
    (payload.user_details && typeof payload.user_details === "object" && payload.user_details) ||
    (payload.user && typeof payload.user === "object" && payload.user) ||
    (payload.data && typeof payload.data === "object" && payload.data) ||
    payload;

  const userId = toNumber(src?.user_id ?? src?.id ?? payload?.user_id);
  if (!userId) return null;

  const userName = src?.user_name ?? src?.name ?? null;
  const userToken =
    src?.user_token ??
    src?.token ??
    payload?.user_token ??
    payload?.token ??
    payload?.access_token ??
    payload?.accessToken ??
    null;
  const genderRaw = src?.gender ?? src?.user_gender ?? src?.gender_id ?? null;
  const userGender =
    genderRaw === "" || genderRaw == null
      ? null
      : Number.isFinite(Number(genderRaw))
      ? Number(genderRaw)
      : genderRaw;
  const countryCode = src?.select_country_code ?? src?.country_code ?? null;
  const contactNumber =
    src?.contact_number ?? src?.user_phone ?? src?.phone ?? src?.mobile ?? null;
  const imagePick = pickFirstPresentValueWithSource([
    { source: "src.profile_image", value: src?.profile_image },
    { source: "src.user_profile_image", value: src?.user_profile_image },
    { source: "src.user_profile", value: src?.user_profile },
    { source: "src.user_image", value: src?.user_image },
    { source: "src.image", value: src?.image },
    { source: "src.avatar", value: src?.avatar },
    { source: "payload.profile_image", value: payload?.profile_image },
    { source: "payload.user_profile_image", value: payload?.user_profile_image },
    { source: "payload.user_profile", value: payload?.user_profile },
    { source: "payload.user_image", value: payload?.user_image },
    { source: "payload.customer_image", value: payload?.customer_image },
    { source: "payload.avatar", value: payload?.avatar },
  ]);
  const userImage = normalizeCustomerImageUrl(imagePick.value);
  const userImageSource = imagePick.source;

  maybeLogUserImageSource(userId, userImage, userImageSource, sourceContext);

  return {
    user_id: userId,
    user_name: userName,
    user_gender: userGender,
    user_token: userToken,
    user_phone: contactNumber,
    user_country_code: countryCode,
    user_phone_full: contactNumber && countryCode ? `${countryCode}${contactNumber}` : null,
    user_image: userImage,
    user_image_source: userImageSource,
  };
};

// ? NEW: timer helpers (so frontend can build countdown) - all fields in SECONDS
const normalizeEpochSeconds = (value) => {
  const n = toNumber(value);
  if (n === null) return null;
  return n > 1e11 ? Math.floor(n / 1000) : Math.floor(n);
};

const normalizeDurationSeconds = (value) => {
  const n = toNumber(value);
  if (n === null) return null;
  return n > 10000 ? Math.floor(n / 1000) : Math.floor(n);
};

const normalizeTimerFields = (rideDetails) => {
  if (!rideDetails || typeof rideDetails !== "object") return null;

  const serverTime = normalizeEpochSeconds(rideDetails.server_time);
  const expiresAt = normalizeEpochSeconds(rideDetails.expires_at);
  let timeoutSec = normalizeDurationSeconds(rideDetails.timeout_ms);

  if (serverTime === null || expiresAt === null || expiresAt < serverTime) {
    return null;
  }

  const inferred = Math.max(0, Math.floor(expiresAt - serverTime));
  if (timeoutSec === null || Math.abs(timeoutSec - inferred) > 5) {
    timeoutSec = inferred;
  }

  return {
    server_time: serverTime,
    expires_at: expiresAt,
    timeout_ms: timeoutSec,
  };
};

const hasTimer = (rideDetails) => {
  return normalizeTimerFields(rideDetails) !== null;
};

const makeTimer = () => {
  const now = Math.floor(Date.now() / 1000);
  const timeoutSec = Math.max(0, Math.floor(toNumber(RIDE_TIMEOUT_S) ?? 90));
  return {
    server_time: now,
    expires_at: now + timeoutSec,
    timeout_ms: timeoutSec,
  };
};

const ensureRideTimer = (rideId, rideDetails) => {
  if (!rideId) return null;

  if (hasTimer(rideDetails)) {
    const normalized = normalizeTimerFields(rideDetails);
    if (!normalized) return makeTimer();

    if (rideDetails && typeof biddingSocket.saveRideDetails === "function") {
      const rawServer = toNumber(rideDetails.server_time);
      const rawExpires = toNumber(rideDetails.expires_at);
      const rawTimeout = toNumber(rideDetails.timeout_ms);
      const needsNormalize =
        rawServer !== normalized.server_time ||
        rawExpires !== normalized.expires_at ||
        rawTimeout !== normalized.timeout_ms;
      if (needsNormalize) {
        try {
          biddingSocket.saveRideDetails(rideId, { ...rideDetails, ...normalized });
        } catch (_) {}
      }
    }

    return normalized;
  }

  const timer = makeTimer();

  if (rideDetails && typeof biddingSocket.saveRideDetails === "function") {
    try {
      biddingSocket.saveRideDetails(rideId, { ...rideDetails, ...timer });
    } catch (_) {}
  }

  return timer;
};

const setupUserSocket = (io, socket) => {
  socket.isUser = false;
  socket.userId = null;
  socket.userToken = null;

  socket.nearbyCenter = null; // { lat, long }
  socket.nearbyDriversInterval = null;
  socket.nearbyVehicleTypesInterval = null;
  socket.nearbyDriversInFlight = false;
  socket.nearbyVehicleTypesInFlight = false;
  socket.nearbyRadius = DEFAULT_NEARBY_RADIUS_METERS;
  socket.nearbyDispatchStagesMeters = [DEFAULT_NEARBY_RADIUS_METERS];
  socket.nearbyDispatchTimeoutS = NEARBY_DISPATCH_TIMEOUT_S;
  socket.nearbyDriversSearchStartedAt = null;
  socket.nearbyVehicleTypesSearchStartedAt = null;
  socket.nearbyCandidateCache = new Map();

  socket.nearbyServiceTypeId = null;
  socket.nearbyServiceCategoryId = null;
  socket.nearbyRouteDistanceKm = null;
  socket.nearbyRouteDurationMin = null;
  socket.nearbyRequiredGender = null;
  socket.nearbyNeedChildSeat = null;
  socket.nearbyNeedHandicap = null;

  socket.lastVehicleTypesSig = null;
  socket.currentRideId = null;

  socket.nearbyFareCache = new Map();
  socket.lastPricingSnapshotSigByRide = new Map();

  const canDriverAppearInNearby = (driverId) => {
    const safeDriverId = toNumber(driverId);
    if (!safeDriverId) return false;

    if (typeof biddingSocket.canDriverReceiveNewRideRequests === "function") {
      return !!biddingSocket.canDriverReceiveNewRideRequests(safeDriverId);
    }

    return !getActiveRideByDriver(safeDriverId);
  };

  const getNearbyRadiusFromRideSnapshot = () => {
    const rideId = toNumber(socket.currentRideId);
    if (!rideId || typeof biddingSocket.getRideDetails !== "function") {
      return null;
    }

    const rideDetails = biddingSocket.getRideDetails(rideId);
    return resolveNearbyRadiusFromPayload(rideDetails);
  };

  const getNearbyServiceCategoryFromRideSnapshot = () => {
    const rideId = toNumber(socket.currentRideId);
    if (!rideId || typeof biddingSocket.getRideDetails !== "function") {
      return null;
    }

    const rideDetails = biddingSocket.getRideDetails(rideId);
    if (!rideDetails || typeof rideDetails !== "object") return null;

    return (
      extractServiceCategoryIdFromPayload(rideDetails) ??
      normalizeServiceCategoryId(rideDetails?.service_category_id ?? rideDetails?.service_cat_id)
    );
  };

  const getNearbyDispatchConfigFromRideSnapshot = () => {
    const rideId = toNumber(socket.currentRideId);
    if (!rideId || typeof biddingSocket.getRideDetails !== "function") {
      return null;
    }

    const rideDetails = biddingSocket.getRideDetails(rideId);
    if (!rideDetails || typeof rideDetails !== "object") return null;

    const radiusMeters = resolveNearbyRadiusFromPayload(rideDetails);
    if (radiusMeters === null) return null;

    const dispatchStages =
      extractDispatchRadiusStagesMetersFromPayload(rideDetails, radiusMeters) ??
      normalizeNearbyDispatchStagesMeters(radiusMeters, []);
    const dispatchTimeoutSeconds = extractDispatchTimeoutSecondsFromPayload(
      rideDetails,
      socket.nearbyDispatchTimeoutS ?? RIDE_TIMEOUT_S
    );

    return {
      radius_m: normalizeNearbyRadiusMeters(radiusMeters),
      dispatch_radius_stages_m: dispatchStages,
      dispatch_timeout_s: dispatchTimeoutSeconds,
    };
  };

  const resolveActiveNearbyRadiusPlan = (startedAtMs = null) => {
    const baseRadius = normalizeNearbyRadiusMeters(
      socket.nearbyRadius,
      DEFAULT_NEARBY_RADIUS_METERS
    );
    const stagedRadii = Array.isArray(socket.nearbyDispatchStagesMeters)
      ? normalizeNearbyDispatchStagesMeters(baseRadius, socket.nearbyDispatchStagesMeters)
      : [baseRadius];

    const timeoutSeconds = normalizeNearbyDispatchTimeoutSeconds(
      socket.nearbyDispatchTimeoutS,
      RIDE_TIMEOUT_S
    );
    const startedAt = toNumber(startedAtMs);
    const elapsedMs = startedAt === null ? 0 : Math.max(0, Date.now() - startedAt);
    const stageIndex = Math.max(
      0,
      Math.min(
        Math.floor(elapsedMs / (timeoutSeconds * 1000)),
        stagedRadii.length - 1
      )
    );
    const roadRadius = normalizeNearbyRadiusMeters(stagedRadii[stageIndex], baseRadius);

    return {
      roadRadius,
      stagesMeters: stagedRadii,
      stageIndex,
      stageNumber: stageIndex + 1,
      stageTotal: stagedRadii.length,
      nextRadiusMeters: stagedRadii[stageIndex + 1] ?? null,
      timeoutSeconds,
      elapsedMs,
    };
  };

  const setNearbyServiceCategoryId = (value, source = "payload") => {
    const normalized = normalizeServiceCategoryId(value);
    if (normalized === null) return false;

    if (socket.nearbyServiceCategoryId !== normalized) {
      socket.nearbyServiceCategoryId = normalized;
      socket.nearbyFareCache.clear();
      socket.lastVehicleTypesSig = null;
      console.log("[nearby-service-category] updated", {
        service_category_id: normalized,
        source,
      });
      return true;
    }

    return false;
  };

  const syncRideContextFromPayload = (payload = {}, source = "unknown") => {
    const payloadRideId = toNumber(
      payload?.ride_id ?? payload?.booking_id ?? payload?.trip_id ?? null
    );
    const payloadUserId = toNumber(payload?.user_id ?? socket.userId ?? null);
    const activeRideIdFromUser =
      payloadUserId && typeof biddingSocket.getActiveRideIdForUser === "function"
        ? toNumber(biddingSocket.getActiveRideIdForUser(payloadUserId))
        : null;
    const resolvedRideId =
      payloadRideId ?? activeRideIdFromUser ?? toNumber(socket.currentRideId) ?? null;

    if (resolvedRideId && socket.currentRideId !== resolvedRideId) {
      socket.currentRideId = resolvedRideId;
      socket.lastVehicleTypesSig = null;
    }

    const snapshotServiceCategoryId = getNearbyServiceCategoryFromRideSnapshot();
    if (snapshotServiceCategoryId !== null) {
      const payloadServiceCategoryId = extractServiceCategoryIdFromPayload(payload);
      if (
        payloadServiceCategoryId !== null &&
        payloadServiceCategoryId !== snapshotServiceCategoryId
      ) {
        nearbyLog("[nearby-service-category] payload/snapshot mismatch", {
          source,
          payload_service_category_id: payloadServiceCategoryId,
          snapshot_service_category_id: snapshotServiceCategoryId,
          ride_id: resolvedRideId ?? null,
        });
      }
      setNearbyServiceCategoryId(snapshotServiceCategoryId, `${source}:ride-snapshot`);
    }
  };

const syncNearbyRadius = async (payload = {}) => {
  const payloadServiceCategoryId = extractServiceCategoryIdFromPayload(payload);
  if (payloadServiceCategoryId !== null) {
    setNearbyServiceCategoryId(payloadServiceCategoryId, "payload");
  }

  const snapshotServiceCategoryId = getNearbyServiceCategoryFromRideSnapshot();
  if (snapshotServiceCategoryId !== null) {
    setNearbyServiceCategoryId(snapshotServiceCategoryId, "ride-snapshot");
  }

  let serviceCategoryId =
    normalizeServiceCategoryId(socket.nearbyServiceCategoryId) ??
    snapshotServiceCategoryId;
  const payloadServiceTypeId =
    extractServiceTypeIdFromPayload(payload) ??
    normalizeServiceCategoryId(socket.nearbyServiceTypeId);

  let nextRadius = null;
  let nextDispatchStagesMeters = null;
  let nextDispatchTimeoutSeconds = null;
  let source = null;

  const usedVehicleTypeFallback = !serviceCategoryId && payloadServiceTypeId !== null;

  if (serviceCategoryId || payloadServiceTypeId) {
    const apiConfig = await fetchServiceSearchRadiusFromApi(
      serviceCategoryId,
      payloadServiceTypeId
    );
    if (apiConfig && toNumber(apiConfig.radius_m) !== null) {
      nextRadius = apiConfig.radius_m;
      nextDispatchStagesMeters = apiConfig.dispatch_radius_stages_m ?? null;
      nextDispatchTimeoutSeconds = extractDispatchTimeoutSecondsFromPayload(
        apiConfig,
        socket.nearbyDispatchTimeoutS ?? RIDE_TIMEOUT_S
      );
      const resolvedServiceCategoryId = toNumber(apiConfig?.service_category_id);
      if (
        resolvedServiceCategoryId !== null &&
        normalizeServiceCategoryId(socket.nearbyServiceCategoryId) !== resolvedServiceCategoryId
      ) {
        setNearbyServiceCategoryId(resolvedServiceCategoryId, "service-setting-api");
        serviceCategoryId = resolvedServiceCategoryId;
      }
      source = usedVehicleTypeFallback
        ? "service-setting-api:vehicle-type"
        : "service-setting-api";
    }
  }

  if (nextRadius === null) {
    const payloadRadius = resolveNearbyRadiusFromPayload(payload);
    if (payloadRadius !== null) {
      nextRadius = payloadRadius;
      nextDispatchStagesMeters = extractDispatchRadiusStagesMetersFromPayload(
        payload,
        payloadRadius
      );
      nextDispatchTimeoutSeconds = extractDispatchTimeoutSecondsFromPayload(
        payload,
        socket.nearbyDispatchTimeoutS ?? RIDE_TIMEOUT_S
      );
      source = "payload";
    }
  }

  if (nextRadius === null) {
    const snapshotConfig = getNearbyDispatchConfigFromRideSnapshot();
    if (snapshotConfig && toNumber(snapshotConfig.radius_m) !== null) {
      nextRadius = snapshotConfig.radius_m;
      nextDispatchStagesMeters = snapshotConfig.dispatch_radius_stages_m ?? null;
      nextDispatchTimeoutSeconds = extractDispatchTimeoutSecondsFromPayload(
        snapshotConfig,
        socket.nearbyDispatchTimeoutS ?? RIDE_TIMEOUT_S
      );
      source = "ride-snapshot";
    }
  }

  const normalizedRadius = normalizeNearbyRadiusMeters(
    nextRadius,
    socket.nearbyRadius ?? DEFAULT_NEARBY_RADIUS_METERS
  );
  const hasExplicitDispatchStages =
    Array.isArray(nextDispatchStagesMeters) && nextDispatchStagesMeters.length > 0;
  const dispatchStagesSeed =
    hasExplicitDispatchStages
      ? nextDispatchStagesMeters
      : buildDefaultNearbyDispatchStagesMeters(normalizedRadius);
  const normalizedDispatchStages = normalizeNearbyDispatchStagesMeters(
    normalizedRadius,
    dispatchStagesSeed
  );
  const normalizedDispatchTimeoutSeconds = normalizeNearbyDispatchTimeoutSeconds(
    nextDispatchTimeoutSeconds,
    NEARBY_DISPATCH_TIMEOUT_S
  );

  const radiusChanged = socket.nearbyRadius !== normalizedRadius;
  const stagesChanged =
    JSON.stringify(socket.nearbyDispatchStagesMeters ?? []) !==
    JSON.stringify(normalizedDispatchStages);
  const timeoutChanged =
    toNumber(socket.nearbyDispatchTimeoutS) !== normalizedDispatchTimeoutSeconds;

  nearbyLog("[nearby-radius][resolve]", {
    service_category_id: serviceCategoryId ?? null,
    source: source ?? "fallback",
    radius_m: normalizedRadius,
    dispatch_stages_source: hasExplicitDispatchStages ? source ?? "payload" : "default-fallback",
    dispatch_stages_m: normalizedDispatchStages,
    dispatch_timeout_s: normalizedDispatchTimeoutSeconds,
  });

  if (radiusChanged) {
    socket.nearbyRadius = normalizedRadius;
  }
  if (stagesChanged) {
    socket.nearbyDispatchStagesMeters = normalizedDispatchStages;
  }
  if (timeoutChanged) {
    socket.nearbyDispatchTimeoutS = normalizedDispatchTimeoutSeconds;
  }

  if (radiusChanged || stagesChanged || timeoutChanged) {
    socket.lastVehicleTypesSig = null;
    nearbyLog("[nearby-radius] updated", {
      service_category_id: serviceCategoryId ?? null,
      radius_m: normalizedRadius,
      source: source ?? "fallback",
      dispatch_stages_m: normalizedDispatchStages,
      dispatch_timeout_s: normalizedDispatchTimeoutSeconds,
    });
  }

  nearbyLog("[nearby-radius][vehicle-types]", {
    service_category_id: serviceCategoryId ?? null,
    source: source ?? "fallback",
    radius_m: normalizedRadius,
    dispatch_stages_m: normalizedDispatchStages,
    dispatch_timeout_s: normalizedDispatchTimeoutSeconds,
    stage_total: normalizedDispatchStages.length,
  });

  return normalizedRadius;
};
  const applyNearbyFiltersFromPayload = (payload = {}, options = {}) => {
    const { resetMissing = false } = options || {};
    const base = payload && typeof payload === "object" ? payload : {};
    const nested = [
      base,
      base?.filters && typeof base.filters === "object" ? base.filters : null,
      base?.filter && typeof base.filter === "object" ? base.filter : null,
      base?.preferences && typeof base.preferences === "object" ? base.preferences : null,
      base?.meta && typeof base.meta === "object" ? base.meta : null,
      base?.ride_details && typeof base.ride_details === "object" ? base.ride_details : null,
    ].filter(Boolean);
    const readFirstDefined = (...keys) => {
      for (const src of nested) {
        const value = pickFirstDefined(...keys.map((key) => src?.[key]));
        if (value !== undefined) return value;
      }
      return undefined;
    };
    let changed = false;

    const genderInput = readFirstDefined(
      "required_driver_gender",
      "required_gender",
      "driver_gender",
      "gender",
      "driverGender",
      "requiredDriverGender"
    );
    const hasGender = genderInput !== undefined;
    let nextGender = hasGender
      ? toGenderFilter(genderInput)
      : socket.nearbyRequiredGender;

    const childSeatInput = readFirstDefined(
      "need_child_seat",
      "child_seat",
      "require_child_seat",
      "need_smoking",
      "smoking",
      "smoking_value",
      "child_seat_accessibility"
    );
    const hasChildSeat = childSeatInput !== undefined;
    let nextChildSeat = hasChildSeat
      ? toOptionalBinaryRequirement(childSeatInput)
      : socket.nearbyNeedChildSeat;

    const handicapInput = readFirstDefined(
      "need_handicap",
      "handicap",
      "require_handicap",
      "special_needs",
      "need_special_needs",
      "handicap_accessibility",
      "can_receive_special_needs"
    );
    const hasHandicap = handicapInput !== undefined;
    let nextHandicap = hasHandicap
      ? toOptionalBinaryRequirement(handicapInput)
      : socket.nearbyNeedHandicap;

    const hasAnyFilterKey = hasGender || hasChildSeat || hasHandicap;
    if (resetMissing && hasAnyFilterKey) {
      if (!hasGender) nextGender = null;
      if (!hasChildSeat) nextChildSeat = null;
      if (!hasHandicap) nextHandicap = null;
    }

    if (nextGender !== socket.nearbyRequiredGender) {
      socket.nearbyRequiredGender = nextGender;
      changed = true;
    }

    if (nextChildSeat !== socket.nearbyNeedChildSeat) {
      socket.nearbyNeedChildSeat = nextChildSeat;
      changed = true;
    }

    if (nextHandicap !== socket.nearbyNeedHandicap) {
      socket.nearbyNeedHandicap = nextHandicap;
      changed = true;
    }

    if (changed) {
      socket.lastVehicleTypesSig = null;
      console.log("[nearby-filters] updated", {
        socket_id: socket.id,
        required_gender: socket.nearbyRequiredGender,
        need_child_seat: socket.nearbyNeedChildSeat,
        need_handicap: socket.nearbyNeedHandicap,
      });
    }
  };
  const applyNearbyDriverPreferenceFilters = (drivers = []) => {
    if (!Array.isArray(drivers) || drivers.length === 0) return [];

    const requiredGender = toGenderFilter(socket.nearbyRequiredGender);
    const requiredChildSeat = toOptionalBinaryRequirement(socket.nearbyNeedChildSeat);
    const requiredHandicap = toOptionalBinaryRequirement(socket.nearbyNeedHandicap);

    return drivers.filter((driver) => {
      if (requiredGender === 1 || requiredGender === 2) {
        const driverGender = toGenderFilter(
          driver?.driver_gender ?? driver?.gender ?? null
        );
        if (driverGender !== requiredGender) return false;
      }

      if (requiredChildSeat === 1) {
        const driverChildSeat = toBinaryFlag(
          driver?.child_seat ??
            driver?.smoking ??
            driver?.smoking_value ??
            driver?.child_seat_accessibility ??
            null
        );
        if (driverChildSeat !== requiredChildSeat) return false;
      }

      if (requiredHandicap === 1) {
        const driverHandicap = toBinaryFlag(
          driver?.handicap ??
            driver?.handicap_accessibility ??
            driver?.special_needs ??
            null
        );
        if (driverHandicap !== requiredHandicap) return false;
      }

      return true;
    });
  };

  const clearNearbyCandidateCache = () => {
    if (!(socket.nearbyCandidateCache instanceof Map)) {
      socket.nearbyCandidateCache = new Map();
      return;
    }
    socket.nearbyCandidateCache.clear();
  };

  const pruneNearbyCandidateCache = () => {
    if (!(socket.nearbyCandidateCache instanceof Map)) {
      socket.nearbyCandidateCache = new Map();
      return;
    }

    const now = Date.now();
    for (const [key, entry] of socket.nearbyCandidateCache.entries()) {
      if (!entry || typeof entry !== "object") {
        socket.nearbyCandidateCache.delete(key);
        continue;
      }
      if (entry.promise) continue;
      if (!Number.isFinite(Number(entry.expiresAt)) || Number(entry.expiresAt) <= now) {
        socket.nearbyCandidateCache.delete(key);
      }
    }
  };

  const buildNearbyCandidateCacheKey = ({
    lat,
    long,
    roadRadius,
    airCandidateRadius,
    serviceTypeId = null,
  }) => {
    const normalizedLat = normalizeCoordForRouteKey(lat);
    const normalizedLong = normalizeCoordForRouteKey(long);
    const requiredGender = toGenderFilter(socket.nearbyRequiredGender);
    const requiredChildSeat = toOptionalBinaryRequirement(socket.nearbyNeedChildSeat);
    const requiredHandicap = toOptionalBinaryRequirement(socket.nearbyNeedHandicap);
    const normalizedServiceTypeId = toNumber(serviceTypeId);
    const normalizedServiceCategoryId = normalizeServiceCategoryId(
      socket.nearbyServiceCategoryId
    );

    return [
      `lat:${normalizedLat ?? "na"}`,
      `long:${normalizedLong ?? "na"}`,
      `road:${Math.round(toNumber(roadRadius) ?? 0)}`,
      `air:${Math.round(toNumber(airCandidateRadius) ?? 0)}`,
      `service_type:${normalizedServiceTypeId ?? "all"}`,
      `service_category:${normalizedServiceCategoryId ?? "all"}`,
      `gender:${requiredGender ?? "all"}`,
      `child_seat:${requiredChildSeat ?? "all"}`,
      `handicap:${requiredHandicap ?? "all"}`,
    ].join("|");
  };

  const getNearbyCandidatesSnapshot = async ({
    lat,
    long,
    roadRadius,
    airCandidateRadius,
    serviceTypeId = null,
  }) => {
    const safeLat = toNumber(lat);
    const safeLong = toNumber(long);
    const safeRoadRadius = toNumber(roadRadius);
    const safeAirCandidateRadius = toNumber(airCandidateRadius);
    if (
      safeLat === null ||
      safeLong === null ||
      safeRoadRadius === null ||
      safeAirCandidateRadius === null
    ) {
      return {
        nearbyAll: [],
        nearbyPreferenceMatched: [],
        nearbyAvailable: [],
        roadFilteredCandidates: [],
        finalCandidates: [],
        usingAirFallback: false,
      };
    }

    pruneNearbyCandidateCache();
    const cacheKey = buildNearbyCandidateCacheKey({
      lat: safeLat,
      long: safeLong,
      roadRadius: safeRoadRadius,
      airCandidateRadius: safeAirCandidateRadius,
      serviceTypeId,
    });

    const cachedEntry = socket.nearbyCandidateCache.get(cacheKey);
    const now = Date.now();
    if (cachedEntry?.value && Number(cachedEntry.expiresAt) > now) {
      return cachedEntry.value;
    }
    if (cachedEntry?.promise) {
      return cachedEntry.promise;
    }

    const requestPromise = (async () => {
      const opts = {
        only_online: true,
        max_age_ms: MAX_DRIVER_LOCATION_AGE_MS,
      };
      const normalizedServiceTypeId = toNumber(serviceTypeId);
      const requiredGender = toGenderFilter(socket.nearbyRequiredGender);
      const requiredChildSeat = toOptionalBinaryRequirement(socket.nearbyNeedChildSeat);
      const requiredHandicap = toOptionalBinaryRequirement(socket.nearbyNeedHandicap);

      if (normalizedServiceTypeId !== null) {
        opts.service_type_id = normalizedServiceTypeId;
      }
      if (requiredGender !== null) {
        opts.required_gender = requiredGender;
      }
      if (requiredChildSeat !== null) {
        opts.need_child_seat = requiredChildSeat;
      }
      if (requiredHandicap !== null) {
        opts.need_handicap = requiredHandicap;
      }

      const nearbyAll = driverLocationService.getNearbyDriversFromMemory(
        safeLat,
        safeLong,
        safeAirCandidateRadius,
        opts
      );
      const nearbyPreferenceMatched = applyNearbyDriverPreferenceFilters(nearbyAll);
      const nearbyAvailable = nearbyPreferenceMatched.filter((driver) => {
        const driverId = toNumber(driver?.driver_id);
        return canDriverAppearInNearby(driverId);
      });
      const roadFilteredCandidates = await filterDriversByRoadRadius(
        nearbyAvailable,
        safeLat,
        safeLong,
        safeRoadRadius
      );
      const finalCandidates =
        roadFilteredCandidates.length > 0
          ? roadFilteredCandidates
          : buildAirFallbackCandidates(nearbyAvailable);

      return {
        nearbyAll,
        nearbyPreferenceMatched,
        nearbyAvailable,
        roadFilteredCandidates,
        finalCandidates,
        usingAirFallback:
          roadFilteredCandidates.length === 0 && nearbyAvailable.length > 0,
      };
    })();

    socket.nearbyCandidateCache.set(cacheKey, {
      promise: requestPromise,
      expiresAt: now + NEARBY_CANDIDATE_QUERY_CACHE_TTL_MS,
    });

    try {
      const value = await requestPromise;
      const nextExpiresAt = Date.now() + NEARBY_CANDIDATE_QUERY_CACHE_TTL_MS;
      if (NEARBY_CANDIDATE_QUERY_CACHE_TTL_MS > 0) {
        socket.nearbyCandidateCache.set(cacheKey, {
          value,
          expiresAt: nextExpiresAt,
        });
      } else {
        socket.nearbyCandidateCache.delete(cacheKey);
      }
      return value;
    } catch (error) {
      socket.nearbyCandidateCache.delete(cacheKey);
      throw error;
    }
  };

  const setNearbyRouteDistanceKm = (value) => {
    const n = toNumber(value);
    if (n === null) return;
    const rounded = roundMoney(n);
    if (socket.nearbyRouteDistanceKm !== rounded) {
      socket.nearbyRouteDistanceKm = rounded;
      socket.nearbyFareCache.clear();
      socket.lastVehicleTypesSig = null;
    }
  };

  const setNearbyRouteDurationMin = (value) => {
    const n = toNumber(value);
    if (n === null) return;
    const rounded = roundMoney(n);
    if (socket.nearbyRouteDurationMin !== rounded) {
      socket.nearbyRouteDurationMin = rounded;
      socket.lastVehicleTypesSig = null;
    }
  };

  const resetNearbyStateForUserSwitch = () => {
    stopNearby();
    socket.nearbyCenter = null;
    socket.nearbyServiceTypeId = null;
    socket.nearbyServiceCategoryId = null;
    socket.nearbyRouteDistanceKm = null;
    socket.nearbyRouteDurationMin = null;
    socket.nearbyRequiredGender = null;
    socket.nearbyNeedChildSeat = null;
    socket.nearbyNeedHandicap = null;
    socket.nearbyRadius = DEFAULT_NEARBY_RADIUS_METERS;
    socket.nearbyDispatchStagesMeters = [DEFAULT_NEARBY_RADIUS_METERS];
    socket.nearbyDispatchTimeoutS = NEARBY_DISPATCH_TIMEOUT_S;
    socket.nearbyDriversSearchStartedAt = null;
    socket.nearbyVehicleTypesSearchStartedAt = null;
    clearNearbyCandidateCache();
    socket.nearbyFareCache.clear();
    socket.lastVehicleTypesSig = null;
    socket.lastPricingSnapshotSigByRide.clear();
  };

  const switchSocketUserSession = (
    nextUserIdRaw,
    nextTokenRaw = null,
    source = "session"
  ) => {
    const previousUserId = toNumber(socket.userId);
    const nextUserId = toNumber(nextUserIdRaw);
    const nextToken = normalizeToken(nextTokenRaw);

    if (previousUserId && nextUserId && previousUserId !== nextUserId) {
      const previousUserRoom = userRoom(previousUserId);
      const previousRideId = toNumber(socket.currentRideId);

      socket.leave(previousUserRoom);
      if (previousRideId) {
        socket.leave(`ride:${previousRideId}`);
      }
      socket.currentRideId = null;
      socket.userToken = null;
      resetNearbyStateForUserSwitch();

      const roomMembers = io?.sockets?.adapter?.rooms?.get(previousUserRoom);
      if (!roomMembers || roomMembers.size === 0) {
        deleteUserDetails(previousUserId);
      }

      console.log(
        `[${source}] switched account on socket ${socket.id}: ${previousUserId} -> ${nextUserId}`
      );
    }

    if (nextUserId) {
      socket.isUser = true;
      socket.userId = nextUserId;
      socket.join(userRoom(nextUserId));
    }

    if (nextToken) {
      socket.userToken = nextToken;
    }
  };

  const resolvePayloadUserId = (payload = {}) => {
    const explicitUserId = toNumber(payload?.user_id ?? null);
    if (explicitUserId) return explicitUserId;

    const payloadToken = normalizeToken(
      payload?.access_token ?? payload?.token ?? payload?.user_token ?? null
    );
    if (!payloadToken) return null;

    const byToken = getUserDetailsByToken(payloadToken);
    return toNumber(byToken?.user_id ?? null);
  };

const registerUser = (payload, source = "user:loginInfo") => {
  debugLog(source, payload, socket.id);
  
  // استخراج تفاصيل المستخدم
  const details = extractUserDetails(payload, source);
  if (!details) {
    console.warn(`[${source}] Missing user_id in payload`);
    return;
  }

  // استخراج صورة المستخدم إذا كانت موجودة
  const userImage = details.user_image ?? null;

  const loginToken = normalizeToken(
    details?.user_token ?? payload?.access_token ?? payload?.token ?? payload?.user_token ?? null
  );

  // تغيير الجلسة الخاصة بالمستخدم
  switchSocketUserSession(details.user_id, loginToken, source);

  // دمج التفاصيل مع صورة المستخدم
  const normalizedDetails = {
    ...details,
    user_id: toNumber(details.user_id),
    user_image: userImage,  // تأكد من تضمين صورة المستخدم هنا
    ...(socket.userToken
      ? {
          user_token: socket.userToken,
          token: socket.userToken,
          access_token: socket.userToken,
        }
      : {}),
  };

  // حفظ تفاصيل المستخدم بما في ذلك الصورة
  setUserDetails(normalizedDetails.user_id, normalizedDetails);

  // إذا كان موجودًا، يتم تحديث تفاصيل المستخدم عبر السوكيت
  if (typeof biddingSocket.refreshUserDetailsForUserId === "function") {
    biddingSocket.refreshUserDetailsForUserId(io, normalizedDetails.user_id, normalizedDetails);
  }

  // التحقق من الركوب النشط للمستخدم
  const rideIdFromPayload = toNumber(payload?.ride_id ?? payload?.booking_id ?? null);
  const activeRideId =
    rideIdFromPayload ??
    (typeof biddingSocket.getActiveRideIdForUser === "function"
      ? toNumber(biddingSocket.getActiveRideIdForUser(normalizedDetails.user_id))
      : null);

  if (activeRideId) {
    socket.join(`ride:${activeRideId}`);
    socket.currentRideId = activeRideId;
    socket.lastVehicleTypesSig = null;
    if (typeof biddingSocket.touchUserActiveRide === "function") {
      biddingSocket.touchUserActiveRide(normalizedDetails.user_id, activeRideId);
    }
    socket.emit("ride:joined", { ride_id: activeRideId });
    emitRideStatusCatchup(activeRideId, source);
    ensureNearbyCenterFromRide(activeRideId);
    void emitNearbyVehicleTypesGuarded();
    console.log(
      `[${source}] auto-rejoin user ${normalizedDetails.user_id} -> ride:${activeRideId} (socket:${socket.id})`
    );
  }

  console.log(`[${source}] user details:`, normalizedDetails);
};

  const resolveRideIdFromPayload = (payload = {}) => {
    return (
      toNumber(payload?.ride_id) ??
      toNumber(payload?.booking_id) ??
      toNumber(socket.currentRideId) ??
      null
    );
  };

  const ensureNearbyCenterFromRide = (rideId) => {
    const safeRideId = toNumber(rideId);
    if (!safeRideId) return false;
    if (socket.nearbyCenter?.lat != null && socket.nearbyCenter?.long != null) {
      return true;
    }
    if (typeof biddingSocket.getRideDetails !== "function") return false;

    const rideDetails = biddingSocket.getRideDetails(safeRideId);
    if (!rideDetails || typeof rideDetails !== "object") return false;

    let lat = toNumber(
      rideDetails?.pickup_lat ??
        rideDetails?.pickupLat ??
        rideDetails?.meta?.pickup_lat ??
        null
    );
    let long = toNumber(
      rideDetails?.pickup_long ??
        rideDetails?.pickupLong ??
        rideDetails?.meta?.pickup_long ??
        null
    );

    if (lat === null || long === null) {
      const parsedPair = parseLatLongPair(
        rideDetails?.pickup_latlong ?? rideDetails?.pickupLatLong ?? null
      );
      if (parsedPair) {
        lat = parsedPair.lat;
        long = parsedPair.long;
      }
    }

    if (lat === null || long === null) return false;
    socket.nearbyCenter = { lat, long };
    return true;
  };

const emitRideStatusCatchup = (rideId, source = "user:joinRideRoom") => {
    const safeRideId = toNumber(rideId);
    if (!safeRideId) return;

    const snapshot = getRideStatusSnapshot(safeRideId);
    if (!snapshot) return;

    const rideStatus = toNumber(snapshot?.ride_status);
    if (rideStatus === null) return;

    const snapshotDriverId = toNumber(snapshot?.driver_id);
    const snapshotUserId = toNumber(snapshot?.user_id);
    const snapshotWayPointStatus = toNumber(snapshot?.way_point_status);
    const snapshotReasonId = toNumber(snapshot?.reason_id);

    // استخراج التقييم من الـ snapshot أو من مكان آخر إذا كان متاحًا
    const rating = snapshot?.rating ?? null;  // إذا كان التقييم موجودًا في الـ snapshot أو يمكن استرجاعه من مكان آخر

    const catchupPayload = {
      ride_id: safeRideId,
      ride_status: rideStatus,
      rating: rating,  // إضافة التقييم هنا
      ...(snapshotDriverId ? { driver_id: snapshotDriverId } : {}),
      ...(snapshotUserId ? { user_id: snapshotUserId } : {}),
      ...(snapshotWayPointStatus !== null ? { way_point_status: snapshotWayPointStatus } : {}),
      ...(snapshot?.cancel_by ? { cancel_by: snapshot.cancel_by } : {}),
      ...(snapshotReasonId !== null ? { reason_id: snapshotReasonId } : {}),
      ...(snapshot?.ended ? { ended: true } : {}),
      source: `${source}:snapshot`,
      replay: 1,
      updated_at: toNumber(snapshot?.updated_at) ?? Date.now(),
      at: Date.now(),
    };

    socket.emit("ride:statusUpdated", catchupPayload);
    socket.emit("ride:statusSnapshot", catchupPayload);
    if (catchupPayload.ended) {
      socket.emit("ride:ended", catchupPayload);
    }
    if (catchupPayload.ride_status === 4) {
      socket.emit("ride:cancelled", catchupPayload);
    }

    console.log(
      `[${source}] catchup status -> ride:${safeRideId} status:${catchupPayload.ride_status} rating:${catchupPayload.rating}`
    );
};

  const persistRideRouteMetrics = (payload = {}, routeKm = null, routeDurationMin = null, etaMin = null) => {
    const rideId = resolveRideIdFromPayload(payload);
    if (!rideId) return;

    const safeDistanceKm = toNumber(routeKm);
    const safeDurationMin = toNumber(routeDurationMin ?? etaMin ?? null);
    if (safeDistanceKm === null && safeDurationMin === null) return;

    if (typeof biddingSocket.upsertRideRouteMetrics === "function") {
      biddingSocket.upsertRideRouteMetrics(
        io,
        rideId,
        {
          ...(safeDistanceKm !== null ? { distanceKm: safeDistanceKm } : {}),
          ...(safeDurationMin !== null ? { durationMin: safeDurationMin } : {}),
        },
        { emit_bid_request: false }
      );
    }
  };

  const emitRouteEtaToDriver = (routeKm, etaMin, payload = {}) => {
    if (routeKm === null && etaMin === null) return;

    const rideId = resolveRideIdFromPayload(payload);
    if (!rideId) return;

    const snapshot =
      typeof biddingSocket.getRideDetails === "function"
        ? biddingSocket.getRideDetails(rideId)
        : null;

    let changed = false;
    if (snapshot) {
      if (routeKm !== null && snapshot.route !== routeKm) changed = true;
      if (etaMin !== null && snapshot.eta_min !== etaMin) changed = true;

      if (changed && typeof biddingSocket.saveRideDetails === "function") {
        biddingSocket.saveRideDetails(rideId, {
          ...snapshot,
          ...(routeKm !== null ? { route: routeKm } : {}),
          ...(etaMin !== null ? { eta_min: etaMin } : {}),
          meta: {
            ...(snapshot.meta ?? {}),
            ...(routeKm !== null ? { route: routeKm } : {}),
            ...(etaMin !== null ? { eta_min: etaMin } : {}),
          },
        });
      }
    }

    const driverId = getActiveDriverByRide(rideId);
    if (!driverId) return;
    if (!changed && snapshot) return;

    io.to(`driver:${driverId}`).emit("ride:statusPreUpdate", {
      ride_id: rideId,
      ...(routeKm !== null ? { route: routeKm } : {}),
      ...(etaMin !== null ? { eta_min: etaMin } : {}),
      source: "user",
      at: Date.now(),
    });
  };

  socket.on("user:loginInfo", (payload) => {
    registerUser(payload, "user:loginInfo");
  });

  socket.on("user:initialData", (payload) => {
    registerUser(payload, "user:initialData");
  });

  socket.on("user:joinRideRoom", (payload = {}) => {
    debugLog("user:joinRideRoom", payload, socket.id);

    const rideId = toNumber(payload?.ride_id);
    const joinToken = normalizeToken(
      payload?.access_token ?? payload?.token ?? payload?.user_token ?? null
    );
    const userId =
      resolvePayloadUserId(payload) ?? toNumber(payload?.user_id) ?? toNumber(socket.userId);

    if (!rideId) {
      console.log("[user:joinRideRoom] missing/invalid ride_id", payload);
      return;
    }

    switchSocketUserSession(userId ?? socket.userId, joinToken, "user:joinRideRoom");
    if (socket.userId && socket.userToken) {
      setUserDetails(socket.userId, {
        user_id: socket.userId,
        user_token: socket.userToken,
        token: socket.userToken,
        access_token: socket.userToken,
      });
    }

    if (socket.currentRideId && socket.currentRideId !== rideId) {
      socket.leave(`ride:${socket.currentRideId}`);
    }
    socket.join(`ride:${rideId}`);
    socket.currentRideId = rideId;
    socket.lastVehicleTypesSig = null;
    if (typeof biddingSocket.touchUserActiveRide === "function" && socket.userId) {
      biddingSocket.touchUserActiveRide(socket.userId, rideId);
    }

    socket.emit("ride:joined", { ride_id: rideId });
    emitRideStatusCatchup(rideId, "user:joinRideRoom");
    stopNearbyVehicleTypesLoop();
stopNearbyDriversLoop();
    ensureNearbyCenterFromRide(rideId);
emitCachedPricingSnapshotToSocket(rideId, "user:joinRideRoom-cache");
    console.log(
      `?? User ${socket.userId || "unknown"} joined ride room ride:${rideId} (socket:${socket.id})`
    );
  });

  socket.on("user:rejoinRideRoom", (payload = {}) => {
    debugLog("user:rejoinRideRoom", payload, socket.id);

    const rejoinToken = normalizeToken(
      payload?.access_token ?? payload?.token ?? payload?.user_token ?? null
    );
    const userId =
      resolvePayloadUserId(payload) ?? toNumber(payload?.user_id) ?? toNumber(socket.userId);
    const rideIdFromPayload = toNumber(payload?.ride_id ?? payload?.booking_id);
    const rideIdFromState =
      userId && typeof biddingSocket.getActiveRideIdForUser === "function"
        ? toNumber(biddingSocket.getActiveRideIdForUser(userId))
        : null;
    const rideId = rideIdFromPayload ?? rideIdFromState;

    if (!userId || !rideId) {
      socket.emit("ride:rejoinFailed", {
        user_id: userId ?? null,
        ride_id: rideId ?? null,
        message: "No active ride found for rejoin",
        at: Date.now(),
      });
      return;
    }

    switchSocketUserSession(userId, rejoinToken, "user:rejoinRideRoom");
    if (socket.userId && socket.userToken) {
      setUserDetails(socket.userId, {
        user_id: socket.userId,
        user_token: socket.userToken,
        token: socket.userToken,
        access_token: socket.userToken,
      });
    }
    if (socket.currentRideId && socket.currentRideId !== rideId) {
      socket.leave(`ride:${socket.currentRideId}`);
    }
    socket.join(`ride:${rideId}`);
    socket.currentRideId = rideId;
    socket.lastVehicleTypesSig = null;
    if (typeof biddingSocket.touchUserActiveRide === "function") {
      biddingSocket.touchUserActiveRide(userId, rideId);
    }

    socket.emit("ride:joined", { ride_id: rideId });
    socket.emit("ride:rejoined", {
      user_id: userId,
      ride_id: rideId,
      at: Date.now(),
    });
    emitRideStatusCatchup(rideId, "user:rejoinRideRoom");

    stopNearbyVehicleTypesLoop();
stopNearbyDriversLoop();
    ensureNearbyCenterFromRide(rideId);
emitCachedPricingSnapshotToSocket(rideId, "user:rejoinRideRoom-cache");
    console.log(`?? User ${userId} rejoined ride room ride:${rideId} (socket:${socket.id})`);
  });

  const stopNearbyDriversLoop = () => {
    if (socket.nearbyDriversInterval) {
      clearInterval(socket.nearbyDriversInterval);
      socket.nearbyDriversInterval = null;
    }
    socket.nearbyDriversInFlight = false;
  };

  const stopNearbyVehicleTypesLoop = () => {
    if (socket.nearbyVehicleTypesInterval) {
      clearInterval(socket.nearbyVehicleTypesInterval);
      socket.nearbyVehicleTypesInterval = null;
      console.log("[nearbyVehicleTypes] loop stopped", {
        socket_id: socket.id,
      });
    }
    socket.nearbyVehicleTypesInFlight = false;
  };

  const stopNearby = () => {
    stopNearbyDriversLoop();
    stopNearbyVehicleTypesLoop();
    clearNearbyCandidateCache();
  };

  const buildNearbyVehicleTypes = async (lat, long) => {
    const radiusPlan = resolveActiveNearbyRadiusPlan(
      socket.nearbyVehicleTypesSearchStartedAt
    );
    const roadRadius = radiusPlan.roadRadius;
    const airCandidateRadius = Math.max(
      roadRadius,
      DEFAULT_AIR_CANDIDATE_RADIUS_METERS
    );

    const nearbySnapshot = await getNearbyCandidatesSnapshot({
      lat,
      long,
      roadRadius,
      airCandidateRadius,
      serviceTypeId: null,
    });
    const nearbyAll = nearbySnapshot.nearbyAll;
    const nearbyPreferenceMatched = nearbySnapshot.nearbyPreferenceMatched;
    const nearbyAvailable = nearbySnapshot.nearbyAvailable;
    const nearbyDrivers = nearbySnapshot.roadFilteredCandidates;
    const nearbyForTypes = nearbySnapshot.finalCandidates;

    nearbyLog("[nearbyVehicleTypes] candidates", {
      socket_id: socket.id,
      stage_number: radiusPlan.stageNumber,
      stage_total: radiusPlan.stageTotal,
      road_radius_m: roadRadius,
      air_candidates: nearbyAll.length,
      preference_filtered_candidates: nearbyPreferenceMatched.length,
      available_candidates: nearbyAvailable.length,
      road_filtered_candidates: nearbyDrivers.length,
      next_radius_m: radiusPlan.nextRadiusMeters,
      using_air_fallback: nearbySnapshot.usingAirFallback,
    });

    const typesMap = new Map();

    for (const d of nearbyForTypes) {
      const typeId = toNumber(d.service_type_id);
      if (!typeId) continue;

      const key = typeId;

      if (!typesMap.has(key)) {
        typesMap.set(key, {
          service_type_id: typeId,
          service_category_id: d.service_category_id ?? null,
          vehicle_type_name: d.vehicle_type_name ?? "",
          vehicle_type_icon: normalizeVehicleTypeIconUrl(d.vehicle_type_icon),
          driver_id: toNumber(d.driver_id),
          driver_name: d.driver_name ?? "",
          driver_image: normalizeDriverImageUrl(d.driver_image),
          rating: toNumber(d.rating),
          drivers_count: 1,

          // keep already computed road metrics if present
          driver_to_pickup_distance_m: toNumber(d.driver_to_pickup_distance_m),
          driver_to_pickup_distance_km:
            d.driver_to_pickup_distance_km != null
              ? roundMoney(toNumber(d.driver_to_pickup_distance_km))
              : null,
          driver_to_pickup_duration_s: toNumber(d.driver_to_pickup_duration_s),
          driver_to_pickup_duration_min:
            d.driver_to_pickup_duration_min != null
              ? roundMoney(toNumber(d.driver_to_pickup_duration_min))
              : null,
        });
      } else {
        const existing = typesMap.get(key);
        existing.drivers_count++;
        if (existing.service_category_id == null && d.service_category_id != null) {
          existing.service_category_id = d.service_category_id;
        }
        if (!existing.driver_image && d.driver_image) {
          existing.driver_image = normalizeDriverImageUrl(d.driver_image);
        }

        // keep nearest road metrics if this candidate is closer
        const currentM = toNumber(existing.driver_to_pickup_distance_m);
        const nextM = toNumber(d.driver_to_pickup_distance_m);
        if (nextM !== null && (currentM === null || nextM < currentM)) {
          existing.driver_to_pickup_distance_m = nextM;
          existing.driver_to_pickup_distance_km =
            d.driver_to_pickup_distance_km != null
              ? roundMoney(toNumber(d.driver_to_pickup_distance_km))
              : roundMoney(nextM / 1000);
          existing.driver_to_pickup_duration_s = toNumber(d.driver_to_pickup_duration_s);
          existing.driver_to_pickup_duration_min =
            d.driver_to_pickup_duration_min != null
              ? roundMoney(toNumber(d.driver_to_pickup_duration_min))
              : null;
        }
      }
    }

    const result = Array.from(typesMap.values()).sort(
      (a, b) => (b.drivers_count ?? 0) - (a.drivers_count ?? 0)
    );

    const distanceKm = toNumber(socket.nearbyRouteDistanceKm);
    const fixedServiceCatIdRaw = toNumber(socket.nearbyServiceCategoryId);
    const snapshotServiceCatId = getNearbyServiceCategoryFromRideSnapshot();
    const fixedServiceCatId =
      (fixedServiceCatIdRaw !== null && fixedServiceCatIdRaw > 0 ? fixedServiceCatIdRaw : null) ??
      snapshotServiceCatId;
    const pickupLat = toNumber(lat);
    const pickupLong = toNumber(long);

    if (distanceKm !== null) {
      try {
        const groups = new Map();
        for (const item of result) {
          const serviceCatId = fixedServiceCatId ?? toNumber(item.service_category_id);
          if (!serviceCatId) continue;
          if (!groups.has(serviceCatId)) groups.set(serviceCatId, new Set());
          groups.get(serviceCatId).add(item.service_type_id);
        }

        const shouldIncludePickupForFareLookup = nearbyDrivers.length === 0;
        for (const [serviceCatId, typeSet] of groups.entries()) {
          let cacheEntry = socket.nearbyFareCache.get(serviceCatId);
          const cacheKeyChanged =
            !cacheEntry ||
            cacheEntry.distanceKm !== distanceKm ||
            cacheEntry.pickupLat !== pickupLat ||
            cacheEntry.pickupLong !== pickupLong;

          if (cacheKeyChanged) {
            cacheEntry = {
              distanceKm,
              pickupLat,
              pickupLong,
              configVersion: toNumber(cacheEntry?.configVersion),
              cachedAt: 0,
              map: new Map(),
            };
            socket.nearbyFareCache.set(serviceCatId, cacheEntry);
            socket.lastVehicleTypesSig = null;
          }

          const cacheExpired =
            NEARBY_FARE_CACHE_TTL_MS === 0 ||
            !Number.isFinite(Number(cacheEntry.cachedAt)) ||
            Date.now() - Number(cacheEntry.cachedAt) > NEARBY_FARE_CACHE_TTL_MS;

          let fareConfigVersion = toNumber(cacheEntry.configVersion);
          if (cacheExpired) {
            const refreshedVersion = await fetchVehicleFareVersionFromApi(serviceCatId);
            if (refreshedVersion !== null) {
              fareConfigVersion = refreshedVersion;
            }
          }

          const configVersionChanged =
            fareConfigVersion !== null &&
            toNumber(cacheEntry.configVersion) !== fareConfigVersion;
          if (configVersionChanged) {
            cacheEntry.configVersion = fareConfigVersion;
            cacheEntry.cachedAt = 0;
            socket.lastVehicleTypesSig = null;
          }

          const missingTypeIds = [];
          for (const typeId of typeSet) {
            if (!cacheEntry.map.has(typeId)) missingTypeIds.push(typeId);
          }

          const typeIdsToRefresh = configVersionChanged || cacheExpired
            ? Array.from(typeSet)
            : missingTypeIds;

          if (typeIdsToRefresh.length > 0) {
            const fareMap = await fetchVehicleFaresFromApi(
              serviceCatId,
              distanceKm,
              typeIdsToRefresh,
              shouldIncludePickupForFareLookup ? pickupLat : null,
              shouldIncludePickupForFareLookup ? pickupLong : null
            );
            for (const [id, item] of fareMap.entries()) {
              cacheEntry.map.set(id, item);
            }
            if (fareMap.size > 0) {
              if (fareConfigVersion !== null) {
                cacheEntry.configVersion = fareConfigVersion;
              }
              cacheEntry.cachedAt = Date.now();
            }
          }

          for (const item of result) {
            const itemCatId = fixedServiceCatId ?? toNumber(item.service_category_id);
            if (itemCatId !== serviceCatId) continue;
            const itemTypeId = toNumber(item.service_type_id);
            if (!itemTypeId) continue;
            const fare = cacheEntry.map.get(itemTypeId);
            if (!fare) continue;

            item.service_category_id = serviceCatId;
           item.cost_per_km = roundMoney(toNumber(fare.cost_per_km ?? 0));
item.distance_km = roundMoney(distanceKm);

const computedBounds = buildPriceBounds(
  fare.base_fare,
  fare.estimated_fare,
  distanceKm
);
const explicitBounds = normalizePriceBoundsPair(
  toNumber(fare?.min_price ?? fare?.min_fare ?? fare?.min_fare_amount ?? null),
  toNumber(fare?.max_price ?? fare?.max_fare ?? fare?.max_fare_amount ?? null)
);
const hasExplicitBounds =
  explicitBounds.min_price !== null && explicitBounds.max_price !== null;

item.base_fare =
  roundMoney(toNumber(fare?.base_fare ?? null)) ?? computedBounds.base_fare;
item.estimated_fare =
  roundMoney(toNumber(fare?.estimated_fare ?? null)) ?? computedBounds.estimated_fare;
item.min_price = hasExplicitBounds
  ? explicitBounds.min_price
  : computedBounds.min_price;
item.max_price = hasExplicitBounds
  ? explicitBounds.max_price
  : computedBounds.max_price;

            const driverDistanceM = toNumber(fare.driver_to_pickup_distance_m ?? null);
            const driverDurationS = toNumber(fare.driver_to_pickup_duration_s ?? null);

            // prefer fare API values if present, otherwise keep road-filter values
            if (driverDistanceM !== null) {
              item.driver_to_pickup_distance_m = driverDistanceM;
              item.driver_to_pickup_distance_km = roundMoney(driverDistanceM / 1000);
            }
            if (driverDurationS !== null) {
              item.driver_to_pickup_duration_s = driverDurationS;
              item.driver_to_pickup_duration_min = roundMoney(driverDurationS / 60);
            }
          }
        }
      } catch (e) {
        console.warn("[nearbyVehicleTypes] fare lookup failed:", e?.message || e);
      }
    }

    return result;
  };

  const emitNearbyVehicleTypes = async () => {
    if (!socket.nearbyCenter) return;

    await syncNearbyRadius({});

    const { lat, long } = socket.nearbyCenter;

  try {
    const radiusPlan = resolveActiveNearbyRadiusPlan(
      socket.nearbyVehicleTypesSearchStartedAt
    );
    const rawTypes = await buildNearbyVehicleTypes(lat, long);
    const nearbyTypes = decorateNearbyVehicleTypes(rawTypes, socket.nearbyServiceTypeId);

    const sig = buildVehicleTypesSignature(nearbyTypes);
    const vehicleTypesChanged = sig !== socket.lastVehicleTypesSig;

  nearbyLog("[nearbyVehicleTypes] tick", {
    socket_id: socket.id,
    stage_number: radiusPlan.stageNumber,
    stage_total: radiusPlan.stageTotal,
    stages_m: radiusPlan.stagesMeters,
    road_radius_m: radiusPlan.roadRadius,
    next_radius_m: radiusPlan.nextRadiusMeters,
    timeout_s: radiusPlan.timeoutSeconds,
    elapsed_s: Math.floor(radiusPlan.elapsedMs / 1000),
    types_count: nearbyTypes.length,
      changed: vehicleTypesChanged,
    });

    if (vehicleTypesChanged) {
      socket.lastVehicleTypesSig = sig;
      nearbyLog("[user:nearbyVehicleTypes] emit ->", {
        stage_number: radiusPlan.stageNumber,
        stage_total: radiusPlan.stageTotal,
        road_radius_m: radiusPlan.roadRadius,
        types: summarizeVehicleTypesForLog(nearbyTypes),
      });
      socket.emit("user:nearbyVehicleTypes", nearbyTypes);
    } else {
      nearbyLog("[user:nearbyVehicleTypes] skipped (no change)", {
        stage_number: radiusPlan.stageNumber,
        stage_total: radiusPlan.stageTotal,
        road_radius_m: radiusPlan.roadRadius,
        types_count: nearbyTypes.length,
      });
    }

    const activeRideIdForUser =
      socket.userId && typeof biddingSocket.getActiveRideIdForUser === "function"
        ? toNumber(biddingSocket.getActiveRideIdForUser(socket.userId))
        : null;
    const rideId = toNumber(socket.currentRideId) ?? activeRideIdForUser;
    if (!rideId) return;
    if (!socket.currentRideId) {
      socket.currentRideId = rideId;
    }

    const rideDetails =
      typeof biddingSocket.getRideDetails === "function"
        ? biddingSocket.getRideDetails(rideId)
        : null;

    const userPrice =
      rideDetails?.updatedPrice ??
      rideDetails?.user_bid_price ??
      rideDetails?.min_fare_amount ??
      null;

    const selectedVehicleTypeId = toNumber(rideDetails?.service_type_id ?? null);
    const pricingTypes = decorateNearbyVehicleTypes(
      rawTypes,
      selectedVehicleTypeId ?? socket.nearbyServiceTypeId
    );

    const selectedVehicleType =
      selectedVehicleTypeId !== null
        ? pricingTypes.find((t) => toNumber(t?.service_type_id) === selectedVehicleTypeId) ?? null
        : null;

    const pricingTypesSig = buildVehicleTypesSignature(pricingTypes);

    const snapshotBaseFare =
      toNumber(rideDetails?.base_fare) ??
      toNumber(rideDetails?.ride_details?.base_fare) ??
      toNumber(rideDetails?.meta?.base_fare) ??
      toNumber(rideDetails?.estimated_fare) ??
      toNumber(rideDetails?.ride_details?.estimated_fare) ??
      toNumber(rideDetails?.meta?.estimated_fare) ??
      toNumber(selectedVehicleType?.base_fare) ??
      toNumber(selectedVehicleType?.estimated_fare) ??
      toNumber(rideDetails?.user_bid_price) ??
      toNumber(rideDetails?.min_fare_amount) ??
      null;
    const snapshotEstimatedFare =
      toNumber(rideDetails?.estimated_fare) ??
      toNumber(rideDetails?.ride_details?.estimated_fare) ??
      toNumber(rideDetails?.meta?.estimated_fare) ??
      toNumber(rideDetails?.base_fare) ??
      toNumber(rideDetails?.ride_details?.base_fare) ??
      toNumber(rideDetails?.meta?.base_fare) ??
      toNumber(selectedVehicleType?.estimated_fare) ??
      toNumber(selectedVehicleType?.base_fare) ??
      toNumber(rideDetails?.user_bid_price) ??
      toNumber(rideDetails?.min_fare_amount) ??
      null;

    const snapshotMinPrice =
      toNumber(rideDetails?.min_price) ??
      toNumber(rideDetails?.min_fare) ??
      toNumber(rideDetails?.ride_details?.min_price) ??
      toNumber(rideDetails?.ride_details?.min_fare) ??
      toNumber(rideDetails?.meta?.min_price) ??
      toNumber(rideDetails?.meta?.min_fare) ??
      toNumber(selectedVehicleType?.min_price) ??
      null;

    const snapshotMaxPrice =
      toNumber(rideDetails?.max_price) ??
      toNumber(rideDetails?.max_fare) ??
      toNumber(rideDetails?.ride_details?.max_price) ??
      toNumber(rideDetails?.ride_details?.max_fare) ??
      toNumber(rideDetails?.meta?.max_price) ??
      toNumber(rideDetails?.meta?.max_fare) ??
      toNumber(selectedVehicleType?.max_price) ??
      null;

    const timer = ensureRideTimer(rideId, rideDetails);
    const pricingCustomerOfferTimeout =
      toNumber(rideDetails?.customer_offer_timeout_s) ??
      toNumber(rideDetails?.user_timeout) ??
      timer?.timeout_ms ??
      null;
    const pricingUserTimeout =
      toNumber(rideDetails?.user_timeout) ??
      toNumber(rideDetails?.customer_offer_timeout_s) ??
      timer?.timeout_ms ??
      null;

    const pricingSnapshotSig = JSON.stringify({
      ride_id: rideId,
      server_time: timer?.server_time ?? null,
      expires_at: timer?.expires_at ?? null,
      timeout_ms: timer?.timeout_ms ?? null,
      customer_offer_timeout_s: pricingCustomerOfferTimeout,
      user_timeout: pricingUserTimeout,
      user_bid_price: userPrice ?? null,
      isPriceUpdated: !!rideDetails?.isPriceUpdated,
      updatedPrice: rideDetails?.updatedPrice ?? null,
      updatedAt: rideDetails?.updatedAt ?? null,
      base_fare: snapshotBaseFare ?? null,
      estimated_fare: snapshotEstimatedFare ?? null,
      min_price: snapshotMinPrice ?? null,
      max_price: snapshotMaxPrice ?? null,
      vehicle_types_sig: pricingTypesSig,

      pickup_lat: rideDetails?.pickup_lat ?? socket.nearbyCenter?.lat ?? null,
      pickup_long: rideDetails?.pickup_long ?? socket.nearbyCenter?.long ?? null,
      destination_lat: rideDetails?.destination_lat ?? null,
      destination_long: rideDetails?.destination_long ?? null,
    });

    const prevPricingSnapshotSig = socket.lastPricingSnapshotSigByRide.get(rideId);
    if (prevPricingSnapshotSig === pricingSnapshotSig) {
      return;
    }

    socket.lastPricingSnapshotSigByRide.set(rideId, pricingSnapshotSig);

    let pricingEmitter = io.to(`ride:${rideId}`);
    if (socket.userId) {
      pricingEmitter = pricingEmitter.to(userRoom(socket.userId));
    }

const pricingSnapshotPayload = {
  ride_id: rideId,
  user_id: socket.userId ?? null,
  ...(timer ? timer : {}),
  customer_offer_timeout_s: pricingCustomerOfferTimeout,
  user_timeout: pricingUserTimeout,
  user_bid_price: userPrice,
  base_fare: snapshotBaseFare,
  estimated_fare: snapshotEstimatedFare,
  min_price: snapshotMinPrice,
  max_price: snapshotMaxPrice,
  isPriceUpdated: !!rideDetails?.isPriceUpdated,
  updatedPrice: rideDetails?.updatedPrice ?? null,
  updatedAt: rideDetails?.updatedAt ?? null,
  pickup: {
    lat: rideDetails?.pickup_lat ?? socket.nearbyCenter?.lat ?? null,
    long: rideDetails?.pickup_long ?? socket.nearbyCenter?.long ?? null,
    address: rideDetails?.pickup_address ?? null,
  },
  destination: {
    lat: rideDetails?.destination_lat ?? null,
    long: rideDetails?.destination_long ?? null,
    address: rideDetails?.destination_address ?? null,
  },
  vehicle_types: pricingTypes,
  source: "nearbyVehicleTypes",
  at: Date.now(),
};

savePricingSnapshot(rideId, pricingSnapshotPayload);

pricingEmitter.emit("ride:pricingSnapshot", pricingSnapshotPayload);
    if (DEBUG_EVENTS) {
      console.log("[ride:pricingSnapshot] emitted", {
        ride_id: rideId,
        user_id: socket.userId ?? null,
        socket_id: socket.id,
        vehicle_types: pricingTypes.length,
      });
    }
  } catch (e) {
    console.warn("[nearbyVehicleTypes] emit failed:", e?.message || e);
  }
};

  /////////////////////////////////////////////////////////////
const sendNearby = async (eventName = "user:nearbyDrivers") => {
    if (!socket.nearbyCenter) return;

    await syncNearbyRadius({});

    const { lat, long } = socket.nearbyCenter;
    const radiusPlan = resolveActiveNearbyRadiusPlan(
      socket.nearbyDriversSearchStartedAt
    );
    const roadRadius = radiusPlan.roadRadius;
    const airCandidateRadius = Math.max(
      roadRadius,
      DEFAULT_AIR_CANDIDATE_RADIUS_METERS
    );

    const nearbySnapshot = await getNearbyCandidatesSnapshot({
      lat,
      long,
      roadRadius,
      airCandidateRadius,
      serviceTypeId: socket.nearbyServiceTypeId,
    });
    const nearbyAll = nearbySnapshot.nearbyAll;
    const nearbyPreferenceMatched = nearbySnapshot.nearbyPreferenceMatched;
    const nearbyAvailable = nearbySnapshot.nearbyAvailable;
    const nearby = nearbySnapshot.roadFilteredCandidates;
    const nearbyForEmit = nearbySnapshot.finalCandidates;

    const nearbyWithNormalizedIcons = nearbyForEmit.map((d) => ({
      ...d,
      vehicle_type_icon: normalizeVehicleTypeIconUrl(d?.vehicle_type_icon),
      driver_image: normalizeDriverImageUrl(d?.driver_image),
    }));

    socket.emit(eventName, nearbyWithNormalizedIcons);

    nearbyLog(
      `?? Nearby -> ${nearby.length} drivers within road radius ${roadRadius}m (stage ${radiusPlan.stageNumber}/${radiusPlan.stageTotal})`
    );
    nearbyLog("[nearbyDrivers] filter counters", {
      socket_id: socket.id,
      required_gender: socket.nearbyRequiredGender,
      need_child_seat: socket.nearbyNeedChildSeat,
      need_handicap: socket.nearbyNeedHandicap,
      air_candidates: nearbyAll.length,
      preference_filtered_candidates: nearbyPreferenceMatched.length,
      available_candidates: nearbyAvailable.length,
      road_filtered_candidates: nearby.length,
      emitted_candidates: nearbyWithNormalizedIcons.length,
      using_air_fallback: nearbySnapshot.usingAirFallback,
    });
  };

  const sendNearbyGuarded = async (eventName = "user:nearbyDrivers") => {
    if (socket.nearbyDriversInFlight) return;
    socket.nearbyDriversInFlight = true;
    try {
      await sendNearby(eventName);
    } finally {
      socket.nearbyDriversInFlight = false;
    }
  };

  const emitNearbyVehicleTypesGuarded = async () => {
    if (socket.nearbyVehicleTypesInFlight) return;
    socket.nearbyVehicleTypesInFlight = true;
    try {
      await emitNearbyVehicleTypes();
    } finally {
      socket.nearbyVehicleTypesInFlight = false;
    }
  };

  const emitCachedPricingSnapshotToSocket = (rideId, source = "join-cache") => {
  const safeRideId = toNumber(rideId);
  if (!safeRideId) return false;

  const cached = getPricingSnapshot(safeRideId);
  if (!cached) {
    console.log("[pricingSnapshot][join-cache-miss]", {
      ride_id: safeRideId,
      socket_id: socket.id,
      source,
    });
    return false;
  }

  const nowSec = Math.floor(Date.now() / 1000);
  const expiresAt = toNumber(cached?.expires_at);
  const cachedTimeout = toNumber(cached?.timeout_ms);

  const remainingTimeout =
    expiresAt !== null
      ? Math.max(0, Math.floor(expiresAt - nowSec))
      : cachedTimeout !== null
      ? Math.max(0, Math.floor(cachedTimeout))
      : null;

  const payload = {
    ...cached,
    ride_id: safeRideId,
    user_id: toNumber(cached?.user_id) ?? toNumber(socket.userId) ?? null,

    ...(expiresAt !== null
      ? {
          server_time: nowSec,
          expires_at: expiresAt,
          timeout_ms: remainingTimeout,
          customer_offer_timeout_s: remainingTimeout,
          user_timeout: remainingTimeout,
        }
      : {}),

    source,
    cached: 1,
    recalculated: 0,
    at: Date.now(),
  };

  socket.emit("ride:pricingSnapshot", payload);

  console.log("[pricingSnapshot][join-cache-emit]", {
    ride_id: safeRideId,
    user_id: payload.user_id ?? null,
    socket_id: socket.id,
    source,
    timeout_ms: payload.timeout_ms ?? null,
    vehicle_types: Array.isArray(payload.vehicle_types)
      ? payload.vehicle_types.length
      : 0,
  });

  return true;
};

  socket.on("user:findNearbyDrivers", async (payload = {}) => {
    debugLog("user:findNearbyDrivers", payload, socket.id);
    nearbyLog("?? payload from frontend:", payload);
    const { user_id, lat, long, service_type_id } = payload;
    const nearbyToken = normalizeToken(
      payload?.access_token ?? payload?.token ?? payload?.user_token ?? null
    );
    const resolvedUserId =
      toNumber(user_id) ?? resolvePayloadUserId(payload) ?? toNumber(socket.userId);

    switchSocketUserSession(resolvedUserId, nearbyToken, "user:findNearbyDrivers");
    applyNearbyFiltersFromPayload(payload, { resetMissing: true });
    syncRideContextFromPayload(payload, "user:findNearbyDrivers");

    const details = extractUserDetails(payload, "user:findNearbyDrivers");
    const routeKm = extractRouteDistanceKm(payload);
    const routeDurationMin = extractRouteDurationMin(payload);
    const etaMin = toNumber(payload?.eta_min ?? null) ?? routeDurationMin;
    if (details) {
      if (routeKm !== null) details.route = routeKm;
      if (etaMin !== null) details.eta_min = etaMin;
      if (socket.userToken) {
        details.user_token = socket.userToken;
        details.token = socket.userToken;
        details.access_token = socket.userToken;
      }

      setUserDetails(details.user_id, details);
      if (typeof biddingSocket.refreshUserDetailsForUserId === "function") {
        biddingSocket.refreshUserDetailsForUserId(io, details.user_id, details);
      }
    }
    persistRideRouteMetrics(payload, routeKm, routeDurationMin, etaMin);
    emitRouteEtaToDriver(routeKm, etaMin, payload);

    socket.nearbyRadius = DEFAULT_NEARBY_RADIUS_METERS;
    socket.nearbyDispatchStagesMeters = [DEFAULT_NEARBY_RADIUS_METERS];
    socket.nearbyDispatchTimeoutS = NEARBY_DISPATCH_TIMEOUT_S;
    socket.nearbyDriversSearchStartedAt = Date.now();

    const la = toNumber(lat);
    const lo = toNumber(long);
    if (la === null || lo === null) return;

    socket.nearbyCenter = { lat: la, long: lo };

    const st = toNumber(service_type_id);
    socket.nearbyServiceTypeId = st === null ? null : st;

    const sc = extractServiceCategoryIdFromPayload(payload);
    if (sc !== null) setNearbyServiceCategoryId(sc, "user:findNearbyDrivers");

    await syncNearbyRadius(payload);

    if (routeKm !== null) {
      setNearbyRouteDistanceKm(routeKm);
    }
    if (routeDurationMin !== null) {
      setNearbyRouteDurationMin(routeDurationMin);
    }

    socket.lastVehicleTypesSig = null;

    await sendNearbyGuarded("user:nearbyDrivers");

    stopNearbyDriversLoop();
    socket.nearbyDriversInterval = setInterval(() => {
      void sendNearbyGuarded("user:nearbyDrivers:update");
    }, NEARBY_EVERY_MS);
  });

  //////////////////////////////////////////////////////////

const handleGetNearbyVehicleTypes = async (payload = {}) => {
  const { lat, long } = payload;
  debugLog("user:getNearbyVehicleTypes", { lat, long }, socket.id);
  nearbyLog("[user:getNearbyVehicleTypes] payload:", payload);

  const nearbyTypesToken = normalizeToken(
    payload?.access_token ?? payload?.token ?? payload?.user_token ?? null
  );

  const resolvedUserId =
    resolvePayloadUserId(payload) ?? toNumber(payload?.user_id) ?? toNumber(socket.userId);
  switchSocketUserSession(resolvedUserId, nearbyTypesToken, "user:getNearbyVehicleTypes");

  const la = toNumber(lat);
  const lo = toNumber(long);
  if (la === null || lo === null) return;
  const nextCenter = { lat: la, long: lo };
  const previousCenter = socket.nearbyCenter;
  const centerShiftMeters = getNearbyCenterShiftMeters(previousCenter, nextCenter);
  const centerChanged =
    previousCenter == null ||
    centerShiftMeters === null ||
    centerShiftMeters > NEARBY_CENTER_RESET_THRESHOLD_M;

  applyNearbyFiltersFromPayload(payload, { resetMissing: false });
  syncRideContextFromPayload(payload, "user:getNearbyVehicleTypes");

  const payloadServiceTypeId = toNumber(
    payload?.service_type_id ??
      payload?.serviceTypeId ??
      payload?.selected_service_type_id ??
      payload?.selectedServiceTypeId ??
      null
  );
  if (payloadServiceTypeId !== null) {
    socket.nearbyServiceTypeId = payloadServiceTypeId;
  }

  const details = extractUserDetails(payload, "user:getNearbyVehicleTypes");
  const routeKm = extractRouteDistanceKm(payload);
  const routeDurationMin = extractRouteDurationMin(payload);
  const etaMin = toNumber(payload?.eta_min ?? null) ?? routeDurationMin;

  if (details) {
    if (routeKm !== null) details.route = routeKm;
    if (etaMin !== null) details.eta_min = etaMin;
    if (socket.userToken) {
      details.user_token = socket.userToken;
      details.token = socket.userToken;
      details.access_token = socket.userToken;
    }
    setUserDetails(details.user_id, details);
  }

  persistRideRouteMetrics(payload, routeKm, routeDurationMin, etaMin);
  emitRouteEtaToDriver(routeKm, etaMin, payload);

  const sc = extractServiceCategoryIdFromPayload(payload);
  if (sc !== null) setNearbyServiceCategoryId(sc, "user:getNearbyVehicleTypes");
  if (sc === null && normalizeServiceCategoryId(socket.nearbyServiceCategoryId) === null) {
    const inferredSc = inferServiceCategoryIdFromNearbyMemory(
      la,
      lo,
      socket.nearbyServiceTypeId
    );
    if (inferredSc !== null) {
      setNearbyServiceCategoryId(inferredSc, "user:getNearbyVehicleTypes:nearby-memory");
      nearbyLog("[user:getNearbyVehicleTypes] inferred service_category_id", {
        socket_id: socket.id,
        service_category_id: inferredSc,
        service_type_id: socket.nearbyServiceTypeId ?? null,
      });
    } else {
      nearbyLog("[user:getNearbyVehicleTypes] could not infer service_category_id", {
        socket_id: socket.id,
        service_type_id: socket.nearbyServiceTypeId ?? null,
      });
    }
  }

  // نمرر الـ payload هنا إلى `syncNearbyRadius` للحصول على الراديوس من API
  await syncNearbyRadius(payload);

  if (routeKm !== null) {
    setNearbyRouteDistanceKm(routeKm);
  }
  if (routeDurationMin !== null) {
    setNearbyRouteDurationMin(routeDurationMin);
  }

  if (centerChanged || socket.nearbyVehicleTypesSearchStartedAt === null) {
    const now = Date.now();
    socket.nearbyVehicleTypesSearchStartedAt = now;
    if (centerChanged || socket.nearbyDriversSearchStartedAt === null) {
      socket.nearbyDriversSearchStartedAt = now;
    }
  }

  if (centerChanged) {
    socket.lastVehicleTypesSig = null;
  }
  socket.nearbyCenter = nextCenter;

  const resolvedServiceCategoryId =
    normalizeServiceCategoryId(socket.nearbyServiceCategoryId) ??
    getNearbyServiceCategoryFromRideSnapshot();
  nearbyLog("[user:getNearbyVehicleTypes] expansion config", {
    socket_id: socket.id,
    service_category_id: resolvedServiceCategoryId ?? null,
    center_changed: centerChanged,
    center_shift_m: centerShiftMeters,
    center_reset_threshold_m: NEARBY_CENTER_RESET_THRESHOLD_M,
    dispatch_stage_list_m: socket.nearbyDispatchStagesMeters,
    dispatch_stages_m: socket.nearbyDispatchStagesMeters,
    dispatch_timeout_s: socket.nearbyDispatchTimeoutS,
    base_radius_m: socket.nearbyRadius,
  });

  await emitNearbyVehicleTypesGuarded();
  await sendNearbyGuarded("user:nearbyDrivers:update");

  stopNearbyVehicleTypesLoop();
  socket.nearbyVehicleTypesInterval = setInterval(() => {
    void emitNearbyVehicleTypesGuarded();
  }, NEARBY_EVERY_MS);
  nearbyLog("[nearbyVehicleTypes] loop started", {
    socket_id: socket.id,
    every_ms: NEARBY_EVERY_MS,
    dispatch_timeout_s: socket.nearbyDispatchTimeoutS,
    dispatch_stages_m: socket.nearbyDispatchStagesMeters,
  });

  if (!socket.nearbyDriversInterval) {
    socket.nearbyDriversInterval = setInterval(() => {
      void sendNearbyGuarded("user:nearbyDrivers:update");
    }, NEARBY_EVERY_MS);
    nearbyLog("[nearbyDrivers] loop started via vehicle-types", {
      socket_id: socket.id,
      every_ms: NEARBY_EVERY_MS,
      dispatch_timeout_s: socket.nearbyDispatchTimeoutS,
      dispatch_stages_m: socket.nearbyDispatchStagesMeters,
    });
  }

  nearbyLog(`?? Nearby vehicle types requested (socket:${socket.id})`);
};

  socket.on("user:getNearbyVehicleTypes", handleGetNearbyVehicleTypes);
  socket.on("user:getnearByVichleType", handleGetNearbyVehicleTypes);
  socket.on("getUserNearbyVecletype", handleGetNearbyVehicleTypes);

  socket.on("user:updateNearbyCenter", async (payload = {}) => {
    const { lat, long } = payload;
    debugLog("user:updateNearbyCenter", { lat, long }, socket.id);
    if (!socket.nearbyCenter) return;

    const la = toNumber(lat);
    const lo = toNumber(long);
    if (la === null || lo === null) return;
    const nextCenter = { lat: la, long: lo };
    const centerShiftMeters = getNearbyCenterShiftMeters(socket.nearbyCenter, nextCenter);
    const centerChanged =
      centerShiftMeters === null ||
      centerShiftMeters > NEARBY_CENTER_RESET_THRESHOLD_M;
    applyNearbyFiltersFromPayload(payload, { resetMissing: false });
    syncRideContextFromPayload(payload, "user:updateNearbyCenter");

    socket.nearbyCenter = nextCenter;
    if (centerChanged) {
      socket.nearbyDriversSearchStartedAt = Date.now();
      socket.nearbyVehicleTypesSearchStartedAt = Date.now();
      socket.lastVehicleTypesSig = null;
    }
    console.log("[nearby-center] update", {
      socket_id: socket.id,
      center_changed: centerChanged,
      center_shift_m: centerShiftMeters,
      center_reset_threshold_m: NEARBY_CENTER_RESET_THRESHOLD_M,
    });

    const sc = extractServiceCategoryIdFromPayload(payload);
    if (sc !== null) setNearbyServiceCategoryId(sc, "user:updateNearbyCenter");

    await syncNearbyRadius(payload);

    const routeKm = extractRouteDistanceKm(payload);
    const routeDurationMin = extractRouteDurationMin(payload);
    const etaMin = toNumber(payload?.eta_min ?? null) ?? routeDurationMin;
    if (routeKm !== null) {
      setNearbyRouteDistanceKm(routeKm);
    }
    if (routeDurationMin !== null) {
      setNearbyRouteDurationMin(routeDurationMin);
    }
    persistRideRouteMetrics(payload, routeKm, routeDurationMin, etaMin);
    emitRouteEtaToDriver(routeKm, etaMin, payload);

    void sendNearbyGuarded("user:nearbyDrivers:update");
    await emitNearbyVehicleTypesGuarded();
  });

  socket.on("user:setNearbyServiceType", async ({ service_type_id }) => {
    debugLog("user:setNearbyServiceType", { service_type_id }, socket.id);
    const st = toNumber(service_type_id);
    socket.nearbyServiceTypeId = st === null ? null : st;

    socket.nearbyRadius = DEFAULT_NEARBY_RADIUS_METERS;
    socket.nearbyDispatchStagesMeters = [DEFAULT_NEARBY_RADIUS_METERS];
    socket.nearbyDispatchTimeoutS = NEARBY_DISPATCH_TIMEOUT_S;
    socket.nearbyDriversSearchStartedAt = Date.now();
    socket.nearbyVehicleTypesSearchStartedAt = Date.now();
    socket.lastVehicleTypesSig = null;

    await syncNearbyRadius({});

    void sendNearbyGuarded("user:nearbyDrivers:update");

    if (socket.nearbyCenter) {
      await emitNearbyVehicleTypesGuarded();
    }
  });

  socket.on("user:stopNearbyDrivers", () => {
    debugLog("user:stopNearbyDrivers", {}, socket.id);
    if (socket.currentRideId) {
      socket.lastPricingSnapshotSigByRide.delete(socket.currentRideId);
    }
    stopNearbyDriversLoop();
    socket.nearbyDriversSearchStartedAt = null;

    // Keep vehicle-type search active if it is running independently.
    if (socket.nearbyVehicleTypesInterval) {
      return;
    }

    socket.nearbyCenter = null;
    socket.nearbyServiceTypeId = null;
    socket.nearbyServiceCategoryId = null;
    socket.nearbyRouteDistanceKm = null;
    socket.nearbyRouteDurationMin = null;
    socket.nearbyRequiredGender = null;
    socket.nearbyNeedChildSeat = null;
    socket.nearbyNeedHandicap = null;
    socket.nearbyRadius = DEFAULT_NEARBY_RADIUS_METERS;
    socket.nearbyDispatchStagesMeters = [DEFAULT_NEARBY_RADIUS_METERS];
    socket.nearbyDispatchTimeoutS = NEARBY_DISPATCH_TIMEOUT_S;
    socket.nearbyVehicleTypesSearchStartedAt = null;
    socket.nearbyFareCache.clear();
    socket.lastVehicleTypesSig = null;
    socket.currentRideId = null;
  });

  const clearSocketUserSession = (source = "user:logout") => {
    const previousUserId = toNumber(socket.userId);
    const previousRideId = toNumber(socket.currentRideId);
    const previousUserRoom = previousUserId ? userRoom(previousUserId) : null;

    if (previousRideId) {
      socket.leave(`ride:${previousRideId}`);
    }
    if (previousUserRoom) {
      socket.leave(previousUserRoom);
    }

    resetNearbyStateForUserSwitch();
    socket.currentRideId = null;
    socket.isUser = false;
    socket.userToken = null;
    socket.userId = null;

    if (previousUserId && previousUserRoom) {
      const roomMembers = io?.sockets?.adapter?.rooms?.get(previousUserRoom);
      if (!roomMembers || roomMembers.size === 0) {
        deleteUserDetails(previousUserId);
      }
    }

    console.log(
      `[${source}] cleared socket session ${socket.id} (user:${previousUserId ?? "unknown"})`
    );
  };

  socket.on("user:logout", (payload = {}) => {
    debugLog("user:logout", payload, socket.id);
    clearSocketUserSession("user:logout");
  });

  socket.on("disconnect", () => {
    debugLog("disconnect", {}, socket.id);
    const disconnectedUserId = toNumber(socket.userId);
    const disconnectedUserRoom = disconnectedUserId
      ? userRoom(disconnectedUserId)
      : null;
    stopNearby();
    socket.lastPricingSnapshotSigByRide.clear();
    socket.userToken = null;
    socket.currentRideId = null;
    socket.isUser = false;

    if (disconnectedUserId && disconnectedUserRoom) {
      const roomMembers = io?.sockets?.adapter?.rooms?.get(disconnectedUserRoom);
      if (!roomMembers || roomMembers.size === 0) {
        deleteUserDetails(disconnectedUserId);
      }
    }
    socket.userId = null;
  });
};

setupUserSocket.invalidateNearbyServiceSearchRadiusCache = invalidateServiceSearchRadiusCache;

module.exports = setupUserSocket;
