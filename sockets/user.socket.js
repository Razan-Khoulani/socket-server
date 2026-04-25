// sockets/user.socket.js
const driverLocationService = require("../services/driverLocation.service");
const axios = require("axios");
const {
  setUserDetails,
  getUserDetailsByToken,
  deleteUserDetails,
} = require("../store/users.store");
const { getRideStatusSnapshot } = require("../store/rideStatusSnapshots.store");

// ? UPDATED: add getActiveRideByDriver so we can exclude busy drivers from nearby/types
const { getActiveDriverByRide, getActiveRideByDriver } = require("../store/activeRides.store");

const biddingSocket = require("./bidding.socket");

const DEBUG_EVENTS = process.env.DEBUG_SOCKET_EVENTS === "1";
const debugLog = (event, payload, socketId) => {
  if (!DEBUG_EVENTS) return;
  console.log("[user.socket]", event, "socket:", socketId, "payload:", payload);
};
const userRoom = (userId) => `user:${userId}`;

const DEFAULT_NEARBY_RADIUS_METERS = 5000;
const NEARBY_EVERY_MS = 3000;
const MAX_DRIVER_LOCATION_AGE_MS = 2 * 60 * 1000;
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
const NEARBY_FARE_CACHE_TTL_MS = Number.isFinite(
  Number(process.env.NEARBY_FARE_CACHE_TTL_MS)
)
  ? Math.max(0, Number(process.env.NEARBY_FARE_CACHE_TTL_MS))
  : 5000;
const serviceSearchRadiusCache = new Map();

const LARAVEL_BASE_URL =
  process.env.LARAVEL_BASE_URL ||
  process.env.LARAVEL_URL ||
  "https://osbackend.gocab.net";

const LARAVEL_SERVICE_SEARCH_RADIUS_PATH =
  process.env.LARAVEL_SERVICE_SEARCH_RADIUS_PATH ||
  "/api/customer/transport/search-radius";
const LARAVEL_GET_ROUTE_PATH =
  process.env.LARAVEL_GET_ROUTE_PATH || "/api/getRoute";

const LARAVEL_TIMEOUT_MS = 7000;
const VEHICLE_ICON_RELATIVE_DIR = "assets/images/service-category/transport-service-type";
const DRIVER_IMAGE_RELATIVE_DIR = "assets/images/profile-images/provider";

// ? NEW: keep timer unit aligned with bidding.socket (SECONDS)
const RIDE_TIMEOUT_S = 90;

const toNumber = (v) => {
  if (v === null || v === undefined || v === "") return null;
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
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

const getFirstNumber = (...values) => {
  for (const value of values) {
    const parsed = toNumber(value);
    if (parsed !== null) return parsed;
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

const resolveNearbyRadiusFromPayload = (payload = {}) => {
  if (!payload || typeof payload !== "object") return null;

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

const getCachedServiceSearchRadius = (serviceCategoryId) => {
  const safeServiceCategoryId = toNumber(serviceCategoryId);
  if (!safeServiceCategoryId) return null;

  const entry = serviceSearchRadiusCache.get(safeServiceCategoryId);
  if (!entry) return null;

  if (Date.now() - entry.cached_at > SERVICE_RADIUS_CACHE_TTL_MS) {
    serviceSearchRadiusCache.delete(safeServiceCategoryId);
    return null;
  }

  return entry.radius_m;
};

const setCachedServiceSearchRadius = (serviceCategoryId, radiusMeters) => {
  const safeServiceCategoryId = toNumber(serviceCategoryId);
  const safeRadiusMeters = toNumber(radiusMeters);
  if (!safeServiceCategoryId || safeRadiusMeters === null) return;

  serviceSearchRadiusCache.set(safeServiceCategoryId, {
    radius_m: normalizeNearbyRadiusMeters(safeRadiusMeters),
    cached_at: Date.now(),
  });
};

const fetchServiceSearchRadiusFromApi = async (serviceCategoryId) => {
  const safeServiceCategoryId = toNumber(serviceCategoryId);
  if (!safeServiceCategoryId) return null;

  const cachedRadius = getCachedServiceSearchRadius(safeServiceCategoryId);
  if (cachedRadius !== null) return cachedRadius;

  try {
    const res = await axios.post(
      `${LARAVEL_BASE_URL}${LARAVEL_SERVICE_SEARCH_RADIUS_PATH}`,
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

    const radiusMeters = resolveNearbyRadiusFromPayload(data);
    if (radiusMeters === null) return null;

    setCachedServiceSearchRadius(safeServiceCategoryId, radiusMeters);
    return radiusMeters;
  } catch (e) {
    console.warn(
      "[service-search-radius] failed:",
      e?.response?.data || e?.message || e
    );
    return null;
  }
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
  const distance = toNumber(distanceKm);
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
        ? roundMoney(distance !== null && distance <= 1 ? anchor : anchor * 0.7)
        : null,
    max_price: anchor !== null ? roundMoney(anchor * 2) : null,
  };
};

const toBinaryFlag = (v) => {
  const n = toNumber(v);
  return n === 0 || n === 1 ? n : null;
};

const toGenderFilter = (v) => {
  const n = toNumber(v);
  return n === 1 || n === 2 ? n : null;
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

const normalizePublicAssetUrl = (value, defaultRelativeDir = "") => {
  if (value == null) return "";

  const raw = String(value).trim().replace(/\\/g, "/");
  if (!raw) return "";

  if (/^data:/i.test(raw)) return raw;

  const base = String(LARAVEL_BASE_URL || "").trim().replace(/\/+$/, "");

  if (/^https?:\/\//i.test(raw)) {
    if (base.startsWith("https://") && raw.startsWith("http://")) {
      try {
        const baseUrl = new URL(base);
        const iconUrl = new URL(raw);
        if (iconUrl.hostname === baseUrl.hostname) {
          iconUrl.protocol = "https:";
          return iconUrl.toString();
        }
      } catch (_) {}
    }
    return raw;
  }

  if (raw.startsWith("//")) {
    const protocol = base.startsWith("https://") ? "https:" : "http:";
    return `${protocol}${raw}`;
  }

  const cleaned = raw.replace(/^\.\/+/, "");
  const assetsIndex = cleaned.indexOf("assets/");
  if (assetsIndex >= 0) {
    const rel = cleaned.slice(assetsIndex).replace(/^\/+/, "");
    return base ? `${base}/${rel}` : `/${rel}`;
  }

  if (!cleaned.includes("/")) {
    if (!defaultRelativeDir) return base ? `${base}/${cleaned}` : cleaned;
    return base ? `${base}/${defaultRelativeDir}/${cleaned}` : `${defaultRelativeDir}/${cleaned}`;
  }

  const rel = cleaned.replace(/^\/+/, "");
  return base ? `${base}/${rel}` : `/${rel}`;
};

const normalizeVehicleTypeIconUrl = (value) =>
  normalizePublicAssetUrl(value, VEHICLE_ICON_RELATIVE_DIR);

const normalizeDriverImageUrl = (value) =>
  normalizePublicAssetUrl(value, DRIVER_IMAGE_RELATIVE_DIR);

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
  pickupLong = null,
  userId = null
) => {
  if (!serviceCategoryId || distanceKm === null) return new Map();
  try {
    const payload = {
      service_category_id: serviceCategoryId,
      distance_km: distanceKm,
      vehicle_type_ids: vehicleTypeIds,
    };
    if (toNumber(userId)) {
      payload.user_id = toNumber(userId);
    }
    if (pickupLat !== null && pickupLong !== null) {
      payload.pickup_lat = pickupLat;
      payload.pickup_long = pickupLong;
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
      console.log("[vehicle-fares-api] ok", {
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
    console.warn("[vehicle-fares-api] failed:", e?.response?.data || e?.message || e);
    return new Map();
  }
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

  if (la1 === null || lo1 === null || la2 === null || lo2 === null) {
    return {
      road_distance_m: null,
      road_duration_s: null,
      road_duration_min: null,
      raw: null,
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
      timeout: LARAVEL_TIMEOUT_MS,
    });

    const data = res?.data ?? null;
    const roadDistanceM = extractRoadDistanceMeters(data);
    const durationMin = toNumber(data?.duration ?? null);
    const durationS = durationMin !== null ? Math.round(durationMin * 60) : null;

    return {
      road_distance_m: roadDistanceM,
      road_duration_s: durationS,
      road_duration_min: durationMin !== null ? roundMoney(durationMin) : null,
      raw: data,
    };
  } catch (e) {
    console.warn("[driverToPickupRoadMetrics] failed:", e?.response?.data || e?.message || e);
    return {
      road_distance_m: null,
      road_duration_s: null,
      road_duration_min: null,
      raw: null,
    };
  }
};

// ? NEW: final road-based filtering
const filterDriversByRoadRadius = async (drivers, pickupLat, pickupLong, roadRadiusM) => {
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
        driver_to_pickup_distance_km: roundMoney(metrics.road_distance_m / 1000),
        driver_to_pickup_duration_s: metrics.road_duration_s,
        driver_to_pickup_duration_min: metrics.road_duration_min,
      };
    })
  );

  return results.filter(Boolean);
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

const extractUserDetails = (payload) => {
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
  const userImage =
    src?.profile_image ?? src?.user_image ?? src?.image ?? src?.avatar ?? null;

  return {
    user_id: userId,
    user_name: userName,
    user_gender: userGender,
    user_token: userToken,
    user_phone: contactNumber,
    user_country_code: countryCode,
    user_phone_full: contactNumber && countryCode ? `${countryCode}${contactNumber}` : null,
    user_image: userImage,
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

module.exports = (io, socket) => {
  socket.isUser = false;
  socket.userId = null;
  socket.userToken = null;

  socket.nearbyCenter = null; // { lat, long }
  socket.nearbyInterval = null;
  socket.nearbyRadius = DEFAULT_NEARBY_RADIUS_METERS;

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
        console.log("[nearby-service-category] payload/snapshot mismatch", {
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
    const serviceCategoryId =
      normalizeServiceCategoryId(socket.nearbyServiceCategoryId) ??
      snapshotServiceCategoryId;

    let nextRadius = resolveNearbyRadiusFromPayload(payload);
    let source = nextRadius !== null ? "payload" : null;

    if (nextRadius === null) {
      nextRadius = getNearbyRadiusFromRideSnapshot();
      if (nextRadius !== null) source = "ride-snapshot";
    }

    if (nextRadius === null && serviceCategoryId) {
      nextRadius = await fetchServiceSearchRadiusFromApi(serviceCategoryId);
      if (nextRadius !== null) source = "service-setting-api";
    }

    const normalizedRadius = normalizeNearbyRadiusMeters(
      nextRadius,
      socket.nearbyRadius ?? DEFAULT_NEARBY_RADIUS_METERS
    );

    if (socket.nearbyRadius !== normalizedRadius) {
      socket.nearbyRadius = normalizedRadius;
      console.log("[nearby-radius] updated", {
        service_category_id: serviceCategoryId ?? null,
        radius_m: normalizedRadius,
        source: source ?? "fallback",
      });
    }

    return normalizedRadius;
  };

  const applyNearbyFiltersFromPayload = (payload = {}, options = {}) => {
    const { resetMissing = false } = options || {};
    let changed = false;

    const hasGender =
      payload?.required_driver_gender !== undefined ||
      payload?.required_gender !== undefined ||
      payload?.driver_gender !== undefined ||
      payload?.gender !== undefined;
    const nextGender = hasGender
      ? toGenderFilter(
          payload?.required_driver_gender ??
            payload?.required_gender ??
            payload?.driver_gender ??
            payload?.gender
        )
      : resetMissing
      ? null
      : socket.nearbyRequiredGender;

    if (nextGender !== socket.nearbyRequiredGender) {
      socket.nearbyRequiredGender = nextGender;
      changed = true;
    }

    const hasChildSeat =
      payload?.need_child_seat !== undefined ||
      payload?.child_seat !== undefined ||
      payload?.require_child_seat !== undefined ||
      payload?.smoking !== undefined;
    const nextChildSeat = hasChildSeat
      ? toBinaryFlag(
          payload?.need_child_seat ??
            payload?.child_seat ??
            payload?.require_child_seat ??
            payload?.smoking
        )
      : resetMissing
      ? null
      : socket.nearbyNeedChildSeat;

    if (nextChildSeat !== socket.nearbyNeedChildSeat) {
      socket.nearbyNeedChildSeat = nextChildSeat;
      changed = true;
    }

    const hasHandicap =
      payload?.need_handicap !== undefined ||
      payload?.handicap !== undefined ||
      payload?.require_handicap !== undefined;
    const nextHandicap = hasHandicap
      ? toBinaryFlag(
          payload?.need_handicap ??
            payload?.handicap ??
            payload?.require_handicap
        )
      : resetMissing
      ? null
      : socket.nearbyNeedHandicap;

    if (nextHandicap !== socket.nearbyNeedHandicap) {
      socket.nearbyNeedHandicap = nextHandicap;
      changed = true;
    }

    if (changed) {
      socket.lastVehicleTypesSig = null;
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
    const details = extractUserDetails(payload);
    if (!details) {
      console.warn(`[${source}] Missing user_id in payload`);
      return;
    }

    const loginToken = normalizeToken(
      details?.user_token ??
        payload?.access_token ??
        payload?.token ??
        payload?.user_token ??
        null
    );
    switchSocketUserSession(details.user_id, loginToken, source);

    const normalizedDetails = {
      ...details,
      user_id: toNumber(details.user_id),
      ...(socket.userToken
        ? {
            user_token: socket.userToken,
            token: socket.userToken,
            access_token: socket.userToken,
          }
        : {}),
    };
    setUserDetails(normalizedDetails.user_id, normalizedDetails);

    if (typeof biddingSocket.refreshUserDetailsForUserId === "function") {
      biddingSocket.refreshUserDetailsForUserId(io, normalizedDetails.user_id, normalizedDetails);
    }

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
      void emitNearbyVehicleTypes();
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

    const catchupPayload = {
      ride_id: safeRideId,
      ride_status: rideStatus,
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
      `[${source}] catchup status -> ride:${safeRideId} status:${catchupPayload.ride_status}`
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
    ensureNearbyCenterFromRide(rideId);
    void emitNearbyVehicleTypes();

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
    ensureNearbyCenterFromRide(rideId);
    void emitNearbyVehicleTypes();

    console.log(`?? User ${userId} rejoined ride room ride:${rideId} (socket:${socket.id})`);
  });

  const stopNearby = () => {
    if (socket.nearbyInterval) {
      clearInterval(socket.nearbyInterval);
      socket.nearbyInterval = null;
    }
  };

  const buildNearbyVehicleTypes = async (lat, long) => {
    const roadRadius = normalizeNearbyRadiusMeters(
      socket.nearbyRadius,
      DEFAULT_NEARBY_RADIUS_METERS
    );
    const airCandidateRadius = Math.max(
      roadRadius,
      DEFAULT_AIR_CANDIDATE_RADIUS_METERS
    );

    const nearbyAll = driverLocationService.getNearbyDriversFromMemory(
      lat,
      long,
      airCandidateRadius,
      {
        only_online: true,
        max_age_ms: MAX_DRIVER_LOCATION_AGE_MS,
        required_gender: socket.nearbyRequiredGender,
        need_child_seat: socket.nearbyNeedChildSeat,
        need_handicap: socket.nearbyNeedHandicap,
      }
    );

    const nearbyAvailable = nearbyAll.filter((d) => {
      const dId = toNumber(d?.driver_id);
      return canDriverAppearInNearby(dId);
    });

    const nearbyDrivers = await filterDriversByRoadRadius(
      nearbyAvailable,
      lat,
      long,
      roadRadius
    );

    const typesMap = new Map();

    for (const d of nearbyDrivers) {
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

        for (const [serviceCatId, typeSet] of groups.entries()) {
          const currentUserId = toNumber(socket.userId);
          let cacheEntry = socket.nearbyFareCache.get(serviceCatId);
          const cacheKeyChanged =
            !cacheEntry ||
            cacheEntry.distanceKm !== distanceKm ||
            cacheEntry.pickupLat !== pickupLat ||
            cacheEntry.pickupLong !== pickupLong ||
            cacheEntry.userId !== currentUserId;

          if (cacheKeyChanged) {
            cacheEntry = {
              distanceKm,
              pickupLat,
              pickupLong,
              userId: currentUserId,
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

          const missingTypeIds = [];
          for (const typeId of typeSet) {
            if (!cacheEntry.map.has(typeId)) missingTypeIds.push(typeId);
          }

          const typeIdsToRefresh = cacheExpired
            ? Array.from(typeSet)
            : missingTypeIds;

          if (typeIdsToRefresh.length > 0) {
            const fareMap = await fetchVehicleFaresFromApi(
              serviceCatId,
              distanceKm,
              typeIdsToRefresh,
              pickupLat,
              pickupLong,
              socket.userId
            );
            for (const [id, item] of fareMap.entries()) {
              cacheEntry.map.set(id, item);
            }
            if (fareMap.size > 0) {
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

const priceBounds = buildPriceBounds(
  fare.base_fare,
  fare.estimated_fare,
  distanceKm
);
item.base_fare = priceBounds.base_fare;
item.estimated_fare = priceBounds.estimated_fare;
item.min_price = priceBounds.min_price;
item.max_price = priceBounds.max_price;

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
  const { lat, long } = socket.nearbyCenter;

  try {
    const rawTypes = await buildNearbyVehicleTypes(lat, long);
    const nearbyTypes = decorateNearbyVehicleTypes(rawTypes, socket.nearbyServiceTypeId);

    const sig = buildVehicleTypesSignature(nearbyTypes);
    const vehicleTypesChanged = sig !== socket.lastVehicleTypesSig;

    if (vehicleTypesChanged) {
      socket.lastVehicleTypesSig = sig;
      console.log("[user:nearbyVehicleTypes] emit ->", summarizeVehicleTypesForLog(nearbyTypes));
      socket.emit("user:nearbyVehicleTypes", nearbyTypes);
    } else if (DEBUG_EVENTS) {
      console.log("[user:nearbyVehicleTypes] skipped (no change)");
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
      toNumber(selectedVehicleType?.base_fare) ??
      toNumber(selectedVehicleType?.estimated_fare) ??
      toNumber(rideDetails?.base_fare) ??
      toNumber(rideDetails?.estimated_fare) ??
      toNumber(rideDetails?.user_bid_price) ??
      toNumber(rideDetails?.min_fare_amount) ??
      null;
    const snapshotEstimatedFare =
      toNumber(selectedVehicleType?.estimated_fare) ??
      toNumber(selectedVehicleType?.base_fare) ??
      toNumber(rideDetails?.estimated_fare) ??
      toNumber(rideDetails?.base_fare) ??
      toNumber(rideDetails?.user_bid_price) ??
      toNumber(rideDetails?.min_fare_amount) ??
      null;

    const snapshotMinPrice =
      toNumber(selectedVehicleType?.min_price) ??
      toNumber(rideDetails?.min_price) ??
      null;

    const snapshotMaxPrice =
      toNumber(selectedVehicleType?.max_price) ??
      toNumber(rideDetails?.max_price) ??
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

    pricingEmitter.emit("ride:pricingSnapshot", {
      ride_id: rideId,
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
      at: Date.now(),
    });
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

    const { lat, long } = socket.nearbyCenter;
    const roadRadius = normalizeNearbyRadiusMeters(
      socket.nearbyRadius,
      DEFAULT_NEARBY_RADIUS_METERS
    );
    const airCandidateRadius = Math.max(
      roadRadius,
      DEFAULT_AIR_CANDIDATE_RADIUS_METERS
    );

    const opts = { only_online: true };
    if (socket.nearbyServiceTypeId !== null) {
      opts.service_type_id = socket.nearbyServiceTypeId;
    }
    if (socket.nearbyRequiredGender !== null) {
      opts.required_gender = socket.nearbyRequiredGender;
    }
    if (socket.nearbyNeedChildSeat !== null) {
      opts.need_child_seat = socket.nearbyNeedChildSeat;
    }
    if (socket.nearbyNeedHandicap !== null) {
      opts.need_handicap = socket.nearbyNeedHandicap;
    }

    const nearbyAll = driverLocationService.getNearbyDriversFromMemory(
      lat,
      long,
      airCandidateRadius,
      {
        ...opts,
        max_age_ms: MAX_DRIVER_LOCATION_AGE_MS,
      }
    );

    const nearbyAvailable = nearbyAll.filter((d) => {
      const dId = toNumber(d?.driver_id);
      return canDriverAppearInNearby(dId);
    });

    const nearby = await filterDriversByRoadRadius(
      nearbyAvailable,
      lat,
      long,
      roadRadius
    );

    const nearbyWithNormalizedIcons = nearby.map((d) => ({
      ...d,
      vehicle_type_icon: normalizeVehicleTypeIconUrl(d?.vehicle_type_icon),
      driver_image: normalizeDriverImageUrl(d?.driver_image),
    }));

    socket.emit(eventName, nearbyWithNormalizedIcons);

    console.log(
      `?? Nearby -> ${nearby.length} drivers within road radius ${roadRadius}m`
    );
  };

  socket.on("user:findNearbyDrivers", async (payload = {}) => {
    debugLog("user:findNearbyDrivers", payload, socket.id);
    console.log("?? payload from frontend:", payload);
    const { user_id, lat, long, service_type_id } = payload;
    const nearbyToken = normalizeToken(
      payload?.access_token ?? payload?.token ?? payload?.user_token ?? null
    );
    const resolvedUserId =
      toNumber(user_id) ?? resolvePayloadUserId(payload) ?? toNumber(socket.userId);

    switchSocketUserSession(resolvedUserId, nearbyToken, "user:findNearbyDrivers");
    applyNearbyFiltersFromPayload(payload, { resetMissing: true });
    syncRideContextFromPayload(payload, "user:findNearbyDrivers");

    const details = extractUserDetails(payload);
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

    await sendNearby("user:nearbyDrivers");
    await emitNearbyVehicleTypes();

    stopNearby();
    socket.nearbyInterval = setInterval(() => {
      void sendNearby("user:nearbyDrivers:update");
      void emitNearbyVehicleTypes();
    }, NEARBY_EVERY_MS);
  });

  //////////////////////////////////////////////////////////

  const handleGetNearbyVehicleTypes = async (payload = {}) => {
    const { lat, long } = payload;
    debugLog("user:getNearbyVehicleTypes", { lat, long }, socket.id);
    console.log("[user:getNearbyVehicleTypes] payload:", payload);
    const nearbyTypesToken = normalizeToken(
      payload?.access_token ?? payload?.token ?? payload?.user_token ?? null
    );
    const resolvedUserId =
      resolvePayloadUserId(payload) ?? toNumber(payload?.user_id) ?? toNumber(socket.userId);
    switchSocketUserSession(resolvedUserId, nearbyTypesToken, "user:getNearbyVehicleTypes");
    const la = toNumber(lat);
    const lo = toNumber(long);
    if (la === null || lo === null) return;
    applyNearbyFiltersFromPayload(payload, { resetMissing: true });
    syncRideContextFromPayload(payload, "user:getNearbyVehicleTypes");

    const details = extractUserDetails(payload);
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

    await syncNearbyRadius(payload);

    if (routeKm !== null) {
      setNearbyRouteDistanceKm(routeKm);
    }
    if (routeDurationMin !== null) {
      setNearbyRouteDurationMin(routeDurationMin);
    }

    socket.lastVehicleTypesSig = null;
    socket.nearbyCenter = { lat: la, long: lo };
    await emitNearbyVehicleTypes();

    console.log(`?? Nearby vehicle types requested (socket:${socket.id})`);
  };

  socket.on("user:getNearbyVehicleTypes", handleGetNearbyVehicleTypes);
  socket.on("user:getnearByVichleType", handleGetNearbyVehicleTypes);

  socket.on("user:updateNearbyCenter", async (payload = {}) => {
    const { lat, long } = payload;
    debugLog("user:updateNearbyCenter", { lat, long }, socket.id);
    if (!socket.nearbyCenter) return;

    const la = toNumber(lat);
    const lo = toNumber(long);
    if (la === null || lo === null) return;
    applyNearbyFiltersFromPayload(payload, { resetMissing: false });
    syncRideContextFromPayload(payload, "user:updateNearbyCenter");

    socket.nearbyCenter = { lat: la, long: lo };

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

    void sendNearby("user:nearbyDrivers:update");
    await emitNearbyVehicleTypes();
  });

  socket.on("user:setNearbyServiceType", async ({ service_type_id }) => {
    debugLog("user:setNearbyServiceType", { service_type_id }, socket.id);
    const st = toNumber(service_type_id);
    socket.nearbyServiceTypeId = st === null ? null : st;

    socket.nearbyRadius = DEFAULT_NEARBY_RADIUS_METERS;
    socket.lastVehicleTypesSig = null;

    await syncNearbyRadius({});

    void sendNearby("user:nearbyDrivers:update");

    if (socket.nearbyCenter) {
      await emitNearbyVehicleTypes();
    }
  });

  socket.on("user:stopNearbyDrivers", () => {
    debugLog("user:stopNearbyDrivers", {}, socket.id);
    if (socket.currentRideId) {
      socket.lastPricingSnapshotSigByRide.delete(socket.currentRideId);
    }
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
