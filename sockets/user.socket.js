// sockets/user.socket.js
const driverLocationService = require("../services/driverLocation.service");
const axios = require("axios");
const { setUserDetails } = require("../store/users.store");

// ✅ UPDATED: add getActiveRideByDriver so we can exclude busy drivers from nearby/types
const { getActiveDriverByRide, getActiveRideByDriver } = require("../store/activeRides.store");

const biddingSocket = require("./bidding.socket");

const DEBUG_EVENTS = process.env.DEBUG_SOCKET_EVENTS === "1";
const debugLog = (event, payload, socketId) => {
  if (!DEBUG_EVENTS) return;
  console.log("[user.socket]", event, "socket:", socketId, "payload:", payload);
};

const INITIAL_RADIUS = 500; // نبدأ بـ 500 متر
const MAX_RADIUS = 5000; // حد أقصى
const RADIUS_MULTIPLIER = 2;
const NEARBY_EVERY_MS = 3000; // كل قديش نبعت تحديث
const MAX_DRIVER_LOCATION_AGE_MS = 2 * 60 * 1000; // ignore stale drivers (2 minutes)
const LARAVEL_BASE_URL =
  process.env.LARAVEL_BASE_URL ||
  process.env.LARAVEL_URL ||
  "https://aiactive.co.uk/backend/backend-laravel/public";
const LARAVEL_TIMEOUT_MS = 7000;
const VEHICLE_ICON_RELATIVE_DIR = "assets/images/service-category/transport-service-type";
const DRIVER_IMAGE_RELATIVE_DIR = "assets/images/profile-images/provider";

// ✅ NEW: keep timer unit aligned with bidding.socket (SECONDS)
const RIDE_TIMEOUT_S = Number.isFinite(Number(process.env.RIDE_TIMEOUT_S))
  ? Number(process.env.RIDE_TIMEOUT_S)
  : Number.isFinite(Number(process.env.RIDE_TIMEOUT_MS))
  ? Math.round(Number(process.env.RIDE_TIMEOUT_MS) / 1000)
  : 90;

const toNumber = (v) => {
  if (v === null || v === undefined || v === "") return null;
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
};

const roundMoney = (v) => {
  if (!Number.isFinite(v)) return null;
  return Math.round((v + Number.EPSILON) * 100) / 100;
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
    drivers_count: t?.drivers_count ?? null,
    estimated_fare: t?.estimated_fare ?? null,
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

// ✅ NEW: build stable signature so we emit only on change
const buildVehicleTypesSignature = (types) => {
  if (!Array.isArray(types) || types.length === 0) return "[]";

  // make stable ordering regardless of "drivers_count" ties
  const stable = [...types].sort((a, b) => {
    const aId = Number(a?.service_type_id ?? 0);
    const bId = Number(b?.service_type_id ?? 0);
    return aId - bId;
  });

  // include only fields that matter to UI
  // (if any of these changes => emit)
  const compact = stable.map((t) => ({
    service_type_id: toNumber(t?.service_type_id),
    service_category_id: toNumber(t?.service_category_id),
    drivers_count: toNumber(t?.drivers_count ?? 0),

    vehicle_type_name: t?.vehicle_type_name ?? "",
    vehicle_type_icon: normalizeVehicleTypeIconUrl(t?.vehicle_type_icon),
    driver_image: normalizeDriverImageUrl(t?.driver_image),

    distance_km: t?.distance_km != null ? roundMoney(toNumber(t?.distance_km)) : null,
    cost_per_km: t?.cost_per_km != null ? roundMoney(toNumber(t?.cost_per_km)) : null,
    estimated_fare: t?.estimated_fare != null ? roundMoney(toNumber(t?.estimated_fare)) : null,

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
  if (!serviceCategoryId || distanceKm === null) return new Map();
  try {
    const payload = {
      service_category_id: serviceCategoryId,
      distance_km: distanceKm,
      vehicle_type_ids: vehicleTypeIds,
    };
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

  const distanceMaybeKm = toNumber(route.distance ?? null);
  if (distanceMaybeKm !== null) return roundMoney(distanceMaybeKm);

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
  const contactNumber = src?.contact_number ?? src?.user_phone ?? src?.phone ?? src?.mobile ?? null;
  const userImage = src?.profile_image ?? src?.user_image ?? src?.image ?? src?.avatar ?? null;

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

// ✅ NEW: timer helpers (so frontend can build countdown) - all fields in SECONDS
const normalizeEpochSeconds = (value) => {
  const n = toNumber(value);
  if (n === null) return null;
  // Backward compatibility: convert legacy epoch-millis to epoch-seconds.
  return n > 1e11 ? Math.floor(n / 1000) : Math.floor(n);
};

const normalizeDurationSeconds = (value) => {
  const n = toNumber(value);
  if (n === null) return null;
  // Backward compatibility: old flows used milliseconds in timeout_ms.
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

// ensure ride snapshot has stable timer (persist into bidding.socket snapshot)
const ensureRideTimer = (rideId, rideDetails) => {
  if (!rideId) return null;

  if (hasTimer(rideDetails)) {
    const normalized = normalizeTimerFields(rideDetails);
    if (!normalized) return makeTimer();

    // Persist normalized timer back so all subsequent emits keep one unit.
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

  socket.nearbyCenter = null; // { lat, long }
  socket.nearbyInterval = null;
  socket.nearbyRadius = INITIAL_RADIUS;

  // ✅ optional: فلترة حسب نوع السيارة (TransportVehicleType.id)
  socket.nearbyServiceTypeId = null;
  socket.nearbyServiceCategoryId = null;
  socket.nearbyRouteDistanceKm = null;
  socket.nearbyRequiredGender = null;
  socket.nearbyNeedChildSeat = null;
  socket.nearbyNeedHandicap = null;

  // ✅ NEW: dedupe signature for vehicle types
  socket.lastVehicleTypesSig = null;

  // ✅ NEW: current ride id (used for ride:pricingSnapshot)
  socket.currentRideId = null;

  // cache fares per service_category_id + distance_km to avoid repeated API calls
  // Map<serviceCatId, { distanceKm: number, map: Map<vehicleTypeId, fare> }>
  socket.nearbyFareCache = new Map();

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

      // ✅ distance changed => vehicle fares/signature will likely change
      socket.lastVehicleTypesSig = null;
    }
  };

  const registerUser = (payload, source = "user:loginInfo") => {
    debugLog(source, payload, socket.id);
    const details = extractUserDetails(payload);
    if (!details) {
      console.warn(`[${source}] Missing user_id in payload`);
      return;
    }

    socket.isUser = true;
    socket.userId = details.user_id;
    setUserDetails(details.user_id, details);
    // NOTE: user-specific room is intentionally not used (ride room only)

    if (typeof biddingSocket.refreshUserDetailsForUserId === "function") {
      biddingSocket.refreshUserDetailsForUserId(io, details.user_id, details);
    }

    console.log(`[${source}] user details:`, details);
  };

  const emitRouteEtaToDriver = (routeKm, etaMin, payload = {}) => {
    if (routeKm === null && etaMin === null) return;

    const rideId =
      toNumber(payload?.ride_id) ??
      toNumber(payload?.booking_id) ??
      toNumber(socket.currentRideId) ??
      null;
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

  // ✅ customer sends login info explicitly
  socket.on("user:loginInfo", (payload) => {
    registerUser(payload, "user:loginInfo");
  });

  // ✅ some clients send initialData (alias)
  socket.on("user:initialData", (payload) => {
    registerUser(payload, "user:initialData");
  });

  // ✅ NEW: user joins ride room from THIS file so we can use socket.currentRideId safely
  socket.on("user:joinRideRoom", (payload = {}) => {
    debugLog("user:joinRideRoom", payload, socket.id);

    const rideId = toNumber(payload?.ride_id);
    const userId = toNumber(payload?.user_id) ?? toNumber(socket.userId);

    if (!rideId) {
      console.log("[user:joinRideRoom] missing/invalid ride_id", payload);
      return;
    }

    socket.isUser = true;
    socket.userId = userId ?? socket.userId ?? null;

    socket.join(`ride:${rideId}`);
    socket.currentRideId = rideId;

    // ✅ ensure next pricing snapshot can emit even لو ما تغيرت types حسب signature القديمة
    socket.lastVehicleTypesSig = null;

    socket.emit("ride:joined", { ride_id: rideId });

    console.log(
      `👤 User ${socket.userId || "unknown"} joined ride room ride:${rideId} (socket:${socket.id})`
    );
  });

  const stopNearby = () => {
    if (socket.nearbyInterval) {
      clearInterval(socket.nearbyInterval);
      socket.nearbyInterval = null;
    }
  };

  // ✅ helper يرجّع أنواع السيارات المتوفرة ضمن القريبين (online فقط) + ✅ exclude busy drivers
  const buildNearbyVehicleTypes = async (lat, long) => {
    const nearbyAll = driverLocationService.getNearbyDriversFromMemory(
      lat,
      long,
      socket.nearbyRadius,
      {
        only_online: true,
        max_age_ms: MAX_DRIVER_LOCATION_AGE_MS,
        required_gender: socket.nearbyRequiredGender,
        need_child_seat: socket.nearbyNeedChildSeat,
        need_handicap: socket.nearbyNeedHandicap,
      }
    );

    // ✅ NEW: exclude busy drivers (active ride)
    const nearbyDrivers = nearbyAll.filter((d) => {
      const dId = toNumber(d?.driver_id);
      if (!dId) return false;
      const activeRide = getActiveRideByDriver(dId);
      return !activeRide;
    });

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
      }
    }

    // ترتيب حسب عدد السائقين (الأكثر أولاً)
    const result = Array.from(typesMap.values()).sort(
      (a, b) => (b.drivers_count ?? 0) - (a.drivers_count ?? 0)
    );

    const distanceKm = toNumber(socket.nearbyRouteDistanceKm);
    const fixedServiceCatIdRaw = toNumber(socket.nearbyServiceCategoryId);
    const fixedServiceCatId =
      fixedServiceCatIdRaw !== null && fixedServiceCatIdRaw > 0 ? fixedServiceCatIdRaw : null;
    const pickupLat = toNumber(lat);
    const pickupLong = toNumber(long);

    if (distanceKm !== null) {
      try {
        const groups = new Map(); // serviceCatId -> Set<vehicleTypeId>
        for (const item of result) {
          const serviceCatId = fixedServiceCatId ?? toNumber(item.service_category_id);
          if (!serviceCatId) continue;
          if (!groups.has(serviceCatId)) groups.set(serviceCatId, new Set());
          groups.get(serviceCatId).add(item.service_type_id);
        }

        for (const [serviceCatId, typeSet] of groups.entries()) {
          let cacheEntry = socket.nearbyFareCache.get(serviceCatId);
          if (
            !cacheEntry ||
            cacheEntry.distanceKm !== distanceKm ||
            cacheEntry.pickupLat !== pickupLat ||
            cacheEntry.pickupLong !== pickupLong
          ) {
            cacheEntry = { distanceKm, pickupLat, pickupLong, map: new Map() };
            socket.nearbyFareCache.set(serviceCatId, cacheEntry);

            // ✅ pickup changed => fares may change => allow re-emit
            socket.lastVehicleTypesSig = null;
          }

          const missingTypeIds = [];
          for (const typeId of typeSet) {
            if (!cacheEntry.map.has(typeId)) missingTypeIds.push(typeId);
          }

          if (missingTypeIds.length > 0) {
            const fareMap = await fetchVehicleFaresFromApi(
              serviceCatId,
              distanceKm,
              missingTypeIds,
              pickupLat,
              pickupLong
            );
            for (const [id, item] of fareMap.entries()) {
              cacheEntry.map.set(id, item);
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
            item.estimated_fare = roundMoney(toNumber(fare.estimated_fare ?? null));

            const driverDistanceM = toNumber(fare.driver_to_pickup_distance_m ?? null);
            const driverDurationS = toNumber(fare.driver_to_pickup_duration_s ?? null);
            item.driver_to_pickup_distance_m = driverDistanceM;
            item.driver_to_pickup_duration_s = driverDurationS;
            item.driver_to_pickup_distance_km =
              driverDistanceM !== null ? roundMoney(driverDistanceM / 1000) : null;
            item.driver_to_pickup_duration_min =
              driverDurationS !== null ? roundMoney(driverDurationS / 60) : null;
          }
        }
      } catch (e) {
        console.warn("[nearbyVehicleTypes] fare lookup failed:", e?.message || e);
      }
    }

    return result;
  };

  // ✅ NEW: emit vehicle types ONLY if changed (and always on same event)
  const emitNearbyVehicleTypes = async () => {
    if (!socket.nearbyCenter) return;
    const { lat, long } = socket.nearbyCenter;
    try {
      const types = await buildNearbyVehicleTypes(lat, long);

      const sig = buildVehicleTypesSignature(types);
      if (sig === socket.lastVehicleTypesSig) {
        if (DEBUG_EVENTS) {
          console.log("[user:nearbyVehicleTypes] skipped (no change)");
        }
        return;
      }

      socket.lastVehicleTypesSig = sig;

      console.log("[user:nearbyVehicleTypes] emit ->", summarizeVehicleTypesForLog(types));

      // ✅ original event (unchanged)
      socket.emit("user:nearbyVehicleTypes", types);

      // ✅ scenario: also broadcast pricing snapshot to the ride room (if joined)
      const rideId = socket.currentRideId;
      if (rideId) {
        const rideDetails =
          typeof biddingSocket.getRideDetails === "function"
            ? biddingSocket.getRideDetails(rideId)
            : null;

        const userPrice =
          rideDetails?.updatedPrice ?? rideDetails?.user_bid_price ?? rideDetails?.min_fare_amount ?? null;

        // ✅ NEW: ensure timer exists (and persist it) so frontend can countdown
        const timer = ensureRideTimer(rideId, rideDetails);

        io.to(`ride:${rideId}`).emit("ride:pricingSnapshot", {
          ride_id: rideId,

          // ✅ NEW: timer fields
          ...(timer ? timer : {}),

          // ✅ user price if provided
          user_bid_price: userPrice,
          isPriceUpdated: !!rideDetails?.isPriceUpdated,
          updatedPrice: rideDetails?.updatedPrice ?? null,
          updatedAt: rideDetails?.updatedAt ?? null,

          // ✅ start/end points
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

          vehicle_types: types,
          at: Date.now(),
        });
      }
    } catch (e) {
      console.warn("[nearbyVehicleTypes] emit failed:", e?.message || e);
    }
  };

  /////////////////////////////////////////////////////////////
  const sendNearby = (eventName = "user:nearbyDrivers") => {
    if (!socket.nearbyCenter) return;

    const { lat, long } = socket.nearbyCenter;

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
      socket.nearbyRadius,
      {
        ...opts,
        max_age_ms: MAX_DRIVER_LOCATION_AGE_MS,
      }
    );

    // ✅ NEW: exclude busy drivers (active ride) for nearby drivers list too
    const nearby = nearbyAll.filter((d) => {
      const dId = toNumber(d?.driver_id);
      if (!dId) return false;
      const activeRide = getActiveRideByDriver(dId);
      return !activeRide;
    });

    // 🚀 إذا ما في ولا سائق → وسّع الراديوس
    if (nearby.length === 0 && socket.nearbyRadius < MAX_RADIUS) {
      socket.nearbyRadius = Math.min(socket.nearbyRadius * RADIUS_MULTIPLIER, MAX_RADIUS);

      console.log(`🔍 No drivers found, expanding radius to ${socket.nearbyRadius}m`);

      // ✅ radius changed => vehicle types scope changed => allow re-emit
      socket.lastVehicleTypesSig = null;

      return sendNearby(eventName);
    }

    const nearbyWithNormalizedIcons = nearby.map((d) => ({
      ...d,
      vehicle_type_icon: normalizeVehicleTypeIconUrl(d?.vehicle_type_icon),
      driver_image: normalizeDriverImageUrl(d?.driver_image),
    }));

    socket.emit(eventName, nearbyWithNormalizedIcons);

    console.log(`👤 Nearby -> ${nearby.length} drivers within ${socket.nearbyRadius}m`);
  };

  // ✅ واحد فقط: اليوزر يطلب nearby ويبدأ streaming تلقائيًا
  // ✅ إضافة اختيارية: service_type_id للفلترة
  socket.on("user:findNearbyDrivers", (payload = {}) => {
    debugLog("user:findNearbyDrivers", payload, socket.id);
    console.log("📦 payload from frontend:", payload);
    const { user_id, lat, long, service_type_id } = payload;

    socket.isUser = true;
    socket.userId = toNumber(user_id) ?? socket.userId;
    applyNearbyFiltersFromPayload(payload, { resetMissing: true });

    const details = extractUserDetails(payload);
    const routeKm = extractRouteDistanceKm(payload);
    const etaMin = toNumber(payload?.eta_min ?? null);
    if (details) {
      if (routeKm !== null) details.route = routeKm;
      if (etaMin !== null) details.eta_min = etaMin;

      setUserDetails(details.user_id, details);
      if (typeof biddingSocket.refreshUserDetailsForUserId === "function") {
        biddingSocket.refreshUserDetailsForUserId(io, details.user_id, details);
      }
    }
    emitRouteEtaToDriver(routeKm, etaMin, payload);

    // ✅ كل طلب جديد يبدأ من 500 متر
    socket.nearbyRadius = INITIAL_RADIUS;

    const la = toNumber(lat);
    const lo = toNumber(long);
    if (la === null || lo === null) return;

    socket.nearbyCenter = { lat: la, long: lo };

    // ✅ حفظ نوع الفلتر إذا انبعث (إذا ما انبعث => null => كل الأنواع)
    const st = toNumber(service_type_id);
    socket.nearbyServiceTypeId = st === null ? null : st;

    const sc = toNumber(payload.service_category_id ?? payload.service_cat_id);
    if (sc !== null && sc > 0) socket.nearbyServiceCategoryId = sc;

    if (routeKm !== null) {
      setNearbyRouteDistanceKm(routeKm);
    }

    // ✅ NEW: force first emit for vehicle types
    socket.lastVehicleTypesSig = null;

    // snapshot immediately
    sendNearby("user:nearbyDrivers");

    // ✅ ابعت أنواع السيارات المتاحة فوراً (على نفس الايفنت)
    void emitNearbyVehicleTypes();

    // start periodic updates
    stopNearby();
    socket.nearbyInterval = setInterval(() => {
      sendNearby("user:nearbyDrivers:update");

      // ✅ build every interval but emit only if changed
      void emitNearbyVehicleTypes();
    }, NEARBY_EVERY_MS);
  });

  //////////////////////////////////////////////////////////

  // ✅ الواجهة تطلب فقط أنواع السيارات القريبة (بدون تشغيل streaming للdrivers)
  socket.on("user:getNearbyVehicleTypes", async (payload = {}) => {
    const { lat, long } = payload;
    debugLog("user:getNearbyVehicleTypes", { lat, long }, socket.id);
    console.log("[user:getNearbyVehicleTypes] payload:", payload);
    const la = toNumber(lat);
    const lo = toNumber(long);
    if (la === null || lo === null) return;
    applyNearbyFiltersFromPayload(payload, { resetMissing: true });

    const details = extractUserDetails(payload);
    const routeKm = extractRouteDistanceKm(payload);
    const etaMin = toNumber(payload?.eta_min ?? null);
    if (details) {
      if (routeKm !== null) details.route = routeKm;
      if (etaMin !== null) details.eta_min = etaMin;
      setUserDetails(details.user_id, details);
    }
    emitRouteEtaToDriver(routeKm, etaMin, payload);

    const sc = toNumber(payload.service_category_id ?? payload.service_cat_id);
    if (sc !== null && sc > 0) socket.nearbyServiceCategoryId = sc;

    if (routeKm !== null) {
      setNearbyRouteDistanceKm(routeKm);
    }

    // ✅ ensure this is a real snapshot emit even if previously same
    socket.lastVehicleTypesSig = null;

    // set a center so emit helper can work consistently
    socket.nearbyCenter = { lat: la, long: lo };
    await emitNearbyVehicleTypes();

    console.log(`🚗 Nearby vehicle types requested (socket:${socket.id})`);
  });

  // إذا اليوزر تحرك وبدك تغير المركز
  socket.on("user:updateNearbyCenter", async (payload = {}) => {
    const { lat, long } = payload;
    debugLog("user:updateNearbyCenter", { lat, long }, socket.id);
    if (!socket.nearbyCenter) return;

    const la = toNumber(lat);
    const lo = toNumber(long);
    if (la === null || lo === null) return;
    applyNearbyFiltersFromPayload(payload, { resetMissing: false });

    socket.nearbyCenter = { lat: la, long: lo };

    const sc = toNumber(payload.service_category_id ?? payload.service_cat_id);
    if (sc !== null && sc > 0) socket.nearbyServiceCategoryId = sc;

    const routeKm = extractRouteDistanceKm(payload);
    const etaMin = toNumber(payload?.eta_min ?? null);
    if (routeKm !== null) {
      setNearbyRouteDistanceKm(routeKm);
    }
    emitRouteEtaToDriver(routeKm, etaMin, payload);

    sendNearby("user:nearbyDrivers:update");

    // ✅ same event, emit only if changed
    await emitNearbyVehicleTypes();
  });

  // ✅ تغيير الفلتر بدون ما تعيد تشغيل nearby
  socket.on("user:setNearbyServiceType", async ({ service_type_id }) => {
    debugLog("user:setNearbyServiceType", { service_type_id }, socket.id);
    const st = toNumber(service_type_id);
    socket.nearbyServiceTypeId = st === null ? null : st;

    // ✅ فلتر جديد => ابدأ من 500
    socket.nearbyRadius = INITIAL_RADIUS;

    // ✅ allow vehicle types to re-emit
    socket.lastVehicleTypesSig = null;

    sendNearby("user:nearbyDrivers:update");

    if (socket.nearbyCenter) {
      await emitNearbyVehicleTypes();
    }
  });

  // إيقاف التحديثات (مثلاً لما تبدأ رحلة)
  socket.on("user:stopNearbyDrivers", () => {
    debugLog("user:stopNearbyDrivers", {}, socket.id);
    stopNearby();
    socket.nearbyCenter = null;
    socket.nearbyServiceTypeId = null;
    socket.nearbyServiceCategoryId = null;
    socket.nearbyRouteDistanceKm = null;
    socket.nearbyRequiredGender = null;
    socket.nearbyNeedChildSeat = null;
    socket.nearbyNeedHandicap = null;
    socket.nearbyRadius = INITIAL_RADIUS;
    socket.nearbyFareCache.clear();

    // ✅ reset signature
    socket.lastVehicleTypesSig = null;

    // ✅ reset ride id
    socket.currentRideId = null;
  });

  socket.on("disconnect", () => {
    debugLog("disconnect", {}, socket.id);
    stopNearby();
  });
};
