const axios = require("axios");

const PROFILE_CACHE_TTL_MS = Number.isFinite(
  Number(process.env.DRIVER_ADMIN_PROFILE_CACHE_TTL_MS)
)
  ? Math.max(5000, Number(process.env.DRIVER_ADMIN_PROFILE_CACHE_TTL_MS))
  : 60 * 1000;
const profileCache = new Map();

const LARAVEL_BASE_URL = String(
  process.env.LARAVEL_BASE_URL ||
    process.env.LARAVEL_URL ||
    "https://api.catch-syria.com"
).replace(/\/+$/, "");

const LARAVEL_DRIVER_ADMIN_PROFILE_PATH = String(
  process.env.LARAVEL_DRIVER_ADMIN_PROFILE_PATH ||
    "/api/internal/driver-admin-profile"
);

const LARAVEL_PROFILE_TIMEOUT_MS = Number.isFinite(
  Number(process.env.LARAVEL_PROFILE_TIMEOUT_MS)
)
  ? Math.max(1000, Number(process.env.LARAVEL_PROFILE_TIMEOUT_MS))
  : 7000;

const toNumber = (value) => {
  if (value === null || value === undefined || value === "") return null;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
};

const parseServiceCategoryEntries = (value) => {
  const entries = [];
  const seen = new Set();
  const pushEntry = (serviceCategoryId, serviceTypeId = null) => {
    const safeCategoryId = toNumber(serviceCategoryId);
    if (!safeCategoryId || safeCategoryId <= 0) return;
    const safeTypeId = toNumber(serviceTypeId);
    const normalizedTypeId = safeTypeId && safeTypeId > 0 ? safeTypeId : null;
    const key = `${safeCategoryId}:${normalizedTypeId ?? "null"}`;
    if (seen.has(key)) return;
    seen.add(key);
    entries.push({
      service_category_id: safeCategoryId,
      service_type_id: normalizedTypeId,
    });
  };

  const visit = (input) => {
    if (input === null || input === undefined || input === "") return;

    if (Array.isArray(input)) {
      for (const item of input) visit(item);
      return;
    }

    if (typeof input === "string") {
      const raw = input.trim();
      if (!raw) return;
      if (raw.startsWith("[") || raw.startsWith("{")) {
        try {
          const parsed = JSON.parse(raw);
          visit(parsed);
          return;
        } catch (_) {
          // Not JSON, continue with CSV fallback.
        }
      }
      if (raw.includes(",")) {
        raw.split(",").forEach((item) => visit(item));
        return;
      }
      pushEntry(raw, null);
      return;
    }

    if (typeof input === "object") {
      pushEntry(
        input.service_category_id ??
          input.service_cat_id ??
          input.category_id ??
          input.id ??
          null,
        input.service_type_id ??
          input.vehicle_type_id ??
          input.transport_vehicle_type_id ??
          input.type_id ??
          null
      );
      return;
    }

    pushEntry(input, null);
  };

  visit(value);
  return entries;
};

const parseServiceCategoryIds = (value) => {
  const fromEntries = parseServiceCategoryEntries(value)
    .map((item) => toNumber(item?.service_category_id))
    .filter((item, index, arr) => item && arr.indexOf(item) === index);
  if (fromEntries.length > 0) {
    return fromEntries;
  }

  if (Array.isArray(value)) {
    return value
      .map((item) => toNumber(item))
      .filter((item, index, arr) => item && arr.indexOf(item) === index);
  }

  if (typeof value === "string") {
    return value
      .split(",")
      .map((item) => toNumber(item.trim()))
      .filter((item, index, arr) => item && arr.indexOf(item) === index);
  }

  const safeValue = toNumber(value);
  return safeValue ? [safeValue] : [];
};

const resolveServiceTypeId = (profile = {}) => {
  const direct = toNumber(
    profile.service_type_id ??
      profile.vehicle_type_id ??
      profile.transport_vehicle_type?.service_type_id ??
      profile.transport_vehicle_type?.id
  );
  return direct;
};

const resolvePrimaryServiceCategoryId = (profile = {}) => {
  const direct = toNumber(profile.service_category_id ?? profile.service_cat_id);
  if (direct) return direct;

  const preferredTypeId = resolveServiceTypeId(profile);
  const entries = parseServiceCategoryEntries(
    profile.driver_vehicle_service_lists ?? profile.service_category_ids
  );
  if (preferredTypeId && preferredTypeId > 0) {
    const matched = entries.find(
      (item) =>
        toNumber(item?.service_type_id) &&
        Number(item.service_type_id) === Number(preferredTypeId)
    );
    if (matched?.service_category_id) {
      return toNumber(matched.service_category_id);
    }
  }

  const ids = parseServiceCategoryIds(
    profile.service_category_ids ?? profile.driver_vehicle_service_lists
  );
  return ids.length > 0 ? toNumber(ids[0]) : null;
};

const getCachedProfile = (key) => {
  const cached = profileCache.get(key);
  if (!cached) return null;
  if (Date.now() - cached.at > PROFILE_CACHE_TTL_MS) {
    profileCache.delete(key);
    return null;
  }
  return cached.value;
};

const setCachedProfile = (profile = {}) => {
  const safeDriverId = toNumber(profile.driver_id ?? profile.provider_id);
  const safeDriverServiceId = toNumber(profile.driver_service_id);
  const safeDriverDetailId = toNumber(
    profile.driver_detail_id ?? profile.driver_details_id
  );
  const serviceCategoryIds = parseServiceCategoryIds(
    profile.service_category_ids ?? profile.driver_vehicle_service_lists
  );
  const primaryServiceCategoryId = resolvePrimaryServiceCategoryId(profile);
  const payload = {
    driver_id: safeDriverId,
    provider_id: safeDriverId,
    driver_service_id: safeDriverServiceId,
    driver_detail_id: safeDriverDetailId,
    driver_details_id: safeDriverDetailId,
    current_status: toNumber(
      profile.current_status ?? profile.new_status ?? profile.driver_current_status
    ),
    new_status: toNumber(
      profile.new_status ?? profile.current_status ?? profile.driver_current_status
    ),
    driver_current_status: toNumber(
      profile.driver_current_status ?? profile.current_status ?? profile.new_status
    ),
    service_category_id: primaryServiceCategoryId,
    service_cat_id: primaryServiceCategoryId,
    service_category_ids: serviceCategoryIds,
    service_type_id: resolveServiceTypeId(profile),
    vehicle_type_id: toNumber(profile.vehicle_type_id),
    driver_name: String(profile.driver_name ?? profile.name ?? "").trim(),
    name: String(profile.name ?? profile.driver_name ?? "").trim(),
    image: profile.image ?? profile.driver_profile ?? null,
    driver_image: profile.driver_image ?? profile.image ?? profile.driver_profile ?? null,
    phone: String(profile.phone ?? profile.contact_number ?? "").trim(),
    country_code: String(profile.country_code ?? "").trim(),
    driver_gender: toNumber(
      profile.driver_gender ?? profile.gender ?? profile.driverGender
    ),
    child_seat: toNumber(
      profile.child_seat ??
        profile.child_seat_accessibility ??
        profile.smoking ??
        profile.smoking_value
    ),
    smoking: toNumber(
      profile.smoking ??
        profile.smoking_value ??
        profile.child_seat ??
        profile.child_seat_accessibility
    ),
    handicap: toNumber(
      profile.handicap ??
        profile.handicap_accessibility ??
        profile.special_needs ??
        profile.need_special_needs ??
        profile.can_receive_special_needs
    ),
    current_lat: toNumber(profile.current_lat ?? profile.lat),
    current_long: toNumber(profile.current_long ?? profile.lng ?? profile.long),
    lat: toNumber(profile.current_lat ?? profile.lat),
    long: toNumber(profile.current_long ?? profile.lng ?? profile.long),
    remaining_balance: toNumber(profile.remaining_balance),
    not_valid_wallet_balance: toNumber(profile.not_valid_wallet_balance) ?? 0,
    not_valid_wallet_balance_msg: String(
      profile.not_valid_wallet_balance_msg ?? ""
    ),
  };

  const cachedValue = { ...payload };
  if (safeDriverId) {
    profileCache.set(`driver:${safeDriverId}`, {
      at: Date.now(),
      value: cachedValue,
    });
  }
  if (safeDriverServiceId) {
    profileCache.set(`service:${safeDriverServiceId}`, {
      at: Date.now(),
      value: cachedValue,
    });
  }

  return cachedValue;
};

const buildProfileUrl = () => {
  const path = LARAVEL_DRIVER_ADMIN_PROFILE_PATH.startsWith("/")
    ? LARAVEL_DRIVER_ADMIN_PROFILE_PATH
    : `/${LARAVEL_DRIVER_ADMIN_PROFILE_PATH}`;

  return `${LARAVEL_BASE_URL}${path}`;
};

const buildHeaders = () => {
  const headers = {
    Accept: "application/json",
  };
  const secret =
    process.env.SOCKET_INTERNAL_SECRET || process.env.LARAVEL_INTERNAL_SECRET;

  if (secret) {
    headers["X-Socket-Internal-Secret"] = secret;
  }

  return headers;
};

const extractProfile = (body) => {
  if (!body || typeof body !== "object") return null;
  if (body.status === 0 || body.status === false) return null;

  const profile = body.data ?? body.profile ?? body.driver ?? body;
  return profile && typeof profile === "object" ? profile : null;
};

const fetchDriverAdminProfile = async ({ driverId, driverServiceId }) => {
  const payload = {};
  if (driverId) payload.driver_id = driverId;
  if (driverServiceId) payload.driver_service_id = driverServiceId;

  try {
    const response = await axios.post(buildProfileUrl(), payload, {
      timeout: LARAVEL_PROFILE_TIMEOUT_MS,
      headers: buildHeaders(),
    });

    return extractProfile(response.data);
  } catch (error) {
    if (error?.response?.status === 404) {
      return null;
    }

    const message =
      error?.response?.data?.message ||
      error?.response?.data?.error ||
      error?.message ||
      "Laravel driver profile API failed";
    throw new Error(message);
  }
};

const getDriverAdminProfile = async ({
  driverId = null,
  driverServiceId = null,
  forceRefresh = false,
} = {}) => {
  const safeDriverId = toNumber(driverId);
  const safeDriverServiceId = toNumber(driverServiceId);

  if (!safeDriverId && !safeDriverServiceId) {
    return null;
  }

  const cacheKeys = [
    safeDriverServiceId ? `service:${safeDriverServiceId}` : null,
    safeDriverId ? `driver:${safeDriverId}` : null,
  ].filter(Boolean);

  if (!forceRefresh) {
    for (const cacheKey of cacheKeys) {
      const cached = getCachedProfile(cacheKey);
      if (cached) return cached;
    }
  }

  const profile = await fetchDriverAdminProfile({
    driverId: safeDriverId,
    driverServiceId: safeDriverServiceId,
  });

  return profile ? setCachedProfile(profile) : null;
};

module.exports = {
  getDriverAdminProfile,
};
