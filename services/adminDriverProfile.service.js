const axios = require("axios");

const PROFILE_CACHE_TTL_MS = 5 * 60 * 1000;
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

const parseServiceCategoryIds = (value) => {
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
    service_category_id: toNumber(
      profile.service_category_id ?? profile.service_cat_id
    ),
    service_cat_id: toNumber(profile.service_category_id ?? profile.service_cat_id),
    service_category_ids: parseServiceCategoryIds(
      profile.service_category_ids ?? profile.driver_vehicle_service_lists
    ),
    vehicle_type_id: toNumber(profile.vehicle_type_id),
    driver_name: String(profile.driver_name ?? profile.name ?? "").trim(),
    name: String(profile.name ?? profile.driver_name ?? "").trim(),
    image: profile.image ?? profile.driver_profile ?? null,
    driver_image: profile.driver_image ?? profile.image ?? profile.driver_profile ?? null,
    phone: String(profile.phone ?? profile.contact_number ?? "").trim(),
    country_code: String(profile.country_code ?? "").trim(),
    driver_gender: toNumber(profile.driver_gender ?? profile.gender),
    child_seat: toNumber(
      profile.child_seat ??
        profile.child_seat_accessibility ??
        profile.smoking
    ),
    smoking: toNumber(
      profile.smoking ??
        profile.child_seat ??
        profile.child_seat_accessibility
    ),
    handicap: toNumber(
      profile.handicap ?? profile.handicap_accessibility
    ),
    current_lat: toNumber(profile.current_lat ?? profile.lat),
    current_long: toNumber(profile.current_long ?? profile.lng ?? profile.long),
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

  for (const cacheKey of cacheKeys) {
    const cached = getCachedProfile(cacheKey);
    if (cached) return cached;
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
