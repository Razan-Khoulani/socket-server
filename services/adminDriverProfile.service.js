const db = require("../db");

const PROFILE_CACHE_TTL_MS = 5 * 60 * 1000;
const profileCache = new Map();

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

const query = (sql, params = []) =>
  new Promise((resolve, reject) => {
    db.query(sql, params, (error, rows) => {
      if (error) return reject(error);
      resolve(Array.isArray(rows) ? rows : []);
    });
  });

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
  const safeDriverId = toNumber(profile.driver_id);
  const safeDriverServiceId = toNumber(profile.driver_service_id);
  const payload = {
    driver_id: safeDriverId,
    driver_service_id: safeDriverServiceId,
    current_status: toNumber(
      profile.current_status ?? profile.new_status ?? profile.driver_current_status
    ),
    new_status: toNumber(
      profile.new_status ?? profile.current_status ?? profile.driver_current_status
    ),
    driver_current_status: toNumber(
      profile.driver_current_status ?? profile.current_status ?? profile.new_status
    ),
    service_category_id: toNumber(profile.service_category_id),
    service_cat_id: toNumber(profile.service_category_id),
    service_category_ids: parseServiceCategoryIds(
      profile.service_category_ids ?? profile.driver_vehicle_service_lists
    ),
    vehicle_type_id: toNumber(profile.vehicle_type_id),
    driver_name: String(profile.driver_name ?? "").trim(),
    name: String(profile.name ?? profile.driver_name ?? "").trim(),
    image: profile.image ?? null,
    driver_image: profile.driver_image ?? profile.image ?? null,
    phone: String(profile.phone ?? "").trim(),
    country_code: String(profile.country_code ?? "").trim(),
    current_lat: toNumber(profile.current_lat),
    current_long: toNumber(profile.current_long),
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

  const conditions = [];
  const params = [];

  if (safeDriverServiceId) {
    conditions.push("ps.id = ?");
    params.push(safeDriverServiceId);
  }

  if (safeDriverId) {
    conditions.push("ps.provider_id = ?");
    params.push(safeDriverId);
  }

  const sql = `
    SELECT
      ps.provider_id,
      ps.id AS driver_service_id,
      ps.service_cat_id AS service_category_id,
      ps.current_status AS current_status,
      d.id AS driver_details_id,
      d.current_lat,
      d.current_long,
      d.vehicle_type_id,
      tv.driver_vehicle_service_lists,
      COALESCE(p.first_name, '') AS first_name,
      COALESCE(p.last_name, '') AS last_name,
      p.avatar,
      p.country_code,
      p.contact_number
    FROM provider_services ps
    JOIN providers p ON p.id = ps.provider_id
    LEFT JOIN transport_driver_details d ON d.provider_service_id = ps.id
    LEFT JOIN transport_driver_vehicle_lists tv ON tv.id = d.vehicle_type_id
    WHERE (${conditions.join(" OR ")})
      AND ps.status = 1
      AND p.status = 1
      AND p.deleted_at IS NULL
    ORDER BY ps.id DESC
    LIMIT 1
  `;

  const rows = await query(sql, params);
  if (!rows.length) return null;

  const row = rows[0];
  const driverName = `${row.first_name || ""} ${row.last_name || ""}`.trim();

  return setCachedProfile({
    driver_id: row.provider_id,
    driver_service_id: row.driver_service_id,
    current_status: row.current_status,
    new_status: row.current_status,
    driver_current_status: row.current_status,
    service_category_id: row.service_category_id,
    service_category_ids: parseServiceCategoryIds(
      row.driver_vehicle_service_lists ?? row.service_category_id
    ),
    vehicle_type_id: row.vehicle_type_id,
    driver_name: driverName,
    name: driverName,
    image: row.avatar ?? null,
    driver_image: row.avatar ?? null,
    phone: row.contact_number ?? "",
    country_code: row.country_code ?? "",
    current_lat: row.current_lat ?? null,
    current_long: row.current_long ?? null,
  });
};

module.exports = {
  getDriverAdminProfile,
};
