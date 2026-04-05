const driverLocationService = require("./driverLocation.service");
const { getRideStatusSnapshot } = require("../store/rideStatusSnapshots.store");
const { getActiveRideByDriver } = require("../store/activeRides.store");

const ADMIN_DRIVER_ROOM_PREFIX = "admin:drivers:service:";
const DRIVER_MAX_AGE_MS = Number.isFinite(
  Number(process.env.ADMIN_DRIVER_FEED_MAX_AGE_MS)
)
  ? Math.max(60 * 1000, Number(process.env.ADMIN_DRIVER_FEED_MAX_AGE_MS))
  : 120 * 60 * 1000;

const RIDE_START_STATUSES = new Set([5]);
const REACHED_STATUSES = new Set([3]);
const ENROUTE_STATUSES = new Set([0, 1, 2]);

const toNumber = (value) => {
  if (value === null || value === undefined || value === "") return null;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
};

const normalizeServiceCategoryId = (value) => {
  const parsed = toNumber(value);
  return parsed && parsed > 0 ? parsed : null;
};

const normalizeServiceCategoryIds = (value) => {
  if (Array.isArray(value)) {
    return value
      .map((item) => normalizeServiceCategoryId(item))
      .filter((item, index, arr) => item && arr.indexOf(item) === index);
  }

  if (typeof value === "string") {
    return value
      .split(",")
      .map((item) => normalizeServiceCategoryId(item.trim()))
      .filter((item, index, arr) => item && arr.indexOf(item) === index);
  }

  const single = normalizeServiceCategoryId(value);
  return single ? [single] : [];
};

const getAdminDriverRoom = (serviceCategoryId = null) =>
  `${ADMIN_DRIVER_ROOM_PREFIX}${normalizeServiceCategoryId(serviceCategoryId) ?? "all"}`;

const getDriverLastActivity = (meta = {}) => {
  return (
    toNumber(meta.last_activity_at) ??
    toNumber(meta.timestamp) ??
    toNumber(meta.updatedAt) ??
    toNumber(meta.lastSeen) ??
    null
  );
};

const isDriverFresh = (meta = {}) => {
  const ts = getDriverLastActivity(meta);
  if (ts === null) return true;
  return Date.now() - ts <= DRIVER_MAX_AGE_MS;
};

const normalizeDashboardRideStatus = (rawStatus) => {
  const safeStatus = toNumber(rawStatus);
  if (safeStatus === null) return 3;
  if (ENROUTE_STATUSES.has(safeStatus)) return 0;
  if (REACHED_STATUSES.has(safeStatus)) return 1;
  if (RIDE_START_STATUSES.has(safeStatus)) return 2;
  return 3;
};

const buildAdminDriverPayload = (driverId, snapshot = null) => {
  const safeDriverId = toNumber(driverId ?? snapshot?.driver_id);
  if (!safeDriverId) return null;

  const meta =
    snapshot && typeof snapshot === "object"
      ? { ...snapshot }
      : driverLocationService.getMeta(safeDriverId) || {};

  const lat = toNumber(meta.lat ?? meta.latitude ?? meta.current_lat);
  const lng = toNumber(meta.long ?? meta.lng ?? meta.longitude ?? meta.current_long);
  if (lat === null || lng === null) return null;

  const serviceCategoryId = normalizeServiceCategoryId(
    meta.service_category_id ?? meta.service_cat_id
  );
  const serviceCategoryIds = normalizeServiceCategoryIds(
    meta.service_category_ids ??
      meta.driver_vehicle_service_lists ??
      (serviceCategoryId != null ? [serviceCategoryId] : [])
  );
  const rideId =
    toNumber(meta.current_ride_id ?? meta.ride_id) ??
    toNumber(getActiveRideByDriver(safeDriverId));
  const rideSnapshot = rideId ? getRideStatusSnapshot(rideId) : null;
  const rawRideStatus = toNumber(
    rideSnapshot?.ride_status ??
      meta.current_ride_status ??
      meta.raw_ride_status ??
      meta.latest_ride_status ??
      meta.ride_status
  );
  const driverName = String(meta.driver_name ?? meta.name ?? "").trim();
  const phone = String(meta.phone ?? meta.contact_number ?? "").trim();
  const countryCode = String(
    meta.country_code ?? meta.countryCode ?? meta.mobile_country_code ?? ""
  ).trim();
  const image = meta.driver_image ?? meta.driver_image_url ?? meta.image ?? null;
  const explicitStatus = toNumber(
    meta.current_status ??
      meta.driver_current_status ??
      meta.new_status ??
      meta.provider_current_status
  );
  const isOnlineForDashboard =
    meta.dashboard_is_online !== undefined
      ? meta.dashboard_is_online !== false
      : explicitStatus !== null
      ? explicitStatus === 1
      : meta.is_online !== false;

  return {
    id: safeDriverId,
    driver_id: safeDriverId,
    lat,
    lng,
    long: lng,
    latitude: lat,
    longitude: lng,
    ride_id: rideId,
    raw_ride_status: rawRideStatus,
    ride_status: normalizeDashboardRideStatus(rawRideStatus),
    is_online: isOnlineForDashboard,
    last_activity_at: getDriverLastActivity(meta),
    service_category_id: serviceCategoryId,
    service_cat_id: serviceCategoryId,
    service_category_ids: serviceCategoryIds,
    service_type_id: toNumber(meta.service_type_id),
    driver_service_id: toNumber(meta.driver_service_id),
    name: driverName,
    driver_name: driverName,
    phone,
    country_code: countryCode,
    image,
    driver_image: image,
    rating: toNumber(meta.rating),
    vehicle_type_name: meta.vehicle_type_name ?? "",
    child_seat: toNumber(meta.child_seat),
    handicap: toNumber(meta.handicap),
    driver_gender: toNumber(meta.driver_gender),
  };
};

const listAdminDrivers = (serviceCategoryId = null) => {
  const safeServiceCategoryId = normalizeServiceCategoryId(serviceCategoryId);

  return driverLocationService
    .listDrivers()
    .map((item) => buildAdminDriverPayload(item.driver_id, item))
    .filter(Boolean)
    .filter((item) => isDriverFresh(item))
    .filter((item) => {
        if (safeServiceCategoryId === null) return true;
        return (
          item.service_category_id === safeServiceCategoryId ||
          normalizeServiceCategoryIds(item.service_category_ids).includes(
            safeServiceCategoryId
          )
        );
    });
};

const emitAdminDriversSnapshot = (target, serviceCategoryId = null) => {
  if (!target || typeof target.emit !== "function") return;
  const drivers = listAdminDrivers(serviceCategoryId);

  target.emit("admin:drivers:snapshot", {
    service_category_id: normalizeServiceCategoryId(serviceCategoryId),
    drivers,
    totals: {
      drivers: drivers.length,
      customers: 0,
    },
  });
};

const emitAdminDriverUpdate = (io, driverId, snapshot = null) => {
  if (!io) return;
  const payload = buildAdminDriverPayload(driverId, snapshot);
  if (!payload) return;

  io.to(getAdminDriverRoom()).emit("admin:driver:update", payload);
  const targetCategoryIds = normalizeServiceCategoryIds([
    ...(payload.service_category_ids || []),
    payload.service_category_id,
  ]);
  targetCategoryIds.forEach((serviceCategoryId) => {
    io
      .to(getAdminDriverRoom(serviceCategoryId))
      .emit("admin:driver:update", payload);
  });
};

const emitAdminDriverRemove = (io, driverId, meta = null) => {
  if (!io) return;
  const safeDriverId = toNumber(driverId);
  if (!safeDriverId) return;

  const serviceCategoryIds = normalizeServiceCategoryIds(
    meta?.service_category_ids ??
      meta?.driver_vehicle_service_lists ??
      meta?.service_category_id ??
      meta?.service_cat_id ??
      meta
  );
  const primaryServiceCategoryId =
    normalizeServiceCategoryId(
      meta?.service_category_id ?? meta?.service_cat_id
    ) ?? (serviceCategoryIds[0] ?? null);

  const payload = {
    id: safeDriverId,
    driver_id: safeDriverId,
    service_category_id: primaryServiceCategoryId,
    service_cat_id: primaryServiceCategoryId,
    service_category_ids: serviceCategoryIds,
  };

  io.to(getAdminDriverRoom()).emit("admin:driver:remove", payload);
  serviceCategoryIds.forEach((serviceCategoryId) => {
    io
      .to(getAdminDriverRoom(serviceCategoryId))
      .emit("admin:driver:remove", payload);
  });
};

module.exports = {
  buildAdminDriverPayload,
  emitAdminDriverRemove,
  emitAdminDriverUpdate,
  emitAdminDriversSnapshot,
  getAdminDriverRoom,
  listAdminDrivers,
  normalizeDashboardRideStatus,
  normalizeServiceCategoryId,
};
