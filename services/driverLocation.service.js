const db = require("../db");
const { getDistanceMeters } = require("../utils/geo.util");

const driverLocations = new Map();   // driverId -> { lat, long, timestamp, ...meta }
const driverListeners = new Map();   // driverId -> Set<callback>

const toNumber = (value) => {
  if (value === null || value === undefined || value === "") return null;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
};

const normalizeToken = (value) => {
  if (value === null || value === undefined) return "";
  return String(value)
    .trim()
    .toLowerCase()
    .replace(/[\s_-]+/g, "");
};

const toBinaryFlag = (value) => {
  if (value === null || value === undefined || value === "") return null;
  if (typeof value === "boolean") return value ? 1 : 0;

  const n = toNumber(value);
  if (n === 0 || n === 1) return n;

  const token = normalizeToken(value);
  if (!token) return null;

  if (["1", "true", "yes", "on", "required", "need"].includes(token)) return 1;
  if (["0", "false", "no", "off", "none"].includes(token)) return 0;

  return null;
};

const toGenderFilter = (value) => {
  if (value === null || value === undefined || value === "") return null;

  const n = toNumber(value);
  if (n === 1 || n === 2) return n;
  if (n === 0) return 0;

  const token = normalizeToken(value);
  if (!token) return null;

  if (["male", "man", "m", "ذكر"].includes(token)) return 1;
  if (["female", "woman", "f", "انثى", "أنثى"].includes(token)) return 2;
  if (["0", "all", "any", "both", "none"].includes(token)) return 0;

  return null;
};

// ─────────────────────────────────────────────
// DB update (كما هو)
// ─────────────────────────────────────────────
exports.update = (driverId, lat, long) => {
  return new Promise((resolve, reject) => {
    db.query(
      `UPDATE transport_driver_details
       SET current_lat = ?, current_long = ?, updated_at = NOW()
       WHERE id = ?`,
      [lat, long, driverId],
      (err, result) => {
        if (err) return reject(err);
        resolve(result);
      }
    );
  });
};

// ─────────────────────────────────────────────
// Memory update: location only (كما هو لكن يحافظ على meta)
// ─────────────────────────────────────────────
exports.updateMemory = (driverId, lat, long) => {
  const old = driverLocations.get(driverId) || {};

  const next = {
    ...old, // ✅ يحافظ على meta مثل service_type_id, is_online...
    lat,
    long,
    timestamp: Date.now(),
  };

  driverLocations.set(driverId, next);

  if (driverListeners.has(driverId)) {
    driverListeners.get(driverId).forEach((cb) =>
      cb({
        driver_id: driverId,
        lat,
        long,
        timestamp: next.timestamp,
        // ✅ إذا بدك تبعت meta للمستمعين كمان:
        service_type_id: next.service_type_id ?? null,
        is_online: next.is_online ?? true,
      })
    );
  }
};

// ─────────────────────────────────────────────
// ✅ update meta without touching lat/long
// ─────────────────────────────────────────────
exports.updateMeta = (driverId, meta = {}) => {
  const old = driverLocations.get(driverId) || {};

  const next = {
    ...old,
    ...meta,
    timestamp: old.timestamp ?? Date.now(),
  };

  driverLocations.set(driverId, next);
};

// ✅ set online/offline
exports.setOnline = (driverId, isOnline) => {
  exports.updateMeta(driverId, {
    is_online: !!isOnline,
    lastSeen: !isOnline ? Date.now() : undefined,
  });
};

// ✅ get meta/profile (كل شي مخزن)
exports.getMeta = (driverId) => driverLocations.get(driverId) || null;

// ─────────────────────────────────────────────
// Get single driver (كما هو)
// ─────────────────────────────────────────────
exports.getDriver = (driverId) => driverLocations.get(driverId);
exports.listDrivers = () =>
  Array.from(driverLocations.entries()).map(([driverId, data]) => ({
    driver_id: driverId,
    ...data,
  }));

// ─────────────────────────────────────────────
// Remove driver (كما هو)
// ─────────────────────────────────────────────
exports.remove = (driverId) => {
  driverLocations.delete(driverId);
  driverListeners.delete(driverId);
};

// ─────────────────────────────────────────────
// Nearby from memory
// ✅ يرجّع كمان type + online
// ✅ ويدعم فلترة اختيارية opts بدون ما يكسر الاستدعاءات القديمة
// ─────────────────────────────────────────────
exports.getNearbyDriversFromMemory = (lat, long, radius = 5000, opts = null) => {
  const onlyOnline = opts?.only_online ?? true;
  const filterTypeId = opts?.service_type_id ?? null;

  // preference filters (exact-match when provided)
  const requiredGender = toGenderFilter(
    opts?.required_gender ??
      opts?.required_driver_gender ??
      opts?.driver_gender ??
      opts?.gender ??
      null
  );
  const childSeatFilter = toBinaryFlag(
    opts?.need_child_seat ??
      opts?.child_seat ??
      opts?.require_child_seat ??
      opts?.smoking ??
      opts?.need_smoking ??
      null
  );
  const handicapFilter = toBinaryFlag(
    opts?.need_handicap ??
      opts?.handicap ??
      opts?.require_handicap ??
      opts?.need_special_needs ??
      opts?.special_needs ??
      opts?.handicap_accessibility ??
      null
  );

  const maxAgeMs =
    typeof opts?.max_age_ms === "number" && opts.max_age_ms >= 0
      ? opts.max_age_ms
      : null;
  const maxResults =
    typeof opts?.max_results === "number" && opts.max_results > 0
      ? opts.max_results
      : null;

  const now = Date.now();
  const drivers = [];

  for (const [driverId, data] of driverLocations.entries()) {
    if (onlyOnline && data.is_online === false) continue;
    if (filterTypeId) {
      const dataServiceType = Number(data.service_type_id);
      if (!Number.isFinite(dataServiceType) || dataServiceType !== Number(filterTypeId)) continue;
    }
    if (Number(data.not_valid_wallet_balance ?? 0) === 1) continue;

    if (maxAgeMs !== null) {
      const ts = Number(data.timestamp);
      if (!Number.isFinite(ts) || now - ts > maxAgeMs) continue;
    }

    if (data.lat == null || data.long == null) continue;

    // Gender filter (only when requested with 1/2)
    if (requiredGender === 1 || requiredGender === 2) {
      const driverGender = toGenderFilter(data.driver_gender ?? data.gender ?? null);
      if (driverGender !== requiredGender) continue;
    }

    // Child-seat/smoking filter (exact 0/1 when requested)
    if (childSeatFilter === 0 || childSeatFilter === 1) {
      const driverChildSeat = toBinaryFlag(
        data.child_seat ??
          data.child_seat_accessibility ??
          data.smoking ??
          data.smoking_value ??
          null
      );
      if (driverChildSeat !== childSeatFilter) continue;
    }

    // Handicap/special-needs filter (exact 0/1 when requested)
    if (handicapFilter === 0 || handicapFilter === 1) {
      const driverHandicap = toBinaryFlag(
        data.handicap ?? data.handicap_accessibility ?? data.special_needs ?? null
      );
      if (driverHandicap !== handicapFilter) continue;
    }

    const distance = getDistanceMeters(lat, long, data.lat, data.long);
    if (distance <= radius) {
      drivers.push({
        driver_id: driverId,
        lat: data.lat,
        long: data.long,
        distance,
        is_online: data.is_online ?? true,

        service_type_id: data.service_type_id ?? null,
        service_category_id: data.service_category_id ?? null,
        driver_service_id: data.driver_service_id ?? null,

        vehicle_type_name: data.vehicle_type_name ?? "",
        vehicle_type_icon: data.vehicle_type_icon ?? "",
        driver_name: data.driver_name ?? "",
        rating: data.rating ?? null,
        driver_image: data.driver_image ?? "",

        // ✅ keep these in output (useful for debugging/clients)
        handicap: data.handicap ?? 0,
        child_seat: data.child_seat ?? 0,
        driver_gender: data.driver_gender ?? null,
      });
    }
  }

  drivers.sort((a, b) => a.distance - b.distance);

  if (maxResults && drivers.length > maxResults) {
    return drivers.slice(0, maxResults);
  }

  return drivers;
};

// ─────────────────────────────────────────────
// (اختياري) listeners (إذا عندك مستخدمين)
// ─────────────────────────────────────────────
exports.onDriverLocation = (driverId, cb) => {
  if (!driverListeners.has(driverId)) driverListeners.set(driverId, new Set());
  driverListeners.get(driverId).add(cb);

  // return unsubscribe
  return () => {
    if (!driverListeners.has(driverId)) return;
    driverListeners.get(driverId).delete(cb);
    if (driverListeners.get(driverId).size === 0) driverListeners.delete(driverId);
  };
};
