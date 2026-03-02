const db = require("../db");
const { getDistanceMeters } = require("../utils/geo.util");

const driverLocations = new Map();   // driverId -> { lat, long, timestamp, ...meta }
const driverListeners = new Map();   // driverId -> Set<callback>

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

  // ✅ NEW (optional filters)
  const requiredGender = opts?.required_gender; // 1/2 فقط
  const needChildSeat = opts?.need_child_seat === 1; // فلترة فقط إذا 1
  const needHandicap = opts?.need_handicap === 1; // فلترة فقط إذا 1

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
    if (filterTypeId && data.service_type_id !== filterTypeId) continue;

    if (maxAgeMs !== null) {
      const ts = Number(data.timestamp);
      if (!Number.isFinite(ts) || now - ts > maxAgeMs) continue;
    }

    if (data.lat == null || data.long == null) continue;

    // ✅ NEW: gender filter (only if provided 1/2)
    if (requiredGender === 1 || requiredGender === 2) {
      const dg = Number(data.driver_gender);
      if (!(dg === 1 || dg === 2)) continue;
      if (dg !== requiredGender) continue;
    }

    // ✅ NEW: child seat filter (only if requested)
    if (needChildSeat) {
      if (Number(data.child_seat ?? 0) !== 1) continue;
    }

    // ✅ NEW: handicap filter (only if requested)
    if (needHandicap) {
      if (Number(data.handicap ?? 0) !== 1) continue;
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

