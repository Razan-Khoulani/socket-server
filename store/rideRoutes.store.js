// store/rideRoutes.store.js
const routes = new Map(); // rideId -> { points: [{lat,lng,at}], last: {lat,lng,at}, pickup, pickupExitMeters, hasLeftPickupZone }

const ROUTE_POINT_EVERY_MS = Number.isFinite(
  Number(process.env.ROUTE_POINT_EVERY_MS)
)
  ? Number(process.env.ROUTE_POINT_EVERY_MS)
  : 5000;

const ROUTE_POINT_MIN_METERS = Number.isFinite(
  Number(process.env.ROUTE_POINT_MIN_METERS)
)
  ? Number(process.env.ROUTE_POINT_MIN_METERS)
  : 15;

const ROUTE_POINT_EXIT_PICKUP_METERS = Number.isFinite(
  Number(process.env.ROUTE_POINT_EXIT_PICKUP_METERS)
)
  ? Number(process.env.ROUTE_POINT_EXIT_PICKUP_METERS)
  : 80;

const ROUTE_POINT_JITTER_MIN_METERS = Number.isFinite(
  Number(process.env.ROUTE_POINT_JITTER_MIN_METERS)
)
  ? Number(process.env.ROUTE_POINT_JITTER_MIN_METERS)
  : 18;

const ROUTE_POINT_JITTER_WINDOW_MS = Number.isFinite(
  Number(process.env.ROUTE_POINT_JITTER_WINDOW_MS)
)
  ? Number(process.env.ROUTE_POINT_JITTER_WINDOW_MS)
  : 25000;

const ROUTE_POINT_MAX_SPEED_MPS = Number.isFinite(
  Number(process.env.ROUTE_POINT_MAX_SPEED_MPS)
)
  ? Number(process.env.ROUTE_POINT_MAX_SPEED_MPS)
  : 45;

const ROUTE_POINT_MAX_JUMP_METERS = Number.isFinite(
  Number(process.env.ROUTE_POINT_MAX_JUMP_METERS)
)
  ? Number(process.env.ROUTE_POINT_MAX_JUMP_METERS)
  : 180;

const ROUTE_POINT_MAX_JUMP_WINDOW_MS = Number.isFinite(
  Number(process.env.ROUTE_POINT_MAX_JUMP_WINDOW_MS)
)
  ? Number(process.env.ROUTE_POINT_MAX_JUMP_WINDOW_MS)
  : 4000;

const ROUTE_POINT_MAX_POINTS = Number.isFinite(
  Number(process.env.ROUTE_POINT_MAX_POINTS)
)
  ? Number(process.env.ROUTE_POINT_MAX_POINTS)
  : 4000;

const toNumber = (v) => {
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
};

const deg2rad = (deg) => (deg * Math.PI) / 180;

const haversineMeters = (a, b) => {
  if (!a || !b) return null;
  const lat1 = toNumber(a.lat);
  const lng1 = toNumber(a.lng);
  const lat2 = toNumber(b.lat);
  const lng2 = toNumber(b.lng);
  if (lat1 == null || lng1 == null || lat2 == null || lng2 == null) return null;

  const R = 6371000; // meters
  const dLat = deg2rad(lat2 - lat1);
  const dLng = deg2rad(lng2 - lng1);
  const s1 = Math.sin(dLat / 2);
  const s2 = Math.sin(dLng / 2);
  const aVal =
    s1 * s1 + Math.cos(deg2rad(lat1)) * Math.cos(deg2rad(lat2)) * s2 * s2;
  const c = 2 * Math.atan2(Math.sqrt(aVal), Math.sqrt(1 - aVal));
  return R * c;
};

const normalizePoint = (point) => {
  if (!point) return null;
  const lat = toNumber(point.lat);
  const lng = toNumber(point.lng);
  if (lat == null || lng == null) return null;
  const at = Number.isFinite(Number(point.at)) ? Number(point.at) : Date.now();
  return { lat, lng, at };
};

const normalizePickupPoint = (pickup) => {
  if (!pickup || typeof pickup !== "object") return null;
  const lat = toNumber(
    pickup.lat ?? pickup.pickup_lat ?? pickup.pickupLat ?? pickup.latitude
  );
  const lng = toNumber(
    pickup.lng ??
      pickup.long ??
      pickup.pickup_long ??
      pickup.pickupLong ??
      pickup.longitude
  );
  if (lat == null || lng == null) return null;
  return { lat, lng };
};

const getRouteState = (rideId) => {
  if (!rideId) return null;
  let route = routes.get(rideId);
  if (!route) {
    route = {
      points: [],
      last: null,
      pickup: null,
      pickupExitMeters: ROUTE_POINT_EXIT_PICKUP_METERS,
      hasLeftPickupZone: true,
    };
    routes.set(rideId, route);
  }
  return route;
};

const mergeRouteOptions = (route, opts = {}) => {
  if (!route || !opts || typeof opts !== "object") return route;

  const exitMeters = Number.isFinite(Number(opts.pickupExitMeters))
    ? Number(opts.pickupExitMeters)
    : ROUTE_POINT_EXIT_PICKUP_METERS;
  const pickup = normalizePickupPoint(opts.pickup);

  if (pickup) {
    route.pickup = pickup;
    route.pickupExitMeters = exitMeters;
    if (!route.points.length) {
      route.hasLeftPickupZone = false;
    } else if (route.hasLeftPickupZone !== true && route.last) {
      const lastDistanceFromPickup = haversineMeters(route.last, pickup);
      route.hasLeftPickupZone =
        lastDistanceFromPickup == null
          ? true
          : lastDistanceFromPickup >= exitMeters;
    }
  } else if (route.pickup == null) {
    route.hasLeftPickupZone = true;
  }

  return route;
};

const shouldIgnoreNoisySegment = (last, next, opts = {}) => {
  const dist = haversineMeters(last, next);
  if (dist == null || dist <= 0) return true;

  const at = Number.isFinite(Number(next?.at)) ? Number(next.at) : Date.now();
  const lastAt = Number.isFinite(Number(last?.at)) ? Number(last.at) : null;
  if (lastAt == null) return false;

  const elapsedMs = at - lastAt;
  if (elapsedMs <= 0) return true;

  const jitterMeters = Number.isFinite(Number(opts.jitterMeters))
    ? Number(opts.jitterMeters)
    : ROUTE_POINT_JITTER_MIN_METERS;
  const jitterWindowMs = Number.isFinite(Number(opts.jitterWindowMs))
    ? Number(opts.jitterWindowMs)
    : ROUTE_POINT_JITTER_WINDOW_MS;
  const maxSpeedMps = Number.isFinite(Number(opts.maxSpeedMps))
    ? Number(opts.maxSpeedMps)
    : ROUTE_POINT_MAX_SPEED_MPS;
  const maxJumpMeters = Number.isFinite(Number(opts.maxJumpMeters))
    ? Number(opts.maxJumpMeters)
    : ROUTE_POINT_MAX_JUMP_METERS;
  const maxJumpWindowMs = Number.isFinite(Number(opts.maxJumpWindowMs))
    ? Number(opts.maxJumpWindowMs)
    : ROUTE_POINT_MAX_JUMP_WINDOW_MS;

  if (dist < jitterMeters && elapsedMs < jitterWindowMs) return true;
  if (dist > maxJumpMeters && elapsedMs < maxJumpWindowMs) return true;

  const elapsedSeconds = elapsedMs / 1000;
  if (elapsedSeconds > 0 && dist / elapsedSeconds > maxSpeedMps) return true;

  return false;
};

const shouldAddPoint = (last, next, opts = {}) => {
  if (!last) return true;
  if (shouldIgnoreNoisySegment(last, next, opts)) return false;

  const minMs = Number.isFinite(Number(opts.minMs))
    ? Number(opts.minMs)
    : ROUTE_POINT_EVERY_MS;
  const minMeters = Number.isFinite(Number(opts.minMeters))
    ? Number(opts.minMeters)
    : ROUTE_POINT_MIN_METERS;

  const at = Number.isFinite(Number(next?.at)) ? Number(next.at) : Date.now();
  const lastAt = Number.isFinite(Number(last?.at)) ? Number(last.at) : null;
  const timeOk = lastAt == null ? true : at - lastAt >= minMs;

  const dist = haversineMeters(last, next);
  const distOk = dist == null ? true : dist >= minMeters;

  return timeOk && distOk;
};

const startRideRoute = (rideId, point = null, opts = {}) => {
  if (!rideId) return false;
  const route = getRouteState(rideId);
  if (!route) return false;
  mergeRouteOptions(route, opts);
  if (point) appendRidePoint(rideId, point, opts);
  return true;
};

const appendRidePoint = (rideId, point, opts = {}) => {
  if (!rideId) return false;
  const route = getRouteState(rideId);
  if (!route) return false;
  mergeRouteOptions(route, opts);
  const p = normalizePoint(point);
  if (!p) return false;

  if (route.pickup && route.hasLeftPickupZone !== true) {
    const distanceFromPickup = haversineMeters(route.pickup, p);
    if (
      distanceFromPickup != null &&
      distanceFromPickup < route.pickupExitMeters
    ) {
      return false;
    }
    route.hasLeftPickupZone = true;
  }

  if (route.pickup && route.hasLeftPickupZone === true) {
    const distanceFromPickup = haversineMeters(route.pickup, p);
    if (
      distanceFromPickup != null &&
      distanceFromPickup < route.pickupExitMeters
    ) {
      return false;
    }
  }

  if (!shouldAddPoint(route.last, p, opts)) return false;

  if (route.points.length >= ROUTE_POINT_MAX_POINTS) {
    // avoid unbounded memory growth
    return false;
  }

  route.points.push({ lat: p.lat, lng: p.lng, at: p.at });
  route.last = { lat: p.lat, lng: p.lng, at: p.at };
  return true;
};

const getRideRoutePoints = (rideId) => {
  if (!rideId) return [];
  const route = routes.get(rideId);
  if (!route) return [];
  return route.points.slice();
};

const clearRideRoute = (rideId) => {
  if (!rideId) return;
  routes.delete(rideId);
};

const hasRideRoute = (rideId) => routes.has(rideId);

module.exports = {
  startRideRoute,
  appendRidePoint,
  getRideRoutePoints,
  clearRideRoute,
  hasRideRoute,
};
