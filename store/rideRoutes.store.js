// store/rideRoutes.store.js
const routes = new Map(); // rideId -> { points: [{lat,lng,at}], last: {lat,lng,at} }

const ROUTE_POINT_EVERY_MS = Number.isFinite(
  Number(process.env.ROUTE_POINT_EVERY_MS)
)
  ? Number(process.env.ROUTE_POINT_EVERY_MS)
  : 5000;

const ROUTE_POINT_MIN_METERS = Number.isFinite(
  Number(process.env.ROUTE_POINT_MIN_METERS)
)
  ? Number(process.env.ROUTE_POINT_MIN_METERS)
  : 10;

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

const shouldAddPoint = (last, next, opts = {}) => {
  if (!last) return true;
  const minMs =
    Number.isFinite(Number(opts.minMs)) ? Number(opts.minMs) : ROUTE_POINT_EVERY_MS;
  const minMeters =
    Number.isFinite(Number(opts.minMeters))
      ? Number(opts.minMeters)
      : ROUTE_POINT_MIN_METERS;

  const at = Number.isFinite(Number(next?.at)) ? Number(next.at) : Date.now();
  const lastAt = Number.isFinite(Number(last?.at)) ? Number(last.at) : null;
  const timeOk = lastAt == null ? true : at - lastAt >= minMs;

  const dist = haversineMeters(last, next);
  const distOk = dist == null ? true : dist >= minMeters;

  return timeOk || distOk;
};

const normalizePoint = (point) => {
  if (!point) return null;
  const lat = toNumber(point.lat);
  const lng = toNumber(point.lng);
  if (lat == null || lng == null) return null;
  const at = Number.isFinite(Number(point.at)) ? Number(point.at) : Date.now();
  return { lat, lng, at };
};

const startRideRoute = (rideId, point = null) => {
  if (!rideId) return false;
  if (routes.has(rideId)) return true;
  routes.set(rideId, { points: [], last: null });
  if (point) appendRidePoint(rideId, point);
  return true;
};

const appendRidePoint = (rideId, point, opts = {}) => {
  if (!rideId) return false;
  const route = routes.get(rideId);
  if (!route) return false;
  const p = normalizePoint(point);
  if (!p) return false;

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
