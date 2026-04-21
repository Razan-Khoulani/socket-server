/* eslint-disable no-console */
const { io } = require("socket.io-client");

const toNumber = (value) => {
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
};

const toBool = (value, defaultValue = false) => {
  if (value == null) return defaultValue;
  if (typeof value === "boolean") return value;
  return ["1", "true", "yes", "y", "on"].includes(
    String(value).trim().toLowerCase()
  );
};

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

const nowIso = () => new Date().toISOString();
const log = (label, payload) => {
  if (payload === undefined) {
    console.log(`[${nowIso()}] ${label}`);
    return;
  }
  console.log(`[${nowIso()}] ${label}`, payload);
};

const haversineMeters = (lat1, lon1, lat2, lon2) => {
  const toRad = (deg) => (deg * Math.PI) / 180;
  const R = 6371000;
  const dLat = toRad(lat2 - lat1);
  const dLon = toRad(lon2 - lon1);
  const a =
    Math.sin(dLat / 2) * Math.sin(dLat / 2) +
    Math.cos(toRad(lat1)) *
      Math.cos(toRad(lat2)) *
      Math.sin(dLon / 2) *
      Math.sin(dLon / 2);
  const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
  return R * c;
};

const interpolatePoint = (a, b, t) => ({
  lat: a.lat + (b.lat - a.lat) * t,
  long: a.long + (b.long - a.long) * t,
});

const SOCKET_URL = process.env.SOCKET_URL || "https://socket.gocab.net";
const DRIVER_ID = toNumber(process.env.DRIVER_ID);
const DRIVER_SERVICE_ID = toNumber(process.env.DRIVER_SERVICE_ID);
const DRIVER_ACCESS_TOKEN = process.env.DRIVER_ACCESS_TOKEN || "";
const RIDE_ID = toNumber(process.env.RIDE_ID);
const SERVICE_CATEGORY_ID = toNumber(process.env.SERVICE_CATEGORY_ID);

const START_LAT = toNumber(process.env.START_LAT);
const START_LONG = toNumber(process.env.START_LONG);
const BASELINE_LAT = toNumber(process.env.BASELINE_LAT);
const BASELINE_LONG = toNumber(process.env.BASELINE_LONG);
const END_LAT = toNumber(process.env.END_LAT);
const END_LONG = toNumber(process.env.END_LONG);

const STEP_METERS = Math.max(30, toNumber(process.env.STEP_METERS) ?? 120);
const STEP_DELAY_MS = Math.max(1000, toNumber(process.env.STEP_DELAY_MS) ?? 3000);
const PAUSE_AFTER_ONLINE_MS = Math.max(
  200,
  toNumber(process.env.PAUSE_AFTER_ONLINE_MS) ?? 600
);
const PAUSE_AFTER_DECISION_MS = Math.max(
  300,
  toNumber(process.env.PAUSE_AFTER_DECISION_MS) ?? 1500
);
const PAUSE_BEFORE_STATUS7_MS = Math.max(
  500,
  toNumber(process.env.PAUSE_BEFORE_STATUS7_MS) ?? 1200
);
const TIMEOUT_MS = Math.max(20000, toNumber(process.env.TIMEOUT_MS) ?? 180000);
const SEND_STATUS_8 = toBool(process.env.SEND_STATUS_8, false);
const SEND_STATUS_9 = toBool(process.env.SEND_STATUS_9, false);

if (
  !DRIVER_ID ||
  !DRIVER_SERVICE_ID ||
  !DRIVER_ACCESS_TOKEN ||
  !RIDE_ID ||
  !SERVICE_CATEGORY_ID
) {
  console.log(
    "Missing required env: DRIVER_ID, DRIVER_SERVICE_ID, DRIVER_ACCESS_TOKEN, RIDE_ID, SERVICE_CATEGORY_ID"
  );
  process.exit(1);
}

if (
  START_LAT == null ||
  START_LONG == null ||
  BASELINE_LAT == null ||
  BASELINE_LONG == null ||
  END_LAT == null ||
  END_LONG == null
) {
  console.log(
    "Missing required env: START_LAT, START_LONG, BASELINE_LAT, BASELINE_LONG, END_LAT, END_LONG"
  );
  process.exit(1);
}

const state = {
  ready: false,
  decisionAck: null,
  extraAccepted: null,
  invoice: null,
  passedAccepted: null,
  statusUpdated: [],
  current: { lat: START_LAT, long: START_LONG },
};

const sameRide = (payload = {}) => toNumber(payload?.ride_id) === RIDE_ID;

const socket = io(SOCKET_URL, {
  transports: ["websocket"],
  timeout: 15000,
});

const emitUpdateLocation = async (lat, long, note = "") => {
  const payload = { lat, long };
  socket.emit("update-location", payload);
  state.current = { lat, long };
  log(`[emit] update-location${note ? ` (${note})` : ""}`, payload);
};

const emitStatus = async (rideStatus, { lat, long } = {}) => {
  const payload = {
    ride_id: RIDE_ID,
    driver_id: DRIVER_ID,
    ride_status: rideStatus,
    way_point_status: 0,
    service_category_id: SERVICE_CATEGORY_ID,
    driver_service_id: DRIVER_SERVICE_ID,
    access_token: DRIVER_ACCESS_TOKEN,
    current_lat: lat ?? state.current.lat,
    current_long: long ?? state.current.long,
  };
  socket.emit("driver:updateRideStatus", payload);
  log(`[emit] driver:updateRideStatus (${rideStatus})`, payload);
};

const emitDecisionAccepted = async ({ lat, long }) => {
  const payload = {
    ride_id: RIDE_ID,
    driver_id: DRIVER_ID,
    accepted: 1,
    current_lat: lat ?? state.current.lat,
    current_long: long ?? state.current.long,
  };
  socket.emit("driver:passedDestinationDecision", payload);
  log("[emit] driver:passedDestinationDecision", payload);
};

const buildSteps = (from, to, stepMeters) => {
  const totalMeters = haversineMeters(from.lat, from.long, to.lat, to.long);
  const steps = Math.max(1, Math.ceil(totalMeters / stepMeters));
  const points = [];
  for (let i = 1; i <= steps; i += 1) {
    const t = i / steps;
    points.push(interpolatePoint(from, to, t));
  }
  return { totalMeters, steps, points };
};

const driveSegment = async (from, to, label) => {
  const { totalMeters, steps, points } = buildSteps(from, to, STEP_METERS);
  log(`[drive] ${label} start`, {
    from,
    to,
    total_m: Math.round(totalMeters),
    steps,
    step_meters: STEP_METERS,
    step_delay_ms: STEP_DELAY_MS,
  });
  for (let i = 0; i < points.length; i += 1) {
    const p = points[i];
    await emitUpdateLocation(p.lat, p.long, `${label} ${i + 1}/${points.length}`);
    await sleep(STEP_DELAY_MS);
  }
  log(`[drive] ${label} done`);
};

const finalize = (exitCode = null) => {
  const extraKm = toNumber(
    state.extraAccepted?.extra_distance_km ??
      state.passedAccepted?.extra_distance_km ??
      null
  );
  const ackAccepted = toNumber(state.decisionAck?.accepted) === 1;
  const summary = {
    ride_id: RIDE_ID,
    decision_ack_seen: !!state.decisionAck,
    decision_ack_accepted: ackAccepted,
    passed_destination_accepted_seen: !!state.passedAccepted,
    extra_distance_accepted_seen: !!state.extraAccepted,
    extra_distance_km: extraKm,
    invoice_seen: !!state.invoice,
    statuses_seen: state.statusUpdated.map((s) => toNumber(s?.ride_status)).filter(Boolean),
  };
  log("[summary]", summary);

  const ok = ackAccepted && toNumber(extraKm) > 0;
  const code = exitCode != null ? exitCode : ok ? 0 : 1;
  socket.disconnect();
  setTimeout(() => process.exit(code), 120);
};

const runScenario = async () => {
  const onlinePayload = {
    driver_id: DRIVER_ID,
    driver_service_id: DRIVER_SERVICE_ID,
    access_token: DRIVER_ACCESS_TOKEN,
    lat: START_LAT,
    long: START_LONG,
  };
  socket.emit("driver-online", onlinePayload);
  log("[emit] driver-online", onlinePayload);

  await sleep(PAUSE_AFTER_ONLINE_MS);
  await emitStatus(5, { lat: START_LAT, long: START_LONG });

  await driveSegment(
    { lat: START_LAT, long: START_LONG },
    { lat: BASELINE_LAT, long: BASELINE_LONG },
    "to-baseline"
  );

  await emitStatus(6, { lat: BASELINE_LAT, long: BASELINE_LONG });
  await sleep(800);
  await emitDecisionAccepted({ lat: BASELINE_LAT, long: BASELINE_LONG });
  await sleep(PAUSE_AFTER_DECISION_MS);

  await driveSegment(
    { lat: BASELINE_LAT, long: BASELINE_LONG },
    { lat: END_LAT, long: END_LONG },
    "extra-distance"
  );

  await sleep(PAUSE_BEFORE_STATUS7_MS);
  await emitStatus(7, { lat: END_LAT, long: END_LONG });

  if (SEND_STATUS_8) {
    await sleep(2500);
    await emitStatus(8, { lat: END_LAT, long: END_LONG });
  }
  if (SEND_STATUS_9) {
    await sleep(2500);
    await emitStatus(9, { lat: END_LAT, long: END_LONG });
  }
};

socket.on("connect", async () => {
  log("[socket] connected", { id: socket.id, url: SOCKET_URL });
  try {
    await runScenario();
  } catch (error) {
    log("[scenario] failed", { message: error?.message || String(error) });
    finalize(1);
  }
});

socket.on("driver:ready", (payload) => {
  state.ready = true;
  log("[event] driver:ready", payload);
});

socket.on("ride:statusUpdated", (payload) => {
  if (!sameRide(payload)) return;
  state.statusUpdated.push(payload);
  log("[event] ride:statusUpdated", payload);
});

socket.on("ride:passedDestinationDecisionAck", (payload) => {
  if (!sameRide(payload)) return;
  state.decisionAck = payload;
  log("[event] ride:passedDestinationDecisionAck", payload);
});

socket.on("ride:passedDestinationAccepted", (payload) => {
  if (!sameRide(payload)) return;
  state.passedAccepted = payload;
  log("[event] ride:passedDestinationAccepted", payload);
});

socket.on("ride:extraDistanceAccepted", (payload) => {
  if (!sameRide(payload)) return;
  state.extraAccepted = payload;
  log("[event] ride:extraDistanceAccepted", payload);
});

socket.on("ride:invoice", (payload) => {
  if (!sameRide(payload)) return;
  state.invoice = payload;
  log("[event] ride:invoice", payload);
});

socket.on("connect_error", (error) => {
  log("[socket] connect_error", { message: error?.message || String(error) });
  finalize(1);
});

setTimeout(() => {
  log("[timeout] reached", { timeout_ms: TIMEOUT_MS });
  finalize();
}, TIMEOUT_MS);

