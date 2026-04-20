/* eslint-disable no-console */
const { io } = require("socket.io-client");

const toNumber = (value) => {
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
};

const toBool = (value, defaultValue = false) => {
  if (value == null) return defaultValue;
  if (typeof value === "boolean") return value;
  const normalized = String(value).trim().toLowerCase();
  return ["1", "true", "yes", "y", "on"].includes(normalized);
};

const pickNumber = (...values) => {
  for (const value of values) {
    const n = toNumber(value);
    if (n != null) return n;
  }
  return null;
};

const hasPositive = (value) => {
  const n = toNumber(value);
  return n != null && n > 0;
};

const SOCKET_URL = process.env.SOCKET_URL || "https://socket.gocab.net";
const DRIVER_ID = toNumber(process.env.DRIVER_ID);
const DRIVER_SERVICE_ID = toNumber(process.env.DRIVER_SERVICE_ID);
const DRIVER_ACCESS_TOKEN = process.env.DRIVER_ACCESS_TOKEN || "";
const RIDE_ID = toNumber(process.env.RIDE_ID);

const SERVICE_CATEGORY_ID = toNumber(process.env.SERVICE_CATEGORY_ID);

const CURRENT_LAT = toNumber(process.env.CURRENT_LAT);
const CURRENT_LONG = toNumber(process.env.CURRENT_LONG);
const DECISION_LAT = toNumber(process.env.DECISION_LAT);
const DECISION_LONG = toNumber(process.env.DECISION_LONG);
const STATUS7_LAT = toNumber(process.env.STATUS7_LAT);
const STATUS7_LONG = toNumber(process.env.STATUS7_LONG);

const AUTO_ACCEPT = toBool(process.env.AUTO_ACCEPT, false);
const AUTO_STATUS_7 = toBool(process.env.AUTO_STATUS_7, false);
const FORCE_DECISION = toBool(process.env.FORCE_DECISION, false);
const STRICT = toBool(process.env.STRICT, true);

const DECISION_DELAY_MS = Math.max(0, toNumber(process.env.DECISION_DELAY_MS) ?? 400);
const STATUS_7_DELAY_MS = Math.max(0, toNumber(process.env.STATUS_7_DELAY_MS) ?? 5000);
const FORCE_DECISION_DELAY_MS = Math.max(
  0,
  toNumber(process.env.FORCE_DECISION_DELAY_MS) ?? 1500
);
const TIMEOUT_MS = Math.max(5000, toNumber(process.env.TIMEOUT_MS) ?? 180000);

if (!DRIVER_ID || !DRIVER_SERVICE_ID || !DRIVER_ACCESS_TOKEN || !RIDE_ID) {
  console.log(
    "Usage env: DRIVER_ID, DRIVER_SERVICE_ID, DRIVER_ACCESS_TOKEN, RIDE_ID are required"
  );
  process.exit(1);
}

if (AUTO_STATUS_7 && !SERVICE_CATEGORY_ID) {
  console.log("AUTO_STATUS_7=1 requires SERVICE_CATEGORY_ID");
  process.exit(1);
}

const state = {
  connected: false,
  sentDecision: false,
  sentStatus7: false,
  passedEvent: null,
  decisionAck: null,
  acceptedEvent: null,
  extraEvent: null,
  invoiceEvent: null,
};

const sameRide = (payload = {}) => toNumber(payload?.ride_id) === RIDE_ID;

const nowIso = () => new Date().toISOString();
const log = (label, data) => {
  if (data === undefined) {
    console.log(`[${nowIso()}] ${label}`);
    return;
  }
  console.log(`[${nowIso()}] ${label}`, data);
};

const socket = io(SOCKET_URL, {
  transports: ["websocket"],
  timeout: 10000,
});

const finalize = (forcedCode = null) => {
  const extraKm = pickNumber(
    state.extraEvent?.extra_distance_km,
    state.acceptedEvent?.extra_distance_km
  );
  const acceptedAck = toNumber(state.decisionAck?.accepted) === 1;

  const summary = {
    ride_id: RIDE_ID,
    passed_destination_seen: !!state.passedEvent,
    decision_ack_seen: !!state.decisionAck,
    decision_ack_accepted: acceptedAck,
    passed_destination_accepted_seen: !!state.acceptedEvent,
    extra_distance_accepted_seen: !!state.extraEvent,
    extra_distance_km: extraKm,
    invoice_seen: !!state.invoiceEvent,
  };

  log("[summary]", summary);

  if (forcedCode != null) {
    socket.disconnect();
    setTimeout(() => process.exit(forcedCode), 150);
    return;
  }

  if (!STRICT) {
    socket.disconnect();
    setTimeout(() => process.exit(0), 150);
    return;
  }

  const ok =
    summary.passed_destination_seen &&
    summary.decision_ack_seen &&
    summary.decision_ack_accepted &&
    summary.passed_destination_accepted_seen &&
    summary.extra_distance_accepted_seen &&
    hasPositive(summary.extra_distance_km);

  socket.disconnect();
  setTimeout(() => process.exit(ok ? 0 : 1), 150);
};

const maybeSendDecision = () => {
  if (!AUTO_ACCEPT || state.sentDecision || !state.passedEvent) return;

  const lat = pickNumber(
    DECISION_LAT,
    CURRENT_LAT,
    state.passedEvent?.lat,
    state.passedEvent?.current_lat,
    state.passedEvent?.destination?.lat
  );
  const long = pickNumber(
    DECISION_LONG,
    CURRENT_LONG,
    state.passedEvent?.long,
    state.passedEvent?.current_long,
    state.passedEvent?.destination?.long
  );

  setTimeout(() => {
    const payload = {
      ride_id: RIDE_ID,
      driver_id: DRIVER_ID,
      accepted: 1,
      ...(lat != null ? { current_lat: lat } : {}),
      ...(long != null ? { current_long: long } : {}),
    };
    socket.emit("driver:passedDestinationDecision", payload);
    state.sentDecision = true;
    log("[emit] driver:passedDestinationDecision", payload);
  }, DECISION_DELAY_MS);
};

const maybeForceSendDecision = () => {
  if (!FORCE_DECISION || state.sentDecision) return;
  setTimeout(() => {
    const payload = {
      ride_id: RIDE_ID,
      driver_id: DRIVER_ID,
      accepted: 1,
      ...(pickNumber(DECISION_LAT, CURRENT_LAT) != null
        ? { current_lat: pickNumber(DECISION_LAT, CURRENT_LAT) }
        : {}),
      ...(pickNumber(DECISION_LONG, CURRENT_LONG) != null
        ? { current_long: pickNumber(DECISION_LONG, CURRENT_LONG) }
        : {}),
    };
    socket.emit("driver:passedDestinationDecision", payload);
    state.sentDecision = true;
    log("[emit] driver:passedDestinationDecision (forced)", payload);
  }, FORCE_DECISION_DELAY_MS);
};

const maybeSendStatus7 = () => {
  if (!AUTO_STATUS_7 || state.sentStatus7) return;
  if (toNumber(state.decisionAck?.accepted) !== 1) return;

  const lat = pickNumber(
    STATUS7_LAT,
    CURRENT_LAT,
    state.passedEvent?.lat,
    state.passedEvent?.current_lat,
    state.passedEvent?.destination?.lat
  );
  const long = pickNumber(
    STATUS7_LONG,
    CURRENT_LONG,
    state.passedEvent?.long,
    state.passedEvent?.current_long,
    state.passedEvent?.destination?.long
  );

  setTimeout(() => {
    const payload = {
      ride_id: RIDE_ID,
      driver_id: DRIVER_ID,
      ride_status: 7,
      way_point_status: 0,
      service_category_id: SERVICE_CATEGORY_ID,
      driver_service_id: DRIVER_SERVICE_ID,
      access_token: DRIVER_ACCESS_TOKEN,
      ...(lat != null ? { current_lat: lat } : {}),
      ...(long != null ? { current_long: long } : {}),
    };
    socket.emit("driver:updateRideStatus", payload);
    state.sentStatus7 = true;
    log("[emit] driver:updateRideStatus (7)", payload);
  }, STATUS_7_DELAY_MS);
};

socket.on("connect", () => {
  state.connected = true;
  log("[socket] connected", { id: socket.id, url: SOCKET_URL });

  const onlinePayload = {
    driver_id: DRIVER_ID,
    driver_service_id: DRIVER_SERVICE_ID,
    access_token: DRIVER_ACCESS_TOKEN,
    ...(CURRENT_LAT != null ? { lat: CURRENT_LAT } : {}),
    ...(CURRENT_LONG != null ? { long: CURRENT_LONG } : {}),
  };
  socket.emit("driver-online", onlinePayload);
  log("[emit] driver-online", onlinePayload);
  maybeForceSendDecision();
});

socket.on("driver:ready", (payload) => {
  log("[event] driver:ready", payload);
});

socket.on("ride:passedDestination", (payload) => {
  if (!sameRide(payload)) return;
  state.passedEvent = payload;
  log("[event] ride:passedDestination", payload);
  maybeSendDecision();
});

socket.on("ride:passedDestinationDecisionAck", (payload) => {
  if (!sameRide(payload)) return;
  state.decisionAck = payload;
  log("[event] ride:passedDestinationDecisionAck", payload);
  maybeSendStatus7();
});

socket.on("ride:passedDestinationAccepted", (payload) => {
  if (!sameRide(payload)) return;
  state.acceptedEvent = payload;
  log("[event] ride:passedDestinationAccepted", payload);
});

socket.on("ride:extraDistanceAccepted", (payload) => {
  if (!sameRide(payload)) return;
  state.extraEvent = payload;
  log("[event] ride:extraDistanceAccepted", payload);
  if (!STRICT) {
    finalize(0);
    return;
  }
  if (hasPositive(payload?.extra_distance_km)) {
    finalize(0);
  }
});

socket.on("ride:invoice", (payload) => {
  if (!sameRide(payload)) return;
  state.invoiceEvent = payload;
  log("[event] ride:invoice", payload);
});

socket.on("connect_error", (err) => {
  log("[socket] connect_error", { message: err?.message || String(err) });
  finalize(1);
});

setTimeout(() => {
  log("[timeout] reached", { timeout_ms: TIMEOUT_MS });
  finalize();
}, TIMEOUT_MS);
