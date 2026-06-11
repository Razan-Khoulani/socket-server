/* eslint-disable no-console */
const axios = require("axios");
const mysql = require("mysql2/promise");
const { io } = require("socket.io-client");

const SOCKET_URL = process.env.SOCKET_URL || "https://socket.gocab.net";
const SOCKET_HTTP_URL = process.env.SOCKET_HTTP_URL || SOCKET_URL;
const LARAVEL_URL = process.env.LARAVEL_URL || "https://osbackend.gocab.net";

const DB_HOST = process.env.DB_HOST || "127.0.0.1";
const DB_PORT = Number(process.env.DB_PORT || 3306);
const DB_USER = process.env.DB_USER || "";
const DB_PASSWORD = process.env.DB_PASSWORD || "";
const DB_NAME = process.env.DB_NAME || "";

const LIVE_LOAD_CONFIRM =
  process.env.LIVE_LOAD_CONFIRM || "";
const CONFIRM_PHRASE = "I_UNDERSTAND_THIS_CREATES_REAL_RIDES";

const SCENARIO_COUNT = Number.isFinite(Number(process.env.SCENARIO_COUNT))
  ? Math.max(1, Math.floor(Number(process.env.SCENARIO_COUNT)))
  : 30;
const DRIVER_COUNT = Number.isFinite(Number(process.env.DRIVER_COUNT))
  ? Math.max(1, Math.floor(Number(process.env.DRIVER_COUNT)))
  : SCENARIO_COUNT;
const USER_COUNT = Number.isFinite(Number(process.env.USER_COUNT))
  ? Math.max(1, Math.floor(Number(process.env.USER_COUNT)))
  : SCENARIO_COUNT;
const CONNECT_BATCH_SIZE = Number.isFinite(Number(process.env.CONNECT_BATCH_SIZE))
  ? Math.max(1, Math.floor(Number(process.env.CONNECT_BATCH_SIZE)))
  : 10;
const SCENARIO_CONCURRENCY = Number.isFinite(Number(process.env.SCENARIO_CONCURRENCY))
  ? Math.max(1, Math.floor(Number(process.env.SCENARIO_CONCURRENCY)))
  : SCENARIO_COUNT;
const SCENARIO_STAGGER_MS = Number.isFinite(Number(process.env.SCENARIO_STAGGER_MS))
  ? Math.max(0, Number(process.env.SCENARIO_STAGGER_MS))
  : 0;
const TEST_WINDOW_MS = Number.isFinite(Number(process.env.TEST_WINDOW_MS))
  ? Math.max(5000, Number(process.env.TEST_WINDOW_MS))
  : 45000;
const AUTO_CLEANUP = String(process.env.AUTO_CLEANUP || "1") === "1";
const CLEANUP_CONCURRENCY = Number.isFinite(Number(process.env.CLEANUP_CONCURRENCY))
  ? Math.max(1, Math.floor(Number(process.env.CLEANUP_CONCURRENCY)))
  : 10;
const CANCEL_REASON_ID = Number.isFinite(Number(process.env.CANCEL_REASON_ID))
  ? Math.max(1, Math.floor(Number(process.env.CANCEL_REASON_ID)))
  : 1;
const RETRY_ACTIVE_RIDE_CANCEL = String(process.env.RETRY_ACTIVE_RIDE_CANCEL || "1") === "1";

const SERVICE_CATEGORY_ID = Number.isFinite(Number(process.env.SERVICE_CATEGORY_ID))
  ? Number(process.env.SERVICE_CATEGORY_ID)
  : 5;
const REQUEST_SERVICE_TYPE_ID = Number.isFinite(Number(process.env.REQUEST_SERVICE_TYPE_ID))
  ? Number(process.env.REQUEST_SERVICE_TYPE_ID)
  : 2;
const PAYMENT_TYPE = Number.isFinite(Number(process.env.PAYMENT_TYPE))
  ? Number(process.env.PAYMENT_TYPE)
  : 1;
const ESTIMATED_TIME = String(process.env.ESTIMATED_TIME || "20");
const TOTAL_DISTANCE = String(process.env.TOTAL_DISTANCE || "10");
const BIDDING_OFFER = Number.isFinite(Number(process.env.BIDDING_OFFER))
  ? Number(process.env.BIDDING_OFFER)
  : 5000;
const HANDICAP = String(process.env.HANDICAP || "0");
const CHILD_SEAT = String(process.env.CHILD_SEAT || "0");

const BASE_PICKUP_LAT = Number.isFinite(Number(process.env.PICKUP_LAT))
  ? Number(process.env.PICKUP_LAT)
  : 33.4935628;
const BASE_PICKUP_LONG = Number.isFinite(Number(process.env.PICKUP_LONG))
  ? Number(process.env.PICKUP_LONG)
  : 36.2406966;
const BASE_DEST_LAT = Number.isFinite(Number(process.env.DEST_LAT))
  ? Number(process.env.DEST_LAT)
  : 33.498339615409;
const BASE_DEST_LONG = Number.isFinite(Number(process.env.DEST_LONG))
  ? Number(process.env.DEST_LONG)
  : 36.244756439568;
const COORDINATE_SPACING = Number.isFinite(Number(process.env.COORDINATE_SPACING))
  ? Math.max(0.00001, Number(process.env.COORDINATE_SPACING))
  : 0.00025;

const DRIVER_READY_WAIT_MS = Number.isFinite(Number(process.env.DRIVER_READY_WAIT_MS))
  ? Math.max(500, Number(process.env.DRIVER_READY_WAIT_MS))
  : 1500;
const USER_READY_WAIT_MS = Number.isFinite(Number(process.env.USER_READY_WAIT_MS))
  ? Math.max(200, Number(process.env.USER_READY_WAIT_MS))
  : 500;
const SOCKET_CONNECT_TIMEOUT_MS = Number.isFinite(Number(process.env.SOCKET_CONNECT_TIMEOUT_MS))
  ? Math.max(1000, Number(process.env.SOCKET_CONNECT_TIMEOUT_MS))
  : 10000;
const SOCKET_HTTP_TIMEOUT_MS = Number.isFinite(Number(process.env.SOCKET_HTTP_TIMEOUT_MS))
  ? Math.max(1000, Number(process.env.SOCKET_HTTP_TIMEOUT_MS))
  : 10000;
const LARAVEL_HTTP_TIMEOUT_MS = Number.isFinite(Number(process.env.LARAVEL_HTTP_TIMEOUT_MS))
  ? Math.max(1000, Number(process.env.LARAVEL_HTTP_TIMEOUT_MS))
  : 10000;
const HEARTBEAT_INTERVAL_MS = Number.isFinite(Number(process.env.HEARTBEAT_INTERVAL_MS))
  ? Math.max(1000, Number(process.env.HEARTBEAT_INTERVAL_MS))
  : 2000;
const DRIVER_APP_STATE = String(process.env.DRIVER_APP_STATE || "foreground").trim().toLowerCase();
const MATCH_SCENARIO_DRIVER_SERVICE_TYPE =
  String(process.env.MATCH_SCENARIO_DRIVER_SERVICE_TYPE || "0") === "1";
const TARGET_ASSIGNED_DRIVER_ONLY =
  String(process.env.TARGET_ASSIGNED_DRIVER_ONLY || "0") === "1";
const MANUAL_DISPATCH_AFTER_BOOKING =
  String(process.env.MANUAL_DISPATCH_AFTER_BOOKING || "1") === "1";

const ENABLE_USER_COUNTER = String(process.env.ENABLE_USER_COUNTER || "1") === "1";
const ENABLE_DRIVER_ACCEPT_COUNTER =
  String(process.env.ENABLE_DRIVER_ACCEPT_COUNTER || "1") === "1";
const ENABLE_USER_ACCEPT = String(process.env.ENABLE_USER_ACCEPT || "1") === "1";
const AUTO_USER_ACCEPT_FIRST_BID =
  String(process.env.AUTO_USER_ACCEPT_FIRST_BID || "0") === "1";
const USER_COUNTER_DELTA = Number.isFinite(Number(process.env.USER_COUNTER_DELTA))
  ? Math.max(0, Number(process.env.USER_COUNTER_DELTA))
  : 700;
const DRIVER_BID_MIN = Number.isFinite(Number(process.env.DRIVER_BID_MIN))
  ? Number(process.env.DRIVER_BID_MIN)
  : 5000;
const DRIVER_BID_MAX = Number.isFinite(Number(process.env.DRIVER_BID_MAX))
  ? Math.max(DRIVER_BID_MIN, Number(process.env.DRIVER_BID_MAX))
  : 6500;
const DRIVER_BID_DELAY_MS = Number.isFinite(Number(process.env.DRIVER_BID_DELAY_MS))
  ? Math.max(0, Number(process.env.DRIVER_BID_DELAY_MS))
  : 250;
const USER_COUNTER_DELAY_MS = Number.isFinite(Number(process.env.USER_COUNTER_DELAY_MS))
  ? Math.max(0, Number(process.env.USER_COUNTER_DELAY_MS))
  : 150;
const DRIVER_ACCEPT_DELAY_MS = Number.isFinite(Number(process.env.DRIVER_ACCEPT_DELAY_MS))
  ? Math.max(0, Number(process.env.DRIVER_ACCEPT_DELAY_MS))
  : 150;
const USER_ACCEPT_DELAY_MS = Number.isFinite(Number(process.env.USER_ACCEPT_DELAY_MS))
  ? Math.max(0, Number(process.env.USER_ACCEPT_DELAY_MS))
  : 0;

const USER_IDS = String(process.env.USER_IDS || "")
  .split(",")
  .map((value) => Number(value.trim()))
  .filter((value) => Number.isFinite(value) && value > 0);
const DRIVER_IDS = String(process.env.DRIVER_IDS || "")
  .split(",")
  .map((value) => Number(value.trim()))
  .filter((value) => Number.isFinite(value) && value > 0);

const httpSocket = axios.create({
  baseURL: SOCKET_HTTP_URL,
  timeout: SOCKET_HTTP_TIMEOUT_MS,
});
const httpLaravel = axios.create({
  baseURL: LARAVEL_URL,
  timeout: LARAVEL_HTTP_TIMEOUT_MS,
});

const metrics = {
  rideCreateMs: [],
  dispatchMs: [],
  firstBidMs: [],
  counterToDriverAcceptMs: [],
  userAcceptedMs: [],
  ridesCreated: 0,
  rideCreateFailures: 0,
  dispatchSent: 0,
  dispatchFailures: 0,
  driverBidRequests: 0,
  driverBidsSubmitted: 0,
  userNewBids: 0,
  userCountersSent: 0,
  driverCounterAcceptsSent: 0,
  userAcceptsSent: 0,
  userAcceptedEvents: 0,
  cleanupAttempted: 0,
  cleanupSucceeded: 0,
  cleanupFailed: 0,
};

const driverClients = new Map();
const userClients = new Map();
const scenariosByRideId = new Map();
const createdRideIds = new Set();

const sleep = (ms) =>
  new Promise((resolve) => setTimeout(resolve, Math.max(0, ms)));

const randomInt = (min, max) => {
  const safeMin = Math.floor(Math.min(min, max));
  const safeMax = Math.floor(Math.max(min, max));
  return safeMin + Math.floor(Math.random() * (safeMax - safeMin + 1));
};

const toFiniteNumber = (value) => {
  const numericValue = Number(value);
  return Number.isFinite(numericValue) ? numericValue : null;
};

const resolveBidBounds = (payload = {}) => {
  const minPrice = toFiniteNumber(
    payload?.min_price ??
      payload?.minimum_price ??
      payload?.price_anchor_min_price ??
      null
  );
  const maxPrice = toFiniteNumber(
    payload?.max_price ??
      payload?.maximum_price ??
      payload?.price_anchor_max_price ??
      null
  );

  if (minPrice !== null && maxPrice !== null) {
    return {
      min: Math.min(minPrice, maxPrice),
      max: Math.max(minPrice, maxPrice),
      source: "payload",
    };
  }

  return {
    min: DRIVER_BID_MIN,
    max: DRIVER_BID_MAX,
    source: "fallback",
  };
};

const chooseDriverBidPrice = (payload = {}) => {
  const bounds = resolveBidBounds(payload);
  const integerMin = Math.ceil(bounds.min);
  const integerMax = Math.floor(bounds.max);

  if (Number.isFinite(integerMin) && Number.isFinite(integerMax) && integerMin <= integerMax) {
    return randomInt(integerMin, integerMax);
  }

  const fallbackValue =
    toFiniteNumber(bounds.min) ??
    toFiniteNumber(bounds.max) ??
    DRIVER_BID_MIN;

  return Math.max(1, Math.round(fallbackValue));
};

const chooseUserCounterPrice = (payload = {}) => {
  const bounds = resolveBidBounds(payload);
  const offeredPrice = toFiniteNumber(payload?.offered_price ?? payload?.price ?? null);
  const minBound = toFiniteNumber(bounds.min) ?? 1;
  const maxBound = toFiniteNumber(bounds.max) ?? offeredPrice ?? minBound;

  if (offeredPrice === null) {
    return Math.max(1, Math.round(Math.min(Math.max(minBound, 1), maxBound)));
  }

  const desiredCounter = offeredPrice - USER_COUNTER_DELTA;
  const highestAllowedCounter = Math.min(maxBound, offeredPrice - 1);
  const clampedCounter = Math.max(minBound, Math.min(highestAllowedCounter, desiredCounter));

  if (!Number.isFinite(clampedCounter) || highestAllowedCounter < minBound) {
    return null;
  }

  return Math.max(1, Math.round(clampedCounter));
};

const resolveScenarioDispatchStartedAt = (scenario = {}) =>
  scenario?.timestamps?.dispatchRequestedAt ??
  scenario?.timestamps?.dispatchSentAt ??
  scenario?.timestamps?.rideCreatedAt ??
  scenario?.timestamps?.startedAt ??
  null;

const round2 = (value) =>
  Number.isFinite(value) ? Math.round(value * 100) / 100 : null;

const percentile = (values, p) => {
  if (!Array.isArray(values) || values.length === 0) return null;
  const sorted = [...values].sort((a, b) => a - b);
  const index = Math.min(
    sorted.length - 1,
    Math.max(0, Math.ceil(sorted.length * p) - 1)
  );
  return round2(sorted[index]);
};

const summarizeLatencies = (values) => ({
  count: values.length,
  p50_ms: percentile(values, 0.5),
  p95_ms: percentile(values, 0.95),
  p99_ms: percentile(values, 0.99),
  max_ms: values.length ? round2(Math.max(...values)) : null,
  avg_ms: values.length
    ? round2(values.reduce((sum, value) => sum + value, 0) / values.length)
    : null,
});

const log = (scope, message, payload) => {
  const line = `[${new Date().toISOString()}] [${scope}] ${message}`;
  if (payload !== undefined) {
    console.log(line, payload);
  } else {
    console.log(line);
  }
};

const toUserName = (row = {}) => {
  const first = String(row.first_name || "").trim();
  const last = String(row.last_name || "").trim();
  const joined = `${first} ${last}`.trim();
  return joined || `User ${row.user_id}`;
};

const toDriverName = (row = {}) => {
  const first = String(row.first_name || "").trim();
  const last = String(row.last_name || "").trim();
  const joined = `${first} ${last}`.trim();
  return joined || `Driver ${row.driver_id}`;
};

const assertConfirmed = () => {
  if (LIVE_LOAD_CONFIRM === CONFIRM_PHRASE) return;
  throw new Error(
    `Refusing to run live load script without LIVE_LOAD_CONFIRM=${CONFIRM_PHRASE}`
  );
};

const createDbPool = () => {
  if (!DB_USER || !DB_NAME) {
    throw new Error("Missing DB_USER or DB_NAME");
  }

  return mysql.createPool({
    host: DB_HOST,
    port: DB_PORT,
    user: DB_USER,
    password: DB_PASSWORD,
    database: DB_NAME,
    waitForConnections: true,
    connectionLimit: 10,
    queueLimit: 0,
  });
};

const fetchUsers = async (pool) => {
  const limit = Math.max(USER_COUNT, SCENARIO_COUNT, USER_IDS.length || 0);
  if (USER_IDS.length > 0) {
    const placeholders = USER_IDS.map(() => "?").join(",");
    const [rows] = await pool.query(
      `
        SELECT
          u.id AS user_id,
          CAST(u.access_token AS CHAR) AS access_token,
          u.first_name,
          u.last_name
        FROM users u
        WHERE u.id IN (${placeholders})
          AND u.deleted_at IS NULL
          AND u.access_token IS NOT NULL
          AND TRIM(u.access_token) <> ''
      `,
      USER_IDS
    );
    const rowById = new Map(
      rows.map((row) => [Number(row.user_id), { ...row, user_name: toUserName(row) }])
    );
    return USER_IDS.map((id) => rowById.get(id)).filter(Boolean);
  }

  const [rows] = await pool.query(
    `
      SELECT
        u.id AS user_id,
        CAST(u.access_token AS CHAR) AS access_token,
        u.first_name,
        u.last_name
      FROM users u
      WHERE u.deleted_at IS NULL
        AND u.access_token IS NOT NULL
        AND TRIM(u.access_token) <> ''
      ORDER BY u.id ASC
      LIMIT ?
    `,
    [limit]
  );

  return rows.map((row) => ({ ...row, user_name: toUserName(row) }));
};

const fetchDrivers = async (pool) => {
  const limit = Math.max(DRIVER_COUNT, DRIVER_IDS.length || 0);
  if (DRIVER_IDS.length > 0) {
    const placeholders = DRIVER_IDS.map(() => "?").join(",");
    const params = [...DRIVER_IDS, SERVICE_CATEGORY_ID];
    let typeFilterSql = "";
    if (
      !MATCH_SCENARIO_DRIVER_SERVICE_TYPE &&
      Number.isFinite(REQUEST_SERVICE_TYPE_ID) &&
      REQUEST_SERVICE_TYPE_ID > 0
    ) {
      typeFilterSql = " AND tdvl.vehicle_type_id = ? ";
      params.push(REQUEST_SERVICE_TYPE_ID);
    }
    const [rows] = await pool.query(
      `
        SELECT
          p.id AS driver_id,
          CAST(p.access_token AS CHAR) AS access_token,
          p.first_name,
          p.last_name,
          ps.id AS driver_service_id,
          ps.service_cat_id AS service_category_id,
          COALESCE(tdvl.vehicle_type_id, 0) AS service_type_id
        FROM providers p
        INNER JOIN provider_services ps
          ON ps.provider_id = p.id
        LEFT JOIN transport_driver_details td
          ON td.provider_service_id = ps.id
        LEFT JOIN transport_driver_vehicle_lists tdvl
          ON tdvl.id = td.vehicle_type_id
        WHERE p.id IN (${placeholders})
          AND p.deleted_at IS NULL
          AND p.access_token IS NOT NULL
          AND TRIM(p.access_token) <> ''
          AND ps.service_cat_id = ?
          ${typeFilterSql}
        ORDER BY ps.id ASC
      `,
      params
    );
    const rowById = new Map(
      rows.map((row) => [Number(row.driver_id), { ...row, driver_name: toDriverName(row) }])
    );
    return DRIVER_IDS.map((id) => rowById.get(id)).filter(Boolean);
  }

  const params = [SERVICE_CATEGORY_ID, limit];
  let typeFilterSql = "";
  if (
    !MATCH_SCENARIO_DRIVER_SERVICE_TYPE &&
    Number.isFinite(REQUEST_SERVICE_TYPE_ID) &&
    REQUEST_SERVICE_TYPE_ID > 0
  ) {
    typeFilterSql = " AND tdvl.vehicle_type_id = ? ";
    params.splice(1, 0, REQUEST_SERVICE_TYPE_ID);
  }
  const [rows] = await pool.query(
    `
      SELECT
        p.id AS driver_id,
        CAST(p.access_token AS CHAR) AS access_token,
        p.first_name,
        p.last_name,
        ps.id AS driver_service_id,
        ps.service_cat_id AS service_category_id,
        COALESCE(tdvl.vehicle_type_id, 0) AS service_type_id
      FROM providers p
      INNER JOIN provider_services ps
        ON ps.provider_id = p.id
      LEFT JOIN transport_driver_details td
        ON td.provider_service_id = ps.id
      LEFT JOIN transport_driver_vehicle_lists tdvl
        ON tdvl.id = td.vehicle_type_id
      WHERE p.deleted_at IS NULL
        AND p.access_token IS NOT NULL
        AND TRIM(p.access_token) <> ''
        AND ps.service_cat_id = ?
        ${typeFilterSql}
      ORDER BY ps.id ASC
      LIMIT ?
    `,
    params
  );

  return rows.map((row) => ({ ...row, driver_name: toDriverName(row) }));
};

const buildScenarioCoordinates = (index) => {
  const gridCols = Math.max(1, Math.ceil(Math.sqrt(SCENARIO_COUNT)));
  const row = Math.floor(index / gridCols);
  const col = index % gridCols;
  const latOffset = row * COORDINATE_SPACING;
  const longOffset = col * COORDINATE_SPACING;
  return {
    pickup: {
      lat: BASE_PICKUP_LAT + latOffset,
      long: BASE_PICKUP_LONG + longOffset,
      address: `Load Pickup ${index + 1}`,
    },
    destination: {
      lat: BASE_DEST_LAT + latOffset,
      long: BASE_DEST_LONG + longOffset,
      address: `Load Destination ${index + 1}`,
    },
  };
};

const buildAddressList = (scenario) => [
  {
    address: scenario.pickup.address,
    place_id: null,
    address_lat: scenario.pickup.lat,
    address_long: scenario.pickup.long,
  },
  {
    address: scenario.destination.address,
    place_id: null,
    address_lat: scenario.destination.lat,
    address_long: scenario.destination.long,
  },
];

const mapWithConcurrency = async (items, concurrency, mapper) => {
  const list = Array.isArray(items) ? items : [];
  const safeConcurrency = Math.max(1, Math.floor(concurrency || 1));
  const results = new Array(list.length);
  let nextIndex = 0;

  const runWorker = async () => {
    while (nextIndex < list.length) {
      const currentIndex = nextIndex;
      nextIndex += 1;
      results[currentIndex] = await mapper(list[currentIndex], currentIndex);
    }
  };

  const workers = Array.from(
    { length: Math.min(safeConcurrency, list.length || 1) },
    () => runWorker()
  );
  await Promise.all(workers);
  return results;
};

const waitForSocketConnect = (socket, label) =>
  new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      cleanup();
      reject(new Error(`${label} connect timeout`));
    }, SOCKET_CONNECT_TIMEOUT_MS);

    const cleanup = () => {
      clearTimeout(timer);
      socket.off("connect", onConnect);
      socket.off("connect_error", onError);
      socket.off("error", onError);
    };

    const onConnect = () => {
      cleanup();
      resolve();
    };

    const onError = (error) => {
      cleanup();
      reject(error instanceof Error ? error : new Error(String(error)));
    };

    socket.once("connect", onConnect);
    socket.once("connect_error", onError);
    socket.once("error", onError);
  });

const connectDriverClients = async (drivers) => {
  await mapWithConcurrency(drivers, CONNECT_BATCH_SIZE, async (driver, index) => {
    const coordinates = buildScenarioCoordinates(index);
    const socket = io(SOCKET_URL, {
      transports: ["websocket"],
      reconnection: false,
    });

    const client = {
      ...driver,
      socket,
      pickup: coordinates.pickup,
      submittedRideIds: new Set(),
      acceptedCounterRideIds: new Set(),
      acceptedRideId: null,
      heartbeat: null,
    };

    driverClients.set(Number(driver.driver_id), client);

    socket.on("ride:bidRequest", (payload = {}) => {
      const rideId = Number(payload?.ride_id);
      if (!Number.isFinite(rideId) || rideId <= 0) return;
      metrics.driverBidRequests += 1;
      const scenario = scenariosByRideId.get(rideId);
      if (!scenario) return;
      if (client.acceptedRideId && client.acceptedRideId !== rideId) return;
      if (client.submittedRideIds.has(rideId)) return;

      client.submittedRideIds.add(rideId);
      const offeredPrice = chooseDriverBidPrice(payload);
      const bidDelayMs =
        DRIVER_BID_DELAY_MS > 0 ? randomInt(0, DRIVER_BID_DELAY_MS) : 0;

      setTimeout(() => {
        if (!socket.connected) return;
        socket.emit("driver:submitBid", {
          ride_id: rideId,
          offered_price: offeredPrice,
          driver_service_id: client.driver_service_id,
          access_token: client.access_token,
          service_type_id:
            Number.isFinite(client.service_type_id) && client.service_type_id > 0
              ? client.service_type_id
              : REQUEST_SERVICE_TYPE_ID,
          service_category_id: SERVICE_CATEGORY_ID,
        });
        scenario.bidDrivers.add(Number(client.driver_id));
        metrics.driverBidsSubmitted += 1;
      }, bidDelayMs);
    });

    socket.on("ride:userResponse", (payload = {}) => {
      const rideId = Number(payload?.ride_id);
      if (!Number.isFinite(rideId) || rideId <= 0) return;
      const scenario = scenariosByRideId.get(rideId);
      if (!scenario) return;
      if (!ENABLE_DRIVER_ACCEPT_COUNTER) return;
      if (client.acceptedRideId && client.acceptedRideId !== rideId) return;
      if (client.acceptedCounterRideIds.has(rideId)) return;

      client.acceptedCounterRideIds.add(rideId);
      const acceptDelayMs =
        DRIVER_ACCEPT_DELAY_MS > 0 ? randomInt(0, DRIVER_ACCEPT_DELAY_MS) : 0;

      setTimeout(() => {
        if (!socket.connected) return;
        socket.emit("driver:acceptOffer", {
          ride_id: rideId,
          offered_price: Number(payload?.price ?? payload?.offered_price ?? null),
        });
        scenario.timestamps.acceptOfferSentAt = Date.now();
        metrics.driverCounterAcceptsSent += 1;
      }, acceptDelayMs);
    });

    socket.on("ride:userAccepted", (payload = {}) => {
      const rideId = Number(payload?.ride_id);
      if (!Number.isFinite(rideId) || rideId <= 0) return;
      if (Number(payload?.driver_id) !== Number(client.driver_id)) return;
      client.acceptedRideId = rideId;
    });

    socket.on("ride:cancelled", (payload = {}) => {
      const rideId = Number(payload?.ride_id);
      if (client.acceptedRideId === rideId) {
        client.acceptedRideId = null;
      }
    });

    await waitForSocketConnect(socket, `driver:${driver.driver_id}`);

    socket.emit("driver-online", {
      driver_id: client.driver_id,
      driver_service_id: client.driver_service_id,
      access_token: client.access_token,
      lat: client.pickup.lat,
      long: client.pickup.long,
      service_type_id:
        Number.isFinite(client.service_type_id) && client.service_type_id > 0
          ? client.service_type_id
          : REQUEST_SERVICE_TYPE_ID,
      service_category_id: SERVICE_CATEGORY_ID,
    });

    if (DRIVER_APP_STATE === "foreground" || DRIVER_APP_STATE === "background") {
      socket.emit("driver:appState", {
        driver_id: client.driver_id,
        state: DRIVER_APP_STATE,
      });
    }

    client.heartbeat = setInterval(() => {
      if (!socket.connected) return;
      socket.emit("update-location", {
        lat: client.pickup.lat + (Math.random() - 0.5) / 10000,
        long: client.pickup.long + (Math.random() - 0.5) / 10000,
      });
    }, HEARTBEAT_INTERVAL_MS);

    await sleep(DRIVER_READY_WAIT_MS);
  });
};

const connectUserClients = async (users) => {
  await mapWithConcurrency(users, CONNECT_BATCH_SIZE, async (user) => {
    const socket = io(SOCKET_URL, {
      transports: ["websocket"],
      reconnection: false,
    });

    const client = {
      ...user,
      socket,
    };

    userClients.set(Number(user.user_id), client);

    socket.on("ride:newBid", (payload = {}) => {
      const rideId = Number(payload?.ride_id);
      if (!Number.isFinite(rideId) || rideId <= 0) return;
      const scenario = scenariosByRideId.get(rideId);
      if (!scenario || Number(scenario.user.user_id) !== Number(user.user_id)) return;
      metrics.userNewBids += 1;

      if (!scenario.timestamps.firstBidAt) {
        scenario.timestamps.firstBidAt = Date.now();
        const dispatchStartedAt = resolveScenarioDispatchStartedAt(scenario);
        if (dispatchStartedAt) {
          metrics.firstBidMs.push(
            scenario.timestamps.firstBidAt - dispatchStartedAt
          );
        }
      }

      if (AUTO_USER_ACCEPT_FIRST_BID) {
        if (!ENABLE_USER_ACCEPT) return;
        if (scenario.userAcceptSent || scenario.userAccepted) return;

        const driverId = Number(payload?.driver_id);
        const offeredPrice = Number(payload?.offered_price ?? payload?.price ?? null);
        if (!Number.isFinite(driverId) || driverId <= 0) return;
        if (!Number.isFinite(offeredPrice) || offeredPrice <= 0) return;

        scenario.selectedDriverId = driverId;
        scenario.userAcceptSent = true;

        setTimeout(() => {
          if (!socket.connected) return;
          socket.emit("user:acceptOffer", {
            ride_id: rideId,
            driver_id: driverId,
            offered_price: offeredPrice,
            user_id: scenario.user.user_id,
            access_token: scenario.user.access_token,
          });
          scenario.timestamps.userAcceptSentAt = Date.now();
          metrics.userAcceptsSent += 1;
        }, USER_ACCEPT_DELAY_MS);

        return;
      }

      if (!ENABLE_USER_COUNTER) return;
      if (scenario.counterSent) return;

      const counterPrice = chooseUserCounterPrice(payload);
      if (counterPrice === null) return;
      scenario.counterSent = true;
      scenario.selectedDriverId = Number(payload?.driver_id);
      const counterDelayMs =
        USER_COUNTER_DELAY_MS > 0 ? randomInt(0, USER_COUNTER_DELAY_MS) : 0;

      setTimeout(() => {
        if (!socket.connected) return;
        socket.emit("user:respondToDriver", {
          ride_id: rideId,
          driver_id: scenario.selectedDriverId,
          type: "counter",
          price: counterPrice,
          message: "Counter offer",
        });
        scenario.timestamps.counterSentAt = Date.now();
        metrics.userCountersSent += 1;
      }, counterDelayMs);
    });

    socket.on("ride:acceptedByDriver", (payload = {}) => {
      const rideId = Number(payload?.ride_id);
      if (!Number.isFinite(rideId) || rideId <= 0) return;
      const scenario = scenariosByRideId.get(rideId);
      if (!scenario || Number(scenario.user.user_id) !== Number(user.user_id)) return;
      scenario.timestamps.acceptedByDriverAt = Date.now();
      if (scenario.timestamps.counterSentAt) {
        metrics.counterToDriverAcceptMs.push(
          scenario.timestamps.acceptedByDriverAt - scenario.timestamps.counterSentAt
        );
      }
      if (!ENABLE_USER_ACCEPT) return;
      if (scenario.userAcceptSent) return;

      const driverId = Number(payload?.driver_id);
      const offeredPrice = Number(payload?.offered_price ?? payload?.price ?? null);
      if (!Number.isFinite(driverId) || driverId <= 0) return;
      if (!Number.isFinite(offeredPrice) || offeredPrice <= 0) return;

      scenario.userAcceptSent = true;
      scenario.selectedDriverId = driverId;

      setTimeout(() => {
        if (!socket.connected) return;
        socket.emit("user:acceptOffer", {
          ride_id: rideId,
          driver_id: driverId,
          offered_price: offeredPrice,
          user_id: scenario.user.user_id,
          access_token: scenario.user.access_token,
        });
        scenario.timestamps.userAcceptSentAt = Date.now();
        metrics.userAcceptsSent += 1;
      }, USER_ACCEPT_DELAY_MS);
    });

    socket.on("ride:userAccepted", (payload = {}) => {
      const rideId = Number(payload?.ride_id);
      if (!Number.isFinite(rideId) || rideId <= 0) return;
      const scenario = scenariosByRideId.get(rideId);
      if (!scenario || Number(scenario.user.user_id) !== Number(user.user_id)) return;
      if (scenario.userAccepted) return;

      scenario.userAccepted = true;
      scenario.timestamps.userAcceptedAt = Date.now();
      metrics.userAcceptedEvents += 1;
      const dispatchStartedAt = resolveScenarioDispatchStartedAt(scenario);
      if (dispatchStartedAt) {
        metrics.userAcceptedMs.push(
          scenario.timestamps.userAcceptedAt - dispatchStartedAt
        );
      }
    });

    await waitForSocketConnect(socket, `user:${user.user_id}`);
    socket.emit("user:loginInfo", {
      user_id: client.user_id,
      token: client.access_token,
      access_token: client.access_token,
      user_name: client.user_name,
    });
    await sleep(USER_READY_WAIT_MS);
  });
};

const createRideForScenario = async (scenario, attempt = 0) => {
  const startedAt = Date.now();
  const body = new URLSearchParams();
  body.set("user_id", String(scenario.user.user_id));
  body.set("access_token", String(scenario.user.access_token));
  body.set("service_category_id", String(scenario.serviceCategoryId));
  body.set("service_type_id", String(scenario.requestServiceTypeId));
  body.set("payment_type", String(PAYMENT_TYPE));
  body.set("estimated_time", ESTIMATED_TIME);
  body.set("total_distance", TOTAL_DISTANCE);
  body.set("bidding_offer", String(BIDDING_OFFER));
  body.set("handicap", HANDICAP);
  body.set("child_seat", CHILD_SEAT);
  body.set("address_list", JSON.stringify(buildAddressList(scenario)));

  try {
    const res = await httpLaravel.post(
      "/api/customer/transport/ride-booking",
      body.toString(),
      {
        headers: { "Content-Type": "application/x-www-form-urlencoded" },
      }
    );
    if (!res?.data || Number(res.data.status) !== 1 || !Number(res.data.ride_id)) {
      if (
        RETRY_ACTIVE_RIDE_CANCEL &&
        res?.data?.active_ride_id &&
        attempt < 1
      ) {
        await cancelRideForScenario(
          {
            user: scenario.user,
            rideId: Number(res.data.active_ride_id),
          },
          { force: true }
        );
        return createRideForScenario(scenario, attempt + 1);
      }
      throw new Error(`ride booking failed: ${JSON.stringify(res?.data ?? {})}`);
    }

    const rideId = Number(res.data.ride_id);
    scenario.rideId = rideId;
    scenario.timestamps.rideCreatedAt = Date.now();
    metrics.rideCreateMs.push(scenario.timestamps.rideCreatedAt - startedAt);
    metrics.ridesCreated += 1;
    createdRideIds.add(rideId);
    scenariosByRideId.set(rideId, scenario);
    return rideId;
  } catch (error) {
    metrics.rideCreateFailures += 1;
    throw error;
  }
};

const joinRideRoomForScenario = (scenario) => {
  const client = userClients.get(Number(scenario.user.user_id));
  if (!client) throw new Error(`Missing user client ${scenario.user.user_id}`);

  client.socket.emit("user:joinRideRoom", {
    user_id: scenario.user.user_id,
    ride_id: scenario.rideId,
    access_token: scenario.user.access_token,
    token: scenario.user.access_token,
  });
  client.socket.emit("user:findNearbyDrivers", {
    user_id: scenario.user.user_id,
    lat: scenario.pickup.lat,
    long: scenario.pickup.long,
    token: scenario.user.access_token,
    service_type_id: scenario.requestServiceTypeId,
    service_category_id: scenario.serviceCategoryId,
  });
};

const dispatchScenario = async (scenario) => {
  const startedAt = Date.now();
  scenario.timestamps.dispatchRequestedAt = startedAt;
  const payload = {
    ride_id: scenario.rideId,
    service_category_id: scenario.serviceCategoryId,
    service_type_id: scenario.requestServiceTypeId,
    pickup_lat: scenario.pickup.lat,
    pickup_long: scenario.pickup.long,
    pickup_address: scenario.pickup.address,
    destination_lat: scenario.destination.lat,
    destination_long: scenario.destination.long,
    destination_address: scenario.destination.address,
    radius: 5000,
    user_bid_price: BIDDING_OFFER,
    min_fare_amount: null,
    user_id: scenario.user.user_id,
    user_name: scenario.user.user_name,
    token: scenario.user.access_token,
    access_token: scenario.user.access_token,
    user_details: {
      user_id: scenario.user.user_id,
      user_name: scenario.user.user_name,
      token: scenario.user.access_token,
      access_token: scenario.user.access_token,
    },
    ...(TARGET_ASSIGNED_DRIVER_ONLY && scenario.driver?.driver_id
      ? {
          driver_ids: [scenario.driver.driver_id],
          restrict_to_driver_ids: 1,
        }
      : {}),
  };

  try {
    await httpSocket.post("/events/internal/ride-bid-dispatch", payload);
    scenario.timestamps.dispatchSentAt = Date.now();
    metrics.dispatchMs.push(scenario.timestamps.dispatchSentAt - startedAt);
    metrics.dispatchSent += 1;
  } catch (error) {
    metrics.dispatchFailures += 1;
    throw error;
  }
};

const cancelRideForScenario = async (scenarioLike, options = {}) => {
  const force = options?.force === true;
  if (!force && !AUTO_CLEANUP) return false;
  const rideId = Number(scenarioLike?.rideId ?? null);
  const user = scenarioLike?.user ?? null;
  if (!rideId || !user?.user_id || !user?.access_token) return false;

  metrics.cleanupAttempted += 1;
  const body = new URLSearchParams();
  body.set("ride_id", String(rideId));
  body.set("user_id", String(user.user_id));
  body.set("reason_id", String(CANCEL_REASON_ID));
  body.set("sub_ride_id", "0");
  body.set("access_token", String(user.access_token));

  try {
    await httpLaravel.post("/api/customer/transport/cancel-ride", body.toString(), {
      headers: { "Content-Type": "application/x-www-form-urlencoded" },
    });
    metrics.cleanupSucceeded += 1;
    return true;
  } catch (error) {
    metrics.cleanupFailed += 1;
    return false;
  }
};

const disconnectAllClients = async () => {
  for (const client of driverClients.values()) {
    if (client.heartbeat) clearInterval(client.heartbeat);
    if (client.socket?.connected) client.socket.disconnect();
  }
  for (const client of userClients.values()) {
    if (client.socket?.connected) client.socket.disconnect();
  }
};

const printSummary = (scenarios) => {
  const acceptedScenarios = scenarios.filter((scenario) => scenario.userAccepted).length;
  const counterScenarios = scenarios.filter((scenario) => scenario.counterSent).length;
  const firstBidScenarios = scenarios.filter((scenario) => !!scenario.timestamps.firstBidAt).length;
  const createdScenarios = scenarios.filter((scenario) => !!scenario.rideId).length;

  const summary = {
    config: {
      scenario_count: SCENARIO_COUNT,
      effective_scenario_count: scenarios.length,
      driver_count: DRIVER_COUNT,
      user_count: USER_COUNT,
      connected_user_count: userClients.size,
      service_category_id: SERVICE_CATEGORY_ID,
      match_scenario_driver_service_type: MATCH_SCENARIO_DRIVER_SERVICE_TYPE,
      target_assigned_driver_only: TARGET_ASSIGNED_DRIVER_ONLY,
      auto_user_accept_first_bid: AUTO_USER_ACCEPT_FIRST_BID,
      request_service_type_id: REQUEST_SERVICE_TYPE_ID,
      auto_cleanup: AUTO_CLEANUP,
      manual_dispatch_after_booking: MANUAL_DISPATCH_AFTER_BOOKING,
      test_window_ms: TEST_WINDOW_MS,
      socket_http_timeout_ms: SOCKET_HTTP_TIMEOUT_MS,
      laravel_http_timeout_ms: LARAVEL_HTTP_TIMEOUT_MS,
    },
    counters: {
      rides_created: metrics.ridesCreated,
      ride_create_failures: metrics.rideCreateFailures,
      dispatch_sent: metrics.dispatchSent,
      dispatch_failures: metrics.dispatchFailures,
      driver_bid_requests: metrics.driverBidRequests,
      driver_bids_submitted: metrics.driverBidsSubmitted,
      user_new_bids: metrics.userNewBids,
      user_counters_sent: metrics.userCountersSent,
      driver_counter_accepts_sent: metrics.driverCounterAcceptsSent,
      user_accepts_sent: metrics.userAcceptsSent,
      user_accepted_events: metrics.userAcceptedEvents,
      cleanup_attempted: metrics.cleanupAttempted,
      cleanup_succeeded: metrics.cleanupSucceeded,
      cleanup_failed: metrics.cleanupFailed,
      scenarios_created: createdScenarios,
      scenarios_with_first_bid: firstBidScenarios,
      scenarios_with_counter: counterScenarios,
      scenarios_user_accepted: acceptedScenarios,
    },
    latencies: {
      ride_create_ms: summarizeLatencies(metrics.rideCreateMs),
      dispatch_ms: summarizeLatencies(metrics.dispatchMs),
      first_bid_ms: summarizeLatencies(metrics.firstBidMs),
      counter_to_driver_accept_ms: summarizeLatencies(metrics.counterToDriverAcceptMs),
      user_accepted_ms: summarizeLatencies(metrics.userAcceptedMs),
    },
    scenario_sample: scenarios.slice(0, 5).map((scenario) => ({
      scenario_id: scenario.id,
      user_id: scenario.user.user_id,
      driver_id: scenario.driver?.driver_id ?? null,
      request_service_type_id: scenario.requestServiceTypeId,
      service_category_id: scenario.serviceCategoryId,
      ride_id: scenario.rideId ?? null,
      selected_driver_id: scenario.selectedDriverId ?? null,
      counter_sent: scenario.counterSent,
      user_accept_sent: scenario.userAcceptSent,
      user_accepted: scenario.userAccepted,
      dispatch_requested_at: scenario.timestamps.dispatchRequestedAt,
      first_bid_at: scenario.timestamps.firstBidAt,
      user_accepted_at: scenario.timestamps.userAcceptedAt,
      bid_drivers: Array.from(scenario.bidDrivers).slice(0, 10),
    })),
  };

  console.log(JSON.stringify(summary, null, 2));
};

const main = async () => {
  assertConfirmed();
  const pool = createDbPool();

  try {
    const [usersRaw, driversRaw] = await Promise.all([
      fetchUsers(pool),
      fetchDrivers(pool),
    ]);

    const requiredUserCount = Math.max(USER_COUNT, SCENARIO_COUNT);
    const requiredScenarioCount = Math.min(SCENARIO_COUNT, DRIVER_COUNT);
    const users = usersRaw.slice(0, requiredUserCount);
    const drivers = driversRaw.slice(0, DRIVER_COUNT);

    if (users.length < requiredUserCount) {
      throw new Error(
        `Not enough users with access_token. Needed ${requiredUserCount}, got ${users.length}`
      );
    }
    if (drivers.length < DRIVER_COUNT) {
      throw new Error(
        `Not enough drivers for service_category_id=${SERVICE_CATEGORY_ID} and service_type_id=${REQUEST_SERVICE_TYPE_ID}. Needed ${DRIVER_COUNT}, got ${drivers.length}`
      );
    }

    const scenarios = users.slice(0, requiredScenarioCount).map((user, index) => {
      const coordinates = buildScenarioCoordinates(index);
      const assignedDriver = drivers[index % drivers.length];
      const scenarioServiceCategoryId =
        Number.isFinite(Number(assignedDriver?.service_category_id)) &&
        Number(assignedDriver.service_category_id) > 0
          ? Number(assignedDriver.service_category_id)
          : SERVICE_CATEGORY_ID;
      const scenarioRequestServiceTypeId =
        MATCH_SCENARIO_DRIVER_SERVICE_TYPE &&
        Number.isFinite(Number(assignedDriver?.service_type_id)) &&
        Number(assignedDriver.service_type_id) > 0
          ? Number(assignedDriver.service_type_id)
          : REQUEST_SERVICE_TYPE_ID;
      return {
        id: index + 1,
        user,
        driver: assignedDriver,
        serviceCategoryId: scenarioServiceCategoryId,
        requestServiceTypeId: scenarioRequestServiceTypeId,
        pickup: coordinates.pickup,
        destination: coordinates.destination,
        rideId: null,
        selectedDriverId: null,
        counterSent: false,
        userAcceptSent: false,
        userAccepted: false,
        bidDrivers: new Set(),
        timestamps: {
          startedAt: Date.now(),
          rideCreatedAt: null,
          dispatchRequestedAt: null,
          dispatchSentAt: null,
          firstBidAt: null,
          counterSentAt: null,
          acceptedByDriverAt: null,
          acceptOfferSentAt: null,
          userAcceptSentAt: null,
          userAcceptedAt: null,
        },
      };
    });

    log("setup", "connecting drivers", { count: drivers.length });
    await connectDriverClients(drivers);
    log("setup", "connecting users", { count: users.length });
    await connectUserClients(users);

    const scenarioStartBaseAt = Date.now();
    await mapWithConcurrency(
      scenarios,
      SCENARIO_CONCURRENCY,
      async (scenario, index) => {
        if (SCENARIO_STAGGER_MS > 0) {
          const scheduledAt = scenarioStartBaseAt + index * SCENARIO_STAGGER_MS;
          const delayMs = scheduledAt - Date.now();
          if (delayMs > 0) {
            await sleep(delayMs);
          }
        }
        try {
          await createRideForScenario(scenario);
          joinRideRoomForScenario(scenario);
          if (MANUAL_DISPATCH_AFTER_BOOKING) {
            await dispatchScenario(scenario);
          }
        } catch (error) {
          log("scenario", `scenario ${scenario.id} failed`, error?.response?.data || error?.message || error);
        }
      }
    );

    log("load", "scenarios started", {
      count: scenarios.length,
      test_window_ms: TEST_WINDOW_MS,
    });

    await sleep(TEST_WINDOW_MS);

    if (AUTO_CLEANUP) {
      await mapWithConcurrency(
        scenarios.filter((scenario) => !!scenario.rideId),
        CLEANUP_CONCURRENCY,
        async (scenario) => {
          await cancelRideForScenario(scenario);
        }
      );
    }

    printSummary(scenarios);
    await disconnectAllClients();
  } finally {
    await pool.end();
  }
};

main()
  .then(() => process.exit(0))
  .catch(async (error) => {
    console.error(error);
    await disconnectAllClients();
    process.exit(1);
  });
