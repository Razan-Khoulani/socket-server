const axios = require("axios");
const { io } = require("socket.io-client");

const args = process.argv.slice(2);
const getArg = (name, def = null) => {
  const idx = args.indexOf(`--${name}`);
  if (idx === -1) return def;
  const v = args[idx + 1];
  if (v == null || v.startsWith("--")) return def;
  return v;
};

const num = (v, def = null) => {
  const n = Number(v);
  return Number.isFinite(n) ? n : def;
};

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

const SOCKET_URL =
  getArg("socket", process.env.SOCKET_URL) || "https://aiactive.co.uk:4000";
const SOCKET_HTTP_URL =
  getArg("socketHttp", process.env.SOCKET_HTTP_URL) || SOCKET_URL;
const LARAVEL_BASE_URL =
  getArg("laravel", process.env.LARAVEL_BASE_URL) ||
  "https://aiactive.co.uk/backend/backend-laravel/public";

const DRIVER_ID = num(getArg("driver", process.env.DRIVER_ID), 7);
const DRIVER_SERVICE_ID = num(
  getArg("driverService", process.env.DRIVER_SERVICE_ID),
  7
);
const DRIVER_ACCESS_TOKEN = getArg(
  "driverToken",
  process.env.DRIVER_ACCESS_TOKEN
);
const SERVICE_TYPE_ID = num(getArg("serviceType", process.env.SERVICE_TYPE_ID), 2);

const USER_ID = num(getArg("user", process.env.USER_ID), 2);
const USER_NAME = getArg("userName", process.env.USER_NAME) || "Test User";
const USER_TOKEN = getArg("userToken", process.env.USER_TOKEN) || "";

const RIDE_ID = num(getArg("ride", process.env.RIDE_ID), 9);
const SERVICE_CATEGORY_ID = num(
  getArg("serviceCategory", process.env.SERVICE_CATEGORY_ID),
  5
);

const OFFERED_PRICE = num(getArg("price", process.env.OFFERED_PRICE), 500);

const PICKUP_LAT = num(getArg("pickupLat", process.env.PICKUP_LAT), 33.49356);
const PICKUP_LONG = num(getArg("pickupLong", process.env.PICKUP_LONG), 36.24070);
const DEST_LAT = num(getArg("destLat", process.env.DEST_LAT), 33.48312);
const DEST_LONG = num(getArg("destLong", process.env.DEST_LONG), 36.24139);

const log = (scope, msg, data) => {
  const line = `[${new Date().toISOString()}] [${scope}] ${msg}`;
  if (data !== undefined) {
    console.log(line, JSON.stringify(data));
  } else {
    console.log(line);
  }
};

const wireEvents = (socket, scope) => {
  const events = [
    "driver:ready",
    "ride:bidRequest",
    "driver:rides:list",
    "driver:rides:list:update",
    "ride:newBid",
    "ride:userAccepted",
    "ride:acceptedByDriver",
    "ride:driverAccepted",
    "ride:statusUpdated",
    "ride:ended",
    "ride:invoice",
    "ride:arrived:ack",
    "ride:locationUpdate",
    "driver:moved",
    "user:nearbyDrivers",
    "user:nearbyDrivers:update",
    "user:nearbyVehicleTypes",
    "user:nearbyVehicleTypes:update",
    "ride:cancelled",
    "ride:completed",
  ];
  events.forEach((ev) => socket.on(ev, (payload) => log(scope, ev, payload)));
};

const main = async () => {
  log("config", "Using", {
    SOCKET_URL,
    SOCKET_HTTP_URL,
    LARAVEL_BASE_URL,
    DRIVER_ID,
    DRIVER_SERVICE_ID,
    SERVICE_TYPE_ID,
    USER_ID,
    RIDE_ID,
  });

  const driverSocket = io(SOCKET_URL, { transports: ["websocket"] });
  const userSocket = io(SOCKET_URL, { transports: ["websocket"] });

  wireEvents(driverSocket, "driver");
  wireEvents(userSocket, "user");

  await Promise.all([
    new Promise((resolve) => driverSocket.on("connect", resolve)),
    new Promise((resolve) => userSocket.on("connect", resolve)),
  ]);

  log("driver", "connected", { id: driverSocket.id });
  log("user", "connected", { id: userSocket.id });

  // 1) driver-online
  driverSocket.emit("driver-online", {
    driver_id: DRIVER_ID,
    driver_service_id: DRIVER_SERVICE_ID,
    access_token: DRIVER_ACCESS_TOKEN,
    lat: PICKUP_LAT,
    long: PICKUP_LONG,
    service_type_id: SERVICE_TYPE_ID,
  });
  log("driver", "driver-online sent");

  // 2) user initial data + nearby search
  userSocket.emit("user:initialData", {
    token: USER_TOKEN,
    user_name: USER_NAME,
    user_id: USER_ID,
    gender: 0,
    image: "",
  });
  log("user", "user:initialData sent");

  userSocket.emit("user:findNearbyDrivers", {
    user_id: USER_ID,
    lat: PICKUP_LAT,
    long: PICKUP_LONG,
    token: USER_TOKEN,
    user_name: USER_NAME,
  });
  log("user", "user:findNearbyDrivers sent");

  userSocket.emit("user:joinRideRoom", { user_id: USER_ID, ride_id: RIDE_ID });
  log("user", "user:joinRideRoom sent", { ride_id: RIDE_ID });

  await sleep(800);

  // 3) Dispatch ride to nearby drivers (simulate Laravel internal dispatch)
  try {
    const dispatchPayload = {
      ride_id: RIDE_ID,
      service_category_id: SERVICE_CATEGORY_ID,
      service_type_id: SERVICE_TYPE_ID,
      pickup_lat: PICKUP_LAT,
      pickup_long: PICKUP_LONG,
      pickup_address: "Test Pickup",
      destination_lat: DEST_LAT,
      destination_long: DEST_LONG,
      destination_address: "Test Destination",
      radius: 500,
      user_bid_price: OFFERED_PRICE,
      min_fare_amount: null,
      user_id: USER_ID,
      user_name: USER_NAME,
      token: USER_TOKEN,
    };

    const res = await axios.post(
      `${SOCKET_HTTP_URL}/events/internal/ride-bid-dispatch`,
      dispatchPayload,
      { timeout: 7000 }
    );
    log("http", "ride-bid-dispatch response", res.data);
  } catch (e) {
    log("http", "ride-bid-dispatch error", e?.response?.data || e.message);
  }

  await sleep(1000);

  // 4) Driver submit bid
  driverSocket.emit("driver:submitBid", {
    ride_id: RIDE_ID,
    offered_price: OFFERED_PRICE,
    pickup_address: "Test Pickup",
    destination_address: "Test Destination",
    pickup_lat: PICKUP_LAT,
    pickup_long: PICKUP_LONG,
    destination_lat: DEST_LAT,
    destination_long: DEST_LONG,
  });
  log("driver", "driver:submitBid sent", { ride_id: RIDE_ID, offered_price: OFFERED_PRICE });

  await sleep(800);

  // 5) User accept offer (socket)
  userSocket.emit("user:acceptOffer", {
    ride_id: RIDE_ID,
    driver_id: DRIVER_ID,
    offered_price: OFFERED_PRICE,
    user_id: USER_ID,
    access_token: USER_TOKEN,
  });
  log("user", "user:acceptOffer sent", { ride_id: RIDE_ID, driver_id: DRIVER_ID });

  await sleep(1500);

  // 6) Call Laravel update-ride-status (should trigger ride:statusUpdated)
  if (DRIVER_ACCESS_TOKEN) {
    try {
      const res = await axios.post(
        `${LARAVEL_BASE_URL}/api/driver/update-ride-status`,
        {
          driver_id: DRIVER_ID,
          access_token: DRIVER_ACCESS_TOKEN,
          driver_service_id: DRIVER_SERVICE_ID,
          service_category_id: SERVICE_CATEGORY_ID,
          ride_id: RIDE_ID,
          ride_status: 3,
          way_point_status: 0,
          hail_ride_status: 0,
          current_lat: PICKUP_LAT,
          current_long: PICKUP_LONG,
        },
        { timeout: 7000 }
      );
      log("api", "update-ride-status (3) response", res.data);
    } catch (e) {
      log("api", "update-ride-status (3) error", e?.response?.data || e.message);
    }

    await sleep(1200);

    try {
      const res = await axios.post(
        `${LARAVEL_BASE_URL}/api/driver/update-ride-status`,
        {
          driver_id: DRIVER_ID,
          access_token: DRIVER_ACCESS_TOKEN,
          driver_service_id: DRIVER_SERVICE_ID,
          service_category_id: SERVICE_CATEGORY_ID,
          ride_id: RIDE_ID,
          ride_status: 9,
          way_point_status: 0,
          hail_ride_status: 0,
          current_lat: PICKUP_LAT,
          current_long: PICKUP_LONG,
        },
        { timeout: 7000 }
      );
      log("api", "update-ride-status (9) response", res.data);
    } catch (e) {
      log("api", "update-ride-status (9) error", e?.response?.data || e.message);
    }
  } else {
    log("warn", "Skipping update-ride-status API calls (missing DRIVER_ACCESS_TOKEN)");
  }

  await sleep(2000);

  driverSocket.disconnect();
  userSocket.disconnect();
  log("done", "Test finished");
  process.exit(0);
};

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
