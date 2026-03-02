/* eslint-disable no-console */
const io = require("socket.io-client");
const axios = require("axios");

// =========================
// CONFIG (override via env)
// =========================
const SOCKET_URL = process.env.SOCKET_URL || "http://192.168.100.51:3000";
const SOCKET_HTTP_URL = process.env.SOCKET_HTTP_URL || SOCKET_URL;
const LARAVEL_URL = process.env.LARAVEL_URL || "http://192.168.100.51:8000";

const SCENARIO = (process.env.SCENARIO || "happy").toLowerCase();

const RIDE_ID = Number(process.env.RIDE_ID || 18);
const USER_ID = Number(process.env.USER_ID || 9);
const DRIVER_ID = Number(process.env.DRIVER_ID || 77);

const DRIVER_SERVICE_ID = Number(process.env.DRIVER_SERVICE_ID || 15);
const SERVICE_CATEGORY_ID = Number(process.env.SERVICE_CATEGORY_ID || 5);
const SERVICE_TYPE_ID = Number(process.env.SERVICE_TYPE_ID || 2);

const DRIVER_ACCESS_TOKEN = process.env.DRIVER_ACCESS_TOKEN || "";
const USER_ACCESS_TOKEN = process.env.USER_ACCESS_TOKEN || "";
const TOTAL_AMOUNT = Number(process.env.TOTAL_AMOUNT || 6000);
const ESTIMATED_TIME = String(process.env.ESTIMATED_TIME || "10");
const TOTAL_DISTANCE = String(process.env.TOTAL_DISTANCE || "2");

const DRIVER2_ID = Number(process.env.DRIVER2_ID || 77);
const DRIVER2_SERVICE_ID = Number(process.env.DRIVER2_SERVICE_ID || DRIVER_SERVICE_ID);
const DRIVER2_SERVICE_CATEGORY_ID = Number(
  process.env.DRIVER2_SERVICE_CATEGORY_ID || SERVICE_CATEGORY_ID
);
const DRIVER2_SERVICE_TYPE_ID = Number(
  process.env.DRIVER2_SERVICE_TYPE_ID || SERVICE_TYPE_ID
);
const DRIVER2_ACCESS_TOKEN = process.env.DRIVER2_ACCESS_TOKEN || "";

const PICKUP = {
  lat: Number(process.env.PICKUP_LAT || 33.5142),
  long: Number(process.env.PICKUP_LONG || 36.2774),
};
const DESTINATION = {
  lat: Number(process.env.DEST_LAT || 33.5201),
  long: Number(process.env.DEST_LONG || 36.2812),
};

// STATUS_MODE:
// - socket-event: driver emits driver:updateRideStatus (Node calls Laravel)
// - laravel: direct POST /api/driver/update-ride-status
// - internal: POST /events/internal/ride-status-updated (no DB update)
// - none: skip status flow
const STATUS_MODE = (process.env.STATUS_MODE || "socket-event").toLowerCase();
const STATUS_SEQUENCE = (process.env.STATUS_SEQUENCE || "3")
  .split(",")
  .map((v) => Number(v.trim()))
  .filter((n) => Number.isFinite(n));

const RIDE_TIMEOUT_MS = Number(process.env.RIDE_TIMEOUT_MS || 90000);
const TIMEOUT_CHECK_MS = Number(
  process.env.TIMEOUT_CHECK_MS || RIDE_TIMEOUT_MS + 2000
);

const TEST_DURATION_MS = Number(process.env.TEST_DURATION_MS || 60000);
const DISCONNECT_MS = Number(process.env.DISCONNECT_MS || 4000);
const LARAVEL_FAIL_ONCE = process.env.LARAVEL_FAIL_ONCE === "1";
const LARAVEL_FAIL_URL = process.env.LARAVEL_FAIL_URL || "http://127.0.0.1:5999";

const httpSocket = axios.create({
  baseURL: SOCKET_HTTP_URL,
  timeout: 7000,
});
const httpLaravel = axios.create({
  baseURL: LARAVEL_URL,
  timeout: 7000,
});
const httpLaravelFail = axios.create({
  baseURL: LARAVEL_FAIL_URL,
  timeout: 3000,
});

const state = {
  driverReady: false,
  userReady: false,
  dispatched: false,
  firstBidHandled: false,
  selectedDriverId: null,
  accepted: false,
  userAcceptSent: false,
  laravelFailOnceDone: false,
  statusFlowStarted: false,
  cancelled: false,
  driver2Ready: false,
  lastBids: new Map(),
  serviceCategoryId: Number.isFinite(SERVICE_CATEGORY_ID)
    ? SERVICE_CATEGORY_ID
    : null,
};

const log = (tag, msg, data) => {
  if (data !== undefined) {
    console.log(`[${tag}] ${msg}`, data);
  } else {
    console.log(`[${tag}] ${msg}`);
  }
};

const getStatusDriverContext = () => {
  const chosenId = Number.isFinite(state.selectedDriverId)
    ? Number(state.selectedDriverId)
    : DRIVER_ID;

  if (chosenId === DRIVER2_ID && driver2Socket) {
    return {
      socket: driver2Socket,
      driver_id: DRIVER2_ID,
      driver_service_id: DRIVER2_SERVICE_ID,
      access_token: DRIVER2_ACCESS_TOKEN || undefined,
    };
  }

  return {
    socket: driverSocket,
    driver_id: DRIVER_ID,
    driver_service_id: DRIVER_SERVICE_ID,
    access_token: DRIVER_ACCESS_TOKEN || undefined,
  };
};

const isScenario = (name) => SCENARIO === name;

const setServiceCategoryId = (value, source) => {
  const next = Number(value);
  if (!Number.isFinite(next) || next <= 0) return;
  if (state.serviceCategoryId === next) return;
  state.serviceCategoryId = next;
  log("status", `service_category_id=${next} (${source})`);
};

const requestDriverList = (socket, driverId, label) => {
  if (!socket) return;
  log("driver", `request rides list (${label})`);
  socket.emit("driver:getRidesList", { driver_id: driverId });
};

// =========================
// DRIVER SOCKET
// =========================
const driverSocket = io(SOCKET_URL, { transports: ["websocket"] });

driverSocket.on("connect", () => {
  log("driver", `connected ${driverSocket.id}`);

  driverSocket.emit("driver-online", {
    driver_id: DRIVER_ID,
    lat: PICKUP.lat,
    long: PICKUP.long,
    access_token: DRIVER_ACCESS_TOKEN || undefined,
    driver_service_id: DRIVER_SERVICE_ID,
    service_type_id: SERVICE_TYPE_ID,
    service_category_id: SERVICE_CATEGORY_ID,
  });

  // location heartbeat
  setInterval(() => {
    if (!driverSocket.connected) return;
    const jitterLat = PICKUP.lat + (Math.random() - 0.5) / 5000;
    const jitterLong = PICKUP.long + (Math.random() - 0.5) / 5000;
    driverSocket.emit("update-location", { lat: jitterLat, long: jitterLong });
  }, 2000);
});

driverSocket.on("driver:ready", () => {
  state.driverReady = true;
  log("driver", "ready");
  maybeDispatchRide();
});

driverSocket.on("ride:bidRequest", (payload) => {
  log("driver", "ride:bidRequest", payload);
  setServiceCategoryId(payload?.service_category_id, "driver:bidRequest");
  if (isScenario("timeout")) {
    log("driver", "skip bid (timeout scenario)");
    return;
  }
  const offered_price = 5000 + Math.floor(Math.random() * 1500);

  driverSocket.emit("driver:submitBid", {
    ride_id: payload.ride_id,
    offered_price,
    driver_service_id: DRIVER_SERVICE_ID,
    access_token: DRIVER_ACCESS_TOKEN || undefined,
    service_type_id: SERVICE_TYPE_ID,
    service_category_id: state.serviceCategoryId ?? SERVICE_CATEGORY_ID,
  });
  log("driver", `submitted bid ${offered_price}`);
});

driverSocket.on("ride:userResponse", (payload) => {
  log("driver", "ride:userResponse", payload);
  if (isScenario("cancel") || isScenario("timeout")) {
    log("driver", "skip acceptOffer (scenario)");
    return;
  }
  if (isScenario("driver-reject")) {
    log("driver", "reject counter (driver-reject scenario)");
    return;
  }
  // accept counter automatically
  driverSocket.emit("driver:acceptOffer", {
    ride_id: payload.ride_id,
    offered_price: payload.price,
  });
  log("driver", "acceptOffer sent");
});

driverSocket.on("ride:userAccepted", (payload) => {
  log("driver", "ride:userAccepted", payload);
  state.accepted = true;
  if (isScenario("cancel") || isScenario("timeout")) return;
  startStatusFlow();

  if (isScenario("disconnect")) {
    setTimeout(() => {
      if (driverSocket.connected) {
        log("driver", `disconnecting for ${DISCONNECT_MS}ms`);
        driverSocket.disconnect();
        setTimeout(() => {
          log("driver", "reconnecting...");
          driverSocket.connect();
        }, DISCONNECT_MS);
      }
    }, 2000);
  }
});

driverSocket.on("ride:driverAccepted", (payload) => {
  log("driver", "ride:driverAccepted", payload);
  setServiceCategoryId(
    payload?.ride_details?.service_category_id,
    "driver:ride:driverAccepted"
  );
});

driverSocket.on("ride:statusUpdated", (payload) => {
  log("driver", "ride:statusUpdated", payload);
});

driverSocket.on("ride:ended", (payload) => {
  log("driver", "ride:ended", payload);
});

driverSocket.on("ride:invoice", (payload) => {
  log("driver", "ride:invoice", payload);
});

driverSocket.on("driver:rides:list", (payload) => {
  log("driver", "driver:rides:list", payload);
});

driverSocket.on("driver:rides:list:update", (payload) => {
  log("driver", "driver:rides:list:update", payload);
});

// =========================
// DRIVER 2 SOCKET (multi-driver/driver-reject scenarios)
// =========================
let driver2Socket = null;
if (isScenario("multi-driver") || isScenario("driver-reject")) {
  driver2Socket = io(SOCKET_URL, { transports: ["websocket"] });

  driver2Socket.on("connect", () => {
    log("driver2", `connected ${driver2Socket.id}`);

    driver2Socket.emit("driver-online", {
      driver_id: DRIVER2_ID,
      lat: PICKUP.lat + 0.00005,
      long: PICKUP.long + 0.00005,
      access_token: DRIVER2_ACCESS_TOKEN || undefined,
      driver_service_id: DRIVER2_SERVICE_ID,
      service_type_id: DRIVER2_SERVICE_TYPE_ID,
      service_category_id: DRIVER2_SERVICE_CATEGORY_ID,
    });
  });

  driver2Socket.on("driver:ready", () => {
    state.driver2Ready = true;
    log("driver2", "ready");
    maybeDispatchRide();
  });

  driver2Socket.on("ride:bidRequest", (payload) => {
    log("driver2", "ride:bidRequest", payload);
    if (process.env.DRIVER2_AUTO_BID === "1") {
      const offered_price = 4500 + Math.floor(Math.random() * 1200);
      driver2Socket.emit("driver:submitBid", {
        ride_id: payload.ride_id,
        offered_price,
        driver_service_id: DRIVER2_SERVICE_ID,
        access_token: DRIVER2_ACCESS_TOKEN || undefined,
        service_type_id: DRIVER2_SERVICE_TYPE_ID,
        service_category_id: DRIVER2_SERVICE_CATEGORY_ID,
      });
      log("driver2", `submitted bid ${offered_price}`);
    }
  });

  driver2Socket.on("driver:rides:list", (payload) => {
    log("driver2", "driver:rides:list", payload);
  });

  driver2Socket.on("driver:rides:list:update", (payload) => {
    log("driver2", "driver:rides:list:update", payload);
  });
}

// =========================
// USER SOCKET
// =========================
const userSocket = io(SOCKET_URL, { transports: ["websocket"] });

userSocket.on("connect", () => {
  log("user", `connected ${userSocket.id}`);

  userSocket.emit("user:loginInfo", {
    user_id: USER_ID,
    token: USER_ACCESS_TOKEN || undefined,
  });

  userSocket.emit("user:joinRideRoom", {
    user_id: USER_ID,
    ride_id: RIDE_ID,
  });

  userSocket.emit("user:findNearbyDrivers", {
    user_id: USER_ID,
    lat: PICKUP.lat,
    long: PICKUP.long,
    service_type_id: SERVICE_TYPE_ID,
  });

  state.userReady = true;
  maybeDispatchRide();
});

userSocket.on("ride:joined", (payload) => {
  log("user", "ride:joined", payload);
});

userSocket.on("user:nearbyDrivers", (payload) => {
  log("user", "user:nearbyDrivers", payload);
});

userSocket.on("ride:newBid", (bid) => {
  log("user", "ride:newBid", bid);
  setServiceCategoryId(bid?.service_category_id, "user:ride:newBid");
  if (Number.isFinite(bid?.driver_id) && Number.isFinite(bid?.offered_price)) {
    state.lastBids.set(Number(bid.driver_id), Number(bid.offered_price));
  }

  if (isScenario("timeout")) {
    log("user", "skip counter (timeout scenario)");
    return;
  }

  if (isScenario("cancel") && !state.cancelled) {
    state.cancelled = true;
    userSocket.emit("ride:cancel", {
      ride_id: RIDE_ID,
      user_id: USER_ID,
      reason: 1,
      access_token: USER_ACCESS_TOKEN || undefined,
    });
    log("user", "ride:cancel sent");
    return;
  }
  if (isScenario("cancel") && state.cancelled) {
    log("user", "skip counter (already cancelled)");
    return;
  }

  if (state.accepted || state.userAcceptSent) return;
  if (state.selectedDriverId && state.selectedDriverId !== bid.driver_id) return;

  if (!state.firstBidHandled) {
    state.firstBidHandled = true;
    state.selectedDriverId = bid.driver_id;

    const counterPrice = Math.max(4000, Number(bid.offered_price) - 700);
    userSocket.emit("user:respondToDriver", {
      ride_id: RIDE_ID,
      driver_id: bid.driver_id,
      type: "counter",
      price: counterPrice,
      message: "Counter offer",
    });
    log("user", `counter sent ${counterPrice}`);

    if (isScenario("driver-reject")) {
      setTimeout(() => {
        if (state.accepted || state.userAcceptSent) return;
        const altDriverId =
          DRIVER2_ID && DRIVER2_ID !== state.selectedDriverId
            ? DRIVER2_ID
            : null;
        const altPrice = altDriverId
          ? state.lastBids.get(Number(altDriverId))
          : null;
        if (!altDriverId || !altPrice) {
          log("user", "driver-reject: no alternative bid to accept");
          return;
        }
        userSocket.emit("user:acceptOffer", {
          ride_id: RIDE_ID,
          driver_id: altDriverId,
          offered_price: altPrice,
          user_id: USER_ID,
          access_token: USER_ACCESS_TOKEN || undefined,
        });
        state.selectedDriverId = altDriverId;
        state.userAcceptSent = true;
        log("user", `driver-reject: accepted alt driver ${altDriverId} price ${altPrice}`);
      }, 2500);
    }
  }
});

userSocket.on("ride:acceptedByDriver", (payload) => {
  if (state.accepted || state.userAcceptSent) return;
  log("user", "ride:acceptedByDriver", payload);
  if (Number.isFinite(payload?.driver_id)) {
    state.selectedDriverId = Number(payload.driver_id);
  }
  if (isScenario("cancel") || isScenario("timeout")) {
    log("user", "skip acceptOffer (scenario)");
    return;
  }

  userSocket.emit("user:acceptOffer", {
    ride_id: payload.ride_id,
    driver_id: payload.driver_id,
    offered_price: payload.offered_price,
    user_id: USER_ID,
    access_token: USER_ACCESS_TOKEN || undefined,
  });
  state.userAcceptSent = true;
  log("user", "user:acceptOffer sent");
});

userSocket.on("ride:userAccepted", (payload) => {
  if (state.accepted) return;
  log("user", "ride:userAccepted", payload);
  state.accepted = true;
  if (Number.isFinite(payload?.driver_id)) {
    state.selectedDriverId = Number(payload.driver_id);
  }
  setServiceCategoryId(
    payload?.ride_details?.service_category_id,
    "user:ride:userAccepted"
  );
  if (isScenario("cancel") || isScenario("timeout")) return;
  startStatusFlow();
  if (isScenario("multi-driver")) {
    setTimeout(() => {
      requestDriverList(driver2Socket, DRIVER2_ID, "after-accept");
    }, 2000);
  }
});

userSocket.on("ride:statusUpdated", (payload) => {
  log("user", "ride:statusUpdated", payload);
});

userSocket.on("ride:locationUpdate", (payload) => {
  log("user", "ride:locationUpdate", payload);
});

userSocket.on("ride:arrived", (payload) => {
  log("user", "ride:arrived", payload);
});

userSocket.on("ride:cancelled", (payload) => {
  log("user", "ride:cancelled", payload);
  requestDriverList(driverSocket, DRIVER_ID, "after-cancel");
  if (isScenario("multi-driver")) {
    requestDriverList(driver2Socket, DRIVER2_ID, "after-cancel");
  }
});

userSocket.on("ride:closed", (payload) => {
  log("user", "ride:closed", payload);
});

userSocket.on("ride:ended", (payload) => {
  log("user", "ride:ended", payload);
});

// =========================
// DISPATCH (simulate Laravel)
// =========================
async function maybeDispatchRide() {
  if (!state.driverReady || !state.userReady || state.dispatched) return;
  if (isScenario("multi-driver") && !state.driver2Ready) return;
  state.dispatched = true;

  const serviceCategoryId = state.serviceCategoryId ?? SERVICE_CATEGORY_ID;
  const payload = {
    ride_id: RIDE_ID,
    service_category_id: serviceCategoryId,
    service_type_id: SERVICE_TYPE_ID,
    pickup_lat: PICKUP.lat,
    pickup_long: PICKUP.long,
    pickup_address: "Test Pickup",
    destination_lat: DESTINATION.lat,
    destination_long: DESTINATION.long,
    destination_address: "Test Destination",
    radius: 5000,
    user_bid_price: 5500,
    min_fare_amount: 4000,
    user_id: USER_ID,
    token: USER_ACCESS_TOKEN || undefined,
    user_details: {
      user_id: USER_ID,
    },
  };

  try {
    await httpSocket.post("/events/internal/ride-bid-dispatch", payload);
    log("dispatch", "ride-bid-dispatch sent");
  } catch (e) {
    log("dispatch", "ride-bid-dispatch failed", e?.response?.data || e.message);
  }

  if (isScenario("timeout")) {
    setTimeout(() => {
      requestDriverList(driverSocket, DRIVER_ID, "pre-timeout");
    }, 2000);
    setTimeout(() => {
      requestDriverList(driverSocket, DRIVER_ID, "post-timeout");
    }, TIMEOUT_CHECK_MS);
  }
}

// =========================
// STATUS FLOW
// =========================
async function sendRideStatus(status) {
  const statusDriver = getStatusDriverContext();
  const serviceCategoryId = state.serviceCategoryId ?? SERVICE_CATEGORY_ID;
  const basePayload = {
    ride_id: RIDE_ID,
    driver_id: statusDriver.driver_id,
    ride_status: status,
    way_point_status: 0,
    service_category_id: serviceCategoryId,
    driver_service_id: statusDriver.driver_service_id,
    access_token: statusDriver.access_token,
    current_lat: PICKUP.lat,
    current_long: PICKUP.long,
  };

  if (!serviceCategoryId) {
    log("status", "skip: missing service_category_id", { status });
    return;
  }

  if (status === 5) {
    basePayload.destination_address = "Test Destination";
    basePayload.destination_lat = DESTINATION.lat;
    basePayload.destination_long = DESTINATION.long;
    basePayload.estimated_time = ESTIMATED_TIME;
    basePayload.total_distance = TOTAL_DISTANCE;
  }

  if (status === 6) {
    basePayload.route_lat_long_list = JSON.stringify([
      { lat: PICKUP.lat, lng: PICKUP.long },
      { lat: DESTINATION.lat, lng: DESTINATION.long },
    ]);
  }

  if (status === 7) {
    basePayload.total_amount = Number.isFinite(TOTAL_AMOUNT) ? TOTAL_AMOUNT : 6000;
  }

  if (process.env.OTP) {
    basePayload.otp = process.env.OTP;
  }

  if (STATUS_MODE === "none") return;

  if (STATUS_MODE === "socket-event") {
    if (!statusDriver.access_token || !statusDriver.driver_service_id || !serviceCategoryId) {
      log("status", "socket-event skipped (missing token/service/category)");
      return;
    }
    statusDriver.socket.emit("driver:updateRideStatus", basePayload);
    log("status", `socket-event ride_status=${status} sent`);
    return;
  }

  if (STATUS_MODE === "laravel") {
    if (!statusDriver.access_token || !statusDriver.driver_service_id || !serviceCategoryId) {
      log("status", "laravel skipped (missing token/service/category)");
      return;
    }
    try {
      if (LARAVEL_FAIL_ONCE && !state.laravelFailOnceDone) {
        state.laravelFailOnceDone = true;
        try {
          await httpLaravelFail.post("/api/driver/update-ride-status", basePayload);
        } catch (e) {
          log("status", `laravel (fail-once) ride_status=${status} failed`, e?.message || e);
        }
      }
      await httpLaravel.post("/api/driver/update-ride-status", basePayload);
      log("status", `laravel ride_status=${status} ok`);
    } catch (e) {
      log("status", `laravel ride_status=${status} failed`, e?.response?.data || e.message);
    }
    return;
  }

  if (STATUS_MODE === "internal") {
    try {
      await httpSocket.post("/events/internal/ride-status-updated", {
        ride_id: RIDE_ID,
        driver_id: DRIVER_ID,
        ride_status: status,
        lat: PICKUP.lat,
        long: PICKUP.long,
        user_id: USER_ID,
        payload: {},
      });
      log("status", `internal ride_status=${status} sent`);
    } catch (e) {
      log("status", `internal ride_status=${status} failed`, e?.response?.data || e.message);
    }
  }
}

function startStatusFlow() {
  if (state.statusFlowStarted) return;
  state.statusFlowStarted = true;

  log("status", "starting flow", {
    ride_id: RIDE_ID,
    service_category_id: state.serviceCategoryId ?? SERVICE_CATEGORY_ID,
    sequence: STATUS_SEQUENCE,
  });

  let delay = 5000;
  STATUS_SEQUENCE.forEach((status) => {
    setTimeout(() => {
      void sendRideStatus(status);
    }, delay);
    delay += 2000;
  });
}

// =========================
// CLEANUP
// =========================
setTimeout(() => {
  log("test", "done, closing sockets");
  driverSocket.disconnect();
  userSocket.disconnect();
  if (driver2Socket) driver2Socket.disconnect();
  process.exit(0);
}, TEST_DURATION_MS);
