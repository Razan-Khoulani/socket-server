const io = require("socket.io-client");
const axios = require("axios");

const SERVER_URL = process.env.SOCKET_SERVER_URL || "http://127.0.0.1:3000";
const RIDE_ID = Number(process.env.TEST_RIDE_ID || 91051);
const DRIVER_ID = Number(process.env.TEST_DRIVER_ID || 5);
const USER_ID = Number(process.env.TEST_USER_ID || 7001);
const RIDE_STATUS = Number(process.env.TEST_RIDE_STATUS || 6);
const START_LAT = Number(process.env.TEST_START_LAT || 33.49352);
const START_LONG = Number(process.env.TEST_START_LONG || 36.2407);
const DEST_LAT = Number(process.env.TEST_DEST_LAT || 33.49357);
const DEST_LONG = Number(process.env.TEST_DEST_LONG || 36.24075);
const WAIT_MS = Number(process.env.TEST_WAIT_MS || 10000);

const DRIVER_SERVICE_ID =
  process.env.TEST_DRIVER_SERVICE_ID || process.env.DRIVER_SERVICE_ID || null;
const DRIVER_ACCESS_TOKEN =
  process.env.TEST_DRIVER_ACCESS_TOKEN || process.env.DRIVER_ACCESS_TOKEN || null;

const wait = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

const hasValue = (v) => v !== undefined && v !== null && v !== "";
const num = (v) => {
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
};

const stringifySafe = (value) => {
  try {
    return JSON.stringify(value);
  } catch (_) {
    return String(value);
  }
};

function createSocket(label) {
  const socket = io(SERVER_URL, {
    transports: ["websocket"],
    reconnection: false,
    timeout: WAIT_MS,
  });

  socket.on("connect", () => {
    console.log(`[${label}] connected: ${socket.id}`);
  });
  socket.on("connect_error", (error) => {
    console.log(`[${label}] connect_error: ${error?.message || error}`);
  });
  socket.on("disconnect", (reason) => {
    console.log(`[${label}] disconnected: ${reason}`);
  });

  return socket;
}

function waitForConnect(socket, label, timeoutMs = WAIT_MS) {
  return new Promise((resolve, reject) => {
    if (socket.connected) {
      resolve();
      return;
    }

    const timer = setTimeout(() => {
      cleanup();
      reject(new Error(`[${label}] connect timeout after ${timeoutMs}ms`));
    }, timeoutMs);

    const onConnect = () => {
      cleanup();
      resolve();
    };
    const onError = (error) => {
      cleanup();
      reject(new Error(`[${label}] connect_error: ${error?.message || error}`));
    };

    const cleanup = () => {
      clearTimeout(timer);
      socket.off("connect", onConnect);
      socket.off("connect_error", onError);
    };

    socket.on("connect", onConnect);
    socket.on("connect_error", onError);
  });
}

function waitForEvent(socket, eventName, options = {}) {
  const timeoutMs = Number(options.timeout_ms || WAIT_MS);
  const filter =
    typeof options.filter === "function" ? options.filter : () => true;

  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      cleanup();
      reject(new Error(`Timeout waiting for event "${eventName}"`));
    }, timeoutMs);

    const handler = (payload) => {
      try {
        if (!filter(payload)) return;
      } catch (error) {
        cleanup();
        reject(error);
        return;
      }
      cleanup();
      resolve(payload);
    };

    const cleanup = () => {
      clearTimeout(timer);
      socket.off(eventName, handler);
    };

    socket.on(eventName, handler);
  });
}

function attachDebugEvents(socket, label) {
  const events = [
    "ride:joined",
    "ride:statusUpdated",
    "ride:passedDestination",
    "ride:passedDestinationReceivedAck",
    "ride:passedDestinationDecisionAck",
    "ride:passedDestinationAccepted",
    "ride:extraDistanceAccepted",
    "ride:invoice",
  ];

  for (const eventName of events) {
    socket.on(eventName, (payload) => {
      console.log(`[${label}] <- ${eventName}: ${stringifySafe(payload)}`);
    });
  }
}

async function main() {
  console.log("[test] ride:passedDestination smoke test started");
  console.log(
    `[test] server=${SERVER_URL} ride_id=${RIDE_ID} driver_id=${DRIVER_ID} user_id=${USER_ID}`
  );

  const driverSocket = createSocket("driver");
  const userSocket = createSocket("user");
  attachDebugEvents(driverSocket, "driver");
  attachDebugEvents(userSocket, "user");

  try {
    await Promise.all([
      waitForConnect(driverSocket, "driver"),
      waitForConnect(userSocket, "user"),
    ]);

    const serviceIdNum = num(DRIVER_SERVICE_ID);
    driverSocket.emit("driver-online", {
      driver_id: DRIVER_ID,
      lat: START_LAT,
      long: START_LONG,
      ...(serviceIdNum != null ? { driver_service_id: serviceIdNum } : {}),
      ...(hasValue(DRIVER_ACCESS_TOKEN)
        ? { access_token: DRIVER_ACCESS_TOKEN }
        : {}),
    });

    userSocket.emit("user:joinRideRoom", {
      user_id: USER_ID,
      ride_id: RIDE_ID,
    });

    await wait(300);

    const passedDestinationPromise = waitForEvent(
      driverSocket,
      "ride:passedDestination",
      {
        timeout_ms: WAIT_MS,
        filter: (payload) => num(payload?.ride_id) === RIDE_ID,
      }
    );

    const statusPayload = {
      ride_id: RIDE_ID,
      driver_id: DRIVER_ID,
      ride_status: RIDE_STATUS,
      lat: START_LAT,
      long: START_LONG,
      user_id: USER_ID,
      payload: {
        user_id: USER_ID,
        destination_lat: DEST_LAT,
        destination_long: DEST_LONG,
      },
    };

    const statusRes = await axios.post(
      `${SERVER_URL}/events/internal/ride-status-updated`,
      statusPayload,
      { timeout: WAIT_MS }
    );
    console.log(
      `[test] internal status update -> ${statusRes.status} ${stringifySafe(
        statusRes.data
      )}`
    );

    const passedDestinationEvt = await passedDestinationPromise;
    console.log(
      `[test] PASS: driver received ride:passedDestination trace=${passedDestinationEvt?.trace_id || "n/a"}`
    );

    driverSocket.emit("driver:passedDestinationReceived", {
      ride_id: RIDE_ID,
      driver_id: DRIVER_ID,
      trace_id: passedDestinationEvt?.trace_id || null,
    });

    const decisionAckPromise = waitForEvent(
      driverSocket,
      "ride:passedDestinationDecisionAck",
      {
        timeout_ms: WAIT_MS,
        filter: (payload) => num(payload?.ride_id) === RIDE_ID,
      }
    );
    const acceptedEvtPromise = waitForEvent(
      driverSocket,
      "ride:passedDestinationAccepted",
      {
        timeout_ms: 3000,
        filter: (payload) => num(payload?.ride_id) === RIDE_ID,
      }
    ).catch(() => null);
    const extraDistanceEvtPromise = waitForEvent(
      driverSocket,
      "ride:extraDistanceAccepted",
      {
        timeout_ms: 3000,
        filter: (payload) => num(payload?.ride_id) === RIDE_ID,
      }
    ).catch(() => null);

    driverSocket.emit("driver:passedDestinationDecision", {
      ride_id: RIDE_ID,
      driver_id: DRIVER_ID,
      accepted: "yes",
      extra_distance_km: 0.25,
      updated_total_distance_km: 1.1,
      ...(serviceIdNum != null ? { driver_service_id: serviceIdNum } : {}),
      ...(hasValue(DRIVER_ACCESS_TOKEN)
        ? { access_token: DRIVER_ACCESS_TOKEN }
        : {}),
    });

    const decisionAck = await decisionAckPromise;
    const acceptedEvt = await acceptedEvtPromise;
    const extraDistanceEvt = await extraDistanceEvtPromise;

    const ackStatus = num(decisionAck?.status);
    const ackMessage = decisionAck?.message || null;
    const hasAuthContext = serviceIdNum != null && hasValue(DRIVER_ACCESS_TOKEN);

    if (ackStatus === 1) {
      console.log(
        `[test] PASS: decision ack success. accepted_event=${acceptedEvt ? "yes" : "no"} extra_event=${extraDistanceEvt ? "yes" : "no"}`
      );
    } else if (!hasAuthContext && ackMessage === "driver_service_id/access_token required") {
      console.log(
        "[test] PASS (smoke): decision ack returned expected auth warning without credentials."
      );
      console.log(
        "[test] Tip: set TEST_DRIVER_SERVICE_ID and TEST_DRIVER_ACCESS_TOKEN for full integration success."
      );
    } else {
      throw new Error(
        `[test] Decision ack failed unexpectedly: ${stringifySafe(decisionAck)}`
      );
    }

    console.log("[test] completed successfully");
  } finally {
    driverSocket.disconnect();
    userSocket.disconnect();
    await wait(150);
  }
}

main()
  .then(() => {
    process.exitCode = 0;
  })
  .catch((error) => {
    console.error("[test] FAILED:", error?.message || error);
    process.exitCode = 1;
  });
