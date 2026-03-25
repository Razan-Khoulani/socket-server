const axios = require("axios");
const io = require("socket.io-client");

const args = process.argv.slice(2);
const getArg = (name) => {
  const idx = args.indexOf(`--${name}`);
  if (idx === -1) return null;
  return args[idx + 1] ?? null;
};

const SOCKET_URL = process.env.SOCKET_URL || "http://localhost:3000";
const SOCKET_HTTP_URL = process.env.SOCKET_HTTP_URL || SOCKET_URL;

const driverId = Number(getArg("driver") || process.env.DRIVER_ID);
const driverServiceId = Number(getArg("service") || process.env.DRIVER_SERVICE_ID);
const accessToken = getArg("token") || process.env.DRIVER_ACCESS_TOKEN;
const rideId = Number(getArg("ride") || process.env.RIDE_ID);
const rideStatus = Number(getArg("status") || process.env.RIDE_STATUS || 7);

const lat = Number(getArg("lat") || process.env.LAT || 33.5138);
const long = Number(getArg("long") || process.env.LONG || 36.2765);

if (
  !Number.isFinite(driverId) ||
  !Number.isFinite(driverServiceId) ||
  !Number.isFinite(rideId) ||
  !Number.isFinite(rideStatus) ||
  !accessToken
) {
  console.log(
    "Usage: node tests/03-socket-invoice-flow.js --driver <id> --service <id> --token <access_token> --ride <id> --status <7|9>"
  );
  process.exit(1);
}

console.log("[SOCKET] URL:", SOCKET_URL);
console.log("[HTTP] URL:", SOCKET_HTTP_URL);

const socket = io(SOCKET_URL, { transports: ["websocket"] });

const done = (code = 0) => {
  socket.disconnect();
  setTimeout(() => process.exit(code), 300);
};

socket.on("connect", () => {
  console.log("[SOCKET] connected:", socket.id);

  socket.emit("driver-online", {
    driver_id: driverId,
    lat,
    long,
    access_token: accessToken,
    driver_service_id: driverServiceId,
  });

  console.log("[SOCKET] driver-online emitted");

  setTimeout(async () => {
    try {
      const payload = {
        ride_id: rideId,
        driver_id: driverId,
        ride_status: rideStatus,
      };

      console.log("[HTTP] POST /events/internal/ride-status-updated", payload);
      const res = await axios.post(
        `${SOCKET_HTTP_URL}/events/internal/ride-status-updated`,
        payload,
        { timeout: 7000 }
      );
      console.log("[HTTP] response:", res.data);
    } catch (e) {
      console.error("[HTTP] error:", e?.response?.data || e.message);
    }
  }, 800);
});

socket.on("ride:statusUpdated", (evt) => {
  console.log("[EVENT] ride:statusUpdated", evt);
});

socket.on("ride:ended", (evt) => {
  console.log("[EVENT] ride:ended", evt);
});

socket.on("ride:invoice", (evt) => {
  console.log("[EVENT] ride:invoice", evt);
  done(0);
});

socket.on("connect_error", (err) => {
  console.error("[SOCKET] connect_error:", err.message);
  done(1);
});

setTimeout(() => {
  console.warn("[TIMEOUT] No ride:invoice received.");
  done(1);
}, 15000);

