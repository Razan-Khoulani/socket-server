const axios = require("axios");

const args = process.argv.slice(2);
const getArg = (name) => {
  const idx = args.indexOf(`--${name}`);
  if (idx === -1) return null;
  return args[idx + 1] ?? null;
};

const baseUrl =
  process.env.LARAVEL_BASE_URL || "http://192.168.43.240:3000";

const driverId = Number(getArg("driver") || process.env.DRIVER_ID);
const driverServiceId = Number(getArg("service") || process.env.DRIVER_SERVICE_ID);
const accessToken = getArg("token") || process.env.DRIVER_ACCESS_TOKEN;
const rideId = Number(getArg("ride") || process.env.RIDE_ID);

if (
  !Number.isFinite(driverId) ||
  !Number.isFinite(driverServiceId) ||
  !Number.isFinite(rideId) ||
  !accessToken
) {
  console.log(
    "Usage: node tests/02-api-invoice.js --driver <id> --service <id> --token <access_token> --ride <id>"
  );
  process.exit(1);
}

const url = `${baseUrl}/api/driver/transport-ride-invoice`;
const payload = {
  driver_id: driverId,
  access_token: accessToken,
  driver_service_id: driverServiceId,
  ride_id: rideId,
};

console.log("[API] POST", url);
console.log("[API] Payload:", payload);

axios
  .post(url, payload, { timeout: 7000 })
  .then((res) => {
    console.log("[API] Response:", res.data);
  })
  .catch((err) => {
    console.error("[API] Error:", err?.response?.data || err.message);
  });

