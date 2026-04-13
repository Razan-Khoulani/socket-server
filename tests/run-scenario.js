/* eslint-disable no-console */
const axios = require("axios");
const { spawn } = require("child_process");
const path = require("path");

const LARAVEL_URL = process.env.LARAVEL_URL || "https://osbackend.gocab.net";
const SOCKET_URL = process.env.SOCKET_URL || "https://aiactive.co.uk:4000";
const SCENARIO = (process.env.SCENARIO || "happy").toLowerCase();

const USER_ID = Number(process.env.USER_ID || 9);
const USER_ACCESS_TOKEN = process.env.USER_ACCESS_TOKEN || "";
const SERVICE_CATEGORY_ID = Number(process.env.SERVICE_CATEGORY_ID || 5);
const SERVICE_TYPE_ID = Number(process.env.SERVICE_TYPE_ID || 3);

const PAYMENT_TYPE = Number(process.env.PAYMENT_TYPE || 1);
const ESTIMATED_TIME = String(process.env.ESTIMATED_TIME || "20");
const TOTAL_DISTANCE = String(process.env.TOTAL_DISTANCE || "10");
const BIDDING_OFFER = String(process.env.BIDDING_OFFER || "500");
const HANDICAP = String(process.env.HANDICAP || "0");
const CHILD_SEAT = String(process.env.CHILD_SEAT || "0");

const PICKUP = {
  lat: Number(process.env.PICKUP_LAT || 33.4935628),
  long: Number(process.env.PICKUP_LONG || 36.2406966),
  address:
    process.env.PICKUP_ADDRESS ||
    "Choco Swamp ، الوليد إبن عبد الملك، حي الجلاء",
};
const DESTINATION = {
  lat: Number(process.env.DEST_LAT || 33.498339615409),
  long: Number(process.env.DEST_LONG || 36.244756439568),
  address:
    process.env.DEST_ADDRESS ||
    "حي مزة جبل ، بلدية المزة، ناحية دمشق",
};

const CANCEL_AFTER = process.env.CANCEL_AFTER === "1";
const AUTO_CANCEL_ACTIVE = process.env.AUTO_CANCEL_ACTIVE === "1";

const httpLaravel = axios.create({
  baseURL: LARAVEL_URL,
  timeout: 7000,
});

const rideBookingUrl = "/api/customer/transport/ride-booking";
const cancelRideUrl = "/api/customer/transport/cancel-ride";

const buildAddressList = () => [
  {
    address: PICKUP.address,
    place_id: null,
    address_lat: PICKUP.lat,
    address_long: PICKUP.long,
  },
  {
    address: DESTINATION.address,
    place_id: null,
    address_lat: DESTINATION.lat,
    address_long: DESTINATION.long,
  },
];

async function cancelRide(rideId, reasonId = 1) {
  const body = new URLSearchParams();
  body.set("ride_id", String(rideId));
  body.set("user_id", String(USER_ID));
  body.set("reason_id", String(reasonId));
  body.set("sub_ride_id", "0");
  body.set("access_token", USER_ACCESS_TOKEN);

  const res = await httpLaravel.post(cancelRideUrl, body.toString(), {
    headers: { "Content-Type": "application/x-www-form-urlencoded" },
  });
  console.log("[scenario] cancel ride response:", res.data);
}

async function createRide(attempt = 0) {
  if (process.env.RIDE_ID) {
    return Number(process.env.RIDE_ID);
  }

  const body = new URLSearchParams();
  body.set("user_id", String(USER_ID));
  body.set("access_token", USER_ACCESS_TOKEN);
  body.set("service_category_id", String(SERVICE_CATEGORY_ID));
  body.set("service_type_id", String(SERVICE_TYPE_ID));
  body.set("payment_type", String(PAYMENT_TYPE));
  body.set("estimated_time", ESTIMATED_TIME);
  body.set("total_distance", TOTAL_DISTANCE);
  body.set("bidding_offer", BIDDING_OFFER);
  body.set("handicap", HANDICAP);
  body.set("child_seat", CHILD_SEAT);
  body.set("address_list", JSON.stringify(buildAddressList()));

  const res = await httpLaravel.post(rideBookingUrl, body.toString(), {
    headers: { "Content-Type": "application/x-www-form-urlencoded" },
  });

  if (!res?.data || res.data.status !== 1) {
    console.error("[scenario] ride booking failed:", res?.data);

    if (AUTO_CANCEL_ACTIVE && res?.data?.active_ride_id && attempt < 1) {
      console.log("[scenario] canceling active ride:", res.data.active_ride_id);
      await cancelRide(res.data.active_ride_id);
      return createRide(attempt + 1);
    }

    throw new Error("ride booking failed");
  }

  console.log("[scenario] ride created:", res.data.ride_id);
  return Number(res.data.ride_id);
}

function runTest(rideId) {
  return new Promise((resolve) => {
    const env = {
      ...process.env,
      RIDE_ID: String(rideId),
      SOCKET_URL,
      SOCKET_HTTP_URL: SOCKET_URL,
      LARAVEL_URL,
    };
    const child = spawn(process.execPath, [path.join(__dirname, "full-flow-e2e.js")], {
      stdio: "inherit",
      env,
    });
    child.on("exit", (code) => resolve(code ?? 0));
  });
}

async function main() {
  console.log(`[scenario] start: ${SCENARIO}`);
  const rideId = await createRide();
  const code = await runTest(rideId);
  console.log(`[scenario] test exit code: ${code}`);

  if (CANCEL_AFTER) {
    await cancelRide(rideId);
  }
}

main().catch((err) => {
  console.error("[scenario] fatal:", err.message || err);
  process.exit(1);
});
