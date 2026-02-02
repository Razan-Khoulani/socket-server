const io = require("socket.io-client");

// =============
// CONFIG
// =============
// أضف الإعدادات اللازمة
const SERVER = "http://localhost:3000";
const RIDE_ID = 101;
const USER_ID = 55;
const PICKUP = { lat: 33.6, long: 36.28 };
const RADIUS = 5000;

// =========================
// DRIVERS
// =========================
const drivers = [
  { driver_id: 1, lat: 33.6, long: 36.28 },
  { driver_id: 2, lat: 33.6001, long: 36.2801 },
];

drivers.forEach((driver) => {
  const driverSocket = io(SERVER);

  driverSocket.on("connect", () => {
    console.log(`✅ Driver ${driver.driver_id} connected:`, driverSocket.id);

    // ✅ Online + join driver:{id}
    driverSocket.emit("driver-online", {
      driver_id: driver.driver_id,
      lat: driver.lat,
      long: driver.long,
    });

    // ✅ Update location every 1s
    setInterval(() => {
      driver.lat += (Math.random() - 0.5) / 3000;
      driver.long += (Math.random() - 0.5) / 3000;

      driverSocket.emit("update-location", { lat: driver.lat, long: driver.long });
    }, 1000);
  });

  // ✅ Receive bid request from system for a ride
  driverSocket.on("ride:bidRequest", ({ ride_id, pickup_lat, pickup_long }) => {
    console.log(`📩 Driver ${driver.driver_id} got bidRequest for ride ${ride_id} pickup(${pickup_lat},${pickup_long})`);

    // ✅ Submit bid
    const offered_price = 5000 + Math.floor(Math.random() * 2000);
    driverSocket.emit("driver:submitBid", {
      ride_id,
      driver_id: driver.driver_id,
      offered_price,
      meta: { note: "auto-bid from test" },
    });

    console.log(`💰 Driver ${driver.driver_id} submitted bid: ${offered_price}`);
  });

  // ✅ Receive user response (counter/accept/reject)
  driverSocket.on("ride:userResponse", (payload) => {
    console.log(`🗣️ Driver ${driver.driver_id} got userResponse:`, payload);
  });
});

// =========================
// USER
// =========================
setTimeout(() => {
  const userSocket = io(SERVER);

  let firstBidHandled = false;

  userSocket.on("connect", () => {
    console.log("✅ User connected:", userSocket.id);

    // ✅ User joins ride room ONLY
    userSocket.emit("user:joinRideRoom", { user_id: USER_ID, ride_id: RIDE_ID });

    // ✅ (اختياري) للتأكد من nearby drivers على الميموري فقط
    setTimeout(() => {
      userSocket.emit("user:findNearbyDrivers", { lat: PICKUP.lat, long: PICKUP.long, radius: RADIUS });
    }, 800);

    // ✅ Dispatch ride to nearby drivers (the new scenario trigger)
    setTimeout(() => {
      console.log("📢 Dispatching ride to nearby drivers...");
      userSocket.emit("ride:dispatchToNearbyDrivers", {
        ride_id: RIDE_ID,
        pickup_lat: PICKUP.lat,
        pickup_long: PICKUP.long,
        radius: RADIUS,
        user_bid_price: 5500,  // فرضًا القيمة المدخلة من قبل اليوزر
        min_fare_amount: 4000,  // الحد الأدنى للعرض
      });
    }, 1500);
  });

  userSocket.on("ride:joined", (data) => {
    console.log("🟢 User joined ride room:", data);
  });

  // ✅ Nearby list (debug only)
  userSocket.on("user:nearbyDrivers", (drivers) => {
    console.log("🚕 Nearby Drivers received:");
    console.table(drivers);
  });

  // ✅ Receive bids (from ride room)
  userSocket.on("ride:newBid", (bid) => {
    console.log("🧾 ride:newBid =>", bid);

    // ✅ رد على أول عرض فقط (مثال counter)
    if (!firstBidHandled) {
      firstBidHandled = true;

      const counterPrice = Number(bid.offered_price) - 500;

      console.log(`📤 Sending COUNTER to driver ${bid.driver_id}: ${counterPrice}`);
      userSocket.emit("user:respondToDriver", {
        ride_id: RIDE_ID,
        driver_id: bid.driver_id,
        type: "counter",
        price: counterPrice,
        message: "Can you do a bit lower?",
      });
    }
  });

  // ✅ Receive user ACCEPT response (confirm bid)
  userSocket.on("ride:userResponse", (payload) => {
    console.log("🗣️ User accepted the bid:", payload);

    // ✅ Update the database when the user accepts the bid
    if (payload.type === "accept") {
      console.log(`Updating the database for ride ${payload.ride_id} with price ${payload.price}`);
      
      // Mock the database update when user accepts the offer
      // You can replace this with actual database interaction logic

      // Update final bid and user information in the DB here
      console.log(`Updated the database with user acceptance. Ride ID: ${payload.ride_id}, Driver ID: ${payload.driver_id}`);
    }
  });
  
}, 800);
