const io = require("socket.io-client");

// =============
// CONFIG
// =============
const SERVER    = "http://192.168.100.51:3000";
const RIDE_ID   = 42;
const USER_ID   = 7;
const PICKUP    = { lat: 33.6, long: 36.28 };
const RADIUS    = 5000;

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
    console.log(`✅ Driver ${driver.driver_id} connected: ${driverSocket.id}`);

    driverSocket.emit("driver-online", {
      driver_id: driver.driver_id,
      lat: driver.lat,
      long: driver.long,
    });

    // تحديث الموقع كل ثانية (محاكاة حركة بسيطة)
    setInterval(() => {
      driver.lat  += (Math.random() - 0.5) / 3000;
      driver.long += (Math.random() - 0.5) / 3000;

      driverSocket.emit("update-location", {
        lat: driver.lat,
        long: driver.long,
      });
    }, 1000);
  });

  driverSocket.on("ride:bidRequest", ({ ride_id, pickup_lat, pickup_long }) => {
    console.log(`📩 Driver ${driver.driver_id} got bidRequest for ride ${ride_id}`);

    const offered_price = 5000 + Math.floor(Math.random() * 2000);

    driverSocket.emit("driver:submitBid", {
      ride_id,
      driver_id: driver.driver_id,
      offered_price,
      meta: { note: "auto-bid from test" },
    });

    console.log(`💰 Driver ${driver.driver_id} submitted bid: ${offered_price} SAR`);
  });

  driverSocket.on("ride:userResponse", (payload) => {
    console.log(`🗣️ Driver ${driver.driver_id} received userResponse:`, payload);

    if (payload.type === "counter") {
      console.log(`Driver ${driver.driver_id} → accepting counter offer ${payload.price} SAR`);

      driverSocket.emit("driver:acceptOffer", {
        ride_id: payload.ride_id,
        driver_id: driver.driver_id,
        offered_price: payload.price,
      });
    }
  });

  driverSocket.on("driver:location:updated", (payload) => {
    console.log("📍 [driver] driver:location:updated", payload);
  });
});

// =========================
// USER
// =========================
setTimeout(() => {
  const userSocket = io(SERVER);

  let firstBidHandled    = false;
  let selectedDriverId   = null;
  let finalPrice         = null;
  let hasSentFinalAccept = false;

  userSocket.on("connect", () => {
    console.log(`✅ User connected: ${userSocket.id}`);

    userSocket.emit("user:joinRideRoom", { user_id: USER_ID, ride_id: RIDE_ID });

    setTimeout(() => {
      userSocket.emit("user:findNearbyDrivers", {
        lat: PICKUP.lat,
        long: PICKUP.long,
        radius: RADIUS,
      });
    }, 800);

    setTimeout(() => {
      console.log("📢 Dispatching ride to nearby drivers...");
      userSocket.emit("ride:dispatchToNearbyDrivers", {
        ride_id: RIDE_ID,
        pickup_lat: PICKUP.lat,
        pickup_long: PICKUP.long,
        radius: RADIUS,
        user_bid_price: 5500,
        min_fare_amount: 4000,
      });
    }, 1800);  // زيادة بسيطة لإعطاء وقت للسائقين
  });

  userSocket.on("driver:location:updated", (payload) => {
    console.log("📍 [user] driver:location:updated", payload);
  });

  userSocket.on("ride:joined", (data) => {
    console.log("🟢 User joined ride room:", data);
  });

  userSocket.on("user:nearbyDrivers", (drivers) => {
    console.log("🚕 Nearby Drivers received:");
    console.table(drivers);
  });

  // استقبال العروض
  userSocket.on("ride:newBid", (bid) => {
    if (hasSentFinalAccept) {
      console.log(`[IGNORED] عرض جديد من driver ${bid.driver_id} بعد القبول النهائي`);
      return;
    }

    console.log("🧾 New bid received:", bid);

    if (selectedDriverId !== null && bid.driver_id !== selectedDriverId) {
      console.log(`→ تجاهل عرض driver ${bid.driver_id} (ننتظر رد driver ${selectedDriverId})`);
      return;
    }

    if (!firstBidHandled) {
      firstBidHandled = true;
      selectedDriverId = bid.driver_id;

      const counterPrice = Math.max(4000, Number(bid.offered_price) - 700);

      console.log(`📤 Sending COUNTER to driver ${bid.driver_id}: ${counterPrice} SAR`);

      userSocket.emit("user:respondToDriver", {
        ride_id: RIDE_ID,
        driver_id: bid.driver_id,
        type: "counter",
        price: counterPrice,
        message: "شو رأيك بهالسعر؟",
      });
    }
  });

  // ────────────────────────────────────────────────
  // الجزء المهم: استقبال قبول السائق للعرض / الـ counter
  // ────────────────────────────────────────────────
  userSocket.on("ride:acceptedByDriver", (payload) => {
    if (hasSentFinalAccept) return;

    console.log("🟢 Driver accepted the offer/counter →", payload);

    const acceptedPrice = payload.offered_price;

    console.log(
      `🎉 Driver ${payload.driver_id} وافق على السعر ${acceptedPrice} SAR`
    );

    finalPrice = acceptedPrice;
    hasSentFinalAccept = true;

    userSocket.emit("user:acceptOffer", {
      ride_id: RIDE_ID,
      driver_id: payload.driver_id,
      offered_price: finalPrice,
    });

    console.log(
      `✅ تم إرسال user:acceptOffer النهائي | السعر: ${finalPrice} | السائق: ${payload.driver_id}`
    );
  });

  // تأكيد نهائي من السيرفر (اختياري للديبغ)
  userSocket.on("ride:userAccepted", (data) => {
    console.log("🟢 تأكيد نهائي من السيرفر:", data);
  });

}, 2000);
