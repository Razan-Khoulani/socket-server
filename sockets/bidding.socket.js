const driverLocationService = require("../services/driverLocation.service");

// ✅ Maps بالذاكرة (بدون Redis) — مبدئيًا
// rideId -> Set(driverId)
const rideCandidates = new Map();

const rideRoom = (rideId) => `ride:${rideId}`;
const driverRoom = (driverId) => `driver:${driverId}`;

const toNumber = (v) => {
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
};

// 🔒 تحقق بسيط: هل السائق ضمن قائمة المرشحين لهالرحلة؟
const isCandidateDriver = (rideId, driverId) => {
  const set = rideCandidates.get(rideId);
  if (!set) return true; // إذا ما عندنا list محفوظة، ما منمنع
  return set.has(driverId);
};

module.exports = (io, socket) => {

  /**
   * ✅ اليوزر يدخل روم الرحلة
   * front: socket.emit("user:joinRideRoom", { user_id, ride_id })
   */
  socket.on("user:joinRideRoom", ({ user_id, ride_id }) => {
    const rideId = toNumber(ride_id);
    if (!rideId) return;

    socket.isUser = true;
    socket.userId = toNumber(user_id) ?? null;

    socket.join(rideRoom(rideId));
    socket.emit("ride:joined", { ride_id: rideId });

    console.log(`👤 user socket ${socket.id} joined ${rideRoom(rideId)}`);
  });

  /**
   * ✅ Dispatch: نرسل طلب مزاودة للسائقين القريبين (من السيرفر أو من HTTP endpoint)
   * ممكن تستدعيه من Laravel via HTTP endpoint (أنظف) — رح أعطيك كود endpoint تحت
   */
socket.on("ride:dispatchToNearbyDrivers", ({
  ride_id,
  pickup_lat,
  pickup_long,
  radius = 5000,

  // ✅ نفس أسماء Laravel
  user_bid_price,
  min_fare_amount,
}) => {
  const rideId = toNumber(ride_id);
  const lat = toNumber(pickup_lat);
  const long = toNumber(pickup_long);

  const base = toNumber(user_bid_price);     // ممكن null
  const min = toNumber(min_fare_amount);     // ممكن null

  if (!rideId || lat === null || long === null) return;

  const nearby = driverLocationService.getNearbyDriversFromMemory(lat, long, radius);

  rideCandidates.set(rideId, new Set(nearby.map(d => d.driver_id)));

  nearby.forEach(d => {
    io.to(driverRoom(d.driver_id)).emit("ride:bidRequest", {
      ride_id: rideId,
      pickup_lat: lat,
      pickup_long: long,
      radius,

      // ✅ هي اللي السائق لازم يشوفها
      user_bid_price: base,
      min_fare_amount: min,
    });
  });

  console.log(`📢 dispatched ride ${rideId} to ${nearby.length} drivers (user_bid_price=${base}, min_fare_amount=${min})`);
});


  /**
   * ✅ السائق يقدم عرض (Bid)
   * driver app: socket.emit("driver:submitBid", { driver_id, ride_id, offered_price, ... })
   * هذا سيصل للـ user عبر روم الرحلة فقط.
   */


  socket.on("driver:submitBid", (payload) => {
    const rideId = toNumber(payload?.ride_id);
    const driverId = toNumber(socket.driverId);
    const offeredPrice = toNumber(payload?.offered_price);

    if (!driverId || !rideId) return;

    // تحقق من أن السائق مرشح لهذه الرحلة
    if (!isCandidateDriver(rideId, driverId)) {
        console.log(`🛑 driver ${driverId} tried to bid on ride ${rideId} but not candidate`);
        return;
    }

    // إرسال العرض للمستخدم
    io.to(rideRoom(rideId)).emit("ride:newBid", {
        ride_id: rideId,
        driver_id: driverId,
        offered_price: offeredPrice,
        bidding_time: Date.now(),
        meta: payload.meta ?? {},
    });

    console.log(`💰 driver ${driverId} submitted bid: ${offeredPrice}`);
});


  /**
   * ✅ اليوزر يرد على سائق محدد (Counter Offer أو Accept/Reject)
   * user app: socket.emit("user:respondToDriver", { ride_id, driver_id, type, price })
   * الرد يروح فقط على روم السائق (driver:{id})
   */
socket.on("user:respondToDriver", (payload) => {
    const rideId = toNumber(payload?.ride_id);
    const driverId = toNumber(payload?.driver_id);
    if (!rideId || !driverId) return;

    // إرسال الرد إلى السائق
    io.to(driverRoom(driverId)).emit("ride:userResponse", {
        ride_id: rideId,
        driver_id: driverId,
        type: payload.type,      // "counter" | "reject"
        price: payload.price ?? null,
        message: payload.message ?? null,
        at: Date.now(),
    });

    console.log(`🗣️ user response -> driver ${driverId} for ride ${rideId} (${payload.type})`);
});

socket.on("driver:acceptOffer", (payload) => {
    const driverId = toNumber(payload?.driver_id);
    const rideId = toNumber(payload?.ride_id);
    const offeredPrice = toNumber(payload?.offered_price);

    if (!driverId || !rideId || !offeredPrice) return;

    // إرسال القبول للسعر إلى اليوزر دون تحديث البيانات في قاعدة البيانات
    io.to(rideRoom(rideId)).emit("ride:acceptedByDriver", {
        ride_id: rideId,
        driver_id: driverId,
        offered_price: offeredPrice,
        message: "Offer accepted by driver",
        at: Date.now(),
    });

    console.log(`✅ Driver ${driverId} accepted offer for ride ${rideId}`);
});

socket.on("driver:acceptOffer", (payload) => {
    const driverId = toNumber(payload?.driver_id);
    const rideId = toNumber(payload?.ride_id);
    const offeredPrice = toNumber(payload?.offered_price);

    if (!driverId || !rideId || !offeredPrice) return;

    // إرسال القبول للسعر إلى اليوزر دون تحديث البيانات في قاعدة البيانات
    io.to(rideRoom(rideId)).emit("ride:acceptedByDriver", {
        ride_id: rideId,
        driver_id: driverId,
        offered_price: offeredPrice,
        message: "Offer accepted by driver",
        at: Date.now(),
    });

    console.log(`✅ Driver ${driverId} accepted offer for ride ${rideId}`);
});



socket.on("user:acceptOffer", (payload) => {
    const rideId = toNumber(payload?.ride_id);
    const driverId = toNumber(payload?.driver_id);
    const offeredPrice = toNumber(payload?.offered_price);

    if (!rideId || !driverId || !offeredPrice) return;

    // تحديث قاعدة البيانات عندما يقبل اليوزر العرض
    db.query(
        `UPDATE user_ride_booking 
        SET final_confirm_bid_price = ?, user_bid_price = ?, driver_id = ?, status = 1 
        WHERE ride_id = ?`,
        [offeredPrice, offeredPrice, driverId, rideId],
        (err, result) => {
            if (err) {
                console.error("Database update error:", err);
                return;
            }

            // إرسال القبول النهائي إلى السائق
            io.to(driverRoom(driverId)).emit("ride:userAccepted", {
                ride_id: rideId,
                driver_id: driverId,
                offered_price: offeredPrice,
                message: "User accepted the offer",
                at: Date.now(),
            });

            console.log(`✅ User accepted offer for ride ${rideId}, updating database`);
        }
    );
});


  /**
   * ✅ ت لما ما بقى في تفاوض)
   */
  socket.on("ride:close", ({ ride_id }) => {
    const rideId = toNumber(ride_id);
    if (!rideId) return;

    rideCandidates.delete(rideId);
    io.to(rideRoom(rideId)).emit("ride:closed", { ride_id: rideId });

    console.log(`✅ ride ${rideId} closed`);
  });

  socket.on("disconnect", () => {
    // ما منمسح rideCandidates هون لأن هو مرتبط بالرحلة مو بالسوكيت
    if (socket.isUser) console.log("User disconnected:", socket.id);
  });
};
