//rideTracking.js

const { updateRideStatus } = require("../dbQueries"); // استيراد دالة لتحديث حالة الرحلة في قاعدة البيانات
const rideLocations = new Map();
// بدء التتبع للسائق
function startTracking(io, rideId, pickup, destination) {
  if (rideLocations.has(rideId)) return;

  rideLocations.set(rideId, { lat: pickup.lat, long: pickup.long });
  console.log(`🚗 Tracking started for ride ${rideId} at lat=${pickup.lat}, long=${pickup.long}`);

  // تحديث حالة الرحلة إلى "جارية" (status = 5)
  updateRideStatus(rideId, { status: 5 })
    .then(() => {
      // إرسال الموقع الأولي إلى غرفة الرحلة
      io.to(`ride:${rideId}`).emit("ride:locationUpdate", {
        ride_id: rideId,
        lat: pickup.lat,
        long: pickup.long,
      });
    })
    .catch((err) => {
      console.error("Error updating ride status in DB:", err);
    });
}

// تحديث الموقع بشكل دوري عند تلقيه من السائق
function updateLocation(io, rideId, lat, long) {
  if (!rideLocations.has(rideId)) return;

  const loc = rideLocations.get(rideId);
  loc.lat = lat;
  loc.long = long;

  // تحديث الموقع في الذاكرة
  rideLocations.set(rideId, loc);

  // تحديث قاعدة البيانات (تغيير الحالة إلى "الرحلة جارية" إذا كانت حالتها لا تزال غير مكتملة)
  updateRideStatus(rideId, { lat, long, status: 5 }) // 5 يعني الرحلة جارية
    .then(() => {
      // بث الموقع الجديد إلى غرفة الرحلة
      io.to(`ride:${rideId}`).emit("ride:locationUpdate", {
        ride_id: rideId,
        lat,
        long,
      });

      console.log(`🚗 Ride ${rideId} location updated → lat=${lat}, long=${long}`);
    })
    .catch((err) => {
      console.error("Error updating ride status in DB:", err);
    });
}

// عند وصول السائق (Status = 3)
function arriveAtPickup(io, rideId, lat, long) {
  updateRideStatus(rideId, { status: 3, lat, long })
    .then(() => {
      io.to(`ride:${rideId}`).emit("ride:arrived", {
        ride_id: rideId,
        lat,
        long,
      });

      console.log(`🟡 Ride ${rideId} arrived at pickup location`);
    })
    .catch((err) => {
      console.error("Error updating ride status to arrived:", err);
    });
}

// إيقاف التتبع (تم الوصول إلى الوجهة) (Status = 6)
function stopTracking(io, rideId) {
  if (rideLocations.has(rideId)) {
    const lastLoc = rideLocations.get(rideId);
    rideLocations.delete(rideId);
    console.log(`🚗 Tracking stopped for ride ${rideId}`);
    io.to(`ride:${rideId}`).emit("ride:locationUpdate", {
      ride_id: rideId,
      lat: lastLoc?.lat ?? null,
      long: lastLoc?.long ?? null,
    });

    // تحديث حالة الرحلة إلى "تم الوصول" (status = 6)
    updateRideStatus(rideId, { status: 6 })
      .then(() => {
        io.to(`ride:${rideId}`).emit("ride:completed", {
          ride_id: rideId,
          message: "Ride completed - End trip",
        });

        console.log(`🏁 Ride ${rideId} completed at destination`);
      })
      .catch((err) => {
        console.error("Error updating ride status to completed:", err);
      });

    return true;
  }
  return false;
}

module.exports = { startTracking, updateLocation, arriveAtPickup, stopTracking };
