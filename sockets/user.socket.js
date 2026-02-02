const driverLocationService = require("../services/driverLocation.service");

module.exports = (io, socket) => {
  socket.on("user:findNearbyDrivers", ({ lat, long, radius = 5000 }) => {
    socket.isUser = true;

    const nearbyDrivers = driverLocationService.getNearbyDriversFromMemory(lat, long, radius);
    console.log("Nearby drivers found:", nearbyDrivers.length);

    socket.emit("user:nearbyDrivers", nearbyDrivers);

    // ✅ ممنوع join لرومات السائقين حسب طلبك
    // nearbyDrivers.forEach(driver => socket.join(`driver:${driver.driver_id}`));
  });

  socket.on("disconnect", () => {
    if (socket.isUser) console.log("User disconnected:", socket.id);
  });
};
