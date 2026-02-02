const driverLocationService = require("../services/driverLocation.service");

module.exports = (io, socket) => {

  socket.on("driver-online", ({ driver_id, lat, long }) => {
    socket.driverId = driver_id;
    socket.join(`driver_${driver_id}`);

    driverLocationService.updateMemory(driver_id, lat, long);
    console.log(`Driver ${driver_id} online`);

    socket.emit("driver:ready", { driver_id });

    socket.dbInterval = setInterval(() => {
      const driverData = driverLocationService.getDriver(driver_id);
      if (driverData) {
        driverLocationService
          .update(driver_id, driverData.lat, driverData.long)
          .catch(err => console.error("DB update error:", err));
      }
    }, 5000);
  });

  socket.on("update-location", ({ lat, long }) => {
    if (!socket.driverId) return;

    driverLocationService.updateMemory(socket.driverId, lat, long);

    io.to(`driver_${socket.driverId}`).emit("driver:moved", {
      driver_id: socket.driverId,
      lat,
      long,
      timestamp: Date.now()
    });
  });

  socket.on("disconnect", () => {
    if (socket.driverId) {
      driverLocationService.remove(socket.driverId);

      if (socket.dbInterval) {
        clearInterval(socket.dbInterval);
      }

      console.log(`Driver ${socket.driverId} offline`);
    }
  });
};
