const driverLocationService = require("../services/driverLocation.service");

// 🔧 إعدادات
const DB_UPDATE_EVERY_MS = 5000; // خليها 0 إذا بدك توقف interval وتكتب DB عند كل update-location

module.exports = (io, socket) => {

  // ✅ اطبع الرومات المخصصة فقط (بدون رومات socket.id) - عند الحاجة فقط
  const logRooms = (label) => {
    const roomsMap = io.sockets.adapter.rooms;
    const customRooms = [];

    for (const [roomName, socketSet] of roomsMap.entries()) {
      const isPrivateSocketRoom = io.sockets.sockets.has(roomName); // room == socket.id
      if (!isPrivateSocketRoom) {
        customRooms.push({ room: roomName, socketsCount: socketSet.size });
      }
    }

    console.log("========== ROOMS DEBUG ==========");
    console.log("📌", label);
    console.log("🏷️ Custom rooms:", customRooms.length ? customRooms : "none");
    console.log("=================================\n");
  };

  // ✅ helper: تأكد من رقم valid
  const toNumber = (v) => {
    const n = Number(v);
    return Number.isFinite(n) ? n : null;
  };

  // ✅ helper: اسم غرفة السائق
  const driverRoom = (driverId) => `driver:${driverId}`;

  // ✅ حماية: لا تسمح لنفس socket يغيّر driver_id
  // (في تطبيق حقيقي: driver_id يطلع من JWT/auth مش من body)
  const bindDriverOnce = (newDriverId) => {
    if (!socket.driverId) {
      socket.driverId = newDriverId;
      return true;
    }
    return socket.driverId === newDriverId;
  };

  socket.on("driver-online", ({ driver_id, lat, long }) => {
    const driverId = toNumber(driver_id);
    const la = toNumber(lat) ?? 0;
    const lo = toNumber(long) ?? 0;

    if (!driverId) {
      console.log("⚠️ driver-online ignored: invalid driver_id:", driver_id);
      return;
    }

    // ✅ اربط السوكيت بسائق واحد فقط
    if (!bindDriverOnce(driverId)) {
      console.log(`🛑 socket ${socket.id} tried to switch driverId from ${socket.driverId} to ${driverId} (ignored)`);
      return;
    }

    socket.join(driverRoom(driverId));

    driverLocationService.updateMemory(driverId, la, lo);

    console.log(`✅ Driver ${driverId} online (socket: ${socket.id})`);
    socket.emit("driver:ready", { driver_id: driverId });

    logRooms(`after driver-online join ${driverRoom(driverId)}`);

    if (DB_UPDATE_EVERY_MS > 0) {
      if (socket.dbInterval) clearInterval(socket.dbInterval);

      socket.dbInterval = setInterval(() => {
        const d = driverLocationService.getDriver(driverId);
        if (!d) return;

        driverLocationService
          .update(driverId, d.lat, d.long)
          .catch(err => console.error("DB update error:", err));
      }, DB_UPDATE_EVERY_MS);
    }
  });

  socket.on("update-location", ({ lat, long }) => {
    if (!socket.driverId) return;

    const la = toNumber(lat);
    const lo = toNumber(long);
    if (la === null || lo === null) return;

    driverLocationService.updateMemory(socket.driverId, la, lo);

    io.to(driverRoom(socket.driverId)).emit("driver:moved", {
      driver_id: socket.driverId,
      lat: la,
      long: lo,
      timestamp: Date.now(),
    });

    // ✅ إذا بدك كتابة DB عند كل تحديث (أدق لكن أثقل)
    if (DB_UPDATE_EVERY_MS === 0) {
      driverLocationService
        .update(socket.driverId, la, lo)
        .catch(err => console.error("DB update error:", err));
    }
  });

  socket.on("disconnect", () => {
    if (socket.dbInterval) {
      clearInterval(socket.dbInterval);
      socket.dbInterval = null;
    }

    if (socket.driverId) {
      driverLocationService.remove(socket.driverId);
      console.log(`⚫ Driver ${socket.driverId} offline (socket: ${socket.id})`);
      logRooms(`after disconnect driver:${socket.driverId}`);
    }
  });

};
