// driver.test.js
const io = require("socket.io-client");

const socket = io("http://localhost:3000");

socket.on("connect", () => {
  console.log("✅ Connected as Driver test. Socket ID:", socket.id);

  // =========================
  // DRIVER 1 ONLINE
  // =========================
  const driver1 = { driver_id: 1, lat: 33.6, long: 36.28 };
  console.log("➡️ Driver 1 going online");
  socket.emit("driver-online", driver1);

  // تحديث موقع Driver 1 كل ثانية
  const driver1Interval = setInterval(() => {
    driver1.lat += (Math.random() - 0.5) / 1000; // تغييرات صغيرة
    driver1.long += (Math.random() - 0.5) / 1000;
    console.log("📍 Driver 1 update location", { lat: driver1.lat, long: driver1.long });
    socket.emit("update-location", driver1);
  }, 1000);

  // =========================
  // DRIVER 2 ONLINE (قريب جداً)
  // =========================
  const driver2 = { driver_id: 2, lat: 33.6001, long: 36.2801 };
  setTimeout(() => {
    console.log("➡️ Driver 2 going online");
    socket.emit("driver-online", driver2);

    // تحديث موقع Driver 2 كل ثانية
    setInterval(() => {
      driver2.lat += (Math.random() - 0.5) / 1000;
      driver2.long += (Math.random() - 0.5) / 1000;
      console.log("📍 Driver 2 update location", { lat: driver2.lat, long: driver2.long });
      socket.emit("update-location", driver2);
    }, 1000);
  }, 500);

  // =========================
  // Disconnect بعد 15 ثانية
  // =========================
  setTimeout(() => {
    console.log("Test finished, disconnecting...");
    clearInterval(driver1Interval);
    socket.disconnect();
  }, 15000);
});

socket.on("disconnect", () => {
  console.log("❌ Disconnected from server");
});
