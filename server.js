// const http = require("http");
// const { Server } = require("socket.io");
// const initSocket = require("./socket");


// const server = http.createServer();

// const io = new Server(server, {
//   cors: { origin: "*" },
// });

// initSocket(io);

// server.listen(3000, () => {
//   console.log("Socket Server running on port 3000");
// });


const express = require("express");
const http = require("http");
const { Server } = require("socket.io");
const initSocket = require("./socket");

const app = express();
app.use(express.json());

const server = http.createServer(app);

const io = new Server(server, { cors: { origin: "*" } });
initSocket(io);

// ✅ Endpoint تستدعيه Laravel
app.post("/driver/update-status", (req, res) => {
  const { driver_id, lat, long } = req.body;
  if (!driver_id) return res.status(400).json({ status: 0, message: "driver_id required" });

  // بث حدث للسوكيت
  io.emit("driver:ready", { driver_id }); // أو io.to(`driver_${driver_id}`).emit حسب منطقك
  io.to(`driver:${driver_id}`).emit("driver:moved", { driver_id, lat, long, timestamp: Date.now() });

  return res.json({ status: 1 });
});

server.listen(3000, () => console.log("Socket Server running on port 3000"));


