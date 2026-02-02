const driverSocket = require("./sockets/driver.socket");
const userSocket = require("./sockets/user.socket");
const biddingSocket = require("./sockets/bidding.socket"); 


module.exports = (io) => {
  io.on("connection", (socket) => {
    console.log("Connected:", socket.id);

    driverSocket(io, socket);
    userSocket(io, socket);
        biddingSocket(io, socket); 


    socket.on("disconnect", () => {
      console.log("Disconnected:", socket.id);
    });
  });
};
