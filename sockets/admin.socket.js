const {
  emitAdminDriversSnapshot,
  getAdminDriverRoom,
  normalizeServiceCategoryId,
} = require("../services/adminDriverFeed.service");

module.exports = (io, socket) => {
  socket.on("admin:drivers:subscribe", (payload = {}) => {
    const serviceCategoryId = normalizeServiceCategoryId(
      payload?.service_category_id ?? payload?.service_cat_id
    );
    const nextRoom = getAdminDriverRoom(serviceCategoryId);

    if (socket.adminDriversRoom && socket.adminDriversRoom !== nextRoom) {
      socket.leave(socket.adminDriversRoom);
    }

    socket.adminDriversRoom = nextRoom;
    socket.adminDriversServiceCategoryId = serviceCategoryId;
    socket.join(nextRoom);

    emitAdminDriversSnapshot(socket, serviceCategoryId);
  });

  socket.on("admin:drivers:refresh", () => {
    emitAdminDriversSnapshot(socket, socket.adminDriversServiceCategoryId ?? null);
  });
};
