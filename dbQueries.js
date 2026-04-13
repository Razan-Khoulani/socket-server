
//dbQueries.js
const db = require("./db"); // استيراد قاعدة البيانات (مثال)

// دالة لتحديث حالة الرحلة في قاعدة البيانات
function updateRideStatus(rideId, { status }) {
  return new Promise((resolve, reject) => {
    const query = `
      UPDATE user_ride_booking
      SET status = ?
      WHERE id = ?
    `;

    db.query(query, [status, rideId], (err, result) => {
      if (err) {
        return reject(err);
      }
      resolve(result);
    });
  });
}

module.exports = { updateRideStatus };
