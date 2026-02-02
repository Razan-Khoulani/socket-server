// driver.service.js
const db = require('./db');

/**
 * تحديث موقع السائق
 * @param {number} driverDetailsId 
 * @param {number} lat 
 * @param {number} long 
 */
exports.updateDriverLocation = (driverDetailsId, lat, long) => {
  return new Promise((resolve, reject) => {
    db.query(
      `UPDATE transport_driver_details
       SET current_lat = ?, current_long = ?, updated_at = NOW()
       WHERE id = ?`,
      [lat, long, driverDetailsId],
      (err, result) => {
        if (err) return reject(err);
        if (result.affectedRows === 0) console.log('No driver found with id:', driverDetailsId);
        else console.log('Driver updated:', driverDetailsId, lat, long);
        resolve();
      }
    );
  });
};

/**
 * جلب السائقين القريبين ضمن نصف القطر
 * @param {number} lat 
 * @param {number} long 
 * @param {number} radius بالمتر
 * @returns {Promise<Array>}
 */
exports.getNearbyDrivers = (lat, long, radius = 500) => {
  return new Promise((resolve, reject) => {
    const sql = `
      SELECT 
        d.id AS driver_details_id,
        ps.provider_id AS driver_id,
        d.current_lat,
        d.current_long,
        (6371000 * acos(
            cos(radians(?)) 
            * cos(radians(d.current_lat)) 
            * cos(radians(d.current_long) - radians(?)) 
            + sin(radians(?)) * sin(radians(d.current_lat))
        )) AS distance
      FROM transport_driver_details d
      JOIN provider_services ps ON ps.id = d.provider_service_id
      WHERE d.current_lat IS NOT NULL
        AND d.current_long IS NOT NULL
      HAVING distance <= ?
      ORDER BY distance ASC
    `;

    db.query(sql, [lat, long, lat, radius], (err, results) => {
      if (err) return reject(err);

      const nearbyDrivers = results.map(d => ({
        driver_id: d.driver_id,
        lat: d.current_lat,
        long: d.current_long,
        distance: d.distance
      }));

      resolve(nearbyDrivers);
    });
  });
};
