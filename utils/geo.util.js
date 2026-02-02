function getDistanceMeters(lat1, long1, lat2, long2) {
  const R = 6371000;
  const dLat = toRad(lat2 - lat1);
  const dLon = toRad(long2 - long1);

  const a =
    Math.sin(dLat / 2) ** 2 +
    Math.cos(toRad(lat1)) *
      Math.cos(toRad(lat2)) *
      Math.sin(dLon / 2) ** 2;

  return R * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
}

const toRad = (deg) => deg * (Math.PI / 180);

module.exports = { getDistanceMeters };
