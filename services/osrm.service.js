const axios = require("axios");

const OSRM_BASE_URL = process.env.OSRM_BASE_URL || "http://127.0.0.1:5000";

async function getRoadDistanceMeters({ fromLat, fromLong, toLat, toLong }) {
  if (fromLat == null || fromLong == null || toLat == null || toLong == null) return null;

  // OSRM uses lon,lat
  const base = OSRM_BASE_URL.replace(/\/+$/, "");
  const url = `${base}/route/v1/driving/${fromLong},${fromLat};${toLong},${toLat}?overview=false&steps=false&alternatives=false`;

  try {
    const res = await axios.get(url, { timeout: 2500 });
    const dist = res?.data?.routes?.[0]?.distance;
    return Number.isFinite(Number(dist)) ? Math.round(Number(dist)) : null;
  } catch (e) {
    console.warn("[osrm] route failed:", e?.response?.data || e?.message || e);
    return null;
  }
}

module.exports = { getRoadDistanceMeters, OSRM_BASE_URL };