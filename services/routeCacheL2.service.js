const REDIS_URL = String(process.env.REDIS_URL || "").trim();
const ROUTE_REDIS_CACHE_ENABLED = String(
  process.env.ROUTE_REDIS_CACHE_ENABLED || "0"
) === "1";

let redisLib = null;
let redisClient = null;
let initPromise = null;
let initErrorLogged = false;

const isEnabled = () => ROUTE_REDIS_CACHE_ENABLED && REDIS_URL.length > 0;

const logInitErrorOnce = (error) => {
  if (initErrorLogged) return;
  initErrorLogged = true;
  console.warn(
    "[routeCacheL2] disabled:",
    error?.message || error || "Redis init failed"
  );
};

const ensureClient = async () => {
  if (!isEnabled()) return null;
  if (redisClient) return redisClient;
  if (initPromise) return initPromise;

  initPromise = (async () => {
    try {
      // Lazy load so app still works even if redis package is not installed yet.
      if (!redisLib) {
        // eslint-disable-next-line global-require
        redisLib = require("redis");
      }

      const client = redisLib.createClient({ url: REDIS_URL });
      client.on("error", (error) => {
        console.warn(
          "[routeCacheL2] runtime error:",
          error?.message || error || "Redis client error"
        );
      });
      await client.connect();
      redisClient = client;
      return redisClient;
    } catch (error) {
      logInitErrorOnce(error);
      return null;
    } finally {
      initPromise = null;
    }
  })();

  return initPromise;
};

const parseJson = (value) => {
  if (!value || typeof value !== "string") return null;
  try {
    return JSON.parse(value);
  } catch (_) {
    return null;
  }
};

const getJson = async (key) => {
  if (!isEnabled() || !key) return null;
  try {
    const client = await ensureClient();
    if (!client) return null;
    const raw = await client.get(key);
    return parseJson(raw);
  } catch (error) {
    return null;
  }
};

const setJson = async (key, value, ttlSeconds = 15) => {
  if (!isEnabled() || !key || !value || typeof value !== "object") return false;
  try {
    const client = await ensureClient();
    if (!client) return false;
    const payload = JSON.stringify(value);
    const ttl = Math.max(1, Math.floor(Number(ttlSeconds) || 15));
    await client.set(key, payload, { EX: ttl });
    return true;
  } catch (_) {
    return false;
  }
};

const tryAcquireLock = async (key, ttlSeconds = 3) => {
  if (!isEnabled() || !key) return null;
  try {
    const client = await ensureClient();
    if (!client) return null;
    const token = `${Date.now()}:${Math.random().toString(36).slice(2)}`;
    const ttl = Math.max(1, Math.floor(Number(ttlSeconds) || 3));
    const result = await client.set(key, token, { NX: true, EX: ttl });
    return result === "OK" ? token : null;
  } catch (_) {
    return null;
  }
};

const releaseLock = async (key, token) => {
  if (!isEnabled() || !key || !token) return false;
  try {
    const client = await ensureClient();
    if (!client) return false;

    const deleted = await client.eval(
      "if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('DEL', KEYS[1]) else return 0 end",
      {
        keys: [key],
        arguments: [token],
      }
    );

    return Number(deleted) > 0;
  } catch (_) {
    return false;
  }
};

module.exports = {
  isEnabled,
  getJson,
  setJson,
  tryAcquireLock,
  releaseLock,
};

