const REDIS_URL = String(process.env.REDIS_URL || "").trim();
const REDIS_HOST = String(process.env.REDIS_HOST || "").trim();
const REDIS_PORT = Number.isFinite(Number(process.env.REDIS_PORT))
  ? Math.max(1, Math.floor(Number(process.env.REDIS_PORT)))
  : 6379;
const REDIS_USERNAME = String(process.env.REDIS_USERNAME || "").trim();
const REDIS_PASSWORD = String(process.env.REDIS_PASSWORD || "").trim();
const REDIS_DB = Number.isFinite(Number(process.env.REDIS_DB))
  ? Math.max(0, Math.floor(Number(process.env.REDIS_DB)))
  : 0;
const REDIS_LOCKS_ENABLED = String(process.env.REDIS_LOCKS_ENABLED || "1") === "1";

let redisLib = null;
let redisClient = null;
let initPromise = null;
let initErrorLogged = false;

const hasConfig = () => REDIS_URL.length > 0 || REDIS_HOST.length > 0;
const isEnabled = () => REDIS_LOCKS_ENABLED && hasConfig();

const buildClientOptions = () => {
  if (REDIS_URL.length > 0) {
    return { url: REDIS_URL };
  }

  const options = {
    socket: {
      host: REDIS_HOST,
      port: REDIS_PORT,
    },
    database: REDIS_DB,
  };

  if (REDIS_USERNAME) {
    options.username = REDIS_USERNAME;
  }

  if (REDIS_PASSWORD) {
    options.password = REDIS_PASSWORD;
  }

  return options;
};

const logInitErrorOnce = (error) => {
  if (initErrorLogged) return;
  initErrorLogged = true;
  console.warn(
    "[redisClient] disabled:",
    error?.message || error || "Redis init failed"
  );
};

const ensureClient = async () => {
  if (!isEnabled()) return null;
  if (redisClient) return redisClient;
  if (initPromise) return initPromise;

  initPromise = (async () => {
    try {
      if (!redisLib) {
        redisLib = require("redis");
      }

      const client = redisLib.createClient(buildClientOptions());
      client.on("error", (error) => {
        console.warn(
          "[redisClient] runtime error:",
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

module.exports = {
  isEnabled,
  ensureClient,
};
