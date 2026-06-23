const { ensureClient, isEnabled } = require("./redisClient.service");

const normalizeTtlMs = (ttlMs, fallbackMs = 5000) => {
  const safeTtlMs = Number(ttlMs);
  if (!Number.isFinite(safeTtlMs)) return fallbackMs;
  return Math.max(250, Math.floor(safeTtlMs));
};

const buildToken = () =>
  `${Date.now()}:${Math.random().toString(36).slice(2)}:${Math.random()
    .toString(36)
    .slice(2)}`;

const tryAcquireLock = async (key, ttlMs = 5000) => {
  if (!key) {
    return {
      acquired: false,
      token: null,
      reason: "invalid_key",
    };
  }

  if (!isEnabled()) {
    return {
      acquired: false,
      token: null,
      reason: "disabled",
    };
  }

  try {
    const client = await ensureClient();
    if (!client) {
      return {
        acquired: false,
        token: null,
        reason: "unavailable",
      };
    }

    const token = buildToken();
    const ttl = normalizeTtlMs(ttlMs);
    const result = await client.set(key, token, {
      NX: true,
      PX: ttl,
    });

    return result === "OK"
      ? {
          acquired: true,
          token,
          reason: null,
        }
      : {
          acquired: false,
          token: null,
          reason: "locked",
        };
  } catch (_) {
    return {
      acquired: false,
      token: null,
      reason: "unavailable",
    };
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

const forceReleaseLock = async (key) => {
  if (!isEnabled() || !key) return false;

  try {
    const client = await ensureClient();
    if (!client) return false;
    const deleted = await client.del(key);
    return Number(deleted) > 0;
  } catch (_) {
    return false;
  }
};

module.exports = {
  tryAcquireLock,
  releaseLock,
  forceReleaseLock,
};
