const axios = require("axios");
const crypto = require("crypto");

const LARAVEL_BASE_URL = String(
  process.env.LARAVEL_BASE_URL ||
    process.env.LARAVEL_URL ||
    "https://api.catch-syria.com"
).replace(/\/+$/, "");

const LARAVEL_DRIVER_NOTIFICATION_BATCH_PATH =
  process.env.LARAVEL_DRIVER_NOTIFICATION_BATCH_PATH ||
  "/api/internal/driver-update-list-notifications/batch";

const LARAVEL_INTERNAL_SECRET = String(
  process.env.SOCKET_INTERNAL_SECRET ||
    process.env.LARAVEL_INTERNAL_SECRET ||
    ""
).trim();

const LARAVEL_TIMEOUT_MS = Number.isFinite(
  Number(process.env.LARAVEL_TIMEOUT_MS)
)
  ? Math.max(1000, Number(process.env.LARAVEL_TIMEOUT_MS))
  : 7000;

const DRIVER_PUSH_BATCH_MAX_SIZE = Number.isFinite(
  Number(process.env.DRIVER_PUSH_BATCH_MAX_SIZE)
)
  ? Math.max(
      1,
      Math.floor(Number(process.env.DRIVER_PUSH_BATCH_MAX_SIZE))
    )
  : 20;

const DRIVER_PUSH_BATCH_MAX_BYTES = Number.isFinite(
  Number(process.env.DRIVER_PUSH_BATCH_MAX_BYTES)
)
  ? Math.max(
      50_000,
      Math.floor(Number(process.env.DRIVER_PUSH_BATCH_MAX_BYTES))
    )
  : 500_000;

const DRIVER_PUSH_BATCH_WAIT_MS = Number.isFinite(
  Number(process.env.DRIVER_PUSH_BATCH_WAIT_MS)
)
  ? Math.max(20, Number(process.env.DRIVER_PUSH_BATCH_WAIT_MS))
  : 100;

const DRIVER_PUSH_BATCH_MAX_CONCURRENCY = Number.isFinite(
  Number(process.env.DRIVER_PUSH_BATCH_MAX_CONCURRENCY)
)
  ? Math.max(
      1,
      Math.floor(Number(process.env.DRIVER_PUSH_BATCH_MAX_CONCURRENCY))
    )
  : 2;

const DRIVER_PUSH_BATCH_RETRY_MAX = Number.isFinite(
  Number(process.env.DRIVER_PUSH_BATCH_RETRY_MAX)
)
  ? Math.max(
      0,
      Math.floor(Number(process.env.DRIVER_PUSH_BATCH_RETRY_MAX))
    )
  : 1;

const DRIVER_PUSH_BATCH_MAX_PENDING = Number.isFinite(
  Number(process.env.DRIVER_PUSH_BATCH_MAX_PENDING)
)
  ? Math.max(
      100,
      Math.floor(Number(process.env.DRIVER_PUSH_BATCH_MAX_PENDING))
    )
  : 10_000;

const DRIVER_PUSH_RECENT_TTL_MS = Number.isFinite(
  Number(process.env.DRIVER_PUSH_RECENT_TTL_MS)
)
  ? Math.max(
      500,
      Number(process.env.DRIVER_PUSH_RECENT_TTL_MS)
    )
  : 5000;

// requestKey -> { payload, payloadBytes, resolve }
const pendingNotifications = new Map();

// requestKey -> Promise<boolean>
const inFlightNotifications = new Map();

// requestKey -> { at, result }
const recentNotifications = new Map();

let flushTimer = null;
let activeBatchRequests = 0;

const toNumber = (value) => {
  if (
    value === null ||
    value === undefined ||
    value === ""
  ) {
    return null;
  }

  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
};

const toText = (value) => {
  if (value === null || value === undefined) {
    return null;
  }

  const text = String(value).trim();
  return text.length ? text : null;
};

const sleep = (ms) =>
  new Promise((resolve) =>
    setTimeout(resolve, Math.max(0, ms))
  );

const buildInternalHeaders = () => {
  if (!LARAVEL_INTERNAL_SECRET) {
    return undefined;
  }

  return {
    "X-Socket-Internal-Secret":
      LARAVEL_INTERNAL_SECRET,
  };
};

/*
 * المفتاح يتغير إذا تغير محتوى الإشعار.
 * لذلك السعر المعدل أو التايمر الجديد لا ينحسب Duplicate
 * إذا تغير الـ payload فعلياً.
 */
const buildNotificationKey = (payload = {}) => {
  const driverId = toNumber(payload?.driver_id);
  const rideId = toNumber(payload?.ride_id);

  if (!driverId || !rideId) {
    return null;
  }

  let serializedPayload = "";

  try {
    serializedPayload = JSON.stringify(payload);
  } catch (_) {
    serializedPayload = `${driverId}:${rideId}:${Date.now()}`;
  }

  const hash = crypto
    .createHash("sha1")
    .update(serializedPayload)
    .digest("hex");

  return `driver:${driverId}|ride:${rideId}|payload:${hash}`;
};

const cleanupRecentNotifications = () => {
  const now = Date.now();

  for (
    const [key, entry] of recentNotifications.entries()
  ) {
    const at = toNumber(entry?.at) ?? 0;

    if (
      !at ||
      now - at > DRIVER_PUSH_RECENT_TTL_MS
    ) {
      recentNotifications.delete(key);
    }
  }
};

const cleanupTimer = setInterval(
  cleanupRecentNotifications,
  30_000
);

cleanupTimer.unref?.();

const settleEntry = (
  requestKey,
  entry,
  success
) => {
  const resolvedSuccess = success === true;

  recentNotifications.set(requestKey, {
    at: Date.now(),
    result: resolvedSuccess,
  });

  inFlightNotifications.delete(requestKey);

  try {
    entry.resolve(resolvedSuccess);
  } catch (_) {}
};

const getResultMap = (responseData = {}) => {
  const successByKey = new Map();

  const acceptedKeys = Array.isArray(
    responseData?.accepted_keys
  )
    ? responseData.accepted_keys
    : [];

  for (const key of acceptedKeys) {
    const safeKey = toText(key);
    if (safeKey) {
      successByKey.set(safeKey, true);
    }
  }

  const results = Array.isArray(
    responseData?.results
  )
    ? responseData.results
    : [];

  for (const result of results) {
    const requestKey = toText(
      result?.request_key
    );

    if (!requestKey) continue;

    const success =
      result?.success === true ||
      result?.success === 1 ||
      result?.status === 1;

    successByKey.set(requestKey, success);
  }

  return successByKey;
};

const sendBatchRequest = async (
  entries,
  attempt = 0
) => {
  const items = entries.map(
    ([requestKey, entry]) => ({
      request_key: requestKey,
      ...entry.payload,
    })
  );

  try {
    const response = await axios.post(
      `${LARAVEL_BASE_URL}${LARAVEL_DRIVER_NOTIFICATION_BATCH_PATH}`,
      {
        items,
      },
      {
        timeout: LARAVEL_TIMEOUT_MS,
        headers: buildInternalHeaders(),
      }
    );

    const responseData =
      response?.data &&
      typeof response.data === "object"
        ? response.data
        : {};

    const successByKey =
      getResultMap(responseData);

    /*
     * إذا Laravel رجع status=1 بدون accepted_keys،
     * نعتبر كل الدفعة مقبولة.
     */
    const acceptWholeBatch =
      responseData?.status === 1 &&
      successByKey.size === 0;

    let successCount = 0;

    for (
      const [requestKey, entry] of entries
    ) {
      const success =
        acceptWholeBatch ||
        successByKey.get(requestKey) === true;

      if (success) {
        successCount += 1;
      }

      settleEntry(
        requestKey,
        entry,
        success
      );
    }

    console.log("[driver-push-batch] sent", {
      batch_size: entries.length,
      success_count: successCount,
      failed_count:
        entries.length - successCount,
      active_batches: activeBatchRequests,
      pending_items:
        pendingNotifications.size,
    });
  } catch (error) {
    if (
      attempt < DRIVER_PUSH_BATCH_RETRY_MAX
    ) {
      await sleep(300 * (attempt + 1));

      return sendBatchRequest(
        entries,
        attempt + 1
      );
    }

    console.error("[driver-push-batch] failed", {
      batch_size: entries.length,
      attempts: attempt + 1,
      error:
        error?.response?.data ||
        error?.message ||
        error,
    });

    for (
      const [requestKey, entry] of entries
    ) {
      settleEntry(
        requestKey,
        entry,
        false
      );
    }
  }
};

/*
 * يأخذ دفعة مع احترام:
 * - الحد الأقصى لعدد العناصر.
 * - الحد الأقصى لحجم JSON.
 */
const takeNextBatch = () => {
  const entries = [];
  let totalBytes = 0;

  for (
    const [requestKey, entry] of
      pendingNotifications.entries()
  ) {
    const entryBytes =
      Number(entry?.payloadBytes) || 0;

    const exceedsCount =
      entries.length >=
      DRIVER_PUSH_BATCH_MAX_SIZE;

    const exceedsBytes =
      entries.length > 0 &&
      totalBytes + entryBytes >
        DRIVER_PUSH_BATCH_MAX_BYTES;

    if (exceedsCount || exceedsBytes) {
      break;
    }

    pendingNotifications.delete(
      requestKey
    );

    entries.push([
      requestKey,
      entry,
    ]);

    totalBytes += entryBytes;
  }

  return entries;
};

const pumpBatches = () => {
  while (
    activeBatchRequests <
      DRIVER_PUSH_BATCH_MAX_CONCURRENCY &&
    pendingNotifications.size > 0
  ) {
    const entries = takeNextBatch();

    if (entries.length === 0) {
      return;
    }

    activeBatchRequests += 1;

    void sendBatchRequest(entries).finally(
      () => {
        activeBatchRequests = Math.max(
          0,
          activeBatchRequests - 1
        );

        if (
          pendingNotifications.size > 0
        ) {
          scheduleFlush(0);
        }
      }
    );
  }
};

function scheduleFlush(
  delayMs = DRIVER_PUSH_BATCH_WAIT_MS
) {
  const shouldFlushImmediately =
    pendingNotifications.size >=
    DRIVER_PUSH_BATCH_MAX_SIZE;

  const resolvedDelay =
    shouldFlushImmediately ? 0 : delayMs;

  if (flushTimer) {
    if (resolvedDelay > 0) {
      return;
    }

    clearTimeout(flushTimer);
    flushTimer = null;
  }

  flushTimer = setTimeout(() => {
    flushTimer = null;
    pumpBatches();
  }, resolvedDelay);

  flushTimer.unref?.();
}

const enqueueDriverPushNotification = (
  payload = {}
) => {
  const driverId = toNumber(
    payload?.driver_id
  );

  const rideId = toNumber(
    payload?.ride_id
  );

  if (!driverId || !rideId) {
    return Promise.resolve(false);
  }

  const safePayload = {
    ...payload,
    driver_id: driverId,
    ride_id: rideId,
  };

  const requestKey =
    buildNotificationKey(safePayload);

  if (!requestKey) {
    return Promise.resolve(false);
  }

  cleanupRecentNotifications();

  const recent =
    recentNotifications.get(requestKey);

  if (
    recent &&
    Date.now() - Number(recent.at || 0) <=
      DRIVER_PUSH_RECENT_TTL_MS
  ) {
    return Promise.resolve(
      recent.result === true
    );
  }

  const existingPromise =
    inFlightNotifications.get(requestKey);

  if (existingPromise) {
    return existingPromise;
  }

  if (
    pendingNotifications.size >=
    DRIVER_PUSH_BATCH_MAX_PENDING
  ) {
    console.error(
      "[driver-push-batch] queue full",
      {
        pending_items:
          pendingNotifications.size,
        max_pending:
          DRIVER_PUSH_BATCH_MAX_PENDING,
        driver_id: driverId,
        ride_id: rideId,
      }
    );

    return Promise.resolve(false);
  }

  let serializedItem;

  try {
    serializedItem = JSON.stringify({
      request_key: requestKey,
      ...safePayload,
    });
  } catch (error) {
    console.error(
      "[driver-push-batch] invalid payload",
      {
        driver_id: driverId,
        ride_id: rideId,
        error:
          error?.message || error,
      }
    );

    return Promise.resolve(false);
  }

  const payloadBytes =
    Buffer.byteLength(
      serializedItem,
      "utf8"
    );

  const promise = new Promise(
    (resolve) => {
      pendingNotifications.set(
        requestKey,
        {
          payload: safePayload,
          payloadBytes,
          resolve,
        }
      );

      scheduleFlush();
    }
  );

  inFlightNotifications.set(
    requestKey,
    promise
  );

  return promise;
};

const getDriverPushBatchStats = () => ({
  pending:
    pendingNotifications.size,
  in_flight_items:
    inFlightNotifications.size,
  recent:
    recentNotifications.size,
  active_batches:
    activeBatchRequests,
});

module.exports = {
  enqueueDriverPushNotification,
  getDriverPushBatchStats,
};