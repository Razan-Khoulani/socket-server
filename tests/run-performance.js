const http = require("http");
const net = require("net");
const path = require("path");
const { spawn } = require("child_process");
const { performance } = require("perf_hooks");
const { io } = require("socket.io-client");

const SERVER_CWD = path.join(__dirname, "..");
const SOCKET_HOST = "127.0.0.1";
const SOCKET_PORT = Number(process.env.PERF_SOCKET_PORT || 4100);
const LARAVEL_STUB_PORT = Number(process.env.PERF_LARAVEL_STUB_PORT || 5055);
const SOCKET_URL = `http://${SOCKET_HOST}:${SOCKET_PORT}`;
const LARAVEL_STUB_URL = `http://${SOCKET_HOST}:${LARAVEL_STUB_PORT}`;

const CONNECT_COUNTS = (process.env.PERF_CONNECT_COUNTS || "100,300,600")
  .split(",")
  .map((value) => Number(value.trim()))
  .filter((value) => Number.isFinite(value) && value > 0);
const DRIVER_COUNTS = (process.env.PERF_DRIVER_COUNTS || "50,150,300")
  .split(",")
  .map((value) => Number(value.trim()))
  .filter((value) => Number.isFinite(value) && value > 0);
const DRIVER_UPDATE_HZ = Number.isFinite(Number(process.env.PERF_DRIVER_UPDATE_HZ))
  ? Math.max(1, Number(process.env.PERF_DRIVER_UPDATE_HZ))
  : 5;
const DRIVER_UPDATE_DURATION_MS = Number.isFinite(
  Number(process.env.PERF_DRIVER_UPDATE_DURATION_MS)
)
  ? Math.max(1000, Number(process.env.PERF_DRIVER_UPDATE_DURATION_MS))
  : 6000;
const HTTP_STEPS = (process.env.PERF_HTTP_STEPS || "20:1000,50:2000,100:4000")
  .split(",")
  .map((step) => {
    const [concurrencyRaw, totalRaw] = step.split(":");
    const concurrency = Number(concurrencyRaw?.trim());
    const total = Number(totalRaw?.trim());
    if (!Number.isFinite(concurrency) || !Number.isFinite(total)) return null;
    if (concurrency <= 0 || total <= 0) return null;
    return { concurrency, total };
  })
  .filter(Boolean);

const SERVER_START_TIMEOUT_MS = 15000;
const CLIENT_CONNECT_TIMEOUT_MS = 10000;
const CLIENT_BATCH_SIZE = 50;
const HTTP_DRIVER_POOL = 100;
const HTTP_AGENT = new http.Agent({
  keepAlive: true,
  maxSockets: 200,
});

const sleep = (ms) =>
  new Promise((resolve) => setTimeout(resolve, Math.max(0, ms)));

const round2 = (value) =>
  Number.isFinite(value) ? Math.round(value * 100) / 100 : null;

const percentile = (values, p) => {
  if (!Array.isArray(values) || values.length === 0) return null;
  const sorted = [...values].sort((a, b) => a - b);
  const index = Math.min(
    sorted.length - 1,
    Math.max(0, Math.ceil(sorted.length * p) - 1)
  );
  return round2(sorted[index]);
};

const summarizeLatencies = (values) => ({
  count: values.length,
  p50_ms: percentile(values, 0.5),
  p95_ms: percentile(values, 0.95),
  p99_ms: percentile(values, 0.99),
  max_ms: values.length ? round2(Math.max(...values)) : null,
  avg_ms: values.length
    ? round2(values.reduce((sum, value) => sum + value, 0) / values.length)
    : null,
});

const waitForPort = (host, port, timeoutMs = SERVER_START_TIMEOUT_MS) =>
  new Promise((resolve, reject) => {
    const startedAt = Date.now();

    const tryConnect = () => {
      const socket = new net.Socket();
      socket.setTimeout(1000);

      const retry = () => {
        socket.destroy();
        if (Date.now() - startedAt >= timeoutMs) {
          reject(new Error(`Timeout waiting for ${host}:${port}`));
          return;
        }
        setTimeout(tryConnect, 250);
      };

      socket.once("error", retry);
      socket.once("timeout", retry);
      socket.connect(port, host, () => {
        socket.end();
        resolve();
      });
    };

    tryConnect();
  });

const readJsonBody = async (req) => {
  const chunks = [];
  for await (const chunk of req) {
    chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
  }
  if (chunks.length === 0) return {};
  const raw = Buffer.concat(chunks).toString("utf8").trim();
  if (!raw) return {};
  try {
    return JSON.parse(raw);
  } catch (_) {
    return {};
  }
};

const sendJson = (res, statusCode, payload) => {
  const body = Buffer.from(JSON.stringify(payload));
  res.writeHead(statusCode, {
    "Content-Type": "application/json",
    "Content-Length": body.length,
  });
  res.end(body);
};

const buildDriverProfile = (payload = {}) => {
  const driverId = Number(payload.driver_id ?? payload.provider_id ?? 1);
  const driverServiceId = Number(payload.driver_service_id ?? driverId);
  const serviceTypeId = Number(
    payload.service_type_id ?? payload.vehicle_type_id ?? 1
  );
  const serviceCategoryId = Number(payload.service_category_id ?? 1);

  return {
    driver_id: driverId,
    provider_id: driverId,
    driver_service_id: driverServiceId,
    driver_detail_id: driverServiceId,
    current_status: 1,
    new_status: 1,
    driver_current_status: 1,
    service_type_id: Number.isFinite(serviceTypeId) ? serviceTypeId : 1,
    service_category_id: Number.isFinite(serviceCategoryId)
      ? serviceCategoryId
      : 1,
    vehicle_type_id: Number.isFinite(serviceTypeId) ? serviceTypeId : 1,
    vehicle_type_name: "Stub Vehicle",
    vehicle_type_icon: "stub.png",
    vehicle_company: "Stub",
    plat_no: `P-${driverId}`,
    model_year: 2024,
    model_name: "Perf",
    vehicle_color: "black",
    driver_name: `Driver ${driverId}`,
    rating: 4.9,
    phone: "0999999999",
    country_code: "+963",
    driver_gender: 1,
    child_seat: 0,
    handicap: 0,
    current_lat: Number(payload.current_lat ?? payload.lat ?? 33.5138),
    current_long: Number(payload.current_long ?? payload.long ?? 36.2765),
    not_valid_wallet_balance: 0,
    not_valid_wallet_balance_msg: "",
  };
};

const startLaravelStub = async () => {
  const server = http.createServer(async (req, res) => {
    const url = new URL(req.url, `${LARAVEL_STUB_URL}/`);
    const body = await readJsonBody(req);

    if (req.method === "POST" && url.pathname === "/api/internal/driver-admin-profile") {
      return sendJson(res, 200, {
        status: 1,
        data: buildDriverProfile(body),
      });
    }

    if (req.method === "POST" && url.pathname === "/api/driver/update-current-status") {
      return sendJson(res, 200, {
        status: 1,
        ...buildDriverProfile(body),
      });
    }

    if (
      req.method === "POST" &&
      url.pathname === "/api/customer/transport/search-radius"
    ) {
      const serviceCategoryId = Number(body?.service_category_id ?? 1);
      return sendJson(res, 200, {
        status: 1,
        service_category_id: Number.isFinite(serviceCategoryId)
          ? serviceCategoryId
          : 1,
        radius_m: 5000,
        dispatch_timeout_s: 5,
        dispatch_radius_stages_m: [1000, 2000, 3000, 5000],
      });
    }

    if (
      req.method === "POST" &&
      url.pathname === "/api/customer/transport/vehicle-fares-version"
    ) {
      return sendJson(res, 200, {
        status: 1,
        version: 1,
      });
    }

    if (req.method === "GET" && url.pathname === "/api/getRoute") {
      return sendJson(res, 200, {
        distance_m: 1200,
        duration: 3,
      });
    }

    return sendJson(res, 404, {
      status: 0,
      message: "Stub route not found",
      method: req.method,
      path: url.pathname,
    });
  });

  await new Promise((resolve, reject) => {
    server.once("error", reject);
    server.listen(LARAVEL_STUB_PORT, SOCKET_HOST, resolve);
  });

  return server;
};

const startSocketServer = async () => {
  const child = spawn(process.execPath, ["server.js"], {
    cwd: SERVER_CWD,
    env: {
      ...process.env,
      SOCKET_BIND_HOST: SOCKET_HOST,
      SOCKET_BIND_PORT: String(SOCKET_PORT),
      LARAVEL_BASE_URL: LARAVEL_STUB_URL,
      LARAVEL_URL: LARAVEL_STUB_URL,
      DRIVER_ADMIN_PROFILE_CACHE_TTL_MS: "600000",
      DRIVER_ADMIN_PROFILE_SYNC_EVERY_MS: "600000",
      DEBUG_SOCKET_EVENTS: "0",
      VERBOSE_NEARBY_LOGS: "0",
      VERBOSE_DRIVER_LOCATION_LOGS: "0",
      VERBOSE_IMAGE_SOURCE_LOGS: "0",
    },
    stdio: ["ignore", "ignore", "pipe"],
  });

  let stderr = "";
  child.stderr?.on("data", (chunk) => {
    stderr += chunk.toString("utf8");
    if (stderr.length > 8000) {
      stderr = stderr.slice(-8000);
    }
  });

  child.once("exit", (code) => {
    if (code !== null && code !== 0) {
      stderr += `\n[server-exit-code] ${code}`;
    }
  });

  try {
    await waitForPort(SOCKET_HOST, SOCKET_PORT, SERVER_START_TIMEOUT_MS);
  } catch (error) {
    child.kill();
    throw new Error(
      `${error.message}${stderr ? `\n--- server stderr ---\n${stderr}` : ""}`
    );
  }

  return child;
};

const stopServer = async (serverLike) => {
  if (!serverLike) return;

  if (typeof serverLike.close === "function") {
    await new Promise((resolve) => {
      serverLike.close(() => resolve());
    });
    return;
  }

  if (typeof serverLike.kill === "function") {
    if (serverLike.exitCode !== null) return;
    await new Promise((resolve) => {
      const done = () => resolve();
      serverLike.once("exit", done);
      serverLike.kill();
      setTimeout(done, 3000);
    });
  }
};

const connectSocketClient = () =>
  new Promise((resolve, reject) => {
    const startedAt = performance.now();
    const socket = io(SOCKET_URL, {
      transports: ["websocket"],
      forceNew: true,
      reconnection: false,
      timeout: CLIENT_CONNECT_TIMEOUT_MS,
    });

    let settled = false;
    const timer = setTimeout(() => {
      if (settled) return;
      settled = true;
      socket.close();
      reject(new Error("Socket connect timeout"));
    }, CLIENT_CONNECT_TIMEOUT_MS);

    const cleanup = () => {
      clearTimeout(timer);
      socket.off("connect", onConnect);
      socket.off("connect_error", onError);
    };

    const onConnect = () => {
      if (settled) return;
      settled = true;
      cleanup();
      resolve({
        socket,
        latencyMs: performance.now() - startedAt,
      });
    };

    const onError = (error) => {
      if (settled) return;
      settled = true;
      cleanup();
      socket.close();
      reject(error);
    };

    socket.once("connect", onConnect);
    socket.once("connect_error", onError);
  });

const connectSockets = async (count, onConnected = null) => {
  const sockets = [];
  const latencies = [];
  const errors = [];
  const startedAt = performance.now();

  for (let offset = 0; offset < count; offset += CLIENT_BATCH_SIZE) {
    const batchSize = Math.min(CLIENT_BATCH_SIZE, count - offset);
    const batchResults = await Promise.allSettled(
      Array.from({ length: batchSize }, () => connectSocketClient())
    );

    for (const result of batchResults) {
      if (result.status === "fulfilled") {
        sockets.push(result.value.socket);
        latencies.push(result.value.latencyMs);
        if (typeof onConnected === "function") {
          await onConnected(result.value.socket, sockets.length - 1);
        }
      } else {
        errors.push(result.reason?.message || String(result.reason));
      }
    }
  }

  return {
    sockets,
    errors,
    latencies,
    elapsedMs: performance.now() - startedAt,
  };
};

const disconnectSockets = async (sockets = []) => {
  sockets.forEach((socket) => {
    try {
      socket.close();
    } catch (_) {}
  });
  await sleep(500);
};

const runConnectScenario = async (count) => {
  const result = await connectSockets(count);
  await sleep(1000);
  await disconnectSockets(result.sockets);

  return {
    scenario: "socket_connect",
    clients_requested: count,
    clients_connected: result.sockets.length,
    connect_errors: result.errors.length,
    connect_rate_per_s:
      result.elapsedMs > 0
        ? round2((result.sockets.length / result.elapsedMs) * 1000)
        : null,
    elapsed_ms: round2(result.elapsedMs),
    connect_latency: summarizeLatencies(result.latencies),
  };
};

const bringDriverOnline = async (socket, driverId) =>
  new Promise((resolve, reject) => {
    const baseLat = 33.5138 + driverId * 0.00001;
    const baseLong = 36.2765 + driverId * 0.00001;

    const timer = setTimeout(() => {
      socket.off("driver:ready", onReady);
      reject(new Error(`driver:ready timeout for driver ${driverId}`));
    }, CLIENT_CONNECT_TIMEOUT_MS);

    const onReady = () => {
      clearTimeout(timer);
      socket.off("driver:ready", onReady);
      resolve({
        baseLat,
        baseLong,
      });
    };

    socket.once("driver:ready", onReady);
    socket.emit("driver-online", {
      driver_id: driverId,
      lat: baseLat,
      long: baseLong,
      access_token: `token-${driverId}`,
      driver_service_id: driverId,
      service_type_id: 1,
      service_category_id: 1,
      vehicle_type_id: 1,
      driver_gender: 1,
      child_seat: 0,
      handicap: 0,
    });
  });

const runDriverUpdateScenario = async (driverCount) => {
  let received = 0;
  const drivers = [];
  const connectResult = await connectSockets(driverCount, async (socket, index) => {
    const driverId = index + 1;
    socket.on("driver:moved", () => {
      received += 1;
    });
    const onlineMeta = await bringDriverOnline(socket, driverId);
    drivers.push({
      socket,
      driverId,
      baseLat: onlineMeta.baseLat,
      baseLong: onlineMeta.baseLong,
      sent: 0,
    });
  });

  if (connectResult.errors.length > 0) {
    await disconnectSockets(connectResult.sockets);
    return {
      scenario: "socket_driver_updates",
      drivers_requested: driverCount,
      drivers_ready: drivers.length,
      connect_errors: connectResult.errors.length,
      sent_events: 0,
      received_events: 0,
      success_ratio: 0,
      throughput_events_per_s: 0,
      duration_ms: 0,
    };
  }

  const tickMs = Math.max(50, Math.round(1000 / DRIVER_UPDATE_HZ));
  let sent = 0;
  let tick = 0;
  const startedAt = performance.now();

  const interval = setInterval(() => {
    tick += 1;
    for (const driver of drivers) {
      const lat = driver.baseLat + tick * 0.00005;
      const long = driver.baseLong;
      driver.sent += 1;
      sent += 1;
      driver.socket.emit("update-location", {
        lat,
        long,
      });
    }
  }, tickMs);

  await sleep(DRIVER_UPDATE_DURATION_MS);
  clearInterval(interval);
  await sleep(1500);
  const elapsedMs = performance.now() - startedAt;
  await disconnectSockets(connectResult.sockets);

  return {
    scenario: "socket_driver_updates",
    drivers_requested: driverCount,
    drivers_ready: drivers.length,
    connect_errors: connectResult.errors.length,
    sent_events: sent,
    received_events: received,
    success_ratio: sent > 0 ? round2(received / sent) : 0,
    throughput_events_per_s: elapsedMs > 0 ? round2((received / elapsedMs) * 1000) : 0,
    duration_ms: round2(elapsedMs),
    tick_ms: tickMs,
    configured_hz_per_driver: DRIVER_UPDATE_HZ,
  };
};

const httpPostJson = (pathname, payload) =>
  new Promise((resolve, reject) => {
    const body = Buffer.from(JSON.stringify(payload));
    const startedAt = performance.now();
    const req = http.request(
      {
        host: SOCKET_HOST,
        port: SOCKET_PORT,
        path: pathname,
        method: "POST",
        agent: HTTP_AGENT,
        headers: {
          "Content-Type": "application/json",
          "Content-Length": body.length,
        },
      },
      (res) => {
        const chunks = [];
        res.on("data", (chunk) => chunks.push(chunk));
        res.on("end", () => {
          const latencyMs = performance.now() - startedAt;
          const raw = Buffer.concat(chunks).toString("utf8");
          try {
            const parsed = raw ? JSON.parse(raw) : {};
            resolve({
              statusCode: res.statusCode,
              latencyMs,
              body: parsed,
            });
          } catch (error) {
            reject(error);
          }
        });
      }
    );

    req.on("error", reject);
    req.write(body);
    req.end();
  });

const runHttpScenario = async ({ concurrency, total }) => {
  const latencies = [];
  let ok = 0;
  let failed = 0;
  let nextIndex = 0;
  let completed = 0;
  const startedAt = performance.now();

  const worker = async () => {
    while (true) {
      const index = nextIndex;
      nextIndex += 1;
      if (index >= total) return;

      const driverId = (index % HTTP_DRIVER_POOL) + 1;
      const lat = 33.5138 + index * 0.00001;
      const long = 36.2765;

      try {
        const response = await httpPostJson("/events/internal/driver-location", {
          driver_id: driverId,
          driver_service_id: driverId,
          vehicle_type_id: 1,
          current_status: 1,
          lat,
          long,
        });

        latencies.push(response.latencyMs);
        if (response.statusCode === 200 && response.body?.status === 1) {
          ok += 1;
        } else {
          failed += 1;
        }
      } catch (_) {
        failed += 1;
      } finally {
        completed += 1;
      }
    }
  };

  await Promise.all(
    Array.from({ length: concurrency }, () => worker())
  );

  const elapsedMs = performance.now() - startedAt;

  return {
    scenario: "http_driver_location",
    total_requests: total,
    concurrency,
    completed_requests: completed,
    ok_requests: ok,
    failed_requests: failed,
    success_ratio: total > 0 ? round2(ok / total) : 0,
    rps: elapsedMs > 0 ? round2((ok / elapsedMs) * 1000) : 0,
    elapsed_ms: round2(elapsedMs),
    latency: summarizeLatencies(latencies),
  };
};

const run = async () => {
  let laravelStub = null;
  let socketServer = null;

  try {
    laravelStub = await startLaravelStub();
    socketServer = await startSocketServer();

    const summary = {
      environment: {
        socket_url: SOCKET_URL,
        laravel_stub_url: LARAVEL_STUB_URL,
        connect_counts: CONNECT_COUNTS,
        driver_counts: DRIVER_COUNTS,
        driver_update_hz: DRIVER_UPDATE_HZ,
        driver_update_duration_ms: DRIVER_UPDATE_DURATION_MS,
        http_steps: HTTP_STEPS,
      },
      connect: [],
      socket_updates: [],
      http: [],
    };

    for (const count of CONNECT_COUNTS) {
      console.log(`[perf] socket connect scenario: ${count} clients`);
      summary.connect.push(await runConnectScenario(count));
    }

    for (const driverCount of DRIVER_COUNTS) {
      console.log(`[perf] driver update scenario: ${driverCount} drivers`);
      summary.socket_updates.push(await runDriverUpdateScenario(driverCount));
    }

    for (const step of HTTP_STEPS) {
      console.log(
        `[perf] http scenario: concurrency=${step.concurrency} total=${step.total}`
      );
      summary.http.push(await runHttpScenario(step));
    }

    console.log(JSON.stringify(summary, null, 2));
  } finally {
    HTTP_AGENT.destroy();
    await stopServer(socketServer);
    await stopServer(laravelStub);
  }
};

run().catch((error) => {
  console.error("[perf] fatal:", error?.message || error);
  process.exit(1);
});
