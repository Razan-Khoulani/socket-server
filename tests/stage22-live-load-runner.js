/* eslint-disable no-console */
const fs = require("fs");
const { spawn } = require("child_process");

function parseEnvFile(filePath) {
  const text = fs.readFileSync(filePath, "utf8");
  const env = {};

  for (const line of text.split(/\r?\n/)) {
    const trimmed = line.trim();
    if (!trimmed || trimmed.startsWith("#")) continue;

    const eqIndex = trimmed.indexOf("=");
    if (eqIndex < 0) continue;

    const key = trimmed.slice(0, eqIndex).trim();
    let value = trimmed.slice(eqIndex + 1).trim();

    if (
      (value.startsWith('"') && value.endsWith('"')) ||
      (value.startsWith("'") && value.endsWith("'"))
    ) {
      value = value.slice(1, -1);
    }

    env[key] = value;
  }

  return env;
}

const STAGE_NAME = process.env.STAGE_NAME || "stage22-direct-accept";
const USER_IDS =
  process.env.USER_IDS ||
  "1,2,3,4,5,6,7,9,10,14,15,16,17,18,19,20,21,22,23,24,25,26";
const DRIVER_IDS =
  process.env.DRIVER_IDS ||
  "1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22";
const SCENARIO_COUNT = Number.isFinite(Number(process.env.SCENARIO_COUNT))
  ? Math.max(1, Number(process.env.SCENARIO_COUNT))
  : 22;
const SERVICE_CATEGORY_ID = Number.isFinite(Number(process.env.SERVICE_CATEGORY_ID))
  ? Number(process.env.SERVICE_CATEGORY_ID)
  : 2;
const REQUEST_SERVICE_TYPE_ID = Number.isFinite(Number(process.env.REQUEST_SERVICE_TYPE_ID))
  ? Number(process.env.REQUEST_SERVICE_TYPE_ID)
  : 1;
const TEST_WINDOW_MS = Number.isFinite(Number(process.env.TEST_WINDOW_MS))
  ? Math.max(5000, Number(process.env.TEST_WINDOW_MS))
  : 35000;
const SCENARIO_STAGGER_MS = Number.isFinite(Number(process.env.SCENARIO_STAGGER_MS))
  ? Math.max(0, Number(process.env.SCENARIO_STAGGER_MS))
  : 0;
const CONNECT_BATCH_SIZE = Number.isFinite(Number(process.env.CONNECT_BATCH_SIZE))
  ? Math.max(1, Number(process.env.CONNECT_BATCH_SIZE))
  : 22;
const SCENARIO_CONCURRENCY = Number.isFinite(Number(process.env.SCENARIO_CONCURRENCY))
  ? Math.max(1, Number(process.env.SCENARIO_CONCURRENCY))
  : 22;
const SOCKET_HTTP_TIMEOUT_MS = Number.isFinite(
  Number(process.env.SOCKET_HTTP_TIMEOUT_MS)
)
  ? Math.max(1000, Number(process.env.SOCKET_HTTP_TIMEOUT_MS))
  : 10000;
const LARAVEL_HTTP_TIMEOUT_MS = Number.isFinite(
  Number(process.env.LARAVEL_HTTP_TIMEOUT_MS)
)
  ? Math.max(1000, Number(process.env.LARAVEL_HTTP_TIMEOUT_MS))
  : 10000;
const ENABLE_USER_COUNTER =
  String(process.env.ENABLE_USER_COUNTER || "0") === "1" ? "1" : "0";
const ENABLE_DRIVER_ACCEPT_COUNTER =
  String(process.env.ENABLE_DRIVER_ACCEPT_COUNTER || "0") === "1" ? "1" : "0";
const ENABLE_USER_ACCEPT =
  String(process.env.ENABLE_USER_ACCEPT || "1") === "1" ? "1" : "0";
const AUTO_USER_ACCEPT_FIRST_BID =
  String(process.env.AUTO_USER_ACCEPT_FIRST_BID || "1") === "1" ? "1" : "0";

const socketEnv = parseEnvFile("/var/www/socket-server-prod/.env");
const dbEnv = parseEnvFile("/var/www/osbackend/.env");
const port = Number(socketEnv.PORT || socketEnv.SOCKET_BIND_PORT || 4000);
const logPath = `/home/ubuntu/${STAGE_NAME}-live-load.log`;
const logStream = fs.createWriteStream(logPath, { flags: "w" });

const childEnv = {
  ...process.env,
  NODE_TLS_REJECT_UNAUTHORIZED: "0",
  DB_HOST: dbEnv.DB_HOST || "127.0.0.1",
  DB_PORT: dbEnv.DB_PORT || "3306",
  DB_USER: dbEnv.DB_USERNAME,
  DB_PASSWORD: dbEnv.DB_PASSWORD,
  DB_NAME: dbEnv.DB_DATABASE,
  SOCKET_URL: `http://127.0.0.1:${port}`,
  SOCKET_HTTP_URL: `http://127.0.0.1:${port}`,
  LARAVEL_URL:
    socketEnv.LARAVEL_URL ||
    socketEnv.LARAVEL_BASE_URL ||
    "https://osbackend.gocab.net",
  LIVE_LOAD_CONFIRM: "I_UNDERSTAND_THIS_CREATES_REAL_RIDES",
  USER_IDS,
  DRIVER_IDS,
  SCENARIO_COUNT: String(SCENARIO_COUNT),
  USER_COUNT: String(SCENARIO_COUNT),
  DRIVER_COUNT: String(SCENARIO_COUNT),
  SERVICE_CATEGORY_ID: String(SERVICE_CATEGORY_ID),
  REQUEST_SERVICE_TYPE_ID: String(REQUEST_SERVICE_TYPE_ID),
  MATCH_SCENARIO_DRIVER_SERVICE_TYPE: "1",
  TARGET_ASSIGNED_DRIVER_ONLY:
    String(process.env.TARGET_ASSIGNED_DRIVER_ONLY || "1") === "1" ? "1" : "0",
  AUTO_CLEANUP: "1",
  RETRY_ACTIVE_RIDE_CANCEL: "1",
  TEST_WINDOW_MS: String(TEST_WINDOW_MS),
  SCENARIO_STAGGER_MS: String(SCENARIO_STAGGER_MS),
  CONNECT_BATCH_SIZE: String(CONNECT_BATCH_SIZE),
  SCENARIO_CONCURRENCY: String(SCENARIO_CONCURRENCY),
  SOCKET_HTTP_TIMEOUT_MS: String(SOCKET_HTTP_TIMEOUT_MS),
  LARAVEL_HTTP_TIMEOUT_MS: String(LARAVEL_HTTP_TIMEOUT_MS),
  DRIVER_READY_WAIT_MS: String(process.env.DRIVER_READY_WAIT_MS || "1000"),
  USER_READY_WAIT_MS: String(process.env.USER_READY_WAIT_MS || "300"),
  DRIVER_BID_DELAY_MS: String(process.env.DRIVER_BID_DELAY_MS || "80"),
  USER_COUNTER_DELAY_MS: String(process.env.USER_COUNTER_DELAY_MS || "0"),
  DRIVER_ACCEPT_DELAY_MS: String(process.env.DRIVER_ACCEPT_DELAY_MS || "0"),
  USER_ACCEPT_DELAY_MS: String(process.env.USER_ACCEPT_DELAY_MS || "0"),
  ENABLE_USER_COUNTER,
  ENABLE_DRIVER_ACCEPT_COUNTER,
  ENABLE_USER_ACCEPT,
  AUTO_USER_ACCEPT_FIRST_BID,
};

const header = {
  stage: STAGE_NAME,
  log_path: logPath,
  scenario_count: SCENARIO_COUNT,
  service_category_id: SERVICE_CATEGORY_ID,
  fallback_service_type_id: REQUEST_SERVICE_TYPE_ID,
  user_ids: USER_IDS,
  driver_ids: DRIVER_IDS,
  socket_url: childEnv.SOCKET_URL,
  socket_http_url: childEnv.SOCKET_HTTP_URL,
  laravel_url: childEnv.LARAVEL_URL,
  mode:
    AUTO_USER_ACCEPT_FIRST_BID === "1"
      ? "direct-first-bid-accept"
      : ENABLE_USER_COUNTER === "1"
      ? "counter-flow"
      : ENABLE_USER_ACCEPT === "1"
      ? "manual-accept-only"
      : "no-user-response",
  target_assigned_driver_only: childEnv.TARGET_ASSIGNED_DRIVER_ONLY,
  node_tls_reject_unauthorized: childEnv.NODE_TLS_REJECT_UNAUTHORIZED,
  started_at: new Date().toISOString(),
};

const headerText = `${JSON.stringify(header, null, 2)}\n`;
process.stdout.write(headerText);
logStream.write(headerText);

const child = spawn(process.execPath, ["tests/run-live-bidding-load.js"], {
  cwd: "/var/www/socket-server-prod",
  env: childEnv,
  stdio: ["ignore", "pipe", "pipe"],
});

child.stdout.on("data", (chunk) => {
  process.stdout.write(chunk);
  logStream.write(chunk);
});

child.stderr.on("data", (chunk) => {
  process.stderr.write(chunk);
  logStream.write(chunk);
});

child.on("close", (code) => {
  const footer = `\n[runner] stage=${STAGE_NAME} exit_code=${code} log_path=${logPath}\n`;
  process.stdout.write(footer);
  logStream.write(footer);
  logStream.end(() => process.exit(code || 0));
});
