/* eslint-disable no-console */
const { spawn } = require("child_process");
const net = require("net");
const path = require("path");

const SERVER_CWD = path.join(__dirname, "..");
const TEST_CWD = __dirname;

const SOCKET_HOST = process.env.SOCKET_BIND_HOST || "0.0.0.0";
const SOCKET_PORT = Number(process.env.SOCKET_BIND_PORT || 3000);

const withEnv = (extra = {}) => ({
  ...process.env,
  LARAVEL_BASE_URL:
    process.env.LARAVEL_BASE_URL ||
    process.env.LARAVEL_URL ||
    "https://osbackend.gocab.net",
  SOCKET_BIND_HOST: SOCKET_HOST,
  SOCKET_BIND_PORT: String(SOCKET_PORT),
  ...extra,
});

const waitForPort = (host, port, timeoutMs = 10000) =>
  new Promise((resolve, reject) => {
    const start = Date.now();
    const tryConnect = () => {
      const socket = new net.Socket();
      socket.setTimeout(1000);
      socket.once("error", () => {
        socket.destroy();
        if (Date.now() - start > timeoutMs) {
          reject(new Error(`Timeout waiting for ${host}:${port}`));
          return;
        }
        setTimeout(tryConnect, 300);
      });
      socket.once("timeout", () => {
        socket.destroy();
        if (Date.now() - start > timeoutMs) {
          reject(new Error(`Timeout waiting for ${host}:${port}`));
          return;
        }
        setTimeout(tryConnect, 300);
      });
      socket.connect(port, host, () => {
        socket.end();
        resolve();
      });
    };
    tryConnect();
  });

const run = async () => {
  console.log("[runner] starting socket server...");
  const server = spawn(process.execPath, ["server.js"], {
    cwd: SERVER_CWD,
    env: withEnv(),
    stdio: "inherit",
  });

  server.on("exit", (code) => {
    if (code !== null) {
      console.log(`[runner] socket server exited with code ${code}`);
    }
  });

  try {
    await waitForPort(SOCKET_HOST, SOCKET_PORT, 15000);
  } catch (err) {
    console.error("[runner] server failed to start:", err.message);
    server.kill();
    process.exit(1);
  }

  console.log("[runner] running full-flow test...");
  const test = spawn(process.execPath, [path.join(TEST_CWD, "full-flow-e2e.js")], {
    cwd: TEST_CWD,
    env: withEnv(),
    stdio: "inherit",
  });

  test.on("exit", (code) => {
    console.log(`[runner] test finished with code ${code}`);
    server.kill();
    process.exit(code ?? 0);
  });
};

run().catch((err) => {
  console.error("[runner] fatal:", err);
  process.exit(1);
});
