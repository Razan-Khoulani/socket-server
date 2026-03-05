// db.js
// Default behavior is memory-only/no-DB mode.
// Set NODE_DB_ENABLED=1 if you intentionally want real MySQL access.

const NODE_DB_ENABLED = process.env.NODE_DB_ENABLED === "1";
let warnedOnce = false;

const isSelectSql = (sql) =>
  typeof sql === "string" && /^\s*select\b/i.test(sql);

const toNoDbResult = (sql) =>
  isSelectSql(sql) ? [] : { affectedRows: 0, skipped: true };

const normalizeQueryArgs = (sql, params, cb) => {
  if (typeof params === "function") {
    return { sql, params: [], cb: params };
  }
  return { sql, params: params ?? [], cb };
};

const runNoDbQuery = (sql, params, cb) => {
  const normalized = normalizeQueryArgs(sql, params, cb);
  const result = toNoDbResult(normalized.sql);

  if (!warnedOnce) {
    warnedOnce = true;
    console.log("[db] NODE_DB_ENABLED!=1 -> using no-op adapter (no MySQL calls)");
  }

  if (typeof normalized.cb === "function") {
    process.nextTick(() => normalized.cb(null, result));
  }
  return { skipped: true, sql: normalized.sql, params: normalized.params };
};

if (!NODE_DB_ENABLED) {
  module.exports = {
    query: runNoDbQuery,
    execute: runNoDbQuery,
    getConnection: (cb) => {
      const conn = {
        query: runNoDbQuery,
        execute: runNoDbQuery,
        release: () => {},
      };
      if (typeof cb === "function") {
        process.nextTick(() => cb(null, conn));
      }
    },
    promise: () => ({
      query: async (sql) => [toNoDbResult(sql), []],
      execute: async (sql) => [toNoDbResult(sql), []],
    }),
  };
} else {
  const mysql = require("mysql2");
  module.exports = mysql.createPool({
    host: process.env.DB_HOST || "127.0.0.1",
    port: Number(process.env.DB_PORT || 3306),
    user: process.env.DB_USER || process.env.DB_USERNAME || "root",
    password: process.env.DB_PASSWORD || "",
    database: process.env.DB_NAME || process.env.DB_DATABASE || "db_catch_taxi",
    waitForConnections: true,
    connectionLimit: Number(process.env.DB_POOL_SIZE || 10),
    queueLimit: 0,
  });
}
