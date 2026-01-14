const express = require("express");
const multer = require("multer");
const { Pool } = require("pg");

const app = express();

const PORT = process.env.PORT || 3000;
const UPLOAD_TOKEN = process.env.UPLOAD_TOKEN || "";
const DATABASE_URL = process.env.DATABASE_URL || "";

const pool = DATABASE_URL
  ? new Pool({
      connectionString: DATABASE_URL,
      ssl:
        process.env.PGSSLMODE === "require" || process.env.PGSSL === "true"
          ? { rejectUnauthorized: false }
          : undefined,
    })
  : null;

let dbInitPromise = null;

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function isTransientDbError(err) {
  const msg = (err && err.message ? String(err.message) : "").toLowerCase();
  return (
    err?.code === "57P03" ||
    msg.includes("database system is starting up") ||
    msg.includes("the database system is starting up")
  );
}

async function withRetries(fn, { attempts = 6, delayMs = 1000 } = {}) {
  let lastErr = null;
  for (let i = 0; i < attempts; i += 1) {
    try {
      return await fn();
    } catch (err) {
      lastErr = err;
      if (!isTransientDbError(err) || i === attempts - 1) {
        throw err;
      }
      await sleep(delayMs);
    }
  }
  throw lastErr;
}

function ensureDb() {
  if (!pool) {
    throw new Error("DATABASE_URL is not set");
  }
  if (!dbInitPromise) {
    dbInitPromise = withRetries(
      () =>
        pool.query(`
      create table if not exists uploads (
        id bigserial primary key,
        original_name text not null,
        mimetype text not null,
        size_bytes bigint not null,
        uploaded_at timestamptz not null default now(),
        delimiter text not null default ',',
        has_header boolean not null default true,
        content text not null
      );
    `),
      { attempts: 10, delayMs: 1000 }
    );
  }
  return dbInitPromise;
}

const upload = multer({ storage: multer.memoryStorage() });

function isAuthorized(req) {
  if (!UPLOAD_TOKEN) return true;
  const auth = req.header("authorization") || "";
  const prefix = "Bearer ";
  if (!auth.startsWith(prefix)) return false;
  const token = auth.slice(prefix.length).trim();
  return token === UPLOAD_TOKEN;
}

function guessDelimiterFromContent(content) {
  const firstLine = (content || "").split(/\r?\n/)[0] || "";
  const commaCount = (firstLine.match(/,/g) || []).length;
  const semicolonCount = (firstLine.match(/;/g) || []).length;
  if (semicolonCount > commaCount) return ";";
  return ",";
}

app.get("/health", (req, res) => {
  const dbConfigured = Boolean(DATABASE_URL);
  res.status(200).json({ ok: true, dbConfigured });
});

app.get("/health/db", async (req, res) => {
  try {
    await ensureDb();
    await withRetries(() => pool.query("select 1 as ok"), { attempts: 3, delayMs: 1000 });
    res.status(200).json({ ok: true, dbConfigured: true, dbOk: true });
  } catch (err) {
    res.status(200).json({
      ok: true,
      dbConfigured: Boolean(DATABASE_URL),
      dbOk: false,
      error: err?.message || "DB check failed",
    });
  }
});

app.get("/uploads/count", async (req, res) => {
  try {
    await ensureDb();
    const result = await withRetries(() => pool.query("select count(*)::bigint as count from uploads"), {
      attempts: 3,
      delayMs: 1000,
    });
    res.status(200).json({ ok: true, count: Number(result.rows[0]?.count ?? 0) });
  } catch (err) {
    res.status(500).json({ ok: false, error: err?.message || "Failed to count uploads" });
  }
});

app.get("/uploads/latest/meta", async (req, res) => {
  try {
    await ensureDb();
    const result = await withRetries(
      () =>
        pool.query(
          "select id, original_name, mimetype, size_bytes, uploaded_at, delimiter, has_header from uploads order by uploaded_at desc, id desc limit 1"
        ),
      { attempts: 3, delayMs: 1000 }
    );
    if (result.rowCount === 0) {
      return res.status(404).json({ ok: false, error: "No file uploaded yet" });
    }
    res.status(200).json({ ok: true, latest: result.rows[0] });
  } catch (err) {
    if (isTransientDbError(err)) {
      res.status(503).json({ ok: false, error: err?.message || "Database is starting up" });
    } else {
      res.status(500).json({ ok: false, error: err?.message || "Failed to read latest meta" });
    }
  }
});

app.get("/", (req, res) => {
  res.status(200).json({
    name: "hmivar-webservice",
    status: "ok",
    endpoints: {
      health: "GET /health",
      healthDb: "GET /health/db",
      upload: "POST /upload (multipart/form-data, field: file)",
      latest: "GET /latest (or /files/latest)",
      uploadsCount: "GET /uploads/count",
      uploadsLatestMeta: "GET /uploads/latest/meta"
    }
  });
});

app.post("/upload", upload.single("file"), async (req, res) => {
  if (!isAuthorized(req)) {
    return res.status(401).json({ error: "Unauthorized" });
  }

  if (!req.file) {
    return res.status(400).json({ error: "Missing file field 'file'" });
  }

  try {
    await ensureDb();

    const content = req.file.buffer.toString("utf8");
    const delimiter = guessDelimiterFromContent(content);
    const hasHeader = (req.body?.hasHeader ?? "true").toString().toLowerCase() !== "false";

    const result = await withRetries(
      () =>
        pool.query(
          "insert into uploads (original_name, mimetype, size_bytes, delimiter, has_header, content) values ($1, $2, $3, $4, $5, $6) returning id, uploaded_at",
          [req.file.originalname, req.file.mimetype || "text/plain", req.file.size, delimiter, hasHeader, content]
        ),
      { attempts: 3, delayMs: 1000 }
    );

    res.status(200).json({
      ok: true,
      latest: {
        id: result.rows[0].id,
        originalName: req.file.originalname,
        mimetype: req.file.mimetype || "text/plain",
        size: req.file.size,
        uploadedAt: result.rows[0].uploaded_at,
        delimiter,
        hasHeader,
      },
    });
  } catch (err) {
    if (isTransientDbError(err)) {
      res.status(503).json({ error: err?.message || "Database is starting up" });
    } else {
      res.status(500).json({ error: err?.message || "Upload failed" });
    }
  }
});

app.get(["/latest", "/files/latest"], async (req, res) => {
  try {
    await ensureDb();
    const result = await withRetries(
      () =>
        pool.query(
          "select id, original_name, mimetype, size_bytes, uploaded_at, content from uploads order by uploaded_at desc, id desc limit 1"
        ),
      { attempts: 3, delayMs: 1000 }
    );
    if (result.rowCount === 0) {
      return res.status(404).send("No file uploaded yet");
    }

    const row = result.rows[0];
    const originalName = row.original_name || "data.csv";
    if ((originalName || "").toLowerCase().endsWith(".csv")) {
      res.setHeader("Content-Type", "text/csv; charset=utf-8");
    } else {
      res.setHeader("Content-Type", "text/plain; charset=utf-8");
    }
    res.status(200).send(row.content);
  } catch (err) {
    if (isTransientDbError(err)) {
      res.status(503).send(err?.message || "Database is starting up");
    } else {
      res.status(500).send(err?.message || "Failed to fetch latest");
    }
  }
});

app.listen(PORT, () => {
  console.log(`hmivar-webservice listening on port ${PORT}`);
});
