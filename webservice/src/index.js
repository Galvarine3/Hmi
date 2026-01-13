const express = require("express");
const multer = require("multer");
const fs = require("fs");
const path = require("path");

const app = express();

const PORT = process.env.PORT || 3000;
const DATA_DIR = process.env.DATA_DIR || path.join(__dirname, "..", "data");
const UPLOAD_TOKEN = process.env.UPLOAD_TOKEN || "";

function ensureDir(dir) {
  if (!fs.existsSync(dir)) {
    fs.mkdirSync(dir, { recursive: true });
  }
}

ensureDir(DATA_DIR);

const storage = multer.diskStorage({
  destination: function (req, file, cb) {
    cb(null, DATA_DIR);
  },
  filename: function (req, file, cb) {
    const safeOriginal = path.basename(file.originalname).replace(/\s+/g, "_");
    const ts = new Date().toISOString().replace(/[:.]/g, "-");
    cb(null, `${ts}__${safeOriginal}`);
  },
});

const upload = multer({ storage });

function isAuthorized(req) {
  if (!UPLOAD_TOKEN) return true;
  const auth = req.header("authorization") || "";
  const prefix = "Bearer ";
  if (!auth.startsWith(prefix)) return false;
  const token = auth.slice(prefix.length).trim();
  return token === UPLOAD_TOKEN;
}

function metaPath() {
  return path.join(DATA_DIR, "latest.json");
}

function writeLatestMeta(meta) {
  fs.writeFileSync(metaPath(), JSON.stringify(meta, null, 2), "utf8");
}

function readLatestMeta() {
  try {
    const raw = fs.readFileSync(metaPath(), "utf8");
    return JSON.parse(raw);
  } catch (_) {
    return null;
  }
}

app.get("/health", (req, res) => {
  res.status(200).send("ok");
});

app.post("/upload", upload.single("file"), (req, res) => {
  if (!isAuthorized(req)) {
    return res.status(401).json({ error: "Unauthorized" });
  }

  if (!req.file) {
    return res.status(400).json({ error: "Missing file field 'file'" });
  }

  const meta = {
    filename: req.file.filename,
    originalName: req.file.originalname,
    mimetype: req.file.mimetype,
    size: req.file.size,
    uploadedAt: new Date().toISOString(),
  };

  writeLatestMeta(meta);
  res.status(200).json({ ok: true, latest: meta });
});

app.get(["/latest", "/files/latest"], (req, res) => {
  const meta = readLatestMeta();
  if (!meta || !meta.filename) {
    return res.status(404).send("No file uploaded yet");
  }

  const fullPath = path.join(DATA_DIR, meta.filename);
  if (!fs.existsSync(fullPath)) {
    return res.status(404).send("Latest file not found on disk");
  }

  const ext = path.extname(meta.originalName || meta.filename).toLowerCase();
  if (ext === ".csv") {
    res.setHeader("Content-Type", "text/csv; charset=utf-8");
  } else {
    res.setHeader("Content-Type", "text/plain; charset=utf-8");
  }

  fs.createReadStream(fullPath).pipe(res);
});

app.listen(PORT, () => {
  console.log(`hmivar-webservice listening on port ${PORT}`);
});
