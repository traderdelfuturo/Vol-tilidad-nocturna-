/**
 * ISS - Live Aggregator de temporalidades (desde market_data/M1)
 * -------------------------------------------------------------
 * Mantiene actualizado en tiempo real:
 *   M5, M15, M30, H1, H2, H4, H8, H12, D1, W1, MN
 *
 * Cómo ejecutar (Railway):
 *   node market_tf_live.js
 *
 * Recomendación:
 * - Ejecuta PRIMERO el backfill (market_tf_backfill.js) una vez.
 * - Luego deja corriendo este LIVE 24/7.
 *
 * Variables opcionales:
 *   MARKET_ROOT="market_data" (default)
 *   FLUSH_MS=150  -> cada cuánto escribe al DB (debounce). 100-250 suele ir perfecto.
 */

"use strict";

// -----------------------------
// Firebase init (flexible)
// -----------------------------
let admin;
try {
  admin = require("./firebaseApp");
  if (admin && admin.admin) admin = admin.admin;
  if (admin && admin.default) admin = admin.default;
} catch (e) {
  admin = require("firebase-admin");
  if (!admin.apps.length) {
    const svc = process.env.FIREBASE_SERVICE_ACCOUNT_JSON
      ? JSON.parse(process.env.FIREBASE_SERVICE_ACCOUNT_JSON)
      : null;
    const dbUrl = process.env.FIREBASE_DB_URL;
    if (!svc || !dbUrl) {
      throw new Error(
        "Falta firebaseApp.js o variables FIREBASE_SERVICE_ACCOUNT_JSON y FIREBASE_DB_URL."
      );
    }
    admin.initializeApp({
      credential: admin.credential.cert(svc),
      databaseURL: dbUrl,
    });
  }
}

const db = admin.database();

// -----------------------------
// Config
// -----------------------------
const MARKET_ROOT = process.env.MARKET_ROOT || "market_data";
const SOURCE_PATH = `${MARKET_ROOT}/M1`;

const FLUSH_MS = Math.max(50, Math.min(2000, Number(process.env.FLUSH_MS || 150)));

const TFS = [
  { code: "M5",  type: "fixed", sec: 300 },
  { code: "M15", type: "fixed", sec: 900 },
  { code: "M30", type: "fixed", sec: 1800 },
  { code: "H1",  type: "fixed", sec: 3600 },
  { code: "H2",  type: "fixed", sec: 7200 },
  { code: "H4",  type: "fixed", sec: 14400 },
  { code: "H8",  type: "fixed", sec: 28800 },
  { code: "H12", type: "fixed", sec: 43200 },
  { code: "D1",  type: "day",   sec: 86400 },
  { code: "W1",  type: "week" },
  { code: "MN",  type: "month" },
];

// -----------------------------
// Utils
// -----------------------------
function toNum(v) {
  const n = typeof v === "number" ? v : Number(v);
  return Number.isFinite(n) ? n : null;
}

function startOfDayUTC(timeSec) {
  const d = new Date(timeSec * 1000);
  return Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate()) / 1000;
}

function startOfWeekUTC_Monday(timeSec) {
  const d = new Date(timeSec * 1000);
  const day = d.getUTCDay(); // 0..6 (Sun..Sat)
  const daysSinceMonday = (day + 6) % 7;
  const dayStart = Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate()) / 1000;
  return dayStart - (daysSinceMonday * 86400);
}

function startOfMonthUTC(timeSec) {
  const d = new Date(timeSec * 1000);
  return Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), 1) / 1000;
}

function bucketStart(timeSec, tf) {
  if (tf.type === "fixed") return Math.floor(timeSec / tf.sec) * tf.sec;
  if (tf.type === "day") return startOfDayUTC(timeSec);
  if (tf.type === "week") return startOfWeekUTC_Monday(timeSec);
  if (tf.type === "month") return startOfMonthUTC(timeSec);
  return Math.floor(timeSec / 60) * 60;
}

// -----------------------------
// Live aggregator (debounced)
// -----------------------------
const pending = new Map();          // "TF|bucket" -> candle
const aggCache = new Map();         // "TF|bucket" -> candle (mutable)
const currentBucketByTF = new Map(); // TF -> bucket
let flushTimer = null;
let lastEventAt = 0;

async function ensureAgg(tfCode, bucket, m1) {
  const k = `${tfCode}|${bucket}`;
  if (aggCache.has(k)) return aggCache.get(k);

  const snap = await db.ref(`${MARKET_ROOT}/${tfCode}/${bucket}`).once("value");
  let c;
  if (snap.exists()) {
    c = snap.val() || {};
  } else {
    c = { time: bucket, open: m1.open, high: m1.high, low: m1.low, close: m1.close, lastSrcTime: m1.time };
  }
  aggCache.set(k, c);
  return c;
}

function scheduleFlush() {
  if (flushTimer) return;
  flushTimer = setTimeout(async () => {
    flushTimer = null;
    try {
      await flushNow();
    } catch (err) {
      console.error("❌ flush error:", err);
    }
  }, FLUSH_MS);
}

async function flushNow() {
  if (pending.size === 0) return;

  const updates = {};
  for (const [key, candle] of pending.entries()) {
    const [tfCode, bucket] = key.split("|");
    updates[`${MARKET_ROOT}/${tfCode}/${bucket}`] = candle;
  }

  pending.clear();
  await db.ref().update(updates);

  const now = Date.now();
  if (now - lastEventAt > 1500) {
    console.log(`💾 flush OK (${Object.keys(updates).length} writes)`);
  }
}

async function processM1(m1) {
  for (const tf of TFS) {
    const bucket = bucketStart(m1.time, tf);

    const prevBucket = currentBucketByTF.get(tf.code);
    if (prevBucket !== bucket && prevBucket != null) {
      // Cambio de bucket => forzar flush para no perder el cierre del anterior
      await flushNow();
      // limpiar caches viejos
      aggCache.delete(`${tf.code}|${prevBucket}`);
      pending.delete(`${tf.code}|${prevBucket}`);
    }
    currentBucketByTF.set(tf.code, bucket);

    const agg = await ensureAgg(tf.code, bucket, m1);

    // Asegurar estructura
    if (toNum(agg.open) === null) agg.open = m1.open;
    agg.time = bucket;

    const high = toNum(agg.high);
    const low = toNum(agg.low);
    agg.high = Math.max(high ?? m1.high, m1.high);
    agg.low  = Math.min(low  ?? m1.low,  m1.low);

    const lastSrc = toNum(agg.lastSrcTime) ?? 0;
    if (m1.time >= lastSrc) {
      agg.close = m1.close;
      agg.lastSrcTime = m1.time;
    }

    pending.set(`${tf.code}|${bucket}`, { ...agg });
  }

  scheduleFlush();
}

function parseM1(v, key) {
  if (!v) return null;
  const time = toNum(v.time);
  const open = toNum(v.open);
  const high = toNum(v.high);
  const low  = toNum(v.low);
  const close= toNum(v.close);
  if (time === null || open === null || high === null || low === null || close === null) return null;
  return { time, open, high, low, close, key };
}

async function start() {
  console.log("===============================================");
  console.log("ISS Live TF Aggregator (desde M1)");
  console.log("ROOT:", MARKET_ROOT);
  console.log("SOURCE:", SOURCE_PATH);
  console.log("FLUSH_MS:", FLUSH_MS);
  console.log("TFS:", TFS.map(t => t.code).join(", "));
  console.log("===============================================");

  const q = db.ref(SOURCE_PATH).limitToLast(1);

  const handler = async (snap) => {
    lastEventAt = Date.now();
    const m1 = parseM1(snap.val(), snap.key);
    if (!m1) return;
    try {
      await processM1(m1);
    } catch (err) {
      console.error("❌ processM1 error:", err);
    }
  };

  q.on("child_added", handler);
  q.on("child_changed", handler);

  // Graceful shutdown
  const shutdown = async () => {
    try {
      console.log("🧯 shutdown... flushing pending");
      await flushNow();
    } catch {}
    process.exit(0);
  };
  process.on("SIGINT", shutdown);
  process.on("SIGTERM", shutdown);
}

start().catch(err=>{
  console.error("❌ Error al iniciar LIVE:", err);
  process.exit(1);
});
