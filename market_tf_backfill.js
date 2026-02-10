/**
 * ISS - Backfill de temporalidades desde market_data/M1
 * ----------------------------------------------------
 * 1) Lee TODO el histórico de M1 (por key) en batches.
 * 2) Construye y guarda velas agregadas en:
 *    M5, M15, M30, H1, H2, H4, H8, H12, D1, W1, MN
 *
 * IMPORTANTE:
 * - Este script se corre UNA SOLA VEZ (o cuando quieras regenerar).
 * - Luego debes correr el script "LIVE" para mantener todo actualizado en tiempo real.
 *
 * Cómo ejecutar (Railway / local):
 *   node market_tf_backfill.js
 *
 * Variables opcionales:
 *   MARKET_ROOT="market_data" (default)
 *   BATCH_SIZE=5000 (default)
 *   CLEAR_EXISTING=1  -> borra primero las carpetas destino (ojo!)
 */

"use strict";

// -----------------------------
// Firebase init (flexible)
// -----------------------------
let admin;
try {
  // Si tu repo ya tiene firebaseApp.js (recomendado) úsalo.
  // Debe exportar el admin inicializado.
  admin = require("./firebaseApp");
  // Algunos exports pueden venir como { admin }, { default }, etc.
  if (admin && admin.admin) admin = admin.admin;
  if (admin && admin.default) admin = admin.default;
} catch (e) {
  // Fallback self-contained (si NO tienes firebaseApp.js)
  // Requiere variables de entorno:
  //   FIREBASE_SERVICE_ACCOUNT_JSON (string JSON)
  //   FIREBASE_DB_URL
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
const BATCH_SIZE = Math.max(500, Math.min(20000, Number(process.env.BATCH_SIZE || 5000)));
const CLEAR_EXISTING = process.env.CLEAR_EXISTING === "1";

// Temporalidades destino
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
  // fallback
  return Math.floor(timeSec / 60) * 60;
}

async function clearDestinations() {
  console.log("🧹 CLEAR_EXISTING=1 => borrando carpetas destino...");
  for (const tf of TFS) {
    const ref = db.ref(`${MARKET_ROOT}/${tf.code}`);
    await ref.remove();
    console.log(`  - borrado ${MARKET_ROOT}/${tf.code}`);
  }
}

async function main() {
  console.log("===============================================");
  console.log("ISS Backfill Temporalidades (desde M1)");
  console.log("ROOT:", MARKET_ROOT);
  console.log("SOURCE:", SOURCE_PATH);
  console.log("BATCH_SIZE:", BATCH_SIZE);
  console.log("TFS:", TFS.map(t => t.code).join(", "));
  console.log("===============================================");

  if (CLEAR_EXISTING) {
    await clearDestinations();
  }

  const m1Ref = db.ref(SOURCE_PATH);

  // Estado por TF (bucket actual y vela acumulada)
  const acc = new Map(); // tf.code -> { bucket, candle }

  // Batch updates (multi-location)
  let updates = {};
  let updatesCount = 0;

  async function flushUpdates() {
    if (updatesCount === 0) return;
    await db.ref().update(updates);
    updates = {};
    updatesCount = 0;
  }

  function queueCandle(tfCode, bucket, candle) {
    updates[`${MARKET_ROOT}/${tfCode}/${bucket}`] = candle;
    updatesCount++;
  }

  function flushTF(tfCode) {
    const cur = acc.get(tfCode);
    if (!cur) return;

    const { bucket, candle } = cur;
    queueCandle(tfCode, String(bucket), candle);
    acc.delete(tfCode);
  }

  function updateAcc(tf, m1) {
    const bucket = bucketStart(m1.time, tf);
    const code = tf.code;

    const cur = acc.get(code);
    if (!cur || cur.bucket !== bucket) {
      // cambia bucket => flush anterior
      if (cur) {
        queueCandle(code, String(cur.bucket), cur.candle);
      }

      acc.set(code, {
        bucket,
        candle: {
          time: bucket,
          open: m1.open,
          high: m1.high,
          low: m1.low,
          close: m1.close,
          lastSrcTime: m1.time,
        },
      });
      return;
    }

    // mismo bucket => acumula
    const c = cur.candle;
    c.high = Math.max(toNum(c.high) ?? m1.high, m1.high);
    c.low  = Math.min(toNum(c.low)  ?? m1.low,  m1.low);

    const lastSrc = toNum(c.lastSrcTime) ?? 0;
    if (m1.time >= lastSrc) {
      c.close = m1.close;
      c.lastSrcTime = m1.time;
    }
  }

  let startKey = null;
  let totalM1 = 0;
  let batches = 0;

  while (true) {
    let q = m1Ref.orderByKey();
    if (startKey !== null) q = q.startAt(startKey);
    q = q.limitToFirst(BATCH_SIZE);

    const snap = await q.once("value");
    const rows = [];
    snap.forEach(child => {
      rows.push({ key: child.key, val: child.val() });
    });

    if (rows.length === 0) break;

    // Evitar duplicar el primer registro si usamos startAt (incluye startKey)
    if (startKey !== null) rows.shift();

    if (rows.length === 0) {
      // nada nuevo en este batch
      break;
    }

    // Orden por time (por si acaso)
    const candles = rows
      .map(r => {
        const v = r.val || {};
        const time = toNum(v.time);
        const open = toNum(v.open);
        const high = toNum(v.high);
        const low  = toNum(v.low);
        const close= toNum(v.close);
        if (time === null || open === null || high === null || low === null || close === null) return null;
        return { time, open, high, low, close };
      })
      .filter(Boolean)
      .sort((a,b)=>a.time-b.time);

    for (const m1 of candles) {
      for (const tf of TFS) {
        updateAcc(tf, m1);
      }

      totalM1++;
      // Flush updates cada cierto tamaño para no explotar RAM ni el update payload
      if (updatesCount >= 800) {
        await flushUpdates();
      }
    }

    // Preparar próximo startKey
    startKey = rows[rows.length - 1].key;
    batches++;

    console.log(`✅ batch #${batches} | procesadas M1: ${totalM1} | lastKey: ${startKey}`);

    if (rows.length < BATCH_SIZE - 1) {
      // Llegamos al final
      break;
    }
  }

  // Flush buckets pendientes
  for (const tf of TFS) {
    flushTF(tf.code);
  }
  await flushUpdates();

  console.log("===============================================");
  console.log("🎉 Backfill completado.");
  console.log("M1 procesadas:", totalM1);
  console.log("Batches:", batches);
  console.log("===============================================");
  process.exit(0);
}

main().catch(err=>{
  console.error("❌ Error en backfill:", err);
  process.exit(1);
});
