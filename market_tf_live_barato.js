"use strict";

const admin = require("./firebaseApp");
const db = admin.database();

const MARKET_ROOT = "market_data";
const SOURCE_PATH = `${MARKET_ROOT}/M1`;

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
  const day = d.getUTCDay();
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

// Estado: bucket actual por TF
const curBucket = new Map(); // tf.code -> bucket number
let running = false;
let m1Query = null;
let bootstrapped = false;

// 1) Bootstrap: detectar último bucket existente por TF (1 sola vez al prender)
async function bootstrapBuckets() {
  // Esto cuesta casi nada: 11 lecturas de "limitToLast(1)"
  for (const tf of TFS) {
    const snap = await db.ref(`${MARKET_ROOT}/${tf.code}`).orderByKey().limitToLast(1).once("value");
    if (snap.exists()) {
      let lastKey = null;
      snap.forEach(ch => lastKey = ch.key);
      if (lastKey !== null) curBucket.set(tf.code, Number(lastKey));
    }
  }
  bootstrapped = true;
  console.log("✅ Buckets bootstrap OK");
}

// 2) Crear vela del bucket solo si NO existe ya (barato)
async function createBucketIfMissing(tfCode, b, candle) {
  const ref = db.ref(`${MARKET_ROOT}/${tfCode}/${String(b)}`);

  // Una lectura mínima SOLO cuando cambia bucket (pocas veces)
  const snap = await ref.once("value");
  if (snap.exists()) return false;

  await ref.set(candle);
  return true;
}

async function ensureNewBuckets(m1) {
  // Si no ha bootstrap, no hacemos nada raro todavía
  if (!bootstrapped) return;

  let created = 0;

  for (const tf of TFS) {
    const b = bucketStart(m1.time, tf);
    const prev = curBucket.get(tf.code);

    if (prev === b) continue; // no cambió vela

    curBucket.set(tf.code, b);

    const candle = {
      time: b,
      open: m1.close,
      high: m1.close,
      low:  m1.close,
      close:m1.close,
      lastSrcTime: m1.time,
    };

    try {
      const ok = await createBucketIfMissing(tf.code, b, candle);
      if (ok) created++;
    } catch (e) {
      console.error(`❌ createBucketIfMissing ${tf.code} error:`, e);
    }
  }

  if (created > 0) {
    console.log(`🕒 Nuevas velas creadas: ${created}`);
  }
}

async function onM1Snap(snap) {
  const v = snap.val() || {};
  const time = toNum(v.time);
  const close = toNum(v.close);
  if (time === null || close === null) return;

  try {
    await ensureNewBuckets({ time, close });
  } catch (e) {
    console.error("❌ ensureNewBuckets error:", e);
  }
}

function start() {
  if (running) return;
  running = true;
  bootstrapped = false;

  console.log("🟢 TF LIVE BARATO ON");

  // bootstrap primero, luego escuchar M1
  bootstrapBuckets()
    .catch(e => console.error("❌ bootstrapBuckets error:", e))
    .finally(() => {
      // Escuchar última M1 (cuando cambia o se actualiza)
      m1Query = db.ref(SOURCE_PATH).orderByKey().limitToLast(1);
      m1Query.on("child_added", onM1Snap);
      m1Query.on("child_changed", onM1Snap);
    });
}

function stop() {
  if (!running) return;
  running = false;

  console.log("🛑 TF LIVE BARATO OFF");

  if (m1Query) {
    m1Query.off("child_added", onM1Snap);
    m1Query.off("child_changed", onM1Snap);
    m1Query = null;
  }
}

async function ciclo() {
  try {
    // 1) Flag live
    const flagSnap = await db.ref("config/auto_tf_live_barato").once("value");
    const enabled = !!flagSnap.val();

    // 2) Si backfill está prendido, live se apaga solo (armonía garantizada)
    const bfSnap = await db.ref("config/auto_market_tf_backfill").once("value");
    const backfillRunning = !!bfSnap.val();

    if (backfillRunning) {
      if (running) stop();
      return setTimeout(ciclo, 1500);
    }

    if (enabled) start();
    else stop();
  } catch (e) {
    console.error("❌ ciclo error:", e);
  }

  setTimeout(ciclo, 1500);
}

ciclo();
