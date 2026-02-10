"use strict";

const admin = require("./firebaseApp");
const db = admin.database();

// ==============================
// CONFIG BASE
// ==============================
const MARKET_ROOT = "market_data";
const SOURCE_PATH = `${MARKET_ROOT}/M1`;

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

// ==============================
// UTILS
// ==============================
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

// ==============================
// ESTADO LIVE (solo vela actual)
// ==============================
const state = new Map(); // tf.code -> { bucket:number, candle:{...}, lastWriteMs:number, dirty:boolean }

let running = false;
let m1Query = null;

// Throttle dinámico desde config
let THROTTLE_MS = 80;

// ==============================
// WRITE (barato): un solo update multi-path
// ==============================
async function flushDirty(force = false) {
  const now = Date.now();
  const updates = {};
  let count = 0;

  for (const tf of TFS) {
    const s = state.get(tf.code);
    if (!s) continue;

    const due = force || (now - (s.lastWriteMs || 0) >= THROTTLE_MS);
    if (!s.dirty || !due) continue;

    updates[`${MARKET_ROOT}/${tf.code}/${String(s.bucket)}`] = s.candle;
    s.lastWriteMs = now;
    s.dirty = false;
    count++;
  }

  if (count > 0) {
    await db.ref().update(updates);
  }
}

// ==============================
// ACTUALIZA VELA TF con tick M1
// ==============================
function applyTickToTF(tf, m1) {
  const b = bucketStart(m1.time, tf);
  const code = tf.code;

  const s = state.get(code);

  // Si cambia el bucket, “cerramos” la anterior (último flush forzado) y abrimos nueva
  if (!s || s.bucket !== b) {
    if (s) {
      // marcar cierre (opcional)
      // s.candle.isClosed = true;
      s.dirty = true;
    }

    state.set(code, {
      bucket: b,
      candle: {
        time: b,
        open: m1.open,
        high: m1.high,
        low: m1.low,
        close: m1.close,
        lastSrcTime: m1.time,
      },
      lastWriteMs: 0,
      dirty: true,
    });

    return;
  }

  // Mismo bucket: actualizar OHLC “en tiempo real”
  const c = s.candle;

  c.high = Math.max(toNum(c.high) ?? m1.high, m1.high);
  c.low  = Math.min(toNum(c.low)  ?? m1.low,  m1.low);

  // close: siempre el último tick (tu M1 se actualiza muchas veces)
  const lastSrc = toNum(c.lastSrcTime) ?? 0;
  if (m1.time >= lastSrc) {
    c.close = m1.close;
    c.lastSrcTime = m1.time;
  }

  s.dirty = true;
}

// ==============================
// HANDLER M1 (última vela viva)
// ==============================
async function onM1Snap(childSnap) {
  const v = childSnap.val() || {};
  const time = toNum(v.time);
  const open = toNum(v.open);
  const high = toNum(v.high);
  const low  = toNum(v.low);
  const close= toNum(v.close);

  if (time === null || open === null || high === null || low === null || close === null) return;

  const m1 = { time, open, high, low, close };

  // Actualizar estado para todas las TFs
  for (const tf of TFS) applyTickToTF(tf, m1);

  // Escribir barato: solo velas actuales, con throttle
  try {
    await flushDirty(false);
  } catch (e) {
    console.error("❌ flushDirty error:", e);
  }
}

// ==============================
// START/STOP listeners
// ==============================
function startLive() {
  if (running) return;
  running = true;

  console.log("🟢 TF LIVE ON (escuchando última M1 en tiempo real)");

  // Escucha SOLO la última vela de M1:
  // - child_added: primera vez
  // - child_changed: cuando tú actualizas close/high/low en esa misma vela
  m1Query = db.ref(SOURCE_PATH).orderByKey().limitToLast(1);

  // Importante: usar las mismas referencias de función para poder off()
  m1Query.on("child_added", onM1Snap, err => console.error("child_added err:", err));
  m1Query.on("child_changed", onM1Snap, err => console.error("child_changed err:", err));
}

async function stopLive() {
  if (!running) return;
  running = false;

  console.log("🛑 TF LIVE OFF (apagado por flag)");

  try {
    // flush final por si quedó algo sucio
    await flushDirty(true);
  } catch (e) {}

  if (m1Query) {
    m1Query.off("child_added", onM1Snap);
    m1Query.off("child_changed", onM1Snap);
    m1Query = null;
  }
}

// ==============================
// CICLO CONTROLADO POR CONFIG (tu mismo estilo)
// ==============================
async function ciclo() {
  try {
    const [flagSnap, thrSnap] = await Promise.all([
      db.ref("config/auto_tf_live").once("value"),
      db.ref("config/tf_live_throttle_ms").once("value"),
    ]);

    const enabled = !!flagSnap.val();
    const thr = toNum(thrSnap.val());
    if (thr !== null) THROTTLE_MS = Math.max(0, Math.min(5000, thr));

    if (enabled) startLive();
    else await stopLive();

  } catch (e) {
    console.error("❌ ciclo config error:", e);
  }

  // Chequeo rápido para poder apagar/encender sin delay grande
  setTimeout(ciclo, 1500);
}

ciclo();
