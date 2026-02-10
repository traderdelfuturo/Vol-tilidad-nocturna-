"use strict";

const admin = require("./firebaseApp");
const db = admin.database();

// ============================
// CONFIG
// ============================
const MARKET_ROOT = "market_data";
const SOURCE_PATH = `${MARKET_ROOT}/M1`;
const BATCH_SIZE = 5000;

// ============================
// TEMPORALIDADES
// ============================
const TFS = [
  { code: "M5", sec: 300 },
  { code: "M15", sec: 900 },
  { code: "M30", sec: 1800 },
  { code: "H1", sec: 3600 },
  { code: "H2", sec: 7200 },
  { code: "H4", sec: 14400 },
  { code: "H8", sec: 28800 },
  { code: "H12", sec: 43200 },
  { code: "D1", sec: 86400 },
];

// ============================
// UTILS
// ============================
const bucket = (t, sec) => Math.floor(t / sec) * sec;

// ============================
// BACKFILL
// ============================
async function runBackfill() {
  console.log("🚀 BACKFILL ACTIVADO");

  // Limpieza previa
  for (const tf of TFS) {
    await db.ref(`${MARKET_ROOT}/${tf.code}`).remove();
    console.log(`🧹 ${tf.code} limpiado`);
  }

  const snap = await db.ref(SOURCE_PATH).once("value");
  const rows = snap.val();

  if (!rows) {
    console.log("❌ No hay M1");
    return;
  }

  const m1 = Object.values(rows)
    .filter(v => v && typeof v.time === "number")
    .sort((a, b) => a.time - b.time);

  const acc = {};
  for (const tf of TFS) acc[tf.code] = {};

  for (const c of m1) {
    for (const tf of TFS) {
      const b = bucket(c.time, tf.sec);
      const ref = acc[tf.code][b];

      if (!ref) {
        acc[tf.code][b] = {
          time: b,
          open: c.open,
          high: c.high,
          low: c.low,
          close: c.close,
        };
      } else {
        ref.high = Math.max(ref.high, c.high);
        ref.low = Math.min(ref.low, c.low);
        ref.close = c.close;
      }
    }
  }

  const updates = {};
  for (const tf of TFS) {
    for (const [k, v] of Object.entries(acc[tf.code])) {
      updates[`${MARKET_ROOT}/${tf.code}/${k}`] = v;
    }
  }

  await db.ref().update(updates);

  console.log("✅ BACKFILL TERMINADO");
}

// ============================
// CICLO CONTROLADO (ESTILO TUS BOTS)
// ============================
async function ciclo() {
  try {
    const flagSnap = await db.ref("config/auto_market_tf_backfill").once("value");
    const doneSnap = await db.ref("config/auto_market_tf_backfill_done").once("value");

    const enabled = flagSnap.val();
    const done = doneSnap.val();

    if (!enabled || done) {
      return setTimeout(ciclo, 5000);
    }

    // 🔥 Ejecutar UNA SOLA VEZ
    await runBackfill();

    // 🔒 Auto-apagar
    await db.ref("config").update({
      auto_market_tf_backfill: false,
      auto_market_tf_backfill_done: true,
    });

    console.log("🧯 BACKFILL AUTO-DESACTIVADO");
    return;

  } catch (err) {
    console.error("❌ Error backfill:", err);
    setTimeout(ciclo, 10000);
  }
}

// ============================
// START
// ============================
ciclo();
