const admin = require("./firebaseApp");
const db = admin.database();

// ══════════════════════════════════════════════════════════════
// EL LATIDO — velas estilo IQ Option (autogeneración cada 5s)
// No mueve el precio JAMÁS: cuando un balde de 5s arranca y ningún
// mover escribió, le da un toque a la última vela M1 (solo un sello
// hb, close/high/low intactos). Ese evento hace que la pantalla
// ruede vela nueva y que el motor 5s nazca el balde plano en vivo.
// Se autosilencia cuando el mercado está escribiendo solo.
// ══════════════════════════════════════════════════════════════
const SECONDS_PER_BAR = 5;
const bucket5s = () => Math.floor(Math.floor(Date.now() / 1000) / SECONDS_PER_BAR) * SECONDS_PER_BAR;

console.log("💓 LATIDO 5S INICIADO (metrónomo IQ, sin tocar el precio)...");

const mem = { lastIdx: null, lastTick: 0, lastPulseBucket: null };
const refM1 = db.ref("market_data/M1");

// vigía: recuerda el índice vigente y cuándo fue el último tick real
refM1.orderByKey().limitToLast(1).on("child_added", (snap) => {
  mem.lastIdx = snap.key; mem.lastTick = Date.now();
});
refM1.orderByKey().limitToLast(1).on("child_changed", (snap) => {
  const v = snap.val();
  mem.lastIdx = snap.key;
  // un latido propio no cuenta como tick real (solo cambió hb)
  if (!v || v.__hbOnly !== true) mem.lastTick = Date.now();
});

setInterval(async () => {
  try {
    const b = bucket5s();
    if (b === mem.lastPulseBucket) return;            // ya latió este balde
    if (Date.now() - mem.lastTick < 1200) return;      // el mercado está vivo: silencio
    if (!mem.lastIdx) return;
    mem.lastPulseBucket = b;
    await refM1.child(mem.lastIdx).transaction((v) => {
      if (v === null || typeof v.close !== "number") return v;
      const out = { ...v, hb: Date.now() };
      delete out.__hbOnly;
      return out;
    });
  } catch (e) {
    console.error("latido:", e.message);
  }
}, 1000);
