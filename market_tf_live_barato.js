"use strict";

const admin = require("./firebaseApp");
const db = admin.database();

// Segundos por TF
const TF = {
  M5:   300,
  M15:  900,
  M30:  1800,
  H1:   3600,
  H2:   7200,
  H4:   14400,
  H8:   28800,
  H12:  43200,
  D1:   86400,
  W1:   604800,
  // Mensual real calendario sería mejor, pero si así te funcionaba, dejamos esto:
  MN:   2592000 // ~30 días
};

// Bucket por tiempo (segundos)
const bucket = (ts, size) => Math.floor(ts / size) * size;

let running = false;
let m1Ref = null;

function startListeners(){
  if (running) return;
  running = true;

  m1Ref = db.ref("market_data/M1").orderByKey().limitToLast(1);

  m1Ref.on("child_added", snap => {
    const vela = snap.val();
    if (!vela || typeof vela.time !== "number") return;
    rollupToAllTF(vela).catch(()=>{});
  });

  m1Ref.on("child_changed", snap => {
    const vela = snap.val();
    if (!vela || typeof vela.time !== "number") return;
    rollupToAllTF(vela).catch(()=>{});
  });
}

function stopListeners(){
  if (!running) return;
  running = false;

  if (m1Ref){
    m1Ref.off();
    m1Ref = null;
  }
}

async function rollupToAllTF(velaM1) {
  const { time, open, high, low, close, volume = 0 } = velaM1;

  const tasks = Object.entries(TF).map(([tf, secs]) => {
    const b = bucket(time, secs);
    const ref = db.ref(`market_data/${tf}/${b}`);

    // Transaction = vela viva real (como MetaTrader)
    return ref.transaction(v => {
      if (!v) {
        return { time: b, open, high, low, close, volume };
      }
      v.high   = Math.max(v.high, high);
      v.low    = Math.min(v.low,  low);
      v.close  = close;
      v.volume = (v.volume || 0) + volume;
      return v;
    });
  });

  await Promise.all(tasks);
}

// Ciclo controlado por config (tu estilo)
async function ciclo(){
  try{
    const cfg = (await db.ref("config").once("value")).val() || {};

    const enabled = !!cfg.auto_tf_rollup_live;       // NUEVO FLAG
    const backfillRunning = !!cfg.auto_market_tf_backfill;

    if (backfillRunning) {
      stopListeners();
      return setTimeout(ciclo, 1500);
    }

    if (enabled) startListeners();
    else stopListeners();

  }catch(e){
    // silencio, se reintenta
  }
  setTimeout(ciclo, 1500);
}

ciclo();
