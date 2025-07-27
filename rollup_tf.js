// rollup_tf.js
const admin = require("./firebaseApp");
const db = admin.database();

// segundos por TF
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
  MN:   2592000 // ~30 días
};

// Bucket por tiempo (segundos)
const bucket = (ts, size) => Math.floor(ts / size) * size;

function startRollup() {
  const refM1 = db.ref("market_data/M1");

  // Escucha nueva vela M1
  refM1.orderByKey().limitToLast(1).on("child_added", snap => {
    const vela = snap.val();
    if (!vela || typeof vela.time !== "number") return;
    rollupToAllTF(vela).catch(err => console.error("🔥 rollup ADD error:", err));
  });

  // Escucha cambios en la vela viva M1 (actualizaciones en tiempo real)
  refM1.orderByKey().limitToLast(1).on("child_changed", snap => {
    const vela = snap.val();
    if (!vela || typeof vela.time !== "number") return;
    rollupToAllTF(vela).catch(err => console.error("🔥 rollup CHANGE error:", err));
  });

  console.log("🚀 Rollup TF escuchando M1 (added + changed)...");
}

async function rollupToAllTF(velaM1) {
  const { time, open, high, low, close, volume = 0 } = velaM1;

  const tasks = Object.entries(TF).map(([tf, secs]) => {
    const b = bucket(time, secs);                 // clave = inicio del bucket en segundos
    const ref = db.ref(`market_data/${tf}/${b}`);

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

module.exports = { startRollup };
