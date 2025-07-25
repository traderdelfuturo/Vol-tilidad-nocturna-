const admin = require("./firebaseApp");
const db    = admin.database();

const TEMPORALIDADES = [
  { nombre: 'M5',  size: 5 },
  { nombre: 'M15', size: 15 },
  { nombre: 'M30', size: 30 },
  { nombre: 'H1',  size: 60 },
  { nombre: 'H4',  size: 240 },
  { nombre: 'D1',  size: 1440 },
  { nombre: 'W1',  size: 10080 },
  { nombre: 'MN',  size: 43200 }
];

async function cargarM1() {
  const snap = await db.ref("market_data/M1").once("value");
  if (!snap.exists()) return [];
  const obj  = snap.val();
  const keys = Object.keys(obj).map(Number).sort((a,b)=>a-b);
  return keys.map(k => ({ ...obj[k], __k: k }));
}

function agrupar(chunk) {
  const open  = chunk[0].open;
  const close = chunk[chunk.length - 1].close;
  const high  = Math.max(...chunk.map(v => v.high));
  const low   = Math.min(...chunk.map(v => v.low));
  const time  = chunk[0].time;
  return { open, high, low, close, time };
}

(async () => {
  try {
    const m1 = await cargarM1();
    if (!m1.length) {
      console.log("Sin M1, nada que hacer.");
      process.exit(0);
    }

    for (const tf of TEMPORALIDADES) {
      const total = Math.floor(m1.length / tf.size);
      const refTF = db.ref(`market_data/${tf.nombre}`);

      let batch = {};
      let count = 0;
      for (let i = 0; i < total; i++) {
        const chunk = m1.slice(i * tf.size, (i + 1) * tf.size);
        batch[i] = agrupar(chunk);
        count++;

        if (count === 500) {
          await refTF.update(batch);
          batch = {};
          count = 0;
        }
      }
      if (count) await refTF.update(batch);

      console.log(`✅ ${tf.nombre}: ${total} velas creadas`);
    }

    console.log("✅ FULL BUILD terminado.");
    process.exit(0);
  } catch (e) {
    console.error("🔥 ERROR FULL BUILD:", e);
    process.exit(1);
  }
})();
