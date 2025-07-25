// resumen_tf_incremental.js
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

async function ciclo() {
  try {
    // Flag para apagar/prender desde la DB
    const cfgSnap = await db.ref("config/auto_resumen_tf").once("value");
    const habilitado = cfgSnap.exists() ? cfgSnap.val() : true;
    if (!habilitado) return setTimeout(ciclo, 60_000);

    const m1 = await cargarM1();
    if (!m1.length) return setTimeout(ciclo, 60_000);

    for (const tf of TEMPORALIDADES) {
      const size  = tf.size;
      const refTF = db.ref(`market_data/${tf.nombre}`);

      // último índice ya creado
      const lastSnap = await refTF.orderByKey().limitToLast(1).once("value");
      let nextIdx = 0;
      if (lastSnap.exists()) {
        const lastKey = Object.keys(lastSnap.val())[0];
        nextIdx = Number(lastKey) + 1;
      }

      const start = nextIdx * size;
      if (start + size > m1.length) continue; // aún no hay bloque completo

      const updates = {};
      for (let i = start; i + size <= m1.length; i += size) {
        const chunk = m1.slice(i, i + size);
        updates[nextIdx++] = agrupar(chunk);
      }

      if (Object.keys(updates).length) {
        await refTF.update(updates);
        console.log(`➕ ${tf.nombre}: +${Object.keys(updates).length} velas nuevas`);
      }
    }
  } catch (e) {
    console.error("🔥 ERROR incremental:", e);
  }

  setTimeout(ciclo, 60_000); // vuelve a correr en 1 min
}

console.log("🚀 Resumen TF incremental activo");
ciclo();
