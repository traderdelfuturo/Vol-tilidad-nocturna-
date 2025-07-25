// ================== index.js ==================
const functions = require('firebase-functions'); // v1 o compat
const admin     = require('firebase-admin');

admin.initializeApp();
const db = admin.database();

// ---- CONFIG FLAGS EN RTDB ----
const ENABLE_FLAG   = 'config/auto_resumen_tf';        // true = corre incremental
const FULL_BUILT    = 'config/resumen_tf_full_built';  // true = historial ya creado
const FORCE_FULL    = 'config/force_full_build';       // true = rehacer full

// ---- DEFINICIONES ----
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

// ---- HELPERS ----
async function cargarM1() {
  const snap = await db.ref('market_data/M1').once('value');
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

// ---- BUILD FULL (una vez) ----
async function buildFull() {
  const m1 = await cargarM1();
  if (!m1.length) return 'Sin M1';

  for (const tf of TEMPORALIDADES) {
    const total = Math.floor(m1.length / tf.size);
    const refTF = db.ref(`market_data/${tf.nombre}`);

    // Limpia la rama para que no queden residuos
    await refTF.set(null);

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

    functions.logger.info(`FULL ${tf.nombre}: ${total} velas`);
  }

  await db.ref(FULL_BUILT).set(true);
  await db.ref(FORCE_FULL).set(false);
  return 'Full OK';
}

// ---- BUILD INCREMENTAL (cada minuto) ----
async function buildIncremental() {
  const m1 = await cargarM1();
  if (!m1.length) return 'Sin M1';

  for (const tf of TEMPORALIDADES) {
    const size  = tf.size;
    const refTF = db.ref(`market_data/${tf.nombre}`);

    // último índice ya creado
    const lastSnap = await refTF.orderByKey().limitToLast(1).once('value');
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
      functions.logger.info(`+ ${tf.nombre}: ${Object.keys(updates).length} velas`);
    }
  }
  return 'Inc OK';
}

// ---- LOOP GENERAL ----
async function mainLoop() {
  const enabledSnap = await db.ref(ENABLE_FLAG).once('value');
  const enabled = enabledSnap.exists() ? enabledSnap.val() : true;

  if (!enabled) return 'OFF';

  const fullSnap  = await db.ref(FULL_BUILT).once('value');
  const forceSnap = await db.ref(FORCE_FULL).once('value');

  const needFull = !fullSnap.exists() || !fullSnap.val() || (forceSnap.exists() && forceSnap.val());

  if (needFull) {
    functions.logger.info('FULL BUILD requerido...');
    return await buildFull();
  } else {
    return await buildIncremental();
  }
}

// ======================================================
// PUBLICA AQUÍ TUS FUNCIONES CON NOMBRE
// ======================================================

// 1) Ejecuta EL FULL MANUALMENTE (HTTP) una sola vez si quieres
exports.fullBuildTF = functions.https.onRequest(async (req, res) => {
  try {
    const r = await buildFull();
    res.send(r);
  } catch (e) {
    functions.logger.error(e);
    res.status(500).send('ERROR');
  }
});

// 2) Incremental cada minuto (Pub/Sub)
exports.autoResumenTF = functions.pubsub
  .schedule('every 1 minutes')      // ajusta si quieres cada 5 min
  .timeZone('America/Bogota')
  .onRun(async () => {
    return await mainLoop();
  });
