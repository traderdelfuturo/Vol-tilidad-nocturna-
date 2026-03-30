const admin = require("./firebaseApp");
const db = admin.database();

function tsBogotaSeg() {
  const iso = new Intl.DateTimeFormat('sv-SE', {
    timeZone: 'America/Bogota',
    hour12: false,
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit'
  }).format(new Date());
  const isoUTC = iso.replace(' ', 'T') + ':00Z';
  return Math.floor(new Date(isoUTC).getTime() / 1000);
}

function ahoraBogota() {
  const parts = new Intl.DateTimeFormat('en-US', {
    timeZone: 'America/Bogota',
    hour12: false,
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit'
  }).formatToParts(new Date());

  const map = {};
  for (const p of parts) {
    if (p.type !== 'literal') map[p.type] = p.value;
  }

  return {
    year: parseInt(map.year, 10),
    month: parseInt(map.month, 10),
    day: parseInt(map.day, 10),
    hour: parseInt(map.hour, 10),
    minute: parseInt(map.minute, 10),
    second: parseInt(map.second, 10)
  };
}

function msHastaAperturaBogota() {
  const { year, month, day } = ahoraBogota();
  const aperturaUTC = Date.UTC(year, month - 1, day, 23, 0, 0, 0); // 18:00 Bogotá = 23:00 UTC
  return Math.max(0, aperturaUTC - Date.now());
}

async function ciclo() {
  try {
    const configSnap = await db.ref("config/auto_vela_m1").once("value");
    const habilitado = configSnap.exists() ? configSnap.val() : true;
    if (!habilitado) return setTimeout(ciclo, 5000);

    const ahora = ahoraBogota();

    // Pausa de mercado: de 4:00 pm a 5:59:59 pm hora Colombia
    if (ahora.hour >= 16 && ahora.hour < 18) {
      const delay = msHastaAperturaBogota() + 200;
      return setTimeout(ciclo, delay);
    }

    const t = tsBogotaSeg();
    const ref = db.ref('market_data/M1');
    const snap = await ref.orderByKey().limitToLast(1).once('value');
    const data = snap.val();

    if (!data) return setTimeout(ciclo, 5000);

    const lastKey = Object.keys(data)[0];
    const last = data[lastKey];

    if (!last || typeof last.time !== 'number') return setTimeout(ciclo, 5000);

    if (last.time < t) {
      const newKey = (parseInt(lastKey) + 1).toString();
      const vela = {
        time: t,
        open: last.close,
        high: last.close,
        low: last.close,
        close: last.close
      };
      await ref.child(newKey).set(vela);
      console.log(`✅ Vela M1 creada: ${newKey} | ${new Date(t * 1000).toLocaleString("es-CO")}`);
    }
  } catch (err) {
    console.error("🔥 ERROR:", err);
  }

  const now = new Date();
  const ms = now.getSeconds() * 1000 + now.getMilliseconds();
  const delay = 60_000 - ms;
  setTimeout(ciclo, delay);
}

process.on("unhandledRejection", err => {
  console.error("❌ ERROR NO MANEJADO:", err);
});

console.log("🚀 Auto Vela M1 optimizado iniciado");
ciclo();
