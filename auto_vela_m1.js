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

// 5:00 PM a 3:59 PM del día siguiente
async function ciclo() {
  // Configurable flag en base de datos, si quieres
  const configSnap = await db.ref("config/auto_vela_m1").once("value");
  const habilitado = configSnap.exists() ? configSnap.val() : true;
  if (!habilitado) {
    console.log("Auto Vela M1 desactivada (flag)");
    return setTimeout(ciclo, 5000);
  }

  const horaCol = new Intl.DateTimeFormat('sv-SE', {
    timeZone: 'America/Bogota',
    hour12: false,
    hour: '2-digit'
  }).format(new Date());
  const hora = parseInt(horaCol, 10);

  // SOLO entre 17:00 y 15:59 (5PM a 3:59PM, pausa 16:00–16:59)
  if (hora >= 17 || hora < 16) {
    const t = tsBogotaSeg();
    const ref = db.ref('market_data/M1');
    const snap = await ref.orderByKey().limitToLast(1).once('value');
    if (!snap.exists()) return setTimeout(ciclo, 5000);

    const lastKey = Object.keys(snap.val())[0];
    const last = snap.val()[lastKey];

    if (last.time < t) {
      const vela = {
        time: t,
        open: last.close,
        high: last.close,
        low: last.close,
        close: last.close
      };
      const newKey = parseInt(lastKey) + 1;
      await ref.child(String(newKey)).set(vela);
      console.log(`✅ Vela M1 creada: ${newKey}`);
    }
  } else {
    console.log("⏸️ Fuera del horario 17:00-15:59 (5pm-4pm)");
  }

  // Sincroniza con el minuto exacto de Colombia
  const now = new Date();
  const ms = now.getSeconds() * 1000 + now.getMilliseconds();
  const delay = 60_000 - ms;
  setTimeout(ciclo, delay);
}

ciclo();
