const admin = require("./firebaseApp"); // Asegúrate que firebaseApp.js esté bien configurado
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

// ========== CREACIÓN DE VELA M1 ==========

async function ciclo() {
  try {
    const configSnap = await db.ref("config/auto_vela_m1").once("value");
    const habilitado = configSnap.exists() ? configSnap.val() : true;
    if (!habilitado) {
      console.log("🚫 FLAG desactivado en config/auto_vela_m1");
      return setTimeout(ciclo, 5000);
    }

    // Hora Colombia (puedes modificar esto si quieres control por hora real)
    const horaCol = new Intl.DateTimeFormat('sv-SE', {
      timeZone: 'America/Bogota',
      hour12: false,
      hour: '2-digit'
    }).format(new Date());
    const hora = parseInt(horaCol, 10);

    // Para prueba: forzar que siempre se ejecute
    if (true) {
      const t = tsBogotaSeg();
      const ref = db.ref('market_data/M1');
      const snap = await ref.once('value');
      const data = snap.val();

      if (!data) {
        console.log("⚠️ No hay ninguna vela en market_data/M1");
        return setTimeout(ciclo, 5000);
      }

      // Buscar última clave numérica
      const claves = Object.keys(data).filter(k => !isNaN(k)).map(k => parseInt(k));
      if (claves.length === 0) {
        console.log("❌ No hay claves numéricas válidas en market_data/M1");
        return setTimeout(ciclo, 5000);
      }

      const lastKey = Math.max(...claves).toString();
      const last = data[lastKey];

      if (!last || typeof last.time !== 'number') {
        console.log("❌ Última vela inválida:", last);
        return setTimeout(ciclo, 5000);
      }

      if (last.time < t) {
        const vela = {
          time: t,
          open: last.close,
          high: last.close,
          low: last.close,
          close: last.close
        };
        const newKey = (parseInt(lastKey) + 1).toString();
        await ref.child(newKey).set(vela);
        console.log(`✅ Vela M1 creada: ${newKey} | ${new Date(t * 1000).toLocaleString("es-CO")}`);
      } else {
        console.log(`⏭️ Ya existe vela para el minuto ${t}`);
      }
    } else {
      console.log("⏸️ Fuera del horario definido");
    }
  } catch (err) {
    console.error(" ERROR:", err);
  }

  // Esperar al inicio del siguiente minuto exacto
  const now = new Date();
  const ms = now.getSeconds() * 1000 + now.getMilliseconds();
  const delay = 60_000 - ms;
  setTimeout(ciclo, delay);
}

process.on("unhandledRejection", err => {
  console.error("❌ ERROR NO MANEJADO:", err);
});

console.log("🚀 Auto Vela M1 iniciado");
ciclo();
