const admin = require("firebase-admin");

// Lee la clave desde la variable de entorno en Railway
const serviceAccount = JSON.parse(process.env.SERVICE_ACCOUNT_JSON);

admin.initializeApp({
  credential: admin.credential.cert(serviceAccount),
  databaseURL: "https://datos-de-mercado-ofa-level-3-default-rtdb.firebaseio.com"
});

const db = admin.database();

// Utilidad para obtener hora y minuto de Bogotá
function tsBogota() {
  const iso = new Intl.DateTimeFormat('sv-SE', {
    timeZone: 'America/Bogota',
    hour12: false,
    hour: '2-digit',
    minute: '2-digit'
  }).format(new Date());
  const [hora, minuto] = iso.split(':').map(Number);
  return { hora, minuto };
}

// Tiempo aleatorio entre 0.3 y 7 segundos
function randomDelay() {
  return Math.floor(Math.random() * (7000 - 300 + 1)) + 300;
}

// Movimiento aleatorio de 0.10 a 0.49 pips (decimales de EUR/USD)
function randomMovimiento() {
  const pips = (Math.random() * (0.49 - 0.10) + 0.10);
  return +(pips).toFixed(2);
}

async function ciclo() {
  const configSnap = await db.ref("config/auto_volatilidad_noche").once("value");
  const habilitado = configSnap.val();
  if (!habilitado) {
    console.log("Volatilidad nocturna desactivada (flag)");
    return setTimeout(ciclo, 5000);
  }

  // Obtén hora y minuto actuales en Bogotá
  const { hora, minuto } = tsBogota();
  const dentroHorario =
    (hora > 18 && hora < 23) ||
    (hora === 18 && minuto >= 0) ||
    (hora === 23 && minuto <= 40);

  if (!dentroHorario) {
    console.log("Fuera del horario 18:00 a 23:40 Bogotá");
    return setTimeout(ciclo, 10000);
  }

  const ref = db.ref("market_data/M1");
  const snap = await ref.once("value");
  const M1 = snap.val() || {};

  const claves = Object.keys(M1).map(Number).sort((a, b) => a - b);
  const lastIdx = claves[claves.length - 1];
  const last = M1[lastIdx];
  if (!last) return setTimeout(ciclo, 2000);

  // Decide dirección aleatoria y magnitud realista (estructura asiática: muchos rangos pequeños, ocasional salto)
  let cambio = randomMovimiento() * (Math.random() < 0.5 ? -1 : 1);

  // Estructura “realista”: de vez en cuando (cada 15 a 25 movimientos), haz que el movimiento sea el máximo permitido
  if (Math.floor(Math.random() * 20) === 0) {
    cambio = (Math.random() < 0.5 ? -1 : 1) * 0.49;
  }

  // Calcula el nuevo cierre
  const nuevoClose = +(last.close + cambio).toFixed(5);
  const updated = {
    ...last,
    close: nuevoClose,
    high: Math.max(last.high, nuevoClose),
    low: Math.min(last.low, nuevoClose)
  };

  console.log(
    `🕒 Movimiento: ${cambio > 0 ? '+' : ''}${cambio.toFixed(2)} pips`,
    `Hora Bogotá: ${hora}:${String(minuto).padStart(2, '0')}`
  );
  await ref.child(lastIdx).update(updated);

  const delay = randomDelay();
  setTimeout(ciclo, delay);
}

ciclo();
