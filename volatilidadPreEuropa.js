const admin = require("./firebaseApp");
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

// Tiempo aleatorio entre 0.20 y 5 segundos
function randomDelay() {
  return Math.floor(Math.random() * (5000 - 200 + 1)) + 200;
}

// Movimiento aleatorio, menos volátil en la parte alta (máx 0.84)
function randomMovimiento() {
  let pips;
  // 96% de las veces, entre 0.19 y 0.59 (bajé el máximo a 0.59)
  if (Math.random() < 0.96) {
    pips = Math.random() * (0.59 - 0.19) + 0.19;
  } else {
    // 4% de las veces, entre 0.59 y 0.84 (movimientos grandes y raros)
    pips = Math.random() * (0.84 - 0.59) + 0.59;
  }
  const direction = Math.random() < 0.5 ? -1 : 1;
  // Un pip es 0.00010
  const movimiento = direction * +(pips * 0.00010).toFixed(6);
  return movimiento;
}

async function ciclo() {
  const configSnap = await db.ref("config/auto_volatilidad_pre_europa").once("value");
  const habilitado = configSnap.val();
  if (!habilitado) {
    console.log("Volatilidad Pre-Europa desactivada (flag)");
    return setTimeout(ciclo, 5000);
  }

  // Hora y minuto actuales en Bogotá
  const { hora, minuto } = tsBogota();

  // Horario: de 23:40 pm hasta 7:00 am
  const dentroHorario =
    (hora > 23 || hora < 7) || (hora === 23 && minuto >= 40);

  if (!dentroHorario) {
    console.log("Fuera del horario 23:40 a 07:00 Bogotá");
    return setTimeout(ciclo, 10000);
  }

  // Lee solo la última vela
  const ref = db.ref("market_data/M1");
  const query = ref.orderByKey().limitToLast(1);
  const snap = await query.once("value");
  const M1 = snap.val() || {};
  const lastIdx = Object.keys(M1)[0];
  const last = M1[lastIdx];
  if (!last) return setTimeout(ciclo, 2000);

  // Movimiento realista
  let cambio = randomMovimiento();

  // El máximo de 0.84 pips solo ocurre 1 vez cada 39 movimientos (~2.56%)
  if (Math.floor(Math.random() * 39) === 0) {
    const direction = Math.random() < 0.5 ? -1 : 1;
    cambio = direction * 0.84 * 0.00010;
  }

  // Nuevo cierre
  const nuevoClose = +(last.close + cambio).toFixed(5);
  const updated = {
    ...last,
    close: nuevoClose,
    high: Math.max(last.high, nuevoClose),
    low: Math.min(last.low, nuevoClose)
  };

  console.log(
    `🕒 Movimiento: ${cambio > 0 ? '+' : ''}${(cambio / 0.00010).toFixed(2)} pips (${cambio.toFixed(6)})`,
    `Hora Bogotá: ${hora}:${String(minuto).padStart(2, '0')}`
  );
  await ref.child(lastIdx).update(updated);

  const delay = randomDelay();
  setTimeout(ciclo, delay);
}

ciclo();
