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

// Tiempo aleatorio entre 0.20 y 4 segundos
function randomDelay() {
  return Math.floor(Math.random() * (4000 - 200 + 1)) + 200;
}

// Movimiento aleatorio: máximo reducido un 39%, mínimo igual
function randomMovimiento() {
  let pips;
  // 96% de las veces, entre 0.24 y 0.52 (antes era hasta 0.94, ahora recortado)
  if (Math.random() < 0.96) {
    pips = Math.random() * (0.52 - 0.24) + 0.24;
  } else {
    // 4% de las veces, entre 0.52 y 0.66 (movimientos fuertes)
    pips = Math.random() * (0.667 - 0.52) + 0.52;
  }
  const direction = Math.random() < 0.5 ? -1 : 1;
  const movimiento = direction * +(pips * 0.00010).toFixed(6);
  return movimiento;
}

async function ciclo() {
  const configSnap = await db.ref("config/auto_volatilidad_londres").once("value");
  const habilitado = configSnap.val();
  if (!habilitado) {
    console.log("Volatilidad Londres desactivada (flag)");
    return setTimeout(ciclo, 5000);
  }

  // Hora y minuto actuales en Bogotá
  const { hora, minuto } = tsBogota();

  // Horario: de 2:00 am a 8:00 am Bogotá
  const dentroHorario =
    (hora > 2 && hora < 8) ||
    (hora === 2 && minuto >= 0) ||
    (hora === 8 && minuto === 0);

  if (!dentroHorario) {
    console.log("Fuera del horario 02:00 a 08:00 Bogotá");
    return setTimeout(ciclo, 10000);
  }

  // Solo la última vela (ahorra lecturas/costos)
  const ref = db.ref("market_data/M1");
  const query = ref.orderByKey().limitToLast(1);
  const snap = await query.once("value");
  const M1 = snap.val() || {};
  const lastIdx = Object.keys(M1)[0];
  const last = M1[lastIdx];
  if (!last) return setTimeout(ciclo, 2000);

  // Movimiento realista
  let cambio = randomMovimiento();

  // Ocasionalmente, el máximo absoluto (0.667 pips) solo 1 de cada 40 movimientos
  if (Math.floor(Math.random() * 40) === 0) {
    const direction = Math.random() < 0.5 ? -1 : 1;
    cambio = direction * 0.667 * 0.00010;
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
