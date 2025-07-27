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

// Tiempo aleatorio entre 0.3 y 7 segundos
function randomDelay() {
  return Math.floor(Math.random() * (7000 - 300 + 1)) + 300;
}

// Movimiento aleatorio: máximo bajado un 29% y aumentado en 390%
function randomMovimiento() {
  let pips;
  // 95% de las veces, entre 0.10 y 0.28 (antes era 0.39, 0.39 - 29% ≈ 0.277)
  if (Math.random() < 0.95) {
    pips = Math.random() * (0.277 - 0.10) + 0.10;
  } else {
    // 5% de las veces, entre 0.28 y 0.35 (antes era 0.39-0.49, ahora 0.277-0.348)
    pips = Math.random() * (0.348 - 0.277) + 0.277;
  }
  pips = pips * 3.9; // AUMENTO DEL 390%
  const direction = Math.random() < 0.5 ? -1 : 1;
  const movimiento = direction * +(pips * 0.00010).toFixed(6); 
  return movimiento;
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

  // Optimización: lee solo la última vela
  const ref = db.ref("market_data/M1");
  const query = ref.orderByKey().limitToLast(1);
  const snap = await query.once("value");
  const M1 = snap.val() || {};
  const lastIdx = Object.keys(M1)[0];
  const last = M1[lastIdx];
  if (!last) return setTimeout(ciclo, 2000);

  // Movimiento exacto en rango pip realista
  let cambio = randomMovimiento();

  // Máximo absoluto permitido (0.348 pips, 29% menos que 0.49), solo 1 de cada 45 movimientos
  if (Math.floor(Math.random() * 45) === 0) {
    const direction = Math.random() < 0.5 ? -1 : 1;
    cambio = direction * (0.348 * 3.9) * 0.00010; // AUMENTO DEL 390%
  }

  // Calcula el nuevo cierre
  const nuevoClose = +(last.close + cambio).toFixed(5); // 5 decimales, estándar EUR/USD
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
