const admin = require("./firebaseApp"); // Asegúrate que la ruta a tu config de Firebase sea correcta
const db = admin.database();
const crypto = require("crypto");

// ======================================================
// CSPRNG (crypto) - máxima aleatoriedad práctica en servidor
// ======================================================
const TWO_POW_53 = 9007199254740992; // 2^53

// Float uniforme en [0, 1) con 53 bits (resolución tipo Math.random pero CSPRNG)
function cryptoRandomFloat() {
  const x = crypto.randomBytes(8).readBigUInt64BE() >> 11n; // 53 bits
  return Number(x) / TWO_POW_53;
}

// Dirección 50/50 (CSPRNG)
function randomDirection() {
  return crypto.randomInt(0, 2) === 0 ? -1 : 1;
}

// 1 de cada 45 (CSPRNG, sin sesgo por módulo)
function oneIn45() {
  return crypto.randomInt(0, 45) === 0; // 0..44
}

// ======================================================
// Ajuste pedido: +100% tamaño movimientos (doble)
// ======================================================
const MOVEMENT_MULTIPLIER = 3.9 * 1.39 * 2; // 10.842 (doble sobre 5.421)

// Utilidad para obtener hora y minuto de Bogotá (sin cambios)
function tsBogota() {
  const iso = new Intl.DateTimeFormat("sv-SE", {
    timeZone: "America/Bogota",
    hour12: false,
    hour: "2-digit",
    minute: "2-digit",
  }).format(new Date());
  const [hora, minuto] = iso.split(":").map(Number);
  return { hora, minuto };
}

// Tiempo aleatorio entre 0.3 y 7 segundos (CSPRNG, mismo rango)
function randomDelay() {
  return crypto.randomInt(300, 7000 + 1); // max exclusivo
}

// Movimiento aleatorio (misma lógica, CSPRNG + +100% tamaño)
function randomMovimiento() {
  let pips;

  // Misma estructura: 95% rango bajo, 5% rango alto
  if (cryptoRandomFloat() < 0.95) {
    pips = cryptoRandomFloat() * (0.277 - 0.10) + 0.10;
  } else {
    pips = cryptoRandomFloat() * (0.348 - 0.277) + 0.277;
  }

  // +100% aplicado aquí (doble sobre la versión anterior)
  pips = pips * MOVEMENT_MULTIPLIER;

  // Dirección 50/50 CSPRNG
  const direction = randomDirection();

  const movimiento = direction * +(pips * 0.00010).toFixed(6);
  return movimiento;
}

// =======================================================================
// INICIO: FUNCIÓN PARA EL RECORRIDO LÍQUIDO (VERSIÓN ULTRARRÁPIDA)
// =======================================================================
async function executeLiquidMove(ref, lastIdx, startCandle, targetClose) {
  const startClose = startCandle.close;
  const totalMovement = targetClose - startClose;

  // --- PARÁMETROS DE VELOCIDAD MÁXIMA ---
  const numberOfSteps = 10; // 10 Pasos
  const stepDelay = 5; // 5ms de pausa (Total: 10 * 5 = 50ms)
  // --- FIN AJUSTES ---

  // Evitar división por cero si no hay movimiento
  if (Math.abs(totalMovement) < 0.00000001) {
    await ref.child(lastIdx).update({
      ...startCandle,
      close: targetClose,
      high: Math.max(startCandle.high, targetClose),
      low: Math.min(startCandle.low, targetClose),
    });
    return {
      ...startCandle,
      close: targetClose,
      high: Math.max(startCandle.high, targetClose),
      low: Math.min(startCandle.low, targetClose),
    };
  }

  const pricePerStep = totalMovement / numberOfSteps;
  let currentHigh = startCandle.high;
  let currentLow = startCandle.low;
  let currentClose = startCandle.close;
  let updatePromises = [];

  for (let i = 1; i <= numberOfSteps; i++) {
    let intermediateClose;
    if (i === numberOfSteps) {
      intermediateClose = targetClose;
    } else {
      intermediateClose = +(startClose + pricePerStep * i).toFixed(5); // Usar 5 decimales
    }

    currentClose = intermediateClose;
    currentHigh = Math.max(currentHigh, currentClose);
    currentLow = Math.min(currentLow, currentClose);

    const updatedStep = {
      ...startCandle,
      close: currentClose,
      high: currentHigh,
      low: currentLow,
    };

    updatePromises.push(ref.child(lastIdx).update(updatedStep));

    if (i < numberOfSteps) {
      await new Promise((resolve) => setTimeout(resolve, stepDelay));
    }
  }

  await Promise.all(updatePromises);

  const finalCandle = {
    ...startCandle,
    close: targetClose,
    high: currentHigh,
    low: currentLow,
  };
  await ref.child(lastIdx).update(finalCandle);

  return finalCandle;
}
// =======================================================================
// FIN: FUNCIÓN DE RECORRIDO
// =======================================================================

async function ciclo() {
  // --- Lógica de habilitación y horario (sin cambios) ---
  const configSnap = await db.ref("config/auto_volatilidad_noche").once("value");
  const habilitado = configSnap.val();
  if (!habilitado) {
    console.log("Volatilidad nocturna desactivada (flag)");
    return setTimeout(ciclo, 5000);
  }

  const { hora, minuto } = tsBogota();
  const dentroHorario =
    (hora > 18 && hora < 23) ||
    (hora === 18 && minuto >= 0) ||
    (hora === 23 && minuto <= 40);

  if (!dentroHorario) {
    console.log(
      `Fuera del horario 18:00 a 23:40 Bogotá (${hora}:${String(minuto).padStart(2, "0")})`
    );
    return setTimeout(ciclo, 10000);
  }
  // --- Fin Lógica de habilitación y horario ---

  // --- Lectura de la última vela (sin cambios) ---
  const ref = db.ref("market_data/M1");
  const query = ref.orderByKey().limitToLast(1);
  let snap;
  try {
    snap = await query.once("value");
  } catch (error) {
    console.error("Error al leer de Firebase:", error);
    return setTimeout(ciclo, 5000);
  }

  const M1 = snap.val() || {};
  const lastIdx = Object.keys(M1)[0];
  const last = M1[lastIdx];

  if (!last || typeof last.close !== "number") {
    console.warn("Última vela no encontrada o inválida. Reintentando...");
    return setTimeout(ciclo, 2000);
  }
  // --- Fin Lectura de la última vela ---

  // --- Cálculo del movimiento (CSPRNG + +100% tamaños) ---
  let cambio = randomMovimiento();

  // Mantiene tu lógica: 1 de cada 45 fuerza el máximo (ahora también +100%)
  if (oneIn45()) {
    const direction = randomDirection();
    cambio = direction * (0.348 * MOVEMENT_MULTIPLIER) * 0.00010;
  }

  const nuevoClose = +(last.close + cambio).toFixed(5);
  // --- Fin Cálculo del movimiento ---

  console.log(
    `💧 Iniciando recorrido rápido: ${cambio > 0 ? "+" : ""}${(cambio / 0.00010).toFixed(2)} pips (${cambio.toFixed(6)})`,
    `Hora Bogotá: ${hora}:${String(minuto).padStart(2, "0")}`
  );

  try {
    await executeLiquidMove(ref, lastIdx, last, nuevoClose);
    console.log(`✅ Recorrido completado a ${nuevoClose.toFixed(5)}`);
  } catch (error) {
    console.error("Error durante executeLiquidMove:", error);
    try {
      await ref.child(lastIdx).update({
        ...last,
        close: nuevoClose,
        high: Math.max(last.high, nuevoClose),
        low: Math.min(last.low, nuevoClose),
      });
      console.warn("Recorrido falló, se aplicó actualización directa.");
    } catch (updateError) {
      console.error("Error en actualización directa tras fallo de recorrido:", updateError);
    }
  }

  const delay = randomDelay();
  setTimeout(ciclo, delay);
}

try {
  ciclo();
} catch (initialError) {
  console.error("Error al iniciar el ciclo:", initialError);
  setTimeout(ciclo, 10000);
}
