const admin = require("./firebaseApp"); // Asegúrate que la ruta sea correcta
const db = admin.database();
const crypto = require("crypto");

// ======================================================
// CSPRNG (crypto) - máxima aleatoriedad práctica en servidor
// (reemplaza TODO Math.random sin cambiar la lógica)
// ======================================================
const TWO_POW_53 = 9007199254740992; // 2^53

// Float uniforme en [0, 1) con 53 bits (CSPRNG)
function cryptoRandomFloat() {
  const x = crypto.randomBytes(8).readBigUInt64BE() >> 11n; // 53 bits
  return Number(x) / TWO_POW_53;
}

// Dirección 50/50 (CSPRNG)
function randomDirection() {
  return crypto.randomInt(0, 2) === 0 ? -1 : 1;
}

// 1 de cada 39 (CSPRNG)
function oneIn39() {
  return crypto.randomInt(0, 39) === 0; // 0..38
}

// ======================================================
// Ajuste: -45% tamaño movimientos (x0.55)
// ======================================================
const MOVEMENT_MULTIPLIER = 1.3 * 2 * 2.6 * 0.55; // 3.718

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

// Tiempo aleatorio entre 0.20 y 5 segundos (CSPRNG, mismo rango)
function randomDelay() {
  return crypto.randomInt(200, 5000 + 1); // 200..5000
}

// Movimiento aleatorio (misma lógica, CSPRNG)
function randomMovimiento() {
  let pips;

  // 96% de las veces, entre 0.19 y 0.59
  if (cryptoRandomFloat() < 0.96) {
    pips = cryptoRandomFloat() * (0.59 - 0.19) + 0.19;
  } else {
    // 4% de las veces, entre 0.59 y 0.84
    pips = cryptoRandomFloat() * (0.84 - 0.59) + 0.59;
  }

  // Solo cambia el tamaño (-45%)
  pips = pips * MOVEMENT_MULTIPLIER;

  const direction = randomDirection();
  const movimiento = direction * +(pips * 0.00010).toFixed(6);
  return movimiento;
}

// =======================================================================
// INICIO: FUNCIÓN PARA EL RECORRIDO LÍQUIDO (VERSIÓN ULTRARRÁPIDA)
// (Exactamente la misma función que en el script anterior)
// =======================================================================
async function executeLiquidMove(ref, lastIdx, startCandle, targetClose) {
  const startClose = startCandle.close;
  const totalMovement = targetClose - startClose;

  // --- PARÁMETROS DE VELOCIDAD MÁXIMA ---
  const numberOfSteps = 10; // 10 Pasos
  const stepDelay = 5; // 5ms de pausa (Total: 10 * 5 = 50ms)
  // --- FIN AJUSTES ---

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
      intermediateClose = +(startClose + pricePerStep * i).toFixed(5);
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
  const configSnap = await db
    .ref("config/auto_volatilidad_pre_europa")
    .once("value");
  const habilitado = configSnap.val();
  if (!habilitado) {
    console.log("Volatilidad Pre-Europa desactivada (flag)");
    return setTimeout(ciclo, 5000);
  }

  const { hora, minuto } = tsBogota();
  const dentroHorario =
    (hora > 23 || hora < 7) || (hora === 23 && minuto >= 40);

  if (!dentroHorario) {
    console.log(
      `Fuera del horario 23:40 a 07:00 Bogotá (${hora}:${String(minuto).padStart(2, "0")})`
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

  // --- Cálculo del movimiento (misma lógica, CSPRNG) ---
  let cambio = randomMovimiento();

  // El máximo de 0.84 pips solo ocurre 1 vez cada 39 movimientos (misma lógica, CSPRNG)
  if (oneIn39()) {
    const direction = randomDirection();
    cambio = direction * (0.84 * MOVEMENT_MULTIPLIER) * 0.00010;
  }

  const nuevoClose = +(last.close + cambio).toFixed(5);
  // --- Fin Cálculo del movimiento ---

  console.log(
    `💧 Iniciando recorrido rápido (Pre-Europa): ${cambio > 0 ? "+" : ""}${(cambio / 0.00010).toFixed(2)} pips (${cambio.toFixed(6)})`,
    `Hora Bogotá: ${hora}:${String(minuto).padStart(2, "0")}`
  );

  try {
    await executeLiquidMove(ref, lastIdx, last, nuevoClose);
    console.log(`✅ Recorrido (Pre-Europa) completado a ${nuevoClose.toFixed(5)}`);
  } catch (error) {
    console.error("Error durante executeLiquidMove (Pre-Europa):", error);
    try {
      await ref.child(lastIdx).update({
        ...last,
        close: nuevoClose,
        high: Math.max(last.high, nuevoClose),
        low: Math.min(last.low, nuevoClose),
      });
      console.warn("Recorrido falló (Pre-Europa), se aplicó actualización directa.");
    } catch (updateError) {
      console.error("Error en actualización directa tras fallo (Pre-Europa):", updateError);
    }
  }

  // --- Programar el siguiente ciclo (CSPRNG, mismo rango) ---
  const delay = randomDelay();
  setTimeout(ciclo, delay);
}

// Iniciar el ciclo con manejo de errores inicial
try {
  ciclo();
} catch (initialError) {
  console.error("Error al iniciar el ciclo (Pre-Europa):", initialError);
  setTimeout(ciclo, 10000);
}
