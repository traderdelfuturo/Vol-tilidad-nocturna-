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
// Ajuste pedido: +39% tamaño movimientos
// ======================================================
const MOVEMENT_MULTIPLIER = 3.9 * 1.39; // 5.421 (equivale a +39% sobre el 3.9 original)

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

// Movimiento aleatorio (misma lógica, CSPRNG + +39% tamaño)
function randomMovimiento() {
  let pips;

  // Misma estructura: 95% rango bajo, 5% rango alto
  if (cryptoRandomFloat() < 0.95) {
    pips = cryptoRandomFloat() * (0.277 - 0.10) + 0.10;
  } else {
    pips = cryptoRandomFloat() * (0.348 - 0.277) + 0.277;
  }

  // +39% aplicado aquí (sobre tu 3.9 original)
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

async f
