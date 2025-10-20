const admin = require("./firebaseApp"); // Asegúrate que la ruta a tu config de Firebase sea correcta
const db = admin.database();

// Utilidad para obtener hora y minuto de Bogotá (sin cambios)
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

// Tiempo aleatorio entre 0.3 y 7 segundos (sin cambios)
function randomDelay() {
  return Math.floor(Math.random() * (7000 - 300 + 1)) + 300;
}

// Movimiento aleatorio (sin cambios)
function randomMovimiento() {
  let pips;
  if (Math.random() < 0.95) {
    pips = Math.random() * (0.277 - 0.10) + 0.10;
  } else {
    pips = Math.random() * (0.348 - 0.277) + 0.277;
  }
  pips = pips * 3.9;
  const direction = Math.random() < 0.5 ? -1 : 1;
  const movimiento = direction * +(pips * 0.00010).toFixed(6);
  return movimiento;
}

// =======================================================================
// INICIO: NUEVA FUNCIÓN ASÍNCRONA PARA EL RECORRIDO LÍQUIDO
// =======================================================================
async function executeLiquidMove(ref, lastIdx, startCandle, targetClose) {
    const startClose = startCandle.close;
    const totalMovement = targetClose - startClose;
    const numberOfSteps = 10;
    const stepDelay = 20; // 20ms entre pasos (Total: 200ms)
    const pricePerStep = totalMovement / numberOfSteps;

    let currentHigh = startCandle.high;
    let currentLow = startCandle.low;
    let currentClose = startCandle.close;

    for (let i = 1; i <= numberOfSteps; i++) {
        // Calcular el precio del paso actual
        let intermediateClose;
        if (i === numberOfSteps) {
            intermediateClose = targetClose; // Asegurar el precio final exacto
        } else {
            intermediateClose = +(startClose + pricePerStep * i).toFixed(5); // Usar 5 decimales
        }

        currentClose = intermediateClose;
        currentHigh = Math.max(currentHigh, currentClose);
        currentLow = Math.min(currentLow, currentClose);

        const updatedStep = {
            ...startCandle, // Mantener open y time originales de la vela
            close: currentClose,
            high: currentHigh,
            low: currentLow
        };

        // Enviar la actualización a Firebase
        await ref.child(lastIdx).update(updatedStep);

        // Esperar brevemente antes del siguiente paso (excepto en el último)
        if (i < numberOfSteps) {
            await new Promise(resolve => setTimeout(resolve, stepDelay));
        }
    }
     // Devolver la vela final actualizada por si se necesita
     return { ...startCandle, close: targetClose, high: currentHigh, low: currentLow };
}
// =======================================================================
// FIN: NUEVA FUNCIÓN
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
    console.log("Fuera del horario 18:00 a 23:40 Bogotá");
    return setTimeout(ciclo, 10000);
  }
  // --- Fin Lógica de habilitación y horario ---

  // --- Lectura de la última vela (sin cambios) ---
  const ref = db.ref("market_data/M1");
  const query = ref.orderByKey().limitToLast(1);
  const snap = await query.once("value");
  const M1 = snap.val() || {};
  const lastIdx = Object.keys(M1)[0];
  const last = M1[lastIdx];
  if (!last) return setTimeout(ciclo, 2000);
  // --- Fin Lectura de la última vela ---

  // --- Cálculo del movimiento (sin cambios) ---
  let cambio = randomMovimiento();
  if (Math.floor(Math.random() * 45) === 0) {
    const direction = Math.random() < 0.5 ? -1 : 1;
    cambio = direction * (0.348 * 3.9) * 0.00010;
  }
  const nuevoClose = +(last.close + cambio).toFixed(5);
  // --- Fin Cálculo del movimiento ---

  // =======================================================================
  // INICIO: MODIFICACIÓN - LLAMAR A LA FUNCIÓN DE RECORRIDO
  // =======================================================================
  console.log(
    `💧 Iniciando recorrido: ${cambio > 0 ? '+' : ''}${(cambio / 0.00010).toFixed(2)} pips (${cambio.toFixed(6)})`,
    `Hora Bogotá: ${hora}:${String(minuto).padStart(2, '0')}`
  );

  // En lugar de hacer un solo update, ejecutamos el recorrido líquido
  await executeLiquidMove(ref, lastIdx, last, nuevoClose);

  console.log(`✅ Recorrido completado a ${nuevoClose.toFixed(5)}`);
  // =======================================================================
  // FIN: MODIFICACIÓN
  // =======================================================================


  // --- Programar el siguiente ciclo (sin cambios) ---
  const delay = randomDelay();
  setTimeout(ciclo, delay);
}

ciclo();
