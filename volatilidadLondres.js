const admin = require("./firebaseApp"); // Asegúrate que la ruta sea correcta
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

// Tiempo aleatorio entre 0.20 y 4 segundos (sin cambios)
function randomDelay() {
  return Math.floor(Math.random() * (4000 - 200 + 1)) + 200;
}

// Movimiento aleatorio (sin cambios)
function randomMovimiento() {
  let pips;
  // 96% de las veces, entre 0.24 y 0.52
  if (Math.random() < 0.96) {
    pips = Math.random() * (0.52 - 0.24) + 0.24;
  } else {
    // 4% de las veces, entre 0.52 y 0.667
    pips = Math.random() * (0.667 - 0.52) + 0.52;
  }
  pips = pips * 2.0; // AUMENTO DEL 200%
  const direction = Math.random() < 0.5 ? -1 : 1;
  const movimiento = direction * +(pips * 0.00010).toFixed(6);
  return movimiento;
}

// =======================================================================
// INICIO: FUNCIÓN PARA EL RECORRIDO LÍQUIDO (VERSIÓN ULTRARRÁPIDA)
// (Exactamente la misma función que en los scripts anteriores)
// =======================================================================
async function executeLiquidMove(ref, lastIdx, startCandle, targetClose) {
    const startClose = startCandle.close;
    const totalMovement = targetClose - startClose;

    // --- PARÁMETROS DE VELOCIDAD MÁXIMA ---
    const numberOfSteps = 10; // 10 Pasos
    const stepDelay = 5;      // 5ms de pausa (Total: 10 * 5 = 50ms)
    // --- FIN AJUSTES ---

    if (Math.abs(totalMovement) < 0.00000001) {
        await ref.child(lastIdx).update({
             ...startCandle,
             close: targetClose,
             high: Math.max(startCandle.high, targetClose),
             low: Math.min(startCandle.low, targetClose)
        });
        return { ...startCandle, close: targetClose, high: Math.max(startCandle.high, targetClose), low: Math.min(startCandle.low, targetClose) };
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
            low: currentLow
        };

        updatePromises.push(ref.child(lastIdx).update(updatedStep));

        if (i < numberOfSteps) {
            await new Promise(resolve => setTimeout(resolve, stepDelay));
        }
    }
    await Promise.all(updatePromises);

    const finalCandle = { ...startCandle, close: targetClose, high: currentHigh, low: currentLow };
    await ref.child(lastIdx).update(finalCandle);

    return finalCandle;
}
// =======================================================================
// FIN: FUNCIÓN DE RECORRIDO
// =======================================================================


async function ciclo() {
  // --- Lógica de habilitación y horario (sin cambios) ---
  const configSnap = await db.ref("config/auto_volatilidad_londres").once("value");
  const habilitado = configSnap.val();
  if (!habilitado) {
    console.log("Volatilidad Londres desactivada (flag)");
    return setTimeout(ciclo, 5000);
  }
  const { hora, minuto } = tsBogota();
  const dentroHorario =
    (hora >= 2 && hora < 8) || // Simplificado: entre las 2:00 y las 7:59
    (hora === 8 && minuto === 0); // Exactamente las 8:00

  if (!dentroHorario) {
    console.log(`Fuera del horario 02:00 a 08:00 Bogotá (${hora}:${String(minuto).padStart(2, '0')})`);
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
      return setTimeout(ciclo, 5000); // Reintentar después de un error
  }
  const M1 = snap.val() || {};
  const lastIdx = Object.keys(M1)[0];
  const last = M1[lastIdx];
   if (!last || typeof last.close !== 'number') { // Verificar que 'last' y 'last.close' existan y sean válidos
      console.warn("Última vela no encontrada o inválida. Reintentando...");
      return setTimeout(ciclo, 2000);
  }
  // --- Fin Lectura de la última vela ---

  // --- Cálculo del movimiento (sin cambios) ---
  let cambio = randomMovimiento();
  // Ocasionalmente, el máximo absoluto (0.667 pips * 2.0) solo 1 de cada 40 movimientos
  if (Math.floor(Math.random() * 40) === 0) {
    const direction = Math.random() < 0.5 ? -1 : 1;
    cambio = direction * (0.667 * 2.0) * 0.00010; // AUMENTO DEL 200%
  }
  const nuevoClose = +(last.close + cambio).toFixed(5);
  // --- Fin Cálculo del movimiento ---

  // =======================================================================
  // INICIO: MODIFICACIÓN - LLAMAR A LA FUNCIÓN DE RECORRIDO
  // =======================================================================
  console.log(
    `💧 Iniciando recorrido rápido (Londres): ${cambio > 0 ? '+' : ''}${(cambio / 0.00010).toFixed(2)} pips (${cambio.toFixed(6)})`,
    `Hora Bogotá: ${hora}:${String(minuto).padStart(2, '0')}`
  );

  try {
      await executeLiquidMove(ref, lastIdx, last, nuevoClose);
      console.log(`✅ Recorrido (Londres) completado a ${nuevoClose.toFixed(5)}`);
  } catch (error) {
      console.error("Error durante executeLiquidMove (Londres):", error);
      try {
          await ref.child(lastIdx).update({
              ...last,
              close: nuevoClose,
              high: Math.max(last.high, nuevoClose),
              low: Math.min(last.low, nuevoClose)
          });
           console.warn("Recorrido falló (Londres), se aplicó actualización directa.");
      } catch (updateError) {
          console.error("Error en actualización directa tras fallo (Londres):", updateError);
      }
  }
  // =======================================================================
  // FIN: MODIFICACIÓN
  // =======================================================================

  // --- Programar el siguiente ciclo (sin cambios) ---
  const delay = randomDelay();
  setTimeout(ciclo, delay);
}

// Iniciar el ciclo con manejo de errores inicial
try {
    ciclo();
} catch (initialError) {
    console.error("Error al iniciar el ciclo (Londres):", initialError);
    setTimeout(ciclo, 10000);
}
