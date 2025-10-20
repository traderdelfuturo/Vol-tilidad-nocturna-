const admin = require("./firebaseApp"); // << Importa desde firebaseApp.js
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

// Tiempo aleatorio entre 0.15 y 5 segundos (sin cambios)
function randomDelay() {
  return Math.floor(Math.random() * (5000 - 150 + 1)) + 150;
}

// Movimiento aleatorio: entre 0.13 y 0.67 pips, AUMENTADO 480% (sin cambios)
function randomMovimiento() {
  const pips = (Math.random() * (0.67 - 0.13) + 0.13) * 4.8; // 0.13 a 0.67 pips, x4.8
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
            intermediateClose = +(startClose + pricePerStep * i).toFixed(5); // Usar 5 decimales
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
  const configSnap = await db.ref("config/auto_volatilidad_complemento_ny").once("value");
  const habilitado = configSnap.val();
  if (!habilitado) {
    console.log("Volatilidad Complemento NY desactivada (flag)");
    return setTimeout(ciclo, 5000);
  }
  const { hora, minuto } = tsBogota();
  // Horario: de 8:00 am a 16:00 pm Bogotá
  const dentroHorario =
    (hora >= 8 && hora < 16) || // Simplificado: 8:00 hasta 15:59
    (hora === 16 && minuto === 0); // Exactamente las 16:00

  if (!dentroHorario) {
    console.log(`Fuera del horario 08:00 a 16:00 Bogotá (${hora}:${String(minuto).padStart(2, '0')})`);
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
  // Ocasionalmente, el máximo permitido (0.67 pips * 4.8) solo 1 de cada 20 movimientos
  if (Math.floor(Math.random() * 20) === 0) {
    const direction = Math.random() < 0.5 ? -1 : 1;
    cambio = direction * (0.67 * 4.8) * 0.00010; // x4.8
  }
  const nuevoClose = +(last.close + cambio).toFixed(5);
  // --- Fin Cálculo del movimiento ---

  // =======================================================================
  // INICIO: MODIFICACIÓN - LLAMAR A LA FUNCIÓN DE RECORRIDO
  // =======================================================================
  console.log(
    `💧 Iniciando recorrido rápido (NY Comp): ${cambio > 0 ? '+' : ''}${(cambio / 0.00010).toFixed(2)} pips (${cambio.toFixed(6)})`,
    `Hora Bogotá: ${hora}:${String(minuto).padStart(2, '0')}`
  );

  try {
      await executeLiquidMove(ref, lastIdx, last, nuevoClose);
      console.log(`✅ Recorrido (NY Comp) completado a ${nuevoClose.toFixed(5)}`);
  } catch (error) {
      console.error("Error durante executeLiquidMove (NY Comp):", error);
      try {
          await ref.child(lastIdx).update({
              ...last,
              close: nuevoClose,
              high: Math.max(last.high, nuevoClose),
              low: Math.min(last.low, nuevoClose)
          });
           console.warn("Recorrido falló (NY Comp), se aplicó actualización directa.");
      } catch (updateError) {
          console.error("Error en actualización directa tras fallo (NY Comp):", updateError);
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
    console.error("Error al iniciar el ciclo (NY Comp):", initialError);
    setTimeout(ciclo, 10000);
}
