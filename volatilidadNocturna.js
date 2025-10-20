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
// INICIO: FUNCIÓN PARA EL RECORRIDO LÍQUIDO (VERSIÓN ULTRARRÁPIDA)
// =======================================================================
async function executeLiquidMove(ref, lastIdx, startCandle, targetClose) {
    const startClose = startCandle.close;
    const totalMovement = targetClose - startClose;

    // --- PARÁMETROS DE VELOCIDAD MÁXIMA ---
    const numberOfSteps = 10; // 10 Pasos
    const stepDelay = 5;      // 5ms de pausa (Total: 10 * 5 = 50ms)
    // --- FIN AJUSTES ---

    // Evitar división por cero si no hay movimiento
    if (Math.abs(totalMovement) < 0.00000001) {
        // Si no hay movimiento, solo actualiza una vez para asegurar el estado final
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
    let updatePromises = []; // Array para guardar las promesas de actualización

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
            ...startCandle, // Mantener open y time originales
            close: currentClose,
            high: currentHigh,
            low: currentLow
        };

        // Enviar la actualización a Firebase SIN await para máxima velocidad
        // Guardamos la promesa para asegurar que al menos se envíe
        updatePromises.push(ref.child(lastIdx).update(updatedStep));

        // Pausa ultracorta antes del siguiente paso
        if (i < numberOfSteps) {
            await new Promise(resolve => setTimeout(resolve, stepDelay));
        }
    }

    // Esperar a que todas las actualizaciones se hayan enviado (aunque no necesariamente completado)
    await Promise.all(updatePromises);

    // Asegurar el estado final con una última actualización CON await
    const finalCandle = { ...startCandle, close: targetClose, high: currentHigh, low: currentLow };
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
    console.log(`Fuera del horario 18:00 a 23:40 Bogotá (${hora}:${String(minuto).padStart(2, '0')})`);
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
    `💧 Iniciando recorrido rápido: ${cambio > 0 ? '+' : ''}${(cambio / 0.00010).toFixed(2)} pips (${cambio.toFixed(6)})`,
    `Hora Bogotá: ${hora}:${String(minuto).padStart(2, '0')}`
  );

  try {
      // Ejecutamos el recorrido líquido ultrarrápido
      await executeLiquidMove(ref, lastIdx, last, nuevoClose);
      console.log(`✅ Recorrido completado a ${nuevoClose.toFixed(5)}`);
  } catch (error) {
      console.error("Error durante executeLiquidMove:", error);
      // Opcional: intentar una actualización directa si la animación falla
      try {
          await ref.child(lastIdx).update({
              ...last,
              close: nuevoClose,
              high: Math.max(last.high, nuevoClose),
              low: Math.min(last.low, nuevoClose)
          });
           console.warn("Recorrido falló, se aplicó actualización directa.");
      } catch (updateError) {
          console.error("Error en actualización directa tras fallo de recorrido:", updateError);
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
    console.error("Error al iniciar el ciclo:", initialError);
    // Intentar reiniciar después de un breve retraso
    setTimeout(ciclo, 10000);
}
