const admin = require("./firebaseApp"); // << Importa desde firebaseApp.js
const db = admin.database();
const crypto = require("crypto");

// ======================================================
// CSPRNG (crypto) helpers: reemplazo de Math.random()
// ======================================================
const TWO_POW_53 = 9007199254740992; // 2^53

// Float uniforme en [0, 1) con 53 bits (similar resolución a Math.random, pero CSPRNG)
function cryptoRandomFloat() {
  const x = crypto.randomBytes(8).readBigUInt64BE() >> 11n; // 53 bits
  return Number(x) / TWO_POW_53;
}

// Dirección 50/50 (CSPRNG)
function randomDirection() {
  return crypto.randomInt(0, 2) === 0 ? -1 : 1;
}

// 1 de cada 20 (CSPRNG)
function oneIn20() {
  return crypto.randomInt(0, 20) === 0; // 0..19
}

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

// Tiempo aleatorio entre 0.15 y 5 segundos (CSPRNG)
function randomDelay() {
  // Equivalente a: Math.floor(Math.random() * (5000 - 150 + 1)) + 150;
  return crypto.randomInt(150, 5000 + 1); // max exclusivo
}

// Movimiento aleatorio: entre 0.13 y 0.67 pips, AUMENTADO 480% (CSPRNG)
function randomMovimiento() {
  const r = cryptoRandomFloat(); // 0..1
  const pips = (r * (0.67 - 0.13) + 0.13) * 4.8; // 0.13 a 0.67 pips, x4.8
  const direction = randomDirection();
  const movimiento = direction * +(pips * 0.00010).toFixed(6);
  return movimiento;
}

// =======================================================================
// INICIO: FUNCIÓN PARA EL RECORRIDO LÍQUIDO (VERSIÓN RELATIVA ANTI-LATIGAZO)
// El movimiento ya NO se interpola desde una foto vieja hacia un precio
// absoluto: cada paso aplica su DELTA sobre el close VIVO vía transacción.
// Si el libro local corrió el precio entre la lectura y la escritura, el
// mover ondula alrededor del nivel NUEVO en vez de arrastrarlo de vuelta.
// El high/low jamás se pisa con valores viejos: se expande contra lo vivo.
// =======================================================================
async function executeLiquidMove(ref, lastIdx, deltaTotal) {
  // --- PARÁMETROS DE VELOCIDAD MÁXIMA (sin cambios) ---
  const numberOfSteps = 10; // 10 Pasos
  const stepDelay = 5; // 5ms de pausa (Total: 10 * 5 = 50ms)
  // --- FIN AJUSTES ---

  const stepDelta = deltaTotal / numberOfSteps;
  let ultimoClose = null;

  for (let i = 1; i <= numberOfSteps; i++) {
    try {
      const res = await ref.child(lastIdx).transaction((v) => {
        if (v === null || typeof v.close !== "number") return v; // la vela rotó o no existe: no tocar
        const nc = +(v.close + stepDelta).toFixed(5);
        return {
          ...v,
          close: nc,
          high: Math.max(v.high, nc),
          low: Math.min(v.low, nc),
        };
      });
      if (res && res.committed && res.snapshot && res.snapshot.exists()) {
        ultimoClose = res.snapshot.val().close;
      }
    } catch (e) {
      console.error("paso de recorrido falló:", e.message);
    }
    if (i < numberOfSteps) {
      await new Promise((resolve) => setTimeout(resolve, stepDelay));
    }
  }
  return ultimoClose;
}
// =======================================================================
// FIN: FUNCIÓN DE RECORRIDO
// =======================================================================

async function ciclo() {
  // --- Lógica de habilitación y horario (sin cambios) ---
  const configSnap = await db
    .ref("config/auto_volatilidad_complemento_ny")
    .once("value");
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
    console.log(
      `Fuera del horario 08:00 a 16:00 Bogotá (${hora}:${String(minuto).padStart(2, "0")})`
    );
    return setTimeout(ciclo, 10000);
  }
  // --- Fin Lógica de habilitación y horario ---

  // --- Lectura de la última vela (solo para conocer el ÍNDICE vigente) ---
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

  if (!last || typeof last.close !== "number") {
    console.warn("Última vela no encontrada o inválida. Reintentando...");
    return setTimeout(ciclo, 2000);
  }
  // --- Fin Lectura de la última vela ---

  // --- Cálculo del movimiento (CSPRNG, manteniendo lógica y tamaño) ---
  let cambio = randomMovimiento();

  // Ocasionalmente, el máximo permitido (0.67 pips * 4.8) solo 1 de cada 20 movimientos
  if (oneIn20()) {
    const direction = randomDirection();
    cambio = direction * (0.67 * 4.8) * 0.00010; // x4.8
  }
  // --- Fin Cálculo del movimiento (el DELTA viaja tal cual, sin ancla absoluta) ---

  // =======================================================================
  // LLAMADA AL RECORRIDO RELATIVO
  // =======================================================================
  console.log(
    `💧 Iniciando recorrido rápido (NY Comp): ${cambio > 0 ? "+" : ""}${(cambio / 0.00010).toFixed(2)} pips (${cambio.toFixed(6)})`,
    `Hora Bogotá: ${hora}:${String(minuto).padStart(2, "0")}`
  );

  try {
    const fin = await executeLiquidMove(ref, lastIdx, cambio);
    console.log(
      `✅ Recorrido (NY Comp) completado a ${fin === null ? "(vela rotó)" : fin.toFixed(5)}`
    );
  } catch (error) {
    console.error("Error durante executeLiquidMove (NY Comp):", error);
    try {
      await ref.child(lastIdx).transaction((v) => {
        if (v === null || typeof v.close !== "number") return v;
        const nc = +(v.close + cambio).toFixed(5);
        return { ...v, close: nc, high: Math.max(v.high, nc), low: Math.min(v.low, nc) };
      });
      console.warn("Recorrido falló (NY Comp), se aplicó delta directo por transacción.");
    } catch (updateError) {
      console.error("Error en delta directo tras fallo (NY Comp):", updateError);
    }
  }
  // =======================================================================

  // --- Programar el siguiente ciclo (CSPRNG) ---
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
