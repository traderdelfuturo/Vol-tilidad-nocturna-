const admin = require("./firebaseApp"); // Asegúrate que la ruta sea correcta
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

function randomDelay() {
  return Math.floor(Math.random() * (5000 - 200 + 1)) + 200;
}

// Movimiento aleatorio AJUSTADO (4 veces menor)
function randomMovimientoCierre() {
  let pips;
  if (Math.random() < 0.96) {
    pips = Math.random() * (0.59 - 0.19) + 0.19;
  } else {
    pips = Math.random() * (0.84 - 0.59) + 0.59;
  }
  
  // MODIFICACIÓN 1: 4 veces menos movimiento (1.3 / 4 = 0.325)
  pips = (pips * 1.3) / 4; 

  const direction = Math.random() < 0.5 ? -1 : 1;
  const movimiento = direction * +(pips * 0.00010).toFixed(6);
  return movimiento;
}

async function executeLiquidMove(ref, lastIdx, startCandle, targetClose) {
    const startClose = startCandle.close;
    const totalMovement = targetClose - startClose;
    const numberOfSteps = 10; 
    const stepDelay = 5;      

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
    let updatePromises = [];

    for (let i = 1; i <= numberOfSteps; i++) {
        let intermediateClose = (i === numberOfSteps) ? targetClose : +(startClose + pricePerStep * i).toFixed(5);
        currentHigh = Math.max(currentHigh, intermediateClose);
        currentLow = Math.min(currentLow, intermediateClose);

        updatePromises.push(ref.child(lastIdx).update({
            ...startCandle,
            close: intermediateClose,
            high: currentHigh,
            low: currentLow
        }));

        if (i < numberOfSteps) await new Promise(r => setTimeout(r, stepDelay));
    }
    await Promise.all(updatePromises);
    return { ...startCandle, close: targetClose, high: currentHigh, low: currentLow };
}

async function cicloCierre() {
  // Flag de configuración diferente para no chocar
  const configSnap = await db.ref("config/auto_volatilidad_cierre").once("value");
  const habilitado = configSnap.val();
  
  if (!habilitado) {
    console.log("Volatilidad Cierre desactivada (flag)");
    return setTimeout(cicloCierre, 5000);
  }

  const { hora, minuto } = tsBogota();
  
  // MODIFICACIÓN 2: Horario de 16:00 a 18:00 Colombia
  const dentroHorario = (hora >= 16 && hora < 18);

  if (!dentroHorario) {
    console.log(`Fuera horario Cierre 16:00-18:00 (${hora}:${String(minuto).padStart(2, '0')})`);
    return setTimeout(cicloCierre, 10000);
  }

  const ref = db.ref("market_data/M1");
  const query = ref.orderByKey().limitToLast(1);
  let snap;
  try { snap = await query.once("value"); } catch (e) { return setTimeout(cicloCierre, 5000); }

  const M1 = snap.val() || {};
  const lastIdx = Object.keys(M1)[0];
  const last = M1[lastIdx];

  if (!last || typeof last.close !== 'number') return setTimeout(cicloCierre, 2000);

  let cambio = randomMovimientoCierre();
  const nuevoClose = +(last.close + cambio).toFixed(5);

  console.log(`📉 Cierre Mercado: ${(cambio / 0.00010).toFixed(4)} pips. Hora: ${hora}:${minuto}`);

  try {
      await executeLiquidMove(ref, lastIdx, last, nuevoClose);
  } catch (error) {
      await ref.child(lastIdx).update({ ...last, close: nuevoClose, high: Math.max(last.high, nuevoClose), low: Math.min(last.low, nuevoClose) });
  }

  setTimeout(cicloCierre, randomDelay());
}

// Exportar o iniciar
cicloCierre();
