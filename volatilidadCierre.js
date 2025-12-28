const admin = require("./firebaseApp");
const db = admin.database();

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

function randomMovimientoCierre() {
  let pips;
  if (Math.random() < 0.96) {
    pips = Math.random() * (0.59 - 0.19) + 0.19;
  } else {
    pips = Math.random() * (0.84 - 0.59) + 0.59;
  }
  // Movimiento 4 veces menor
  pips = (pips * 1.3) / 4; 
  const direction = Math.random() < 0.5 ? -1 : 1;
  return direction * +(pips * 0.00010).toFixed(6);
}

async function executeLiquidMove(ref, lastIdx, startCandle, targetClose) {
    const startClose = startCandle.close;
    const totalMovement = targetClose - startClose;
    if (Math.abs(totalMovement) < 0.00000001) {
        await ref.child(lastIdx).update({ ...startCandle, close: targetClose });
        return;
    }
    const numberOfSteps = 10; 
    const stepDelay = 5;      
    const pricePerStep = totalMovement / numberOfSteps;
    let currentHigh = startCandle.high;
    let currentLow = startCandle.low;

    for (let i = 1; i <= numberOfSteps; i++) {
        let intermediateClose = (i === numberOfSteps) ? targetClose : +(startClose + pricePerStep * i).toFixed(5);
        currentHigh = Math.max(currentHigh, intermediateClose);
        currentLow = Math.min(currentLow, intermediateClose);
        await ref.child(lastIdx).update({ ...startCandle, close: intermediateClose, high: currentHigh, low: currentLow });
        if (i < numberOfSteps) await new Promise(r => setTimeout(r, stepDelay));
    }
}

async function cicloCierre() {
  const configSnap = await db.ref("config/auto_volatilidad_cierre").once("value");
  if (!configSnap.val()) return setTimeout(cicloCierre, 5000);

  const { hora } = tsBogota();
  // Horario de 16:00 a 18:00 Bogota
  if (!(hora >= 16 && hora < 18)) return setTimeout(cicloCierre, 10000);

  const ref = db.ref("market_data/M1");
  const snap = await ref.orderByKey().limitToLast(1).once("value");
  const M1 = snap.val() || {};
  const lastIdx = Object.keys(M1)[0];
  const last = M1[lastIdx];

  if (last && typeof last.close === 'number') {
    const cambio = randomMovimientoCierre();
    const nuevoClose = +(last.close + cambio).toFixed(5);
    try {
        await executeLiquidMove(ref, lastIdx, last, nuevoClose);
    } catch (e) {
        console.error("Error en Cierre:", e);
    }
  }
  setTimeout(cicloCierre, randomDelay());
}

cicloCierre();
