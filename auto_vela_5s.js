/* ═══════════════════════════════════════════════════════════════════════════════════════════
   MOTOR DE VELAS DE 5 SEGUNDOS — v2
   ═══════════════════════════════════════════════════════════════════════════════════════════

   Esta función NO mueve el precio jamás. Sólo lo GUARDA: escucha cada cambio de la vela M1 y
   va construyendo con él las velas de 5 segundos que se ven en el gráfico.

   POR QUÉ SE REHIZO. Al recargar, las velas salían distintas de las que se habían visto en vivo.
   Tres causas, las tres reales:

   1 · CARRERA ENTRE DOS OYENTES. `child_added` y `child_changed` llamaban los dos a la misma
       función `async`, que tenía dos `await` dentro. Con dos precios seguidos, las dos llamadas
       corrían SOLAPADAS y se pisaban la memoria de la cadena (`lastB`, `lastClose`). Resultado:
       huecos rellenados dos veces, o una vela abierta con el cierre equivocado. Con los motores
       viejos (0,4 escrituras/s) casi no pasaba; con los nuevos (2-3/s) pasa a menudo.
       AHORA: todo entra por una COLA. Nada se solapa nunca, por rápido que llegue.

   2 · `_status` VIVÍA DENTRO DE LAS VELAS. Se escribía en `history_5s/_status`, mezclado con
       ellas. Un nodo sin `time`, sin `open` y sin `close` en medio de la lista de velas: si el
       gráfico recorre los hijos, se lo encuentra. AHORA vive fuera, en `status/velas_5s`.

   3 · UNA ESCRITURA POR CADA MOVIMIENTO. Cada tick disparaba su propia transacción. Con los
       motores nuevos son 2-3 por segundo, seis veces más que antes.
       AHORA se agrupan: se acumulan en memoria el máximo, el mínimo y el último precio, y se
       vuelca una sola vez cada 250 ms. NO SE PIERDE NI UN EXTREMO —los máximos y mínimos se
       guardan mientras tanto— y las escrituras bajan de ~2,5/s a 4/s como mucho... en realidad
       a 4/s de tope pero típicamente 4 por vela en vez de 12-15. Y el cierre de cada balde se
       vuelca SIEMPRE en su propio balde, aunque el volcado caiga después: el balde va marcado.

   LO QUE NO CAMBIA: la cadena sin huecos (los baldes que falten se rellenan planos con el cierre
   previo), el tope de relleno de 2 horas, la limpieza de histórico, y que la apertura de cada
   vela encadena con el cierre de la anterior para que no aparezcan saltos al recargar.
   ═══════════════════════════════════════════════════════════════════════════════════════════ */

const admin = require("./firebaseApp");
const db = admin.database();

// ── Configuración ────────────────────────────────────────────────────────────
const HISTORY_LIMIT = 17280;  // velas que se conservan (17.280 × 5 s = 24 horas exactas)
const SECONDS_PER_BAR = 5;
const FLUSH_MS = 250;         // cada cuánto se vuelca lo acumulado
const MAX_FILL = 2400;        // tope de relleno por evento (2 h). Más allá, hueco histórico honesto.

const bucket5s = () => {
  const now = Math.floor(Date.now() / 1000);
  return Math.floor(now / SECONDS_PER_BAR) * SECONDS_PER_BAR;
};

console.log("🚀 MOTOR 5S v2 — cola serializada, volcado agrupado, sin _status en las velas");

// ══════════════════════════════════════════════════════════════════════════════
// LA COLA. Todo lo que escriba en Firebase pasa por aquí, uno detrás de otro.
// Es lo que impide que dos precios seguidos se pisen la memoria de la cadena.
// ══════════════════════════════════════════════════════════════════════════════
let cadena = Promise.resolve();
function enCola(tarea) {
  cadena = cadena.then(tarea).catch((e) => console.error("5s cola:", e && e.message ? e.message : e));
  return cadena;
}

// ── Memoria de la cadena: último balde escrito y su cierre ───────────────────
const mem = { lastB: null, lastClose: null };

// ── Lo acumulado desde el último volcado ─────────────────────────────────────
// Guarda el balde al que pertenece, para que un volcado tardío no se equivoque de vela.
let pend = null;

async function inicializarCadena() {
  try {
    const snap = await db.ref("history_5s").orderByKey().limitToLast(4).once("value");
    let mejor = null;
    snap.forEach((ch) => {
      const v = ch.val();
      // se ignora cualquier nodo que no sea una vela de verdad (por si quedó basura vieja)
      if (v && typeof v.time === "number" && typeof v.close === "number" && isFinite(v.close)) {
        if (!mejor || v.time > mejor.time) mejor = v;
      }
    });
    if (mejor) {
      mem.lastB = mejor.time;
      mem.lastClose = mejor.close;
      console.log("🔗 cadena reanudada en", mem.lastB, "cierre", mem.lastClose);
    } else {
      console.log("🔗 sin histórico previo: la cadena empezará con el primer precio");
    }
    /* Limpieza de una sola vez: si el _status viejo quedó dentro de las velas, se va. Era él
       quien se colaba en el gráfico como una vela sin datos. */
    await db.ref("history_5s/_status").remove();
  } catch (e) {
    console.error("init cadena:", e.message);
  }
}

/* Rellena los baldes que se saltaron con velas PLANAS al cierre previo. Es la verdad de un
   mercado quieto: no pasó nada, así que la vela no tiene cuerpo. Sin esto, al recargar
   aparecerían huecos donde en vivo se veía una línea continua. */
async function rellenarHuecos(hastaB) {
  if (mem.lastB === null || mem.lastClose === null) return;
  const desde = mem.lastB + SECONDS_PER_BAR;
  if (desde >= hastaB) return;
  const faltan = Math.floor((hastaB - desde) / SECONDS_PER_BAR);
  if (faltan <= 0) return;
  const inicio = faltan > MAX_FILL ? hastaB - MAX_FILL * SECONDS_PER_BAR : desde;
  const updates = {};
  for (let m = inicio; m < hastaB; m += SECONDS_PER_BAR) {
    updates[m] = {
      time: m,
      open: mem.lastClose,
      high: mem.lastClose,
      low: mem.lastClose,
      close: mem.lastClose,
    };
  }
  const n = Object.keys(updates).length;
  if (n === 0) return;
  await db.ref("history_5s").update(updates);
  if (n > 1) console.log(`🧱 ${n} baldes planos rellenados hasta ${hastaB}`);
}

/* EL VOLCADO. Escribe de una sola vez el máximo, el mínimo y el cierre acumulados desde el
   volcado anterior. Va siempre dentro de la cola, así que nunca se solapa con otro. */
async function volcar(p) {
  /* Recibe lo que hay que volcar COMO PARÁMETRO, nunca leyendo la variable global. Leerla era
     un fallo de pérdida de datos: entre que se encolaba el volcado y que la cola lo ejecutaba,
     ya había entrado el primer precio de la vela siguiente — y al ejecutarse, lo pisaba. Se
     perdía el primer precio de cada vela. */
  if (!p) return;

  // si se saltaron baldes desde el último escrito, primero se rellenan
  if (mem.lastB !== null && p.b > mem.lastB + SECONDS_PER_BAR) {
    await rellenarHuecos(p.b);
    mem.lastB = p.b - SECONDS_PER_BAR;
  }

  /* La apertura ENCADENA con el cierre de la vela anterior. Así no aparecen saltos entre una
     vela y la siguiente al recargar el gráfico. Sólo se usa si esta vela es nueva. */
  const apertura =
    mem.lastB !== null && mem.lastClose !== null && p.b > mem.lastB ? mem.lastClose : p.close;

  await db.ref(`history_5s/${p.b}`).transaction((v) => {
    if (v === null) {
      return {
        time: p.b,
        open: apertura,
        high: Math.max(apertura, p.high),
        low: Math.min(apertura, p.low),
        close: p.close,
      };
    }
    // la vela ya existe: se respeta su apertura y se amplían los extremos
    const high = typeof v.high === "number" && isFinite(v.high) ? Math.max(v.high, p.high) : p.high;
    const low = typeof v.low === "number" && isFinite(v.low) ? Math.min(v.low, p.low) : p.low;
    return { ...v, time: p.b, high, low, close: p.close };
  });

  if (mem.lastB === null || p.b >= mem.lastB) {
    mem.lastB = p.b;
    mem.lastClose = p.close;
  }
}

/* LA ENTRADA. Aquí llega cada precio. No escribe: sólo acumula. Y si el precio pertenece a un
   balde distinto del que se venía acumulando, se vuelca el anterior INMEDIATAMENTE para que su
   cierre quede bien y no se mezcle con la vela nueva. */
function anotarPrecio(price) {
  if (typeof price !== "number" || !isFinite(price)) return;
  const b = bucket5s();
  if (pend && pend.b !== b) {
    const cerrado = pend;
    pend = null;
    enCola(() => volcar(cerrado));
  }
  if (!pend) pend = { b, high: price, low: price, close: price };
  else {
    pend.high = Math.max(pend.high, price);
    pend.low = Math.min(pend.low, price);
    pend.close = price;
  }
}

// volcado periódico de lo acumulado
setInterval(() => {
  if (!pend) return;
  const p = pend;
  pend = null; /* se limpia ya: los extremos viven en Firebase y la transacción los amplía */
  enCola(() => volcar(p));
}, FLUSH_MS);

/* GUARDIÁN DE CONTINUIDAD. Si el mercado se queda quieto y nadie escribe, los baldes siguen
   pasando. Cada 5 s se comprueba y, si faltan, se rellenan planos. Así la línea del gráfico
   nunca tiene agujeros aunque el precio no se mueva. */
setInterval(() => {
  enCola(async () => {
    if (mem.lastB === null || mem.lastClose === null) return;
    const b = bucket5s();
    if (b > mem.lastB + SECONDS_PER_BAR) {
      await rellenarHuecos(b);
      mem.lastB = b - SECONDS_PER_BAR;
    }
  });
}, SECONDS_PER_BAR * 1000);

/* LIMPIEZA. Borra lo más viejo para no engordar Firebase sin fin. Se ejecuta cada 20 s y borra
   de 50 en 50; con 720 velas nuevas por hora va de sobra. */
function limpiarViejas() {
  const corte = Math.floor(Date.now() / 1000) - HISTORY_LIMIT * SECONDS_PER_BAR;
  const ref = db.ref("history_5s");
  ref
    .orderByKey()
    .endAt(String(corte))
    .limitToFirst(50)
    .once("value", (snap) => {
      if (!snap.exists()) return;
      const updates = {};
      snap.forEach((ch) => {
        updates[ch.key] = null;
      });
      ref.update(updates).catch((e) => console.error("⚠️ limpieza:", e.message));
    });
}
setInterval(limpiarViejas, 20000);

// ── Oyentes: los dos entran por la misma puerta y la cola los ordena ─────────
const refM1 = db.ref("market_data/M1");
refM1.orderByKey().limitToLast(1).on("child_added", (snap) => {
  const vela = snap.val();
  if (vela && typeof vela.close === "number") anotarPrecio(vela.close);
});
refM1.orderByKey().limitToLast(1).on("child_changed", (snap) => {
  const vela = snap.val();
  if (vela && typeof vela.close === "number") anotarPrecio(vela.close);
});

inicializarCadena();

/* Señal de vida. FUERA de `history_5s`, para que no se cuele entre las velas. */
db.ref("status/velas_5s").set({ activo: true, arrancado: Date.now(), version: "5s-v2" });
