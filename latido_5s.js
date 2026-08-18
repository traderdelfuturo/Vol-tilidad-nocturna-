/* ═══════════════════════════════════════════════════════════════════════════════════════════
   EL LATIDO — v2
   ═══════════════════════════════════════════════════════════════════════════════════════════

   QUÉ ES. Un metrónomo. NO mueve el precio jamás: no toca `close`, ni `high`, ni `low`.
   Lo único que hace es poner un sello con la hora (`hb`) en la última vela M1 cuando arranca un
   balde de 5 segundos y ningún motor ha escrito nada.

   PARA QUÉ SIRVE. El gráfico sólo despierta cuando Firebase le avisa de que algo cambió. Si el
   mercado se queda quieto, nadie escribe, Firebase no avisa, y la pantalla se queda CONGELADA en
   la vela vieja: no rueda a la siguiente. El sello provoca ese aviso y el reloj sigue andando.

   ¿SIGUE HACIENDO FALTA? SÍ. Los motores v3 dejan silencios reales y medidos: Asia hasta 60 s,
   Pre-Europa 11 s, Nueva York 22 s, Londres 13 s. Sin esto, el gráfico se quedaría clavado hasta
   un minuto entero. Lo que SÍ quedó obsoleto es su segunda función —despertar al motor de velas
   de 5 s—, porque ese motor ya tiene su propio guardián por reloj desde la v2.

   ═══ LOS TRES FALLOS QUE TENÍA ═══

   1 · NO SABÍA RECONOCER SU PROPIO LATIDO. Miraba una marca `__hbOnly` para distinguir su sello
       de un movimiento real... pero al escribir la BORRABA en vez de ponerla. Nadie la ponía
       nunca. Así que se contaba a sí mismo como si fuera mercado vivo.
       Y ponerla tampoco habría bastado: los motores escriben con `{ ...v, close, high, low }`,
       o sea que habrían arrastrado la marca del latido anterior y el problema se invertía.
       AHORA no hay marca ninguna. Se guarda una HUELLA de `close|high|low`: si llega un cambio y
       la huella es idéntica, el precio no se movió y no cuenta como tick. Es exacto, no depende
       de que nadie coopere, y no ensucia la vela con campos de más.

   2 · `return v` CON LA VELA VACÍA. En Realtime Database una transacción sólo se aborta
       devolviendo `undefined`. Devolver `v` cuando `v` es null es pedir un BORRADO: el servidor
       lo rechaza, pero se gasta un viaje de red por nada. AHORA es `return;`.

   3 · MARCABA EL BALDE ANTES DE LATIR. Ponía `lastPulseBucket = b` antes del `await`. Si la
       escritura fallaba, el balde quedaba marcado como latido igual y ese balde se perdía.
       AHORA se marca DESPUÉS, y sólo si la escritura salió bien.
   ═══════════════════════════════════════════════════════════════════════════════════════════ */

const admin = require("./firebaseApp");
const db = admin.database();

const SECONDS_PER_BAR = 5;
const SILENCIO_MS = 1200;   // si hubo un precio de verdad hace menos de esto, el latido se calla
const REVISION_MS = 1000;   // cada cuánto se comprueba si toca latir

const bucket5s = () =>
  Math.floor(Math.floor(Date.now() / 1000) / SECONDS_PER_BAR) * SECONDS_PER_BAR;

console.log("LATIDO 5S v2 iniciado — metronomo, no toca el precio jamas");

const mem = {
  lastIdx: null,      // la vela M1 vigente
  lastTick: 0,        // cuándo hubo el último PRECIO de verdad
  lastPulse: null,    // último balde en el que ya se latió
  huella: null,       // close|high|low de la última vela vista
};

const refM1 = db.ref("market_data/M1");

/* LA HUELLA. Sólo mira los tres campos que importan. Si un cambio llega y la huella es la misma,
   es que sólo cambió el sello `hb` — o sea, fue este mismo latido, y no cuenta como mercado. */
function huellaDe(v) {
  if (!v || typeof v.close !== "number") return null;
  return v.close + "|" + v.high + "|" + v.low;
}

const q = refM1.orderByKey().limitToLast(1);

q.on("child_added", (snap) => {
  mem.lastIdx = snap.key;
  mem.huella = huellaDe(snap.val());
  mem.lastTick = Date.now(); // una vela nueva SIEMPRE es actividad real
});

q.on("child_changed", (snap) => {
  const v = snap.val();
  mem.lastIdx = snap.key;
  const h = huellaDe(v);
  // el precio sólo se movió de verdad si la huella cambió
  if (h !== null && h !== mem.huella) mem.lastTick = Date.now();
  mem.huella = h;
});

setInterval(async () => {
  try {
    const b = bucket5s();
    if (b === mem.lastPulse) return;                      // ya se latió en este balde
    if (Date.now() - mem.lastTick < SILENCIO_MS) return;  // el mercado está vivo: silencio
    if (!mem.lastIdx) return;                             // todavía no se sabe qué vela es

    const res = await refM1.child(mem.lastIdx).transaction((v) => {
      /* `return;` (undefined) es lo ÚNICO que aborta una transacción en Realtime Database.
         Y se toca EXCLUSIVAMENTE `hb`: close, high y low quedan intactos, siempre. */
      if (v === null || typeof v.close !== "number") return;
      return { ...v, hb: Date.now() };
    });

    // el balde se marca DESPUÉS y sólo si de verdad se escribió
    if (res && res.committed) mem.lastPulse = b;
  } catch (e) {
    console.error("latido:", e && e.message ? e.message : e);
  }
}, REVISION_MS);
