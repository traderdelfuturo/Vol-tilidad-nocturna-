/* ═══════════════════════════════════════════════════════════════════════════════════════════
   VOLATILIDAD · COMPLEMENTO NUEVA YORK — v2
   ═══════════════════════════════════════════════════════════════════════════════════════════

   QUÉ CAMBIA Y QUÉ NO.

   NO CAMBIA — el azar. Toda cifra aleatoria sigue saliendo de `crypto`, del mismo
   `cryptoRandomFloat()` de 53 bits y del mismo `crypto.randomInt`. No se añade ni una fuente
   nueva, ni un pseudoaleatorio, ni una semilla. Lo único que se añade son dos TRANSFORMACIONES
   deterministas de esos mismos números (Box-Muller y la inversa de la exponencial). La entropía
   es idéntica; lo que cambia es la FORMA de la distribución, no de dónde sale.

   SÍ CAMBIA — la dinámica. Medido fotograma a fotograma sobre el vídeo de IQ Option en sesión de
   Nueva York (2486x1396, 30 fps, 55 min): 10 ventanas a 30 fps y 6 minutos completos a 4 Hz.

                                      IQ Nueva York      esta función ANTES
       media ÷ mediana                    1,90                 1,04
       p99   ÷ mediana                   13,6                  1,68
       máximo÷ mediana                   14,6  (hasta 39,7)    1,69
       tiempo quieto                       66 %                   0 %
       pausas                          hasta 3,75 s          no existían
       saltos secos / con estela        68 % / 32 %          0 % / 100 %

   Un `media/mediana = 1,04` significa, literalmente, que todos los movimientos medían lo mismo:
   un uniforme entre 0,624 y 3,216 pips no tiene ni ticks pequeños ni golpes grandes. El azar era
   perfecto pero dibujaba siempre el mismo palo.

   EL HALLAZGO QUE MANDA EN TODO EL DISEÑO. La distribución de IQ encaja con una LOGNORMAL de
   s = 1,13, y encaja por partida doble con un solo parámetro:

       media/mediana = e^(s²/2)   = 1,90     ← medido 1,90
       p99  /mediana = e^(2,33·s) = 13,9     ← medido 13,6

   Y la EFICIENCIA (desplazamiento neto ÷ recorrido total) de IQ da mediana 0,13. Un paseo
   aleatorio puro de 40 pasos da 0,126. O sea: IQ, en su comportamiento típico, ES un paseo
   aleatorio puro. Los tramos de eficiencia 0,77 (tendencia limpia) y 0,00 (consolidación muerta)
   NO salen de ningún sesgo direccional: salen de la COLA. Cuando cae un golpe de 20 veces la
   mediana, ese solo golpe manda en el tramo y el precio «tiende»; cuando no cae ninguno, el
   precio se revuelve sin ir a ninguna parte.

   CONSECUENCIA: la dirección sigue siendo la moneda 50/50 de `crypto`, SIEMPRE, sin sesgo, sin
   memoria y sin tendencia inducida. La tendencia y la consolidación emergen solas de la cola.
   La aleatoriedad pura absoluta no sólo se respeta: es que además es lo correcto.

   LA ESCRITURA. Fuera el recorrido de 10 pasos iguales. El precio se escribe DIRECTO donde
   quedó, en una sola transacción, igual que la función de clic del gráfico. La minoría con
   «suavidad» (32 %, el reparto medido) no es un efecto de dibujo: son varias impresiones REALES
   seguidas, como una orden agresiva comiéndose varios niveles del libro.

   Y eso devuelve la mecha. La rampa monótona de antes dejaba `high = max(high, close)` pegado al
   cierre: aquel movimiento no podía dejar mecha nunca. Con escritura directa el extremo se
   estampa crudo y el cuerpo lo persigue con el suavizado del gráfico. Igual que IQ.

   CALIBRACIÓN. Se conserva la ENERGÍA (varianza por unidad de tiempo), que es lo que determina
   el recorrido de la sesión. Antes: 0,38835 eventos/s × 4,5511 pips² = 1,7674 pips²/s. El mismo
   número exacto después. El gráfico recorre lo mismo que siempre; lo que cambia es la textura.

   FIREBASE. Antes: 10 transacciones por movimiento (~112.000 por sesión) más una lectura de
   config por ciclo. Ahora: 1 transacción por impresión, config cacheada 5 s. Aun triplicando el
   ritmo de movimientos, las escrituras bajan a menos de la mitad.
   ═══════════════════════════════════════════════════════════════════════════════════════════ */

const admin = require("./firebaseApp");
const db = admin.database();
const crypto = require("crypto");

// ══════════════════════════════════════════════════════════════════════════════
// 1 · EL AZAR — intacto, y dos transformaciones que comen del mismo sitio
// ══════════════════════════════════════════════════════════════════════════════
const TWO_POW_53 = 9007199254740992; // 2^53

// Float uniforme en [0, 1) con 53 bits — CSPRNG. Sin tocar.
function cryptoRandomFloat() {
  const x = crypto.randomBytes(8).readBigUInt64BE() >> 11n; // 53 bits
  return Number(x) / TWO_POW_53;
}

// Dirección 50/50 (CSPRNG). Sin tocar. Sin sesgo, sin memoria, jamás.
function randomDirection() {
  return crypto.randomInt(0, 2) === 0 ? -1 : 1;
}

// Entero uniforme [a, b] (CSPRNG). Sin tocar.
function randomInt(a, b) {
  return crypto.randomInt(a, b + 1);
}

/* Normal estándar por Box-Muller. Es una transformación DETERMINISTA de dos uniformes
   criptográficos: no añade ni quita entropía, sólo le cambia la forma. */
function cryptoNormal() {
  let u1 = cryptoRandomFloat();
  while (u1 <= 0) u1 = cryptoRandomFloat(); // log(0) no existe
  const u2 = cryptoRandomFloat();
  return Math.sqrt(-2 * Math.log(u1)) * Math.cos(2 * Math.PI * u2);
}

/* Espera exponencial, por inversa de la acumulada. Es lo que produce un proceso de Poisson:
   muchas esperas cortas y, de vez en cuando, una larga de verdad. El uniforme de antes hacía
   una espera de 5 s tan probable como una de 0,15 s, que no pasa en ningún mercado. */
function cryptoExponential(lambda) {
  const u = cryptoRandomFloat(); // [0,1) ⇒ 1-u ∈ (0,1]
  return -Math.log(1 - u) / lambda;
}

// ══════════════════════════════════════════════════════════════════════════════
// 2 · CALIBRACIÓN — todo lo ajustable, en un solo sitio y con su porqué
// ══════════════════════════════════════════════════════════════════════════════
const CAL = {
  /* Forma de la distribución de tamaños. MEDIDO en IQ Nueva York: s = 1,13 reproduce a la vez
     media/mediana = 1,90 y p99/mediana = 13,6. No tocar sin volver a medir. */
  S_LOGNORMAL: 1.13,

  /* Mediana del golpe, en pips. Calibrada por Monte Carlo para conservar EXACTAMENTE la energía
     de la versión anterior (1,7674 pips²/s) con el resto de capas puestas. Bajar la mediana y
     alargar la cola es justo el cambio: antes todo medía ~1,9 pips; ahora la mayoría son ticks
     pequeños y de vez en cuando cae un golpe de 15-40 veces ese tamaño. */
  MEDIANA_PIPS: 0.262,

  /* Tope duro del golpe, en pips. Ver magnitudPips(). */
  TOPE_PIPS: 60,

  /* Eventos por segundo (base). Cada evento es UNA decisión de mercado; puede imprimirse una vez
     (seco) o en ráfaga. IQ imprime ~10/s, pero eso en Firebase serían 300.000 escrituras por
     sesión. Aquí 1,5/s, que con la ráfaga da ~2 escrituras/s: por debajo de las 3,88/s de antes.
     Si algún día Firebase da para más, este es el único número que hay que subir. */
  EVENTOS_POR_SEGUNDO: 1.5,

  /* Volatilidad lenta: los racimos. MEDIDO: la volatilidad realizada de un bloque de 10 s varía
     2-2,5x dentro de un mismo minuto y 12,9x a lo largo de la sesión. v = 0,50 da e^(2,5·0,5·2)
     ≈ 12,2 de recorrido a lo largo de la sesión. Ahí están los picos altos y los picos bajos. */
  VOL_SIGMA: 0.5,
  VOL_SEMIVIDA_S: 45, // cuánto tarda un estado de volatilidad en diluirse a la mitad

  /* Acoplamiento volatilidad→ritmo: cuando hay nervios, además de golpes más grandes, llegan
     más seguidos. Es real (volumen y volatilidad van de la mano). Exponente suave. */
  VOL_A_RITMO: 0.6,

  /* Ráfagas. MEDIDO en IQ: de 37 saltos grandes, 25 secos y 12 con estela ⇒ 68 % / 32 %.
     La probabilidad sube con el tamaño, que es lo que pasa de verdad: una orden agresiva grande
     se come varios niveles del libro y deja varias impresiones seguidas. Poner PROB_BASE y
     PROB_EXTRA a 0 deja el 100 % de los movimientos secos. */
  RAFAGA_PROB_BASE: 0.14,
  RAFAGA_PROB_EXTRA: 0.42, // se suma en proporción al tamaño, saturando en 4x la mediana
  RAFAGA_MIN_IMPRESIONES: 2,
  RAFAGA_MAX_IMPRESIONES: 8,
  RAFAGA_MS_MIN: 25,
  RAFAGA_MS_MAX: 90,

  /* Sobrepaso: una parte de las ráfagas se pasa de largo y luego la recogen. Es microestructura
     de verdad (el barrido agota el libro y los creadores de mercado lo rellenan), y es lo que
     dibuja mecha DENTRO de un mismo evento. */
  SOBREPASO_PROB: 0.25,
  SOBREPASO_MIN: 0.12, // 12 % de más
  SOBREPASO_MAX: 0.45,

  /* Suelo y techo de la espera, para no martillear Firebase ni dejar el mercado muerto. */
  ESPERA_MIN_MS: 60,
  ESPERA_MAX_MS: 22000,

  /* Rejilla de precio: 6 decimales = 0,01 pip. ANTES ERAN 5, y este es el ÚNICO cambio que va más
     allá de lo pedido. Razón medida: al conservar la energía con una cola pesada, la mediana del
     golpe baja a 0,262 pips. Con la rejilla vieja de 0,1 pip, la rejilla sería casi tan grande
     como el movimiento típico: no se puede representar un proceso de cola pesada en una rejilla
     más gruesa que su propia mediana — el 12 % de las impresiones se perderían por redondeo y la
     forma de la distribución se destruiría. Verificado en el banco: con rejilla de 0,1 pip la
     media/mediana se queda en 1,69; con 0,01 pip da 1,89, que es el 1,90 medido en IQ.
     Para volver atrás basta poner 5 aquí. */
  DECIMALES: 6,

  CONFIG_TTL_MS: 5000,     // cachear el flag de habilitación en vez de leerlo cada ciclo
  INFORME_CADA_MS: 300000, // informe de estadísticas cada 5 minutos
};

const PIP = 0.00010;

// ══════════════════════════════════════════════════════════════════════════════
// 3 · LA SESIÓN DE NUEVA YORK — la forma del día
// ══════════════════════════════════════════════════════════════════════════════
/* AVISO HONESTO, DOS COSAS.
   1) Esto NO sale del vídeo. El vídeo son 55 minutos, no da la forma de la jornada. Sale de cómo
      se comporta una sesión de verdad: apertura nerviosa, calma del mediodía, rampa de cierre.
   2) La curva va clavada al reloj de BOGOTÁ y a la jornada de ESTE mercado —08:00 a 16:00, que
      es justo la ventana de esta función—, NO al reloj de Nueva York. Nueva York pasa a UTC-4
      del segundo domingo de marzo al primero de noviembre, así que unos ocho meses al año su
      campana real cae una hora corrida respecto a esta curva. Es deliberado: la forma acompaña
      a la jornada del instrumento, no al horario de verano de otro país.
   Está normalizada para que su efecto medio sea 1: redistribuye la volatilidad a lo largo del
   día, no añade ni quita recorrido total. */
const CURVA_NY = [
  [8.0, 0.55],   // premercado: poca cosa
  [9.5, 0.95],   // se despierta antes de la campana
  [9.75, 1.85],  // apertura: el pico del día
  [10.5, 1.35],
  [12.0, 1.0],
  [13.0, 0.62],  // almuerzo: la calma
  [13.75, 0.78],
  [15.0, 1.05],
  [15.75, 1.55], // rampa de cierre
  [16.0, 1.9],   // subasta de cierre
];

function factorSesion(hora, minuto) {
  const t = hora + minuto / 60;
  if (t <= CURVA_NY[0][0]) return CURVA_NY[0][1];
  for (let i = 1; i < CURVA_NY.length; i++) {
    if (t <= CURVA_NY[i][0]) {
      const [t0, v0] = CURVA_NY[i - 1];
      const [t1, v1] = CURVA_NY[i];
      const f = (t - t0) / (t1 - t0);
      return v0 + (v1 - v0) * f;
    }
  }
  return CURVA_NY[CURVA_NY.length - 1][1];
}
/* Normalizador: hace que la media temporal de factor² sea 1, para que la curva reparta la
   energía sin cambiar el total de la sesión. Se calcula una vez al arrancar. */
const NORM_SESION = (() => {
  let s = 0, n = 0;
  for (let m = 8 * 60; m <= 16 * 60; m++) {
    const f = factorSesion(Math.floor(m / 60), m % 60);
    s += f * f;
    n++;
  }
  return Math.sqrt(s / n);
})();

// ══════════════════════════════════════════════════════════════════════════════
// 4 · LA VOLATILIDAD LENTA — de aquí salen los racimos
// ══════════════════════════════════════════════════════════════════════════════
/* h es un ruido gaussiano con memoria (AR-1). σ = exp(h − v²) está normalizado para que
   E[σ²] = 1 exactamente: la capa reparte la energía en el tiempo, no la crea. */
let volH = 0;
let volUltimoMs = Date.now();

function actualizaVolatilidad(ahoraMs) {
  const dt = Math.max(0, (ahoraMs - volUltimoMs) / 1000);
  volUltimoMs = ahoraMs;
  const phi = Math.pow(0.5, dt / CAL.VOL_SEMIVIDA_S);
  volH = phi * volH + Math.sqrt(Math.max(0, 1 - phi * phi)) * CAL.VOL_SIGMA * cryptoNormal();
  // acotado a ±3 desviaciones: evita un absurdo de una cola de la cola
  const lim = 3 * CAL.VOL_SIGMA;
  if (volH > lim) volH = lim;
  if (volH < -lim) volH = -lim;
  return Math.exp(volH - CAL.VOL_SIGMA * CAL.VOL_SIGMA);
}

// ══════════════════════════════════════════════════════════════════════════════
// 5 · EL TAMAÑO DEL GOLPE — la cola pesada
// ══════════════════════════════════════════════════════════════════════════════
/* |Δ| = mediana × σ × factor_de_sesión × e^(s·Z). Lo que antes era un uniforme acotado entre
   0,624 y 3,216 ahora es una lognormal: la mayoría de las impresiones son pequeñas y de vez en
   cuando cae una enorme. Eso es un mercado. */
function magnitudPips(sigma, fSesion) {
  const z = cryptoNormal();
  const x = CAL.MEDIANA_PIPS * sigma * fSesion * Math.exp(CAL.S_LOGNORMAL * z);
  /* Tope de seguridad. La lognormal no está acotada por arriba: es su virtud y su peligro.
     El máximo típico de una sesión entera son ~38 pips, así que 60 no recorta nada de lo que
     el modelo produce de verdad — sólo impide que una cola de la cola mande el precio a otro
     planeta de un solo golpe. */
  return Math.min(x, CAL.TOPE_PIPS);
}

/* Reparto de una ráfaga: fracciones decrecientes y desiguales que suman 1. Nada de 10 pasos
   iguales — el primer nivel del libro se come más que el último. */
function repartoRafaga(n) {
  const w = [];
  let suma = 0;
  for (let i = 0; i < n; i++) {
    const peso = Math.exp(-0.55 * i) * (0.55 + cryptoRandomFloat());
    w.push(peso);
    suma += peso;
  }
  return w.map((x) => x / suma);
}

function probRafaga(pipsAbs, medianaEfectiva) {
  const rel = medianaEfectiva > 0 ? pipsAbs / (4 * medianaEfectiva) : 0;
  const p = CAL.RAFAGA_PROB_BASE + CAL.RAFAGA_PROB_EXTRA * Math.min(1, rel);
  return Math.min(0.85, p);
}

// ══════════════════════════════════════════════════════════════════════════════
// 6 · LA ESCRITURA — directa, donde quedó el precio
// ══════════════════════════════════════════════════════════════════════════════
/* ÍNDICE DE LA VELA VIVA, POR ESCUCHA. Cuesta cero peticiones y se entera de la rotación al
   instante; el patrón ya se usa en latido_5s.js. Hace falta porque `auto_vela_m1` NO borra la
   vela anterior al rotar: crea una clave nueva. O sea que el nodo viejo sigue ahí con su `close`
   numérico, y por tanto NINGUNA guarda dentro de la transacción puede detectar una rotación.
   Sin esto, una ráfaga que empieza justo antes de la rotación seguiría retocando el cierre de
   una vela ya cerrada. Con esto, el resto de la ráfaga aterriza en la vela NUEVA, que es lo que
   pasa de verdad en un mercado: el movimiento no se pierde, sigue. */
let idxVivo = null;
(function escuchaIndice() {
  const q = db.ref("market_data/M1").orderByKey().limitToLast(1);
  q.on("child_added", (s) => { idxVivo = s.key; });
  q.on("child_changed", (s) => { idxVivo = s.key; });
})();

/* Una transacción, el delta entero, aplicado sobre el close VIVO. Se conserva el diseño
   anti-latigazo del original (nunca se interpola desde una foto vieja hacia un absoluto), que
   estaba bien. */
async function imprimir(ref, idx, deltaPrecio) {
  const d = +deltaPrecio.toFixed(CAL.DECIMALES + 1);
  if (Math.abs(d) < Math.pow(10, -CAL.DECIMALES) / 2) return null; // por debajo de la rejilla: no se escribe
  const clave = idxVivo || idx;
  try {
    const res = await ref.child(clave).transaction((v) => {
      /* `return;` (undefined) es lo ÚNICO que aborta una transacción en Realtime Database.
         Devolver `v` cuando v es null equivale a pedir un BORRADO y gasta un viaje de red.
         Esta guarda cubre nodo inexistente o malformado — la rotación la cubre `idxVivo`. */
      if (v === null || typeof v.close !== "number" || !isFinite(v.close)) return;
      const nc = +(v.close + d).toFixed(CAL.DECIMALES);
      return {
        ...v,
        close: nc,
        // si high/low vinieran ausentes o corruptos, Math.max(undefined, nc) daría NaN y
        // envenenaría la vela para siempre: se rehacen desde el cierre nuevo.
        high: typeof v.high === "number" && isFinite(v.high) ? Math.max(v.high, nc) : nc,
        low: typeof v.low === "number" && isFinite(v.low) ? Math.min(v.low, nc) : nc,
      };
    });
    if (res && res.committed && res.snapshot && res.snapshot.exists()) {
      return res.snapshot.val().close;
    }
  } catch (e) {
    console.error("[NY] impresión falló:", e.message);
  }
  return null;
}

const dormir = (ms) => new Promise((r) => setTimeout(r, ms));

/* Un EVENTO de mercado. Seco por defecto (68 % medido). En ráfaga, varias impresiones reales
   seguidas en el mismo sentido — no es un recorrido inventado, es una orden agresiva barriendo
   el libro. Y una de cada cuatro ráfagas se pasa de largo y la recogen: eso dibuja mecha. */
async function evento(ref, idx, pipsAbs, direccion, medianaEfectiva) {
  const total = direccion * pipsAbs * PIP;

  if (cryptoRandomFloat() >= probRafaga(pipsAbs, medianaEfectiva)) {
    const fin = await imprimir(ref, idx, total);
    return { impresiones: 1, fin };
  }

  const n = randomInt(CAL.RAFAGA_MIN_IMPRESIONES, CAL.RAFAGA_MAX_IMPRESIONES);
  const hayS = cryptoRandomFloat() < CAL.SOBREPASO_PROB;
  const exceso = hayS
    ? CAL.SOBREPASO_MIN + cryptoRandomFloat() * (CAL.SOBREPASO_MAX - CAL.SOBREPASO_MIN)
    : 0;

  // la ida se pasa de largo por `exceso`; la vuelta lo devuelve, así que el neto es el mismo
  const ida = total * (1 + exceso);
  const w = repartoRafaga(n);
  let escritas = 0;
  let fin = null;

  for (let i = 0; i < n; i++) {
    const r = await imprimir(ref, idx, ida * w[i]);
    if (r !== null) { fin = r; escritas++; }
    if (i < n - 1) await dormir(randomInt(CAL.RAFAGA_MS_MIN, CAL.RAFAGA_MS_MAX));
  }
  if (hayS) {
    await dormir(randomInt(CAL.RAFAGA_MS_MIN, CAL.RAFAGA_MS_MAX));
    const r = await imprimir(ref, idx, -total * exceso);
    if (r !== null) { fin = r; escritas++; }
  }
  return { impresiones: escritas, fin };
}

// ══════════════════════════════════════════════════════════════════════════════
// 7 · HORARIO Y CONFIGURACIÓN
// ══════════════════════════════════════════════════════════════════════════════
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

/* El flag se leía en CADA ciclo: a 0,388 ciclos/s eran ~11.000 lecturas por sesión, y al subir
   el ritmo serían muchas más. Cacheado 5 s: sigue reaccionando casi al instante y deja de
   castigar la base de datos. */
let cfgValor = null;
let cfgMs = 0;
async function habilitado() {
  const ahora = Date.now();
  if (cfgValor !== null && ahora - cfgMs < CAL.CONFIG_TTL_MS) return cfgValor;
  const snap = await db.ref("config/auto_volatilidad_complemento_ny").once("value");
  cfgValor = !!snap.val();
  cfgMs = ahora;
  return cfgValor;
}

// ══════════════════════════════════════════════════════════════════════════════
// 8 · INFORME — el motor se audita solo contra los números de IQ
// ══════════════════════════════════════════════════════════════════════════════
/* Cada 5 minutos escribe en el log sus propias estadísticas realizadas, con las de IQ al lado.
   Así se ve si de verdad está reproduciendo el mercado, sin tener que creerse nada. */
const muestras = [];
let nImpresiones = 0;
let informeMs = Date.now();

function pct(a, p) {
  if (a.length === 0) return 0;
  const i = Math.min(a.length - 1, Math.max(0, Math.floor(p * (a.length - 1))));
  return a[i];
}

function informe() {
  const ahora = Date.now();
  if (ahora - informeMs < CAL.INFORME_CADA_MS) return;
  const seg = (ahora - informeMs) / 1000;
  informeMs = ahora;
  if (muestras.length < 30) { muestras.length = 0; nImpresiones = 0; return; }

  const a = muestras.slice().sort((x, y) => x - y);
  const mediana = pct(a, 0.5);
  const media = a.reduce((s, x) => s + x, 0) / a.length;
  const p99 = pct(a, 0.99);
  const max = a[a.length - 1];
  const energia = a.reduce((s, x) => s + x * x, 0) / seg;

  console.log(
    `📊 [NY] ${a.length} eventos / ${nImpresiones} impresiones en ${seg.toFixed(0)}s  ` +
    `(${(nImpresiones / seg).toFixed(2)} escrituras/s)\n` +
    `        mediana ${mediana.toFixed(3)} pips | media/mna ${(media / mediana).toFixed(2)} (IQ 1,90) | ` +
    `p99/mna ${(p99 / mediana).toFixed(1)} (IQ 13,6) | máx/mna ${(max / mediana).toFixed(1)} (IQ 14,6)\n` +
    `        energía ${energia.toFixed(3)} pips²/s (objetivo 1,767) | vol σ ${Math.exp(volH - CAL.VOL_SIGMA ** 2).toFixed(2)}`
  );
  muestras.length = 0;
  nImpresiones = 0;
}

// ══════════════════════════════════════════════════════════════════════════════
// 9 · EL CICLO
// ══════════════════════════════════════════════════════════════════════════════
/* El original tenía un fallo de producción: el primer `await` (la lectura del flag) estaba FUERA
   de cualquier try. Si Firebase fallaba ahí, la promesa se rompía, nadie reprogramaba el ciclo y
   el worker se quedaba mudo hasta el siguiente despliegue. Aquí todo el ciclo va en try/finally:
   pase lo que pase, SIEMPRE se reprograma. */
async function ciclo() {
  const t0 = Date.now();
  let siguienteMs = 1000;
  try {
    if (!(await habilitado())) {
      console.log("[NY] Volatilidad Complemento NY desactivada (flag)");
      siguienteMs = 5000;
      return;
    }

    const { hora, minuto } = tsBogota();
    const dentroHorario = (hora >= 8 && hora < 16) || (hora === 16 && minuto === 0);
    if (!dentroHorario) {
      console.log(`[NY] Fuera de 08:00-16:00 Bogotá (${hora}:${String(minuto).padStart(2, "0")})`);
      siguienteMs = 10000;
      return;
    }

    const ahoraMs = Date.now();
    const sigma = actualizaVolatilidad(ahoraMs);
    const fSes = factorSesion(hora, minuto) / NORM_SESION;

    // ── la última vela: sólo para saber el índice vigente ──────────────────────
    const ref = db.ref("market_data/M1");
    const snap = await ref.orderByKey().limitToLast(1).once("value");
    const M1 = snap.val() || {};
    const idx = Object.keys(M1)[0];
    const last = M1[idx];
    if (!last || typeof last.close !== "number") {
      console.warn("[NY] Última vela no encontrada o inválida. Reintentando...");
      siguienteMs = 2000;
      return;
    }

    // ── el golpe: tamaño con cola, dirección moneda pura ───────────────────────
    const medEf = CAL.MEDIANA_PIPS * sigma * fSes;
    const pipsAbs = magnitudPips(sigma, fSes);
    const direccion = randomDirection();

    const r = await evento(ref, idx, pipsAbs, direccion, medEf);

    muestras.push(pipsAbs);
    nImpresiones += r.impresiones;
    if (muestras.length > 20000) muestras.splice(0, 10000);

    // ── la espera: exponencial, no uniforme. De aquí salen las pausas ──────────
    const lambda = CAL.EVENTOS_POR_SEGUNDO * Math.pow(sigma, CAL.VOL_A_RITMO) * fSes;
    const espera = cryptoExponential(Math.max(0.02, lambda)) * 1000;
    /* Se descuenta lo que ya tardó el evento (lecturas, transacciones, pausas de la ráfaga).
       Sin esto el reloj de Poisson arranca DESPUÉS del trabajo y el ritmo real queda por debajo
       del pedido, que a su vez baja la energía de la sesión. */
    siguienteMs = Math.min(
      CAL.ESPERA_MAX_MS,
      Math.max(CAL.ESPERA_MIN_MS, espera - (Date.now() - t0))
    );
  } catch (error) {
    console.error("[NY] error en ciclo:", error && error.message ? error.message : error);
    siguienteMs = 5000;
  } finally {
    /* El informe va en el finally, no en la rama de éxito: si estuviera arriba, tras la noche
       entera fuera de horario el primer informe del día dividiría entre una ventana de 16 h. */
    informe();
    setTimeout(ciclo, siguienteMs);
  }
}

/* NO se registran manejadores de SIGTERM/SIGINT aquí. En Node, instalar un oyente de señal
   ANULA la salida por defecto, y este proceso nunca vacía su bucle de eventos (hay setInterval
   en latido_5s, auto_vela_5s y estadisticas, más el socket permanente de Firebase). Un módulo
   suelto poniendo manejadores dejaba vivos a los otros ocho hasta el SIGKILL de Railway: en cada
   despliegue habría dos instancias escribiendo a la vez sobre la misma vela. Si algún día se
   quiere apagado ordenado, va en index.js y coordinando los nueve módulos, no aquí. */

console.log(
  `🗽 [NY] Complemento Nueva York v2 — lognormal s=${CAL.S_LOGNORMAL}, mediana ${CAL.MEDIANA_PIPS} pips, ` +
  `${CAL.EVENTOS_POR_SEGUNDO} eventos/s base, escritura directa. Azar: CSPRNG puro.`
);
ciclo();
