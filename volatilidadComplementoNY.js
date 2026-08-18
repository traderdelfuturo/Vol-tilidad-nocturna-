/* ═══════════════════════════════════════════════════════════════════════════════════════════
   VOLATILIDAD · COMPLEMENTO NUEVA YORK — v3
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
   v3 · LA VOLATILIDAD PASA A TENER TRES RELOJES. En la v2 la volatilidad tenia UNA sola escala
   de 45 s, asi que el mercado se olvidaba de si estaba nervioso cada 2-3 minutos y cualquier tramo
   de diez minutos se parecia a cualquier otro. Ahora son tres escalas que se multiplican (45 s,
   8 min y 90 min), mas una temperatura por jornada y una estela que deja el golpe grande. Medido
   con la curva del dia APAGADA -para que no la disfrace la estacionalidad- la memoria real pasa
   de 0,002 a 0,04-0,06 a los cinco minutos, y sigue viva a los quince. Nada de esto toca la
   direccion: sigma dice CUANTO se mueve, jamas HACIA DONDE.


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
  MEDIANA_PIPS: 0.250,

  /* Tope duro del golpe, en pips. Medido: con 60 NO disparó ni una sola vez en 100.000 eventos,
     así que se sube a 150 para que sea inequívocamente una red contra un desbocamiento y no un
     límite que le pise el resultado al dado. Ver magnitudPips(). */
  TOPE_PIPS: 150,

  /* Eventos por segundo (base). Cada evento es UNA decisión de mercado; puede imprimirse una vez
     (seco) o en ráfaga. IQ imprime ~10/s, pero eso en Firebase serían 300.000 escrituras por
     sesión. Aquí 1,5/s, que con la ráfaga da ~2 escrituras/s: por debajo de las 3,88/s de antes.
     Si algún día Firebase da para más, este es el único número que hay que subir. */
  EVENTOS_POR_SEGUNDO: 1.5,

  /* ═══ VOLATILIDAD EN CASCADA — v3 ═══
     ANTES había UNA sola escala, de semivida 45 s. Eso hacía que el mercado se olvidara de si
     estaba nervioso o tranquilo cada 2-3 minutos, y por tanto que CUALQUIER tramo de diez
     minutos se pareciera a cualquier otro. Peor: yo creí ver memoria larga porque la
     autocorrelación de |movimiento| daba +0,06 a los 5 minutos, y era falso. Con semivida 45 s
     lo máximo posible a 300 s es 0,19·2^(-300/45) = 0,002. Los 0,06 los ponía la CURVA DEL DÍA:
     como el precio se agita en la apertura y se calma al mediodía en TODAS las sesiones, aparece
     correlación positiva en todos los retardos. Estacionalidad disfrazada de memoria.
     Demostración: «suelo fijo 0,06 + AR-1 de amplitud 0,13» reproduce los OCHO retardos medidos
     con error ≤ 0,015.

     AHORA son TRES relojes que se multiplican, como en un mercado real: uno rápido para el
     nerviosismo del momento, uno medio para las rachas de un cuarto de hora, y uno lento para el
     humor de la tarde entera. Es la aproximación estándar de la cascada de volatilidad.
     NADA de esto toca la dirección: sigma dice CUÁNTO se mueve, nunca HACIA DÓNDE. */
  VOL_ESCALAS: [
    { semividaS: 45, sd: 0.3 },    // el momento
    { semividaS: 480, sd: 0.3 },   // 8 min — las rachas
    { semividaS: 5400, sd: 0.26 }, // 90 min — el humor de la sesión
  ],

  /* Temperatura del día: días tranquilos y días salvajes. Se sortea una vez por jornada. Sin
     esto todas las sesiones tenían prácticamente la misma energía, que es un patrón repetido. */
  VOL_SD_DIA: 0.28,

  /* Realimentación (ARCH): el golpe que ACABA de ocurrir empuja la volatilidad. Es lo que hace
     que un pico de 60 pips DEJE ESTELA en vez de aparecer solo y desaparecer. Sin esto, una vela
     gigante rodeada de velas normales se lee como un tick malo, no como una noticia.
     Va sobre la innovación ln(golpe/mediana), que vale exactamente s·Z: media cero por
     construcción, así que no infla ni desinfla la volatilidad media. */
  VOL_REALIM: 0.03,
  VOL_ARCH_SEMIVIDA_S: 150,

  /* ASIMETRÍA DE LA VOLATILIDAD. En un mercado de verdad los nervios SUBEN DE GOLPE y BAJAN
     DESPACIO: un susto altera el mercado en un segundo y tarda mucho más en calmarse. Antes la
     estela era simétrica, y eso no pasa en ningún mercado. Ahora una sorpresa hacia arriba empuja
     con toda su fuerza y una hacia abajo sólo con el 45 %.
     Se resta VOL_ASIM_MEDIA = s·(1−0,45)/√(2π) = 1,13·0,55·0,39894 para que el empujón siga
     teniendo MEDIA CERO y no infle la volatilidad general ni el recorrido del día. */
  VOL_ASIMETRIA: 0.45,
  VOL_ASIM_MEDIA: 0.2479,
  /* Varianza que la estela añade al total, MEDIDA en el banco (0,0410). Se resta en el exponente
     para que E[sigma²] siga valiendo 1 y la energía de la sesión no se mueva. */
  VOL_ARCH_VAR: 0.041,

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

  /* Suelo y techo de la espera. MEDIDO: el suelo de 60 ms le pisaba el resultado al dado entre el
     6,5 % y el 10,4 % de las veces — y no por realismo, sino por proteger a Firebase. Baja a 5 ms,
     que es sólo lo justo para que una espera de cero no haga girar el bucle en vacío: ahora manda
     el dado en más del 99 % de los casos. El techo de 22 s nunca disparó, se deja de red. */
  ESPERA_MIN_MS: 5,
  ESPERA_MAX_MS: 22000,

  /* ═══ LA CURVA DEL DÍA, PERO SORTEADA ═══
     Que el mercado se agite en la apertura y se duerma al mediodía es un hecho real, no un
     invento. Lo que NO es real es que la curva sea IDÉNTICA todos los días: eso convierte un
     hecho de mercado en un patrón repetido, y era lo único verdaderamente determinista que
     quedaba dentro del horario. Ahora cada jornada sortea la suya:
       · el pico se corre en el tiempo (unos ±25 min)
       · lo pronunciada que es varía (hay días con una U marcadísima y días casi planos)
     Y después se renormaliza ESA curva, la de hoy, para que el recorrido del día no cambie. */
  CURVA_SD_DESPLAZAMIENTO_H: 0.42, // desviación del corrimiento del pico, en horas
  CURVA_SD_FUERZA: 0.38,           // desviación de lo marcada que sale la U

  /* Rejilla de precio: 6 decimales = 0,01 pip. ANTES ERAN 5, y este es el ÚNICO cambio que va más
     allá de lo pedido. Razón medida: al conservar la energía con una cola pesada, la mediana del
     golpe baja a 0,262 pips. Con la rejilla vieja de 0,1 pip, la rejilla sería casi tan grande
     como el movimiento típico: no se puede representar un proceso de cola pesada en una rejilla
     más gruesa que su propia mediana — el 12 % de las impresiones se perderían por redondeo y la
     forma de la distribución se destruiría. Verificado en el banco: con rejilla de 0,1 pip la
     media/mediana se queda en 1,69; con 0,01 pip da 1,89, que es el 1,90 medido en IQ.
     Para volver atrás basta poner 5 aquí. */
  DECIMALES: 6,

  /* LA PERILLA DE TAMAÑO. En `config/tamano_ny`. 1 = como está calibrado, 2 = el doble, hasta 5.
     Cambia SÓLO el tamaño, jamás el ritmo ni las pausas. Se relee cada 5 s junto al interruptor. */
  TAMANO_MIN: 0.25,
  TAMANO_MAX: 5,

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
  [7.0, 0.45],   // 07:00 — PREMERCADO. Las mesas americanas encienden mientras Londres va por su tarde.
  [8.0, 0.62],   // sigue flojo, pero ya se nota
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

/* La forma MEDIA. Ningún día se parece exactamente a ella: es la media de todos. */
function curvaBase(t) {
  if (t <= CURVA_NY[0][0]) return CURVA_NY[0][1];
  for (let i = 1; i < CURVA_NY.length; i++) {
    if (t <= CURVA_NY[i][0]) {
      const [t0, v0] = CURVA_NY[i - 1];
      const [t1, v1] = CURVA_NY[i];
      return v0 + (v1 - v0) * ((t - t0) / (t1 - t0));
    }
  }
  return CURVA_NY[CURVA_NY.length - 1][1];
}

/* La curva DE HOY: la media, corrida en el tiempo y estirada o aplanada, ambas cosas sorteadas
   con `crypto` al abrir la jornada. Así el hecho de mercado (hay una hora punta) se conserva
   mientras deja de ser un patrón calcado día tras día. */
let curvaDespl = 0;
let curvaFuerza = 1;
let curvaNorma = 1;

function curvaCruda(t) {
  return Math.max(0.05, 1 + (curvaBase(t - curvaDespl) - 1) * curvaFuerza);
}

function sorteaCurva() {
  const l1 = 2.5 * CAL.CURVA_SD_DESPLAZAMIENTO_H;
  curvaDespl = Math.max(-l1, Math.min(l1, CAL.CURVA_SD_DESPLAZAMIENTO_H * cryptoNormal()));
  const l2 = 2.5 * CAL.CURVA_SD_FUERZA;
  curvaFuerza = Math.max(
    0.05,
    1 + Math.max(-l2, Math.min(l2, CAL.CURVA_SD_FUERZA * cryptoNormal()))
  );
  /* Se renormaliza LA CURVA DE HOY —no una fija de fábrica— para que, salga como salga el
     sorteo, su efecto medio sobre la energía valga exactamente 1 y el recorrido del día no
     dependa de la forma que le haya tocado. */
  let s = 0, n = 0;
  for (let m = 7 * 60; m <= 16 * 60; m++) {
    const f = curvaCruda(m / 60);
    s += f;
    n++;
  }
  curvaNorma = (s / n) || 1;
}
sorteaCurva(); // por si acaso: nunca se usa sin sortear

function factorSesion(hora, minuto) {
  return curvaCruda(hora + minuto / 60) / curvaNorma;
}

// ══════════════════════════════════════════════════════════════════════════════
// 4 · LA VOLATILIDAD LENTA — de aquí salen los racimos
// ══════════════════════════════════════════════════════════════════════════════
/* Cada escala es un ruido gaussiano con su propia memoria (AR-1 en logaritmos). Se MULTIPLICAN,
   que en logaritmos es sumarlas. Al restar la varianza total en el exponente, E[σ²] = 1 exacto:
   las capas reparten la energía en el tiempo, no la crean. El recorrido del día no se mueve. */
const volH = CAL.VOL_ESCALAS.map(() => 0);
let volArch = 0;
let volDia = 0;
let volUltimoMs = Date.now();
let volDiaActual = null;

const VOL_VAR_TOTAL =
  CAL.VOL_ESCALAS.reduce((s, e) => s + e.sd * e.sd, 0) +
  CAL.VOL_SD_DIA * CAL.VOL_SD_DIA +
  CAL.VOL_ARCH_VAR;

function sorteaDia(fecha) {
  if (fecha === volDiaActual) return;
  volDiaActual = fecha;
  const lim = 4 * CAL.VOL_SD_DIA;
  volDia = Math.max(-lim, Math.min(lim, CAL.VOL_SD_DIA * cryptoNormal()));
  sorteaCurva();
  console.log(
    `[NY] jornada ${fecha} — temperatura ×${Math.exp(volDia).toFixed(2)} · ` +
    `hora punta corrida ${(curvaDespl * 60).toFixed(0)} min · U ×${curvaFuerza.toFixed(2)}`
  );
}

function actualizaVolatilidad(ahoraMs) {
  const dt = Math.max(0, (ahoraMs - volUltimoMs) / 1000);
  volUltimoMs = ahoraMs;
  let suma = 0;
  for (let i = 0; i < CAL.VOL_ESCALAS.length; i++) {
    const e = CAL.VOL_ESCALAS[i];
    const phi = Math.pow(0.5, dt / e.semividaS);
    volH[i] = phi * volH[i] + Math.sqrt(Math.max(0, 1 - phi * phi)) * e.sd * cryptoNormal();
    /* Red a ±4 desviaciones. Con ±3 saltaba el 0,04 % de las veces; con ±4 es, en la práctica,
       inalcanzable: está para que un fallo numérico no mande la volatilidad a otro planeta, no
       para recortarle nada al dado. */
    const lim = 4 * e.sd;
    if (volH[i] > lim) volH[i] = lim;
    if (volH[i] < -lim) volH[i] = -lim;
    suma += volH[i];
  }
  // la estela del golpe anterior también se diluye con el tiempo
  volArch *= Math.pow(0.5, dt / CAL.VOL_ARCH_SEMIVIDA_S);
  return Math.exp(suma + volArch + volDia - VOL_VAR_TOTAL);
}

/* LA ESTELA. Se llama DESPUÉS de cada golpe. ln(golpe/mediana) es exactamente s·Z, o sea la
   sorpresa de ese golpe: positiva si salió más grande de lo normal, negativa si salió pequeño.
   Media cero, así que ni infla ni desinfla la volatilidad media — sólo la mueve. Un zarpazo deja
   el mercado nervioso un rato; una racha de ticks minúsculos lo adormece. */
function estela(pipsAbs, medianaEfectiva) {
  if (!(medianaEfectiva > 0) || !(pipsAbs > 0)) return;
  /* Red a ±4·s en vez de ±2,5·s: antes recortaba la sorpresa el 1,3 % de las veces, ahora
     prácticamente nunca. Un susto enorme entra entero. */
  const lim = 4 * CAL.S_LOGNORMAL;
  const sorpresa = Math.max(-lim, Math.min(lim, Math.log(pipsAbs / medianaEfectiva)));
  /* Asimetría: el susto empuja con todo, la calma sólo arrastra con el 45 %. Los nervios suben
     de golpe y bajan despacio, como en un mercado. Se descuenta la media para que el conjunto
     siga sumando cero y no infle la volatilidad general. */
  const empuje = sorpresa > 0 ? sorpresa : sorpresa * CAL.VOL_ASIMETRIA;
  volArch += CAL.VOL_REALIM * (empuje - CAL.VOL_ASIM_MEDIA);
  /* Red muy holgada: con la de antes saltaba el 7,4 % de las veces — era el tope que más le
     pisaba el resultado al dado, y no había razón de mercado para él. */
  const la = 12 * CAL.VOL_REALIM * CAL.S_LOGNORMAL;
  if (volArch > la) volArch = la;
  if (volArch < -la) volArch = -la;
}

// ══════════════════════════════════════════════════════════════════════════════
// 5 · EL TAMAÑO DEL GOLPE — la cola pesada
// ══════════════════════════════════════════════════════════════════════════════
/* |Δ| = mediana × σ × factor_de_sesión × e^(s·Z). Lo que antes era un uniforme acotado entre
   0,624 y 3,216 ahora es una lognormal: la mayoría de las impresiones son pequeñas y de vez en
   cuando cae una enorme. Eso es un mercado. */
function magnitudPips(sigma) {
  const z = cryptoNormal();
  const x = CAL.MEDIANA_PIPS * tamano * sigma * Math.exp(CAL.S_LOGNORMAL * z);
  /* Tope de seguridad. La lognormal no está acotada por arriba: es su virtud y su peligro.
     El máximo típico de una sesión entera son ~38 pips, así que 60 no recorta nada de lo que
     el modelo produce de verdad — sólo impide que una cola de la cola mande el precio a otro
     planeta de un solo golpe. */
  return Math.min(x, CAL.TOPE_PIPS * tamano); // el techo estira con la perilla
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
  /* El formato sv-SE da «2026-08-17 09:45». Se parte en fecha y hora: la fecha hace falta para
     saber cuándo empieza una jornada nueva y sortearle su temperatura. */
  const iso = new Intl.DateTimeFormat("sv-SE", {
    timeZone: "America/Bogota",
    hour12: false,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
  }).format(new Date());
  const [fecha, reloj] = iso.split(" ");
  const [hora, minuto] = reloj.split(":").map(Number);
  return { fecha, hora, minuto };
}

/* El flag se leía en CADA ciclo: a 0,388 ciclos/s eran ~11.000 lecturas por sesión, y al subir
   el ritmo serían muchas más. Cacheado 5 s: sigue reaccionando casi al instante y deja de
   castigar la base de datos. */
let cfgValor = null;
let cfgMs = 0;

/* LA PERILLA. Vive en Firebase, en `config/tamano_ny`. Se relee cada 5 s en la misma tanda que
   el interruptor de encendido, así que no cuesta ni una petición extra. Cambiarla surte efecto
   en cinco segundos, sin reiniciar nada. */
let tamano = 1;

async function habilitado() {
  const ahora = Date.now();
  if (cfgValor !== null && ahora - cfgMs < CAL.CONFIG_TTL_MS) return cfgValor;
  const [sFlag, sTam] = await Promise.all([
    db.ref("config/auto_volatilidad_complemento_ny").once("value"),
    db.ref("config/tamano_ny").once("value"),
  ]);
  cfgValor = !!sFlag.val();
  const n = Number(sTam.val());
  // si el nodo no existe, o trae texto, o un número absurdo, vale 1: nunca se rompe
  const nuevo = isFinite(n) && n > 0
    ? Math.min(CAL.TAMANO_MAX, Math.max(CAL.TAMANO_MIN, n))
    : 1;
  if (nuevo !== tamano) {
    console.log("[NY] perilla de tamaño: x" + nuevo.toFixed(2) + " (antes x" + tamano.toFixed(2) + ")");
    tamano = nuevo;
  }
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
    `        energía ${energia.toFixed(3)} pips²/s (objetivo 1,767) | σ ${Math.exp(
      volH.reduce((s, h) => s + h, 0) + volArch + volDia - VOL_VAR_TOTAL
    ).toFixed(2)} (momento ${Math.exp(volH[0]).toFixed(2)} · racha ${Math.exp(volH[1]).toFixed(2)} · humor ${Math.exp(volH[2]).toFixed(2)} · estela ${Math.exp(volArch).toFixed(2)} · día ${Math.exp(volDia).toFixed(2)})`
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

    const { fecha, hora, minuto } = tsBogota();
    const dentroHorario = (hora >= 7 && hora < 16) || (hora === 16 && minuto === 0);
    if (!dentroHorario) {
      console.log(`[NY] Fuera de 07:00-16:00 Bogotá (${hora}:${String(minuto).padStart(2, "0")})`);
      siguienteMs = 10000;
      return;
    }

    sorteaDia(fecha); // jornada nueva ⇒ temperatura nueva
    const ahoraMs = Date.now();
    const sigma = actualizaVolatilidad(ahoraMs);
    const fSes = factorSesion(hora, minuto); // ya viene normalizado con la curva de HOY

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
    const medEf = CAL.MEDIANA_PIPS * tamano * sigma; /* la hora NO encoge el golpe */
    const pipsAbs = magnitudPips(sigma);
    const direccion = randomDirection();

    const r = await evento(ref, idx, pipsAbs, direccion, medEf);
    estela(pipsAbs, medEf); // el golpe empuja la volatilidad: el pico deja rastro

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
  `🗽 [NY] Complemento Nueva York v3 — lognormal s=${CAL.S_LOGNORMAL}, mediana ${CAL.MEDIANA_PIPS} pips, ` +
  `${CAL.EVENTOS_POR_SEGUNDO} eventos/s base, volatilidad en cascada de 3 escalas + estela, ` +
  `escritura directa. Azar: CSPRNG puro, dirección moneda 50/50 sin memoria.`
);
ciclo();
