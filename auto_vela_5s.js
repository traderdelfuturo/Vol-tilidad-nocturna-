const admin = require("./firebaseApp");
const db = admin.database();
// Configuración
const HISTORY_LIMIT = 9600; 
const SECONDS_PER_BAR = 5;
console.log("🚀 MOTOR 5S (ROLLUP STYLE + CADENA SIN HUECOS) INICIADO...");
// Bucket por tiempo actual (segundos)
// Usamos Date.now() porque en 5s necesitamos precisión de segundo actual, no del inicio del minuto M1
const bucket5s = () => {
    const now = Math.floor(Date.now() / 1000);
    return Math.floor(now / SECONDS_PER_BAR) * SECONDS_PER_BAR;
};
// ─── MEMORIA DE CADENA: último balde escrito y su cierre ───
// Con esto, si entre tick y tick se saltaron baldes (movers respirando
// entre 0.15 y 5s, o pausas), los faltantes se RELLENAN planos con el
// cierre previo: continuidad garantizada, recargas sin saltos jamás.
const mem = { lastB: null, lastClose: null };
const MAX_FILL = 2400; // tope de relleno por evento (2 horas): más allá, hueco histórico honesto
async function inicializarCadena() {
    try {
        const snap = await db.ref("history_5s").orderByKey().limitToLast(3).once("value");
        let mejor = null;
        snap.forEach((ch) => {
            const v = ch.val();
            if (v && typeof v.time === "number" && typeof v.close === "number") {
                if (!mejor || v.time > mejor.time) mejor = v;
            }
        });
        if (mejor) { mem.lastB = mejor.time; mem.lastClose = mejor.close; }
        console.log("🔗 cadena inicializada:", mem.lastB, mem.lastClose);
    } catch (e) { console.error("init cadena:", e.message); }
}
// Rellenar baldes faltantes: velas planas con el cierre previo (la verdad de un mercado quieto)
async function rellenarHuecos(hastaB) {
    if (mem.lastB === null || mem.lastClose === null) return;
    let desde = mem.lastB + SECONDS_PER_BAR;
    if (desde >= hastaB) return;
    const faltan = Math.floor((hastaB - desde) / SECONDS_PER_BAR);
    if (faltan <= 0) return;
    const inicio = faltan > MAX_FILL ? hastaB - MAX_FILL * SECONDS_PER_BAR : desde;
    const updates = {};
    for (let m = inicio; m < hastaB; m += SECONDS_PER_BAR) {
        updates[m] = { time: m, open: mem.lastClose, high: mem.lastClose, low: mem.lastClose, close: mem.lastClose };
    }
    try {
        await db.ref("history_5s").update(updates);
        console.log(`🧱 rellenados ${Object.keys(updates).length} baldes planos hasta ${hastaB}`);
    } catch (e) { console.error("relleno:", e.message); }
}
// Función Core: Procesa el tick usando Transacciones (Igual que rollup_tf.js)
async function processTick5s(price) {
    if (!price && price !== 0) return;
    const b = bucket5s(); // Clave = inicio del bucket de 5s actual (ej: 1720005000)
    // regla de cadena: primero rellenar lo que faltó, luego el balde vivo
    if (mem.lastB !== null && b > mem.lastB + SECONDS_PER_BAR) {
        await rellenarHuecos(b);
    }
    const ref = db.ref(`history_5s/${b}`);
    const apertura = (mem.lastB !== null && mem.lastClose !== null && b > mem.lastB) ? mem.lastClose : price;
    try {
        await ref.transaction(v => {
            if (v === null) {
                // Vela nueva: la apertura ENCADENA con el cierre previo (gaps imposibles)
                return { time: b, open: apertura, high: Math.max(apertura, price), low: Math.min(apertura, price), close: price };
            }
            
            // Si ya existe, actualizamos High, Low y Close. El Open se respeta.
            // Esto es lo que hace que se vea el movimiento "vivo" dentro de la misma vela
            v.high = Math.max(v.high, price);
            v.low = Math.min(v.low, price);
            v.close = price;
            return v;
        });
        if (mem.lastB === null || b >= mem.lastB) { mem.lastB = b; mem.lastClose = price; }
    } catch (err) {
        console.error("🔥 Transaction 5s error:", err.message);
    }
}
// Función de Limpieza (Garbage Collector)
// Borra velas antiguas para no saturar Firebase
function cleanOldCandles() {
    const cutoffTime = Math.floor(Date.now() / 1000) - (HISTORY_LIMIT * SECONDS_PER_BAR);
    const refHistory = db.ref("history_5s");
    
    // Borramos todo lo anterior al tiempo de corte
    refHistory.orderByKey().endAt(cutoffTime.toString()).limitToFirst(50).once("value", (snap) => {
        if (!snap.exists()) return;
        const updates = {};
        snap.forEach((child) => {
            updates[child.key] = null;
        });
        refHistory.update(updates).catch(err => console.error("⚠️ Cleanup error:", err.message));
    });
}
// --- LISTENERS (IGUAL QUE ROLLUP_TF) ---
const refM1 = db.ref("market_data/M1");
// 1. Escucha child_added (Primer dato al iniciar o nueva vela M1)
refM1.orderByKey().limitToLast(1).on("child_added", snap => {
    const vela = snap.val();
    if (vela && vela.close) processTick5s(vela.close);
});
// 2. Escucha child_changed (CADA MOVIMIENTO en tiempo real)
refM1.orderByKey().limitToLast(1).on("child_changed", snap => {
    const vela = snap.val();
    if (vela && vela.close) processTick5s(vela.close);
});
// 3. Ejecutar limpieza periódica (cada 20 segundos)
setInterval(cleanOldCandles, 20000);
// Arrancar la cadena desde lo último guardado (continuidad entre reinicios del worker)
inicializarCadena();
// Prueba de vida (Escribe un estado para confirmar que el script corre)
db.ref("history_5s/_status").set({ active: true, startedAt: Date.now() });
