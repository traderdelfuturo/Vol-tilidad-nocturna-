const admin = require("./firebaseApp");
const db = admin.database();

// Configuración
const HISTORY_LIMIT = 1600; 
const SECONDS_PER_BAR = 5;

console.log("🚀 MOTOR 5S (ROLLUP STYLE) INICIADO...");

// Bucket por tiempo actual (segundos)
// Usamos Date.now() porque en 5s necesitamos precisión de segundo actual, no del inicio del minuto M1
const bucket5s = () => {
    const now = Math.floor(Date.now() / 1000);
    return Math.floor(now / SECONDS_PER_BAR) * SECONDS_PER_BAR;
};

// Función Core: Procesa el tick usando Transacciones (Igual que rollup_tf.js)
async function processTick5s(price) {
    if (!price && price !== 0) return;

    const b = bucket5s(); // Clave = inicio del bucket de 5s actual (ej: 1720005000)
    const ref = db.ref(`history_5s/${b}`);

    try {
        await ref.transaction(v => {
            if (v === null) {
                // Si la vela no existe, la creamos (Open = precio actual)
                return { time: b, open: price, high: price, low: price, close: price };
            }
            
            // Si ya existe, actualizamos High, Low y Close. El Open se respeta.
            // Esto es lo que hace que se vea el movimiento "vivo" dentro de la misma vela
            v.high = Math.max(v.high, price);
            v.low = Math.min(v.low, price);
            v.close = price;
            return v;
        });
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

// Prueba de vida (Escribe un estado para confirmar que el script corre)
db.ref("history_5s/_status").set({ active: true, startedAt: Date.now() });
