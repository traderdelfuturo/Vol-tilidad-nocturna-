const db = require("./firebaseApp");

// --- ESTADO DEL MOTOR 5S ---
let currentVela = null;
let lastPrice = null;

// Configuración
const HISTORY_LIMIT = 1600; 
const SECONDS_PER_BAR = 5;

console.log("🚀 INICIANDO SISTEMA DE VELAS 5S (PERSISTENTE)...");

// Función segura para procesar ticks
function processTick(price) {
    // 1. BLINDAJE: Si el precio no es un número válido, ignoramos y no rompemos nada.
    if (price === null || price === undefined || isNaN(price)) return;
    
    try {
        lastPrice = parseFloat(price);
        const now = Math.floor(Date.now() / 1000);
        const bucketTime = Math.floor(now / SECONDS_PER_BAR) * SECONDS_PER_BAR;

        if (currentVela && currentVela.time !== bucketTime) {
            closeAndSaveVela(currentVela);
            // Nueva vela
            currentVela = {
                time: bucketTime,
                open: currentVela.close, 
                high: lastPrice,
                low: lastPrice,
                close: lastPrice
            };
        } else if (!currentVela) {
            // Primer arranque
            currentVela = {
                time: bucketTime,
                open: lastPrice, high: lastPrice, low: lastPrice, close: lastPrice
            };
        } else {
            // Actualización
            currentVela.high = Math.max(currentVela.high, lastPrice);
            currentVela.low = Math.min(currentVela.low, lastPrice);
            currentVela.close = lastPrice;
        }
    } catch (error) {
        console.error("⚠️ Error procesando tick 5s:", error);
        // No lanzamos el error para no detener el servidor
    }
}

// --- CONEXIÓN SEGURA A FIREBASE ---
try {
    const refM1 = db.ref("market_data/M1");

    // Usamos funciones flecha con validación previa
    refM1.limitToLast(1).on("child_added", (snap) => {
        const val = snap.val();
        if (val && val.close !== undefined) processTick(val.close);
    });

    refM1.limitToLast(1).on("child_changed", (snap) => {
        const val = snap.val();
        if (val && val.close !== undefined) processTick(val.close);
    });

} catch (err) {
    console.error("🔥 ERROR CRÍTICO AL CONECTAR MOTOR 5S:", err);
}

// --- GUARDADO Y LIMPIEZA ---
function closeAndSaveVela(vela) {
    if(!vela) return;
    
    // Clonamos para evitar problemas de referencia
    const velaToSave = { ...vela };
    
    const refHistory = db.ref("history_5s");
    
    refHistory.child(velaToSave.time).set(velaToSave).then(() => {
        // Limpieza silenciosa (catch interno)
        const cutoffTime = velaToSave.time - (HISTORY_LIMIT * SECONDS_PER_BAR);
        refHistory.orderByKey().endAt(cutoffTime.toString()).once("value", (snap) => {
            if (snap.exists()) {
                const updates = {};
                snap.forEach((child) => { updates[child.key] = null; });
                refHistory.update(updates).catch(() => {}); // Ignorar error de limpieza
            }
        });
    }).catch(err => console.error("Error guardando historial:", err));
}

// Fallback de seguridad (Heartbeat)
setInterval(() => {
    try {
        if (currentVela && lastPrice) {
            const now = Math.floor(Date.now() / 1000);
            const bucketTime = Math.floor(now / SECONDS_PER_BAR) * SECONDS_PER_BAR;
            // Si el tiempo avanzó y no llegó tick, cerramos la vela forzosamente
            if (currentVela.time !== bucketTime) {
                processTick(lastPrice);
            }
        }
    } catch (e) {
        console.error("Error en Heartbeat 5s:", e);
    }
}, 1000);
