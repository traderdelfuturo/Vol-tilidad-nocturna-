const db = require("./firebaseApp"); // Asegúrate de que esto apunta a tu conexión real

// --- ESTADO DEL MOTOR 5S ---
let currentVela = null;
let lastPrice = null;

// Configuración
const HISTORY_LIMIT = 1600; // Guardamos las últimas 1600 (aprox 2.2 horas)
const SECONDS_PER_BAR = 5;

console.log("Servicio de Velas 5s Iniciado (History Tracker)");

// --- 1. ESCUCHAR EL MERCADO (TICKS) ---
// Escuchamos M1 porque es donde tus botones inyectan el precio real
const refM1 = db.ref("market_data/M1");

// Función para procesar cada tick que entra
function processTick(price) {
    if (!price) return;
    lastPrice = price;

    const now = Math.floor(Date.now() / 1000);
    const bucketTime = Math.floor(now / SECONDS_PER_BAR) * SECONDS_PER_BAR; // Tiempo exacto de inicio (ej: 10:00:00, 10:00:05)

    // A) Si cambiamos de bloque de 5s, guardamos la anterior e iniciamos nueva
    if (currentVela && currentVela.time !== bucketTime) {
        closeAndSaveVela(currentVela);
        
        // Iniciar nueva vela usando el cierre anterior como apertura (continuidad)
        currentVela = {
            time: bucketTime,
            open: currentVela.close, 
            high: price,
            low: price,
            close: price
        };
    } 
    // B) Si no tenemos vela (arranque), creamos la primera
    else if (!currentVela) {
        currentVela = {
            time: bucketTime,
            open: price, high: price, low: price, close: price
        };
    } 
    // C) Actualizamos la vela actual en memoria
    else {
        currentVela.high = Math.max(currentVela.high, price);
        currentVela.low = Math.min(currentVela.low, price);
        currentVela.close = price;
    }
    
    // Opcional: Guardar el estado "en vivo" para que el frontend lo vea moverse en tiempo real
    // db.ref("current_5s").set(currentVela).catch(() => {}); 
}

// Escuchar cambios (cuando pulsas botones o volatilidad automática)
refM1.limitToLast(1).on("child_added", (snap) => processTick(snap.val().close));
refM1.limitToLast(1).on("child_changed", (snap) => processTick(snap.val().close));


// --- 2. GUARDADO Y LIMPIEZA (GARBAGE COLLECTOR) ---
function closeAndSaveVela(vela) {
    const refHistory = db.ref("history_5s");
    
    // 1. Guardar la vela terminada
    refHistory.child(vela.time).set(vela).then(() => {
        // 2. Limpieza: Borrar velas más viejas que el límite
        // Calculamos el tiempo de corte: TiempoActual - (1600 * 5 segundos)
        const cutoffTime = vela.time - (HISTORY_LIMIT * SECONDS_PER_BAR);
        
        // Borramos todo lo que sea anterior a ese tiempo
        refHistory.orderByKey().endAt(cutoffTime.toString()).once("value", (snap) => {
            if (snap.exists()) {
                const updates = {};
                snap.forEach((child) => {
                    updates[child.key] = null; // Marcar para borrar
                });
                refHistory.update(updates).catch(err => console.error("Error limpiando history_5s:", err));
            }
        });
    }).catch(err => console.error("Error guardando vela 5s:", err));
}

// Fallback: Si no hay ticks, asegurar que se cierre la vela actual si pasa el tiempo
setInterval(() => {
    if (currentVela) {
        const now = Math.floor(Date.now() / 1000);
        const bucketTime = Math.floor(now / SECONDS_PER_BAR) * SECONDS_PER_BAR;
        if (currentVela.time !== bucketTime && lastPrice) {
            // Forzar cierre si no hubo ticks pero el tiempo pasó
            processTick(lastPrice);
        }
    }
}, 1000);
