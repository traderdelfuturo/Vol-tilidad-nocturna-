const db = require("./firebaseApp");

// --- CONFIGURACIÓN ---
const HISTORY_LIMIT = 1600; 
const SECONDS_PER_BAR = 5;

// Variables de estado
let currentVela = null;
let lastPrice = null;

console.log("--> INICIANDO MOTOR DE VELAS 5S...");

// 1. PRUEBA DE CONEXIÓN INMEDIATA
// Esto verificará si Railway tiene permiso de escribir en tu base de datos
const testRef = db.ref("_TEST_CONNECTION_");
testRef.set({
    status: "OK",
    timestamp: Date.now(),
    message: "Si ves esto, Railway tiene permiso de escritura."
}).then(() => {
    console.log("✅ PRUEBA DE ESCRITURA EXITOSA. Firebase está conectado y aceptando datos.");
}).catch((err) => {
    console.error("❌ ERROR CRÍTICO: Railway no puede escribir en Firebase.", err);
});

// 2. FUNCIÓN DE PROCESAMIENTO DE TICKS
function processTick(rawPrice) {
    if (rawPrice === null || rawPrice === undefined) return;
    
    // Asegurar que es número
    const price = parseFloat(rawPrice);
    if (isNaN(price)) return;

    lastPrice = price;
    const now = Math.floor(Date.now() / 1000);
    // Calcular el bloque de 5 segundos actual (ej: 10:00:00, 10:00:05)
    const bucketTime = Math.floor(now / SECONDS_PER_BAR) * SECONDS_PER_BAR;

    // Lógica de Vela
    if (currentVela && currentVela.time !== bucketTime) {
        // La vela anterior terminó, la guardamos
        saveVelaToFirebase(currentVela);
        
        // Iniciar nueva vela
        currentVela = {
            time: bucketTime,
            open: currentVela.close, // El open es el close anterior
            high: price,
            low: price,
            close: price
        };
    } else if (!currentVela) {
        // Primera vela del sistema
        currentVela = {
            time: bucketTime,
            open: price, high: price, low: price, close: price
        };
    } else {
        // Actualizar vela en curso
        currentVela.high = Math.max(currentVela.high, price);
        currentVela.low = Math.min(currentVela.low, price);
        currentVela.close = price;
    }
}

// 3. GUARDADO EN FIREBASE
function saveVelaToFirebase(vela) {
    const refHistory = db.ref("history_5s");
    
    // Guardar
    refHistory.child(vela.time).set(vela).then(() => {
        // Limpieza (Borrar antiguas)
        const cutoffTime = vela.time - (HISTORY_LIMIT * SECONDS_PER_BAR);
        // Borrado silencioso para no saturar logs
        refHistory.orderByKey().endAt(cutoffTime.toString()).limitToFirst(1).once("value", (snap) => {
            snap.forEach((child) => child.ref.remove());
        });
    }).catch(err => console.error("⚠️ Error guardando vela:", err));
}

// 4. CONEXIÓN A M1 (ENTRADA DE DATOS)
try {
    const refM1 = db.ref("market_data/M1");
    
    console.log("--> Escuchando market_data/M1...");
    
    refM1.limitToLast(1).on("child_added", (snap) => {
        const v = snap.val();
        if (v && v.close) processTick(v.close);
    });
    
    refM1.limitToLast(1).on("child_changed", (snap) => {
        const v = snap.val();
        if (v && v.close) processTick(v.close);
    });

} catch (error) {
    console.error("❌ Error al conectar con M1:", error);
}

// 5. HEARTBEAT (Seguridad)
// Si el mercado se queda quieto, forzamos el cierre de la vela al pasar el tiempo
setInterval(() => {
    if (currentVela && lastPrice) {
        const now = Math.floor(Date.now() / 1000);
        const bucketTime = Math.floor(now / SECONDS_PER_BAR) * SECONDS_PER_BAR;
        
        if (currentVela.time !== bucketTime) {
            processTick(lastPrice); // Simular tick para cerrar
        }
    }
}, 1000);
