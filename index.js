require("./volatilidadNocturna.js");
require("./volatilidadPreEuropa.js");
require("./volatilidadLondres.js");
require("./volatilidadComplementoNY.js");
require("./volatilidadCierre.js"); // <--- NUEVA LÍNEA AÑADIDA
require("./auto_vela_m1.js");
require("./auto_vela_5s.js"); 

// Borré las líneas de startRollup porque ya eliminaste el archivo.

process.on("unhandledRejection", err => console.error("❌ Unhandled:", err));
console.log("✅ Servicio iniciado (auto M1 + auto 5s + Cierre)");
