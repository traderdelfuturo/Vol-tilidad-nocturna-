require("./volatilidadNocturna.js");
require("./volatilidadPreEuropa.js");
require("./volatilidadLondres.js");
require("./volatilidadComplementoNY.js");
require("./auto_vela_m1.js");
require("./auto_vela_5s.js"); 

const { startRollup } = require("./rollup_tf.js");
startRollup();

process.on("unhandledRejection", err => console.error("❌ Unhandled:", err));
console.log("✅ Servicio iniciado (auto M1 + auto 5s + rollup TF)");
