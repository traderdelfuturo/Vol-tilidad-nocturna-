// firebaseApp.js
const admin = require("firebase-admin");

// Solo inicializa si no está inicializado
if (!admin.apps.length) {
  const serviceAccount = JSON.parse(process.env.SERVICE_ACCOUNT_JSON);
  admin.initializeApp({
    credential: admin.credential.cert(serviceAccount),
    databaseURL: "https://datos-de-mercado-ofa-level-3-default-rtdb.firebaseio.com"
  });
}

module.exports = admin;
