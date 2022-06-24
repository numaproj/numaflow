const { createProxyMiddleware } = require("http-proxy-middleware");
const proxy = {
  target: "https://localhost:8443",
  tls: true,
  secure: false,
  changeOrigin: true,
};
module.exports = function (app) {
  app.use("/api/v1", createProxyMiddleware(proxy));
};
