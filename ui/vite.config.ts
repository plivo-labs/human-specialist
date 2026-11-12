import tailwindcss from "@tailwindcss/vite";
import { defineConfig } from "vite";

const hs = process.env.HS_URL ?? "http://127.0.0.1:6004";

export default defineConfig({
  base: "./",
  plugins: [tailwindcss()],
  server: { proxy: Object.fromEntries(["/api", "/plivo", "/status", "/openapi.json"].map((p) => [p, hs])) },
  build: {
    // Plivo's Browser SDK is one 977 kB file, loaded only by the desk.
    chunkSizeWarningLimit: 1000,
    rolldownOptions: { onLog: (level, log, next) => log.code !== "MODULE_LEVEL_DIRECTIVE" && next(level, log) },
  },
});
