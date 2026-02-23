import path from "path"
import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
import tailwindcss from '@tailwindcss/vite'

const basePath = process.env.VITE_BASE_PATH || "/trex"

export default defineConfig({
  base: `${basePath}/`,
  plugins: [react(), tailwindcss()],
  resolve: {
    alias: {
      "@": path.resolve(__dirname, "./src"),
    },
  },
  server: {
    proxy: {
      [`${basePath}/api`]: "http://localhost:8000",
      [`${basePath}/graphql`]: { target: "http://localhost:8000", ws: true },
      [`${basePath}/graphiql`]: "http://localhost:8000",
      [`${basePath}/docs`]: "http://localhost:8000",
    },
  },
})
