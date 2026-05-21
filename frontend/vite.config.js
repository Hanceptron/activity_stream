import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
import tailwindcss from '@tailwindcss/vite'

// Vite dev server config. The proxy forwards every /api request to
// the FastAPI backend on port 8000, so the browser sees same-origin
// requests during development and CORS does not get in the way.
export default defineConfig({
  plugins: [react(), tailwindcss()],
  server: {
    proxy: {
      '/api': 'http://localhost:8000',
    },
  },
})
