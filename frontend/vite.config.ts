import react from '@vitejs/plugin-react'
import { defineConfig } from 'vite'

// The backend runs on :8080. Proxying /api keeps the browser same-origin, so no CORS config is needed.
export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    strictPort: true,
    proxy: {
      '/api': 'http://localhost:8080',
    },
  },
})
