// SPDX-License-Identifier: Apache-2.0
import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
import tailwindcss from '@tailwindcss/vite'

// Standalone admin console. The admin-gateway base is set at build time via
// VITE_ADMIN_API_URL (empty = same-origin, which is the target deploy). No dev
// API proxy here: point VITE_ADMIN_API_URL at the gateway (e.g.
// http://localhost:8082) when running `npm run dev` against a live stack.
export default defineConfig({
  plugins: [react(), tailwindcss()],
  base: '/',
  build: {
    outDir: 'dist',
    emptyOutDir: true,
  },
})
