import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

export default defineConfig({
  plugins: [react()],
  server: {
    host: '0.0.0.0',  // bind to all interfaces — required for same-network access
    port: 3001,
    proxy: {
      '/api': {
        target: 'http://localhost:8502',
        changeOrigin: true,
      },
    },
  },
  build: {
    outDir: 'dist',
    sourcemap: false,
    rollupOptions: {
      output: {
        manualChunks: {
          vendor:  ['react', 'react-dom'],
          plotly:  ['react-plotly.js', 'plotly.js-dist-min'],
          md:      ['react-markdown', 'remark-gfm'],
        },
      },
    },
  },
})
