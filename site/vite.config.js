import { defineConfig } from 'vite'
import vue from '@vitejs/plugin-vue'

export default defineConfig({
  base: '/',
  plugins: [vue()],
  build: {
    rollupOptions: {
      output: {
        manualChunks(id) {
          if (id.includes('node_modules/ol-mapbox-style')) return 'ol-mapbox-style'
          if (id.includes('node_modules/ol-pmtiles')) return 'ol-pmtiles'
          if (id.includes('node_modules/ol/')) return 'ol'
          if (id.includes('node_modules/apache-arrow')) return 'arrow'
          if (id.includes('node_modules/@duckdb')) return 'duckdb'
          if (id.includes('node_modules/vue')) return 'vue'
        },
      },
    },
  },
})
