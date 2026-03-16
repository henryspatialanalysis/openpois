import { ref } from 'vue'
import { NOMINATIM_URL } from '../constants.js'

const results = ref([])
const loading = ref(false)
let debounceTimer = null

async function search(query) {
  if (!query || query.length < 3) {
    results.value = []
    return
  }

  clearTimeout(debounceTimer)
  return new Promise((resolve) => {
    debounceTimer = setTimeout(async () => {
      loading.value = true
      try {
        const params = new URLSearchParams({
          format: 'json',
          q: query,
          countrycodes: 'us',
          limit: '5',
        })
        const resp = await fetch(
          `${NOMINATIM_URL}?${params}`,
          {
            headers: {
              'User-Agent': 'openpois-viewer/1.0',
            },
          }
        )
        results.value = await resp.json()
      } catch (err) {
        console.error('Geocoding error:', err)
        results.value = []
      } finally {
        loading.value = false
        resolve()
      }
    }, 1000) // 1s debounce for Nominatim rate limit
  })
}

function clear() {
  results.value = []
}

export function useGeocoder() {
  return { results, loading, search, clear }
}
