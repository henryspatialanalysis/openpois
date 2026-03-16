import { ref } from 'vue'

const position = ref(null)
const error = ref(null)
const loading = ref(false)

function locate() {
  if (!navigator.geolocation) {
    error.value = 'Geolocation not supported'
    return Promise.reject(error.value)
  }

  loading.value = true
  return new Promise((resolve, reject) => {
    navigator.geolocation.getCurrentPosition(
      (pos) => {
        position.value = {
          lon: pos.coords.longitude,
          lat: pos.coords.latitude,
        }
        loading.value = false
        resolve(position.value)
      },
      (err) => {
        error.value = err.message
        loading.value = false
        reject(err)
      },
      { enableHighAccuracy: true, timeout: 10000 }
    )
  })
}

export function useGeolocation() {
  return { position, error, loading, locate }
}
