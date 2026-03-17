<template>
  <div class="search-container">
    <input
      v-model="query"
      type="text"
      placeholder="Search for an address..."
      @input="onInput"
      @keydown.escape="clear"
    />
    <ul v-if="results.length" class="search-results">
      <li
        v-for="r in results"
        :key="r.properties.id"
        @click="selectResult(r)"
      >
        {{ r.properties.label }}
      </li>
    </ul>
  </div>
</template>

<script setup>
import { ref } from 'vue'
import { useGeocoder } from '../composables/useGeocoder.js'

const emit = defineEmits(['fly-to'])
const query = ref('')
const { results, search, clear: clearResults } = useGeocoder()

function onInput() {
  search(query.value)
}

function selectResult(r) {
  // Stadia/Pelias GeoJSON bbox: [west, south, east, north]
  // Fall back to a small box around the point if bbox is absent.
  const [lng, lat] = r.geometry.coordinates
  let west, south, east, north
  if (r.bbox) {
    ;[west, south, east, north] = r.bbox
  } else {
    const delta = 0.01
    west = lng - delta
    east = lng + delta
    south = lat - delta
    north = lat + delta
  }
  emit('fly-to', { west, south, east, north, lng, lat })
  query.value = r.properties.label
  clearResults()
}

function clear() {
  query.value = ''
  clearResults()
}
</script>
