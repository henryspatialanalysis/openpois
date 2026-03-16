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
        :key="r.place_id"
        @click="selectResult(r)"
      >
        {{ r.display_name }}
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
  // Nominatim returns boundingbox as [south, north, west, east]
  const [south, north, west, east] = r.boundingbox.map(Number)
  emit('fly-to', { south, north, west, east })
  query.value = r.display_name
  clearResults()
}

function clear() {
  query.value = ''
  clearResults()
}
</script>
