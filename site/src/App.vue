<template>
  <div class="top-bar">
    <SourceToggle
      :active-source="activeSource"
      @update:source="setSource"
    />
    <SearchBar @fly-to="handleFlyTo" />
  </div>
  <MapContainer
    ref="mapRef"
    :active-source="activeSource"
    :osm-filters="osmFilters"
    :overture-filters="overtureFilters"
    @zoom-changed="currentZoom = $event"
  />
  <AmenityFilter
    :active-source="activeSource"
    :osm-filters="osmFilters"
    :overture-filters="overtureFilters"
    @update:osm-filters="osmFilters = $event"
    @update:overture-filters="overtureFilters = $event"
  />
</template>

<script setup>
import { ref } from 'vue'
import SourceToggle from './components/SourceToggle.vue'
import SearchBar from './components/SearchBar.vue'
import MapContainer from './components/MapContainer.vue'
import AmenityFilter from './components/AmenityFilter.vue'
import { OSM_FILTER_KEYS, OVERTURE_CATEGORIES } from './constants.js'

const activeSource = ref('osm')
const currentZoom = ref(4)
const mapRef = ref(null)

const osmFilters = ref(
  OSM_FILTER_KEYS.reduce((acc, f) => ({ ...acc, [f.key]: true }), {})
)
const overtureFilters = ref(
  OVERTURE_CATEGORIES.reduce((acc, c) => ({ ...acc, [c.key]: true }), {})
)

function setSource(src) {
  activeSource.value = src
}

function handleFlyTo(bbox) {
  if (mapRef.value) {
    mapRef.value.flyToBbox(bbox)
  }
}
</script>
