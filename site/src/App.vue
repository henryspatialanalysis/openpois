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
    :conflated-filters="conflatedFilters"
    @zoom-changed="currentZoom = $event"
  />
  <AmenityFilter
    :active-source="activeSource"
    :osm-filters="osmFilters"
    :overture-filters="overtureFilters"
    :conflated-filters="conflatedFilters"
    :conflated-labels="CONFLATED_LABELS"
    @update:osm-filters="osmFilters = $event"
    @update:overture-filters="overtureFilters = $event"
    @update:conflated-filters="conflatedFilters = $event"
  />
</template>

<script setup>
import { ref } from 'vue'
import SourceToggle from './components/SourceToggle.vue'
import SearchBar from './components/SearchBar.vue'
import MapContainer from './components/MapContainer.vue'
import AmenityFilter from './components/AmenityFilter.vue'
import {
  OSM_FILTER_KEYS,
  OVERTURE_CATEGORIES,
  CONFLATED_LABELS,
} from './constants.js'

const activeSource = ref('osm')
const currentZoom = ref(4)
const mapRef = ref(null)

const osmFilters = ref(
  OSM_FILTER_KEYS.reduce((acc, f) => ({ ...acc, [f.key]: true }), {})
)
const overtureFilters = ref(
  OVERTURE_CATEGORIES.reduce((acc, c) => ({ ...acc, [c.key]: true }), {})
)
const conflatedFilters = ref(
  CONFLATED_LABELS.reduce((acc, lbl) => ({
    ...acc,
    [lbl]: !lbl.startsWith('Other '),
  }), {})
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
