<template>
  <div class="map-container">
    <div ref="mapEl" style="width: 100%; height: 100%"></div>

    <div v-if="loading" class="loading-indicator">
      <div class="spinner"></div>
      Loading POIs...
    </div>

    <button
      class="geolocation-btn"
      title="My location"
      @click="handleGeolocate"
    >
      <span class="material-symbols-outlined">my_location</span>
    </button>

    <ConfidenceLegend
      v-if="props.activeSource === 'osm' || props.activeSource === 'overture'"
    />

    <div class="basemap-switcher">
      <select v-model="selectedStyle" @change="switchBaseMap">
        <option
          v-for="s in baseMapStyles"
          :key="s.key"
          :value="s.key"
        >
          {{ s.label }}
        </option>
      </select>
    </div>

    <!-- Popup overlay anchor (managed by OL Overlay, not Vue v-if) -->
    <div ref="popupEl">
      <PoiPopup :feature="selectedFeature" @close="closePopup" />
    </div>
  </div>
</template>

<script setup>
import {
  ref, shallowRef, watch, onMounted, onBeforeUnmount, nextTick,
} from 'vue'
import Map from 'ol/Map'
import View from 'ol/View'
import Overlay from 'ol/Overlay'
import TileLayer from 'ol/layer/Tile'
import XYZ from 'ol/source/XYZ'
import { fromLonLat, transformExtent } from 'ol/proj'
import { apply } from 'ol-mapbox-style'
import PoiPopup from './PoiPopup.vue'
import ConfidenceLegend from './ConfidenceLegend.vue'
import { useDuckDB } from '../composables/useDuckDB.js'
import { useGeolocation } from '../composables/useGeolocation.js'
import { createQueryDebouncer } from '../queries/queryDebouncer.js'
import { buildOsmQuery } from '../queries/osmQuery.js'
import {
  getOsmLayer,
  updateOsmFeatures,
  updateClusterMode,
} from '../layers/osmLayer.js'
import {
  getOvertureLayer,
  updateOvertureFilters,
  wrapOvertureFeature,
} from '../layers/overtureLayer.js'
import { arrowToFeatures, extentToLonLatBbox } from '../utils.js'
import {
  BASE_MAP_STYLES,
  INITIAL_CENTER,
  INITIAL_ZOOM,
  MIN_ZOOM_FOR_DATA,
} from '../constants.js'

const props = defineProps({
  activeSource: { type: String, required: true },
  osmFilters: { type: Object, required: true },
  overtureFilters: { type: Object, required: true },

})

const emit = defineEmits(['zoom-changed'])

const mapEl = ref(null)
const popupEl = ref(null)
const map = shallowRef(null)
const popupOverlay = shallowRef(null)
const selectedFeature = shallowRef(null)
const loading = ref(false)
const selectedStyle = ref('positron')
const currentZoom = ref(INITIAL_ZOOM)
const baseMapStyles = BASE_MAP_STYLES

const { initDuckDB, runQuery, ready: duckReady } = useDuckDB()
const { locate } = useGeolocation()
const debouncer = createQueryDebouncer(300)

let geoOverlay = null

// Helper: get all data layers
function getDataLayers() {
  return [getOsmLayer(), getOvertureLayer()]
}

onMounted(async () => {
  const view = new View({
    center: fromLonLat(INITIAL_CENTER),
    zoom: INITIAL_ZOOM,
    minZoom: 14,
  })

  // Start with a raster fallback so the map is visible immediately
  const fallbackBase = new TileLayer({
    source: new XYZ({
      url: 'https://tile.openstreetmap.org/{z}/{x}/{y}.png',
      maxZoom: 19,
    }),
  })

  const osmLyr = getOsmLayer()
  const overtureLyr = getOvertureLayer()
  osmLyr.setVisible(true)
  overtureLyr.setVisible(false)

  const olMap = new Map({
    target: mapEl.value,
    view,
    layers: [fallbackBase, osmLyr, overtureLyr],
  })
  map.value = olMap

  // Try to apply vector tile style (replaces fallback raster)
  applyBaseStyle('positron')

  // Popup overlay
  await nextTick()
  popupOverlay.value = new Overlay({
    element: popupEl.value,
    autoPan: { animation: { duration: 200 } },
    positioning: 'bottom-center',
    offset: [0, -8],
  })
  olMap.addOverlay(popupOverlay.value)

  olMap.on('singleclick', handleClick)

  view.on('change:resolution', () => {
    const z = view.getZoom()
    currentZoom.value = z
    emit('zoom-changed', z)
  })

  olMap.on('moveend', () => {
    const z = view.getZoom()
    currentZoom.value = z
    updateClusterMode(z)
    loadData()
  })

  // Initialise Overture PMTiles filter from props
  updateOvertureFilters(props.overtureFilters)

  initDuckDB()
  handleGeolocate()
})

onBeforeUnmount(() => {
  debouncer.cancel()
  if (map.value) map.value.setTarget(null)
})

async function applyBaseStyle(styleKey) {
  const style = BASE_MAP_STYLES.find(s => s.key === styleKey)
  if (!style || !map.value) return

  const olMap = map.value
  const dataLayers = getDataLayers()

  // Save view state
  const center = olMap.getView().getCenter()
  const zoom = olMap.getView().getZoom()

  // Remove data layers BEFORE apply() to prevent "duplicate item" error.
  // apply() replaces/manages base layers but leaves others — removing them
  // first lets us cleanly re-add them on top afterward.
  const layerArr = olMap.getLayers().getArray()
  for (const lyr of dataLayers) {
    if (layerArr.includes(lyr)) olMap.removeLayer(lyr)
  }

  try {
    await apply(olMap, style.url)
    // Re-add data layers on top of the new base style
    for (const lyr of dataLayers) {
      olMap.addLayer(lyr)
    }
    olMap.getView().setCenter(center)
    olMap.getView().setZoom(zoom)
  } catch (err) {
    console.error('Failed to apply base style:', err)
    // Ensure data layers are present even on error
    const arr = olMap.getLayers().getArray()
    for (const lyr of dataLayers) {
      if (!arr.includes(lyr)) olMap.addLayer(lyr)
    }
  }
}

function switchBaseMap() {
  applyBaseStyle(selectedStyle.value)
}

// ---- Data loading ----

async function loadData() {
  if (props.activeSource !== 'osm') return
  if (currentZoom.value < MIN_ZOOM_FOR_DATA) return
  if (!duckReady.value) return

  try {
    await debouncer.schedule(async () => {
      loading.value = true
      try {
        await loadOsmData()
      } finally {
        loading.value = false
      }
    })
  } catch (err) {
    if (err.name === 'AbortError') return
    console.error('Query error:', err)
    loading.value = false
  }
}

async function loadOsmData() {
  const extent = map.value.getView().calculateExtent()
  const bbox = extentToLonLatBbox(extent)

  const enabledKeys = Object.entries(props.osmFilters)
    .filter(([, v]) => v)
    .map(([k]) => k)

  if (enabledKeys.length === 0) {
    updateOsmFeatures([])
    return
  }

  const sql = buildOsmQuery(bbox, enabledKeys)
  if (!sql) {
    showZoomMessage.value = true
    return
  }

  const result = await runQuery(sql)
  const features = arrowToFeatures(result)
  updateOsmFeatures(features)
}

// ---- Interaction ----

function handleClick(evt) {
  closePopup()

  map.value.forEachFeatureAtPixel(evt.pixel, (feature, lyr) => {
    // Overture PMTiles: VectorTile RenderFeature — wrap before passing to popup
    if (lyr === getOvertureLayer()) {
      selectedFeature.value = wrapOvertureFeature(feature)
      popupOverlay.value.setPosition(evt.coordinate)
      return true
    }

    // OSM VectorLayer (with Cluster)
    const subFeatures = feature.get('features')
    if (subFeatures && subFeatures.length > 1) {
      const geom = feature.getGeometry()
      map.value.getView().animate({
        center: geom.getCoordinates(),
        zoom: map.value.getView().getZoom() + 2,
        duration: 300,
      })
      return true
    }

    const f = subFeatures ? subFeatures[0] : feature
    const geom = f.getGeometry()
    let coord
    if (geom.getType() === 'Point') {
      coord = geom.getCoordinates()
    } else {
      const ext = geom.getExtent()
      coord = [(ext[0] + ext[2]) / 2, (ext[1] + ext[3]) / 2]
    }
    selectedFeature.value = f
    popupOverlay.value.setPosition(coord)
    return true
  })
}

function closePopup() {
  selectedFeature.value = null
  if (popupOverlay.value) {
    popupOverlay.value.setPosition(undefined)
  }
}

// ---- Geolocation ----

async function handleGeolocate() {
  try {
    const pos = await locate()
    const coord = fromLonLat([pos.lon, pos.lat])

    map.value.getView().animate({
      center: coord,
      zoom: 15,
      duration: 500,
    })

    if (!geoOverlay) {
      const el = document.createElement('div')
      el.className = 'geolocation-dot'
      geoOverlay = new Overlay({
        element: el,
        positioning: 'center-center',
      })
      map.value.addOverlay(geoOverlay)
    }
    geoOverlay.setPosition(coord)
  } catch (err) {
    console.warn('Geolocation failed:', err)
  }
}

// ---- Public methods ----

function flyToBbox(bbox) {
  if (!map.value) return
  const extent = transformExtent(
    [bbox.west, bbox.south, bbox.east, bbox.north],
    'EPSG:4326',
    'EPSG:3857'
  )
  map.value.getView().fit(extent, {
    duration: 500,
    maxZoom: 17,
    padding: [50, 50, 50, 50],
  })
}

defineExpose({ flyToBbox })

// ---- Watchers ----

watch(() => props.activeSource, (src) => {
  getOsmLayer().setVisible(src === 'osm')
  getOvertureLayer().setVisible(src === 'overture')
  closePopup()
  loadData()
})

watch(
  () => props.osmFilters,
  () => { if (props.activeSource === 'osm') loadData() },
  { deep: true }
)

watch(
  () => props.overtureFilters,
  (filters) => { updateOvertureFilters(filters) },
  { deep: true }
)


</script>
