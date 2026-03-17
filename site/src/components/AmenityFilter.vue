<template>
  <div class="amenity-filter">
    <div class="amenity-filter-header" @click="collapsed = !collapsed">
      <span>Filters</span>
      <span>{{ collapsed ? '+' : '-' }}</span>
    </div>
    <div v-if="!collapsed" class="amenity-filter-body">
      <template v-if="activeSource === 'osm'">
        <label v-for="f in osmFilterKeys" :key="f.key">
          <input
            type="checkbox"
            :checked="osmFilters[f.key]"
            @change="toggleOsm(f.key)"
          />
          {{ f.label }}
        </label>
      </template>
      <template v-else-if="activeSource === 'overture'">
        <p class="filter-note">
          Overture Maps tiles use granular subcategories. Filtering will be available after the taxonomy stabilizes in June 2026.
        </p>
      </template>
      <template v-else-if="activeSource === 'conflated'">
        <div class="filter-actions">
          <button class="filter-action-btn" @click="selectAllConflated">All</button>
          <button class="filter-action-btn" @click="selectNoneConflated">None</button>
        </div>
        <div class="conflated-filter-list">
          <label v-for="lbl in conflatedLabels" :key="lbl">
            <input
              type="checkbox"
              :checked="conflatedFilters[lbl]"
              @change="toggleConflated(lbl)"
            />
            {{ lbl }}
          </label>
        </div>
      </template>

    </div>
  </div>
</template>

<script setup>
import { ref } from 'vue'
import { OSM_FILTER_KEYS, OVERTURE_CATEGORIES } from '../constants.js'

const props = defineProps({
  activeSource: { type: String, required: true },
  osmFilters: { type: Object, required: true },
  overtureFilters: { type: Object, required: true },
  conflatedFilters: { type: Object, required: true },
  conflatedLabels: { type: Array, required: true },
})

const emit = defineEmits([
  'update:osm-filters',
  'update:overture-filters',
  'update:conflated-filters',
])
const collapsed = ref(false)
const osmFilterKeys = OSM_FILTER_KEYS
const overtureCategories = OVERTURE_CATEGORIES

function toggleOsm(key) {
  emit('update:osm-filters', { ...props.osmFilters, [key]: !props.osmFilters[key] })
}

function toggleOverture(key) {
  emit('update:overture-filters', {
    ...props.overtureFilters,
    [key]: !props.overtureFilters[key],
  })
}

function toggleConflated(label) {
  emit('update:conflated-filters', {
    ...props.conflatedFilters,
    [label]: !props.conflatedFilters[label],
  })
}

function selectAllConflated() {
  const all = {}
  for (const lbl of props.conflatedLabels) all[lbl] = true
  emit('update:conflated-filters', all)
}

function selectNoneConflated() {
  const none = {}
  for (const lbl of props.conflatedLabels) none[lbl] = false
  emit('update:conflated-filters', none)
}

</script>
