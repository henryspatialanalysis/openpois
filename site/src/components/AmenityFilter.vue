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
})

const emit = defineEmits(['update:osm-filters', 'update:overture-filters'])
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


</script>
