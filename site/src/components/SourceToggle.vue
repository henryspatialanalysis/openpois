<template>
  <!-- Dropdown for narrow screens -->
  <select
    class="source-select"
    :value="activeSource"
    @change="$emit('update:source', $event.target.value)"
  >
    <option value="osm">OpenStreetMap</option>
    <option value="overture">Overture Maps</option>
    <option value="conflated">Conflated</option>
  </select>

  <!-- Buttons for wider screens -->
  <div class="source-toggle">
    <button
      :class="{ active: activeSource === 'osm' }"
      @click="$emit('update:source', 'osm')"
    >
      <span class="osm-short">OSM</span>
      <span class="osm-long">OpenStreetMap</span>
    </button>
    <button
      :class="{ active: activeSource === 'overture' }"
      @click="$emit('update:source', 'overture')"
    >
      Overture<span class="overture-suffix"> Maps</span>
    </button>
    <button
      :class="{ active: activeSource === 'conflated' }"
      @click="$emit('update:source', 'conflated')"
    >
      Conflated
    </button>
  </div>
</template>

<script setup>
defineProps({
  activeSource: { type: String, required: true },
})
defineEmits(['update:source'])
</script>
