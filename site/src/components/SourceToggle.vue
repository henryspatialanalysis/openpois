<template>
  <div class="source-toggle">
    <button
      :class="{ active: activeSource === 'osm' }"
      @click="$emit('update:source', 'osm')"
    >
      OpenStreetMap
    </button>
    <button
      :class="{ active: activeSource === 'overture' }"
      @click="$emit('update:source', 'overture')"
    >
      Overture Maps
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
