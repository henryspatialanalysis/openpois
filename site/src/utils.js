import { fromLonLat, toLonLat } from 'ol/proj'

/**
 * Convert an OpenLayers extent [minX, minY, maxX, maxY] in EPSG:3857
 * to a bbox object { minLon, minLat, maxLon, maxLat } in EPSG:4326.
 */
export function extentToLonLatBbox(extent) {
  const [minLon, minLat] = toLonLat([extent[0], extent[1]])
  const [maxLon, maxLat] = toLonLat([extent[2], extent[3]])
  return { minLon, minLat, maxLon, maxLat }
}

/**
 * Convert lon/lat to EPSG:3857 coordinates for OpenLayers.
 */
export function lonLatToMapCoord(lon, lat) {
  return fromLonLat([lon, lat])
}

/**
 * Convert Arrow table rows to GeoJSON-like feature objects.
 * Expects columns: lon, lat, plus any properties.
 */
export function arrowToFeatures(table) {
  const features = []
  const schema = table.schema.fields.map(f => f.name)
  const lonIdx = schema.indexOf('lon')
  const latIdx = schema.indexOf('lat')

  for (let i = 0; i < table.numRows; i++) {
    const row = table.get(i)
    const props = {}
    for (const field of schema) {
      if (field !== 'lon' && field !== 'lat') {
        const val = row[field]
        // Convert BigInt to Number for JSON compatibility
        props[field] = typeof val === 'bigint' ? Number(val) : val
      }
    }
    features.push({
      lon: Number(row['lon']),
      lat: Number(row['lat']),
      properties: props,
    })
  }
  return features
}

/**
 * Merge features at exactly the same lon/lat into a single feature.
 * The merged feature gets conf_mean = mean of the group and
 * _colocated = array of all per-feature property objects.
 * Single-location features are returned unchanged.
 */
export function mergeColocated(arrowFeatures) {
  const groups = new Map()
  for (const f of arrowFeatures) {
    const key = `${f.lon},${f.lat}`
    if (!groups.has(key)) groups.set(key, [])
    groups.get(key).push(f)
  }
  return Array.from(groups.values()).map(group => {
    if (group.length === 1) return group[0]
    const confValues = group
      .map(f => f.properties.conf_mean ?? f.properties.confidence)
      .filter(v => v != null && !isNaN(v))
    const avgConf = confValues.length
      ? confValues.reduce((a, b) => a + b, 0) / confValues.length
      : null
    return {
      lon: group[0].lon,
      lat: group[0].lat,
      properties: {
        ...group[0].properties,
        conf_mean: avgConf,
        confidence: avgConf,
        _colocated: group.map(f => ({ ...f.properties })),
      },
    }
  })
}

/**
 * Discretize confidence to N steps for style cache efficiency.
 */
export function discretizeConf(conf) {
  if (conf == null || isNaN(conf)) return 'null'
  return Math.round(Math.max(0, Math.min(1, conf)) * 20)
}

// Confidence gradient stops: red → yellow → green
const CONF_STOPS = [
  { t: 0.0, r: 215, g: 48,  b: 39  },  // #d73027 red
  { t: 0.5, r: 254, g: 224, b: 139 },  // #fee08b yellow
  { t: 1.0, r: 26,  g: 152, b: 80  },  // #1a9850 green
]

function lerpChannel(a, b, t) {
  return Math.round(a + (b - a) * t)
}

/**
 * Map a confidence value [0,1] to a hex color via red→yellow→green gradient.
 */
export function confidenceColor(value) {
  if (value == null || isNaN(value)) return '#999999'
  const v = Math.max(0, Math.min(1, value))

  // Find the two surrounding stops
  let lo = CONF_STOPS[0]
  let hi = CONF_STOPS[CONF_STOPS.length - 1]
  for (let i = 0; i < CONF_STOPS.length - 1; i++) {
    if (v <= CONF_STOPS[i + 1].t) {
      lo = CONF_STOPS[i]
      hi = CONF_STOPS[i + 1]
      break
    }
  }

  const span = hi.t - lo.t
  const t = span === 0 ? 0 : (v - lo.t) / span
  const r = lerpChannel(lo.r, hi.r, t)
  const g = lerpChannel(lo.g, hi.g, t)
  const b = lerpChannel(lo.b, hi.b, t)
  return `rgb(${r},${g},${b})`
}
