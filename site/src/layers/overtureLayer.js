import VectorTileLayer from 'ol/layer/VectorTile'
import { PMTilesVectorSource } from 'ol-pmtiles'
import { Style, Circle, Fill, Stroke } from 'ol/style'
import { confidenceColor, discretizeConf } from '../utils.js'
import { OVERTURE_PMTILES_URL } from '../constants.js'

let layer = null

// Style cache keyed by conf bucket (same discretisation as OSM layer)
const styleCache = {}

export function getOvertureLayer() {
  if (layer) return layer

  layer = new VectorTileLayer({
    source: new PMTilesVectorSource({ url: OVERTURE_PMTILES_URL }),
    style: overtureTileStyle,
    zIndex: 10,
    visible: false,
  })
  return layer
}

/**
 * No-op: Overture PMTiles tiles contain only granular L1/L2 subcategories
 * (e.g. "coffee_shop", "gym") — not the L0 labels (e.g. "food_and_drink")
 * used in the filter UI. Category filtering is therefore not applied here.
 */
export function updateOvertureFilters(_filtersObj) {}

function overtureTileStyle(feature) {
  const cats = tryParse(feature.get('categories'))
  if (cats?.primary === 'parking') return null

  const conf = feature.get('confidence')
  const bucket = discretizeConf(conf)
  if (!styleCache[bucket]) {
    const color = confidenceColor(conf ?? null)
    styleCache[bucket] = new Style({
      image: new Circle({
        radius: 5,
        fill: new Fill({ color }),
        stroke: new Stroke({ color: '#fff', width: 1 }),
      }),
    })
  }
  return styleCache[bucket]
}

/**
 * Wrap a VectorTile RenderFeature (immutable) in a plain object that
 * exposes the same .get() / .getKeys() / .getGeometry() API as an OL Feature,
 * with Overture's JSON-encoded fields pre-parsed into flat properties.
 */
export function wrapOvertureFeature(rf) {
  const cats = tryParse(rf.get('categories'))
  const brand = tryParse(rf.get('brand'))
  const addrs = tryParse(rf.get('addresses'))
  const addr = Array.isArray(addrs) ? addrs[0] : addrs
  const websites = tryParse(rf.get('websites'))
  const phones = tryParse(rf.get('phones'))

  const props = {
    _source: 'overture',
    name: rf.get('@name') || null,
    id: rf.get('id'),
    confidence: rf.get('confidence'),
    l0: cats?.primary ?? null,
    l1: cats?.alternate?.[0] ?? null,
    brand: brand?.names?.primary ?? null,
    website: Array.isArray(websites) ? websites[0] : null,
    phone: Array.isArray(phones) ? phones[0] : null,
    'addr:street': addr?.freeform ?? null,
    'addr:city': addr?.locality ?? null,
    'addr:state': addr?.region ?? null,
    source_dataset: 'Overture Maps',
  }

  return {
    get: (key) => props[key],
    getKeys: () => Object.keys(props),
    getGeometry: () => rf.getGeometry(),
  }
}

function tryParse(str) {
  if (!str) return null
  try { return JSON.parse(str) } catch { return null }
}
