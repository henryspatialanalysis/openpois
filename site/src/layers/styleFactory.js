import { Style, Circle, Fill, Stroke, Text } from 'ol/style'
import { confidenceColor, discretizeConf } from '../utils.js'

// Cache styles by key to avoid creating thousands of identical objects
const styleCache = {}

function getOrCreate(key, factory) {
  if (!styleCache[key]) {
    styleCache[key] = factory()
  }
  return styleCache[key]
}

/**
 * Style function for confidence-colored POI points.
 * Uses conf_mean property for OSM, confidence for Overture.
 */
export function confidenceStyle(feature) {
  const features = feature.get('features')
  if (features && features.length > 1) {
    return clusterStyle(features.length)
  }

  const f = features ? features[0] : feature
  const conf = f.get('conf_mean') ?? f.get('confidence')
  const bucket = discretizeConf(conf)
  const color = confidenceColor(conf == null || isNaN(conf) ? null : conf)

  return getOrCreate(`poi-${bucket}`, () => new Style({
    image: new Circle({
      radius: 5,
      fill: new Fill({ color }),
      stroke: new Stroke({ color: '#fff', width: 1 }),
    }),
  }))
}

/**
 * Style for Foursquare (all blue) - deferred but defined.
 */
export function foursquareStyle() {
  return getOrCreate('fsq-blue', () => new Style({
    image: new Circle({
      radius: 5,
      fill: new Fill({ color: COLORS.foursquare }),
      stroke: new Stroke({ color: '#fff', width: 1 }),
    }),
  }))
}

/**
 * Cluster style showing a circle with count.
 */
function clusterStyle(count) {
  const bucket = count < 10 ? 'sm'
    : count < 100 ? 'md'
    : count < 1000 ? 'lg' : 'xl'

  return getOrCreate(`cluster-${bucket}`, () => {
    const radius = bucket === 'sm' ? 12
      : bucket === 'md' ? 16
      : bucket === 'lg' ? 22 : 28

    return new Style({
      image: new Circle({
        radius,
        fill: new Fill({ color: 'rgba(99, 102, 241, 0.7)' }),
        stroke: new Stroke({ color: '#fff', width: 2 }),
      }),
      text: new Text({
        text: count >= 1000
          ? `${Math.round(count / 1000)}k`
          : count.toString(),
        fill: new Fill({ color: '#fff' }),
        font: 'bold 12px sans-serif',
      }),
    })
  })
}
