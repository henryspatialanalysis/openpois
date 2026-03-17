import VectorSource from 'ol/source/Vector'
import VectorLayer from 'ol/layer/Vector'
import Cluster from 'ol/source/Cluster'
import Feature from 'ol/Feature'
import Point from 'ol/geom/Point'
import { fromLonLat } from 'ol/proj'
import { confidenceStyle } from './styleFactory.js'
import { mergeColocated } from '../utils.js'
import { CLUSTER_MAX_ZOOM } from '../constants.js'

let vectorSource = null
let clusterSource = null
let layer = null

export function getConflatedLayer() {
  if (layer) return layer

  vectorSource = new VectorSource()
  clusterSource = new Cluster({
    distance: 40,
    minDistance: 20,
    source: vectorSource,
  })

  layer = new VectorLayer({
    source: clusterSource,
    style: confidenceStyle,
    zIndex: 10,
  })

  return layer
}

/**
 * Update the conflated layer with features from an Arrow query result.
 */
export function updateConflatedFeatures(arrowFeatures) {
  if (!vectorSource) return
  vectorSource.clear()

  const olFeatures = mergeColocated(arrowFeatures).map(f => {
    const feature = new Feature({
      geometry: new Point(fromLonLat([f.lon, f.lat])),
    })
    for (const [k, v] of Object.entries(f.properties)) {
      feature.set(k, v)
    }
    feature.set('_source', 'conflated')
    return feature
  })

  vectorSource.addFeatures(olFeatures)
}

/**
 * Toggle clustering based on zoom level.
 */
export function updateConflatedClusterMode(zoom) {
  if (!clusterSource) return
  if (zoom > CLUSTER_MAX_ZOOM) {
    clusterSource.setDistance(0)
  } else {
    clusterSource.setDistance(40)
  }
}
