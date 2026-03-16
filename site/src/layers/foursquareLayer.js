import VectorSource from 'ol/source/Vector'
import VectorLayer from 'ol/layer/Vector'
import Cluster from 'ol/source/Cluster'
import Feature from 'ol/Feature'
import Point from 'ol/geom/Point'
import { fromLonLat } from 'ol/proj'
import { foursquareStyle } from './styleFactory.js'
import { CLUSTER_MAX_ZOOM } from '../constants.js'

let vectorSource = null
let clusterSource = null
let layer = null

export function getFoursquareLayer() {
  if (layer) return layer

  vectorSource = new VectorSource()
  clusterSource = new Cluster({
    distance: 40,
    minDistance: 20,
    source: vectorSource,
  })

  layer = new VectorLayer({
    source: clusterSource,
    style: foursquareStyle,
    zIndex: 10,
  })

  return layer
}

export function updateFoursquareFeatures(arrowFeatures) {
  if (!vectorSource) return
  vectorSource.clear()

  const olFeatures = arrowFeatures.map(f => {
    const feature = new Feature({
      geometry: new Point(fromLonLat([f.lon, f.lat])),
    })
    for (const [k, v] of Object.entries(f.properties)) {
      feature.set(k, v)
    }
    feature.set('_source', 'foursquare')
    return feature
  })

  vectorSource.addFeatures(olFeatures)
}

export function updateFoursquareClusterMode(zoom) {
  if (!clusterSource) return
  clusterSource.setDistance(zoom > CLUSTER_MAX_ZOOM ? 0 : 40)
}
