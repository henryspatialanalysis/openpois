import { FSQ_S3_BASE } from '../constants.js'
import { bboxToGeohashes, geohashesToUrls } from '../composables/useGeohash.js'

/**
 * Build a DuckDB SQL query for Foursquare POIs in the viewport.
 *
 * @param {Object} bbox - { minLon, minLat, maxLon, maxLat }
 * @param {string[]} enabledCategories - Active L1 category names
 * @returns {string|null} SQL string, or null if viewport too wide
 */
export function buildFoursquareQuery(bbox, enabledCategories) {
  const hashes = bboxToGeohashes(
    bbox.minLat, bbox.minLon, bbox.maxLat, bbox.maxLon
  )
  if (!hashes) return null

  const urls = geohashesToUrls(hashes, FSQ_S3_BASE)
  const urlList = urls.map(u => `'${u}'`).join(', ')

  const catList = enabledCategories.map(c => `'${c}'`).join(', ')
  const catFilter = enabledCategories.length > 0
    ? `AND fsq_l1_category IN (${catList})`
    : ''

  return `
    SELECT
      name,
      fsq_place_id,
      fsq_l1_category,
      fsq_category_labels,
      ST_X(ST_Centroid(geometry)) AS lon,
      ST_Y(ST_Centroid(geometry)) AS lat
    FROM read_parquet([${urlList}], union_by_name = true)
    WHERE ST_X(ST_Centroid(geometry)) BETWEEN ${bbox.minLon} AND ${bbox.maxLon}
      AND ST_Y(ST_Centroid(geometry)) BETWEEN ${bbox.minLat} AND ${bbox.maxLat}
      ${catFilter}
  `
}
