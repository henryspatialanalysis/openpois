import { CONFLATED_S3_BASE } from '../constants.js'
import { bboxToGeohashes, geohashesToUrls } from '../composables/useGeohash.js'

/**
 * Build a DuckDB SQL query for conflated POIs in the viewport.
 *
 * @param {Object} bbox - { minLon, minLat, maxLon, maxLat }
 * @param {string[]} enabledLabels - Active shared_label values to include
 * @returns {string|null} SQL string, or null if viewport too wide
 */
export function buildConflatedQuery(bbox, enabledLabels) {
  const hashes = bboxToGeohashes(
    bbox.minLat, bbox.minLon, bbox.maxLat, bbox.maxLon
  )
  if (!hashes) return null

  const urls = geohashesToUrls(hashes, CONFLATED_S3_BASE)
  const urlList = urls.map(u => `'${u}'`).join(', ')

  const labels = enabledLabels.map(l => `'${l.replace(/'/g, "''")}'`).join(', ')

  return `
    WITH pts AS (
      SELECT
        name,
        unified_id,
        source,
        CAST(osm_id AS VARCHAR) AS osm_id,
        overture_id,
        brand,
        shared_label,
        conf_mean,
        conf_lower,
        conf_upper,
        match_score,
        match_distance_m,
        osm_name,
        overture_name,
        osm_brand,
        overture_brand,
        osm_conf_mean,
        overture_confidence,
        ST_X(ST_Centroid(geometry)) AS lon,
        ST_Y(ST_Centroid(geometry)) AS lat
      FROM read_parquet([${urlList}], union_by_name = true)
      WHERE shared_label IN (${labels})
    )
    SELECT * FROM pts
    WHERE lon BETWEEN ${bbox.minLon} AND ${bbox.maxLon}
      AND lat BETWEEN ${bbox.minLat} AND ${bbox.maxLat}
  `
}
