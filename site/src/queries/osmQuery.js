import { OSM_S3_BASE } from '../constants.js'
import { bboxToGeohashes, geohashesToUrls } from '../composables/useGeohash.js'

/**
 * Build a DuckDB SQL query for OSM POIs in the viewport.
 *
 * @param {Object} bbox - { minLon, minLat, maxLon, maxLat }
 * @param {string[]} enabledKeys - Active filter keys, e.g. ['amenity', 'shop']
 * @returns {string|null} SQL string, or null if viewport too wide
 */
export function buildOsmQuery(bbox, enabledKeys) {
  const hashes = bboxToGeohashes(
    bbox.minLat, bbox.minLon, bbox.maxLat, bbox.maxLon
  )
  if (!hashes) return null

  const urls = geohashesToUrls(hashes, OSM_S3_BASE)
  const urlList = urls.map(u => `'${u}'`).join(', ')

  const keyFilters = enabledKeys
    .map(k => `"${k}" IS NOT NULL`)
    .join(' OR ')

  // Use ST_Centroid to handle both point and polygon geometries.
  // CTE avoids computing centroid twice (SELECT + WHERE).
  return `
    WITH pts AS (
      SELECT
        name,
        CAST(osm_id AS BIGINT) AS osm_id,
        osm_type,
        amenity,
        shop,
        leisure,
        healthcare,
        brand,
        cuisine,
        opening_hours,
        phone,
        website,
        "addr:street",
        "addr:city",
        "addr:state",
        conf_mean,
        conf_lower,
        conf_upper,
        last_edited,
        ST_X(ST_Centroid(geometry)) AS lon,
        ST_Y(ST_Centroid(geometry)) AS lat
      FROM read_parquet([${urlList}], union_by_name = true)
      WHERE (${keyFilters || '1=1'})
        AND NOT (amenity IN (
          'parking', 'bicycle_parking', 'toilet', 'bench', 'clock', 'waste_basket',
          'parking_entrance', 'recycling', 'drinking_water', 'bbq', 'vending_machine',
          'parking_space'
        ))
    )
    SELECT * FROM pts
    WHERE lon BETWEEN ${bbox.minLon} AND ${bbox.maxLon}
      AND lat BETWEEN ${bbox.minLat} AND ${bbox.maxLat}
  `
}
