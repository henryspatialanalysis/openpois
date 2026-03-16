import { OVERTURE_S3_BUCKET, OVERTURE_S3_REGION } from '../constants.js'

let cachedRelease = null

/**
 * Discover the latest Overture Maps release date by listing the S3 bucket.
 */
export async function discoverLatestRelease() {
  if (cachedRelease) return cachedRelease

  const listUrl =
    `https://${OVERTURE_S3_BUCKET}.s3.${OVERTURE_S3_REGION}.amazonaws.com/` +
    `?list-type=2&prefix=release%2F&delimiter=%2F`

  const resp = await fetch(listUrl)
  const text = await resp.text()

  // Parse XML for CommonPrefixes/Prefix entries
  const matches = [...text.matchAll(/<Prefix>release\/([^<]+)\/<\/Prefix>/g)]
  if (matches.length === 0) {
    throw new Error('No Overture releases found')
  }

  const releases = matches.map(m => m[1]).sort()
  cachedRelease = releases[releases.length - 1]
  console.log('Overture latest release:', cachedRelease)
  return cachedRelease
}

/**
 * Build a DuckDB SQL query for Overture POIs in the viewport.
 * Uses s3:// protocol so DuckDB's httpfs can glob files via S3 ListObjects.
 *
 * @param {Object} bbox - { minLon, minLat, maxLon, maxLat }
 * @param {string[]} enabledCategories - Active L0 categories
 * @param {string} release - Release date string
 * @returns {string} SQL query
 */
export function buildOvertureQuery(bbox, enabledCategories, release) {
  // Use s3:// so DuckDB's httpfs extension can enumerate files via S3 API.
  // https:// URLs do not support glob expansion in DuckDB-WASM.
  const s3Url =
    `s3://${OVERTURE_S3_BUCKET}/release/${release}/theme=places/type=place/*.parquet`

  const catList = enabledCategories.map(c => `'${c}'`).join(', ')

  // bbox struct filter: for points, xmin==xmax==lon and ymin==ymax==lat.
  // Use centroid for safety in case any entries are polygons.
  return `
    WITH pts AS (
      SELECT
        id,
        names.primary AS name,
        taxonomy.hierarchy[1] AS l0,
        taxonomy.hierarchy[2] AS l1,
        brand.names.primary AS brand,
        confidence,
        sources[1].dataset AS source_dataset,
        ST_X(ST_Centroid(geometry)) AS lon,
        ST_Y(ST_Centroid(geometry)) AS lat
      FROM read_parquet('${s3Url}', hive_partitioning = 0)
      WHERE bbox.xmin >= ${bbox.minLon}
        AND bbox.xmax <= ${bbox.maxLon}
        AND bbox.ymin >= ${bbox.minLat}
        AND bbox.ymax <= ${bbox.maxLat}
        AND taxonomy.hierarchy[1] IN (${catList})
    )
    SELECT * FROM pts
    LIMIT 50000
  `
}
