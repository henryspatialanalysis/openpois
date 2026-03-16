import ngeohash from 'ngeohash'
import { GEOHASH_PRECISION, MAX_GEOHASH_CELLS } from '../constants.js'

/**
 * Enumerate all geohash cells at the given precision that overlap a bbox.
 * Returns null if too many cells (viewport too wide).
 */
export function bboxToGeohashes(
  minLat, minLon, maxLat, maxLon,
  precision = GEOHASH_PRECISION
) {
  const hashes = ngeohash.bboxes(minLat, minLon, maxLat, maxLon, precision)
  if (hashes.length > MAX_GEOHASH_CELLS) return null
  return hashes
}

/**
 * Build S3 URL list for geohash partition files.
 */
export function geohashesToUrls(hashes, baseUrl) {
  return hashes.map(
    h => `${baseUrl}/geohash_prefix=${h}/part-0.parquet`
  )
}
