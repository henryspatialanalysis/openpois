// S3 URLs
export const OSM_S3_BASE =
  'https://openpois-public.s3.us-west-2.amazonaws.com/snapshots/osm/20260313/osm_snapshot_partitioned'

export const FSQ_S3_BASE =
  'https://openpois-public.s3.us-west-2.amazonaws.com/snapshots/foursquare/20260313/foursquare_snapshot_partitioned'

// Overture PMTiles (latest release — update URL on each Overture monthly release)
export const OVERTURE_PMTILES_URL =
  'https://tiles.overturemaps.org/2026-02-18.0/places.pmtiles'

// Confidence color ramp (conf_mean 0-1, 1 = stable)
export const COLORS = {
  low: '#d73027',      // red, conf < 0.3
  medium: '#fee08b',   // yellow, conf 0.3-0.7
  high: '#1a9850',     // green, conf > 0.7
  foursquare: '#3b82f6', // blue (deferred)
  cluster: '#6366f1',  // indigo for clusters
  geolocation: '#60a5fa', // light blue dot
}

export const CONFIDENCE_THRESHOLDS = { low: 0.3, high: 0.7 }

// OSM filter keys and their display labels
export const OSM_FILTER_KEYS = [
  { key: 'amenity', label: 'Amenity' },
  { key: 'shop', label: 'Shop' },
  { key: 'leisure', label: 'Leisure' },
  { key: 'healthcare', label: 'Healthcare' },
]

// Overture L0 categories
export const OVERTURE_CATEGORIES = [
  { key: 'food_and_drink', label: 'Food & Drink' },
  { key: 'shopping', label: 'Shopping' },
  { key: 'arts_and_entertainment', label: 'Arts & Entertainment' },
  { key: 'sports_and_recreation', label: 'Sports & Recreation' },
  { key: 'health_care', label: 'Health Care' },
]

// OpenFreeMap base map styles
export const BASE_MAP_STYLES = [
  {
    key: 'positron',
    label: 'Positron',
    url: 'https://tiles.openfreemap.org/styles/positron',
  },
  {
    key: 'liberty',
    label: 'Liberty',
    url: 'https://tiles.openfreemap.org/styles/liberty',
  },
  {
    key: 'dark',
    label: 'Dark Matter',
    url: 'https://tiles.openfreemap.org/styles/dark',
  },
]

// Foursquare L1 categories
export const FSQ_CATEGORIES = [
  { key: 'Dining and Drinking', label: 'Dining & Drinking' },
  { key: 'Retail', label: 'Retail' },
  { key: 'Arts and Entertainment', label: 'Arts & Entertainment' },
  { key: 'Sports and Recreation', label: 'Sports & Recreation' },
  { key: 'Health and Medicine', label: 'Health & Medicine' },
]

// Geohash config
export const GEOHASH_PRECISION = 4
export const MAX_GEOHASH_CELLS = 50

// Zoom thresholds
export const MIN_ZOOM_FOR_DATA = 14
export const CLUSTER_MAX_ZOOM = 12

// Nominatim
export const NOMINATIM_URL =
  'https://nominatim.openstreetmap.org/search'

// Initial map view — New York City (fallback if geolocation is denied)
export const INITIAL_CENTER = [-74.006, 40.7128]
export const INITIAL_ZOOM = 14
