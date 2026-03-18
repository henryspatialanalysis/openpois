// S3 URLs
export const OSM_S3_BASE =
  'https://openpois-public.s3.us-west-2.amazonaws.com/snapshots/osm/20260313/osm_snapshot_partitioned'

export const FSQ_S3_BASE =
  'https://openpois-public.s3.us-west-2.amazonaws.com/snapshots/foursquare/20260313/foursquare_snapshot_partitioned'

export const CONFLATED_S3_BASE =
  'https://openpois-public.s3.us-west-2.amazonaws.com/snapshots/conflated/20260318/conflated_partitioned'

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

// Conflated shared_label categories
// Sourced from src/openpois/conflation/data/match_radii.csv — update when labels change.
export const CONFLATED_LABELS = [
  'Alternative Medicine',
  'Arcade',
  'Arts Venue',
  'Bakery',
  'Bank',
  'Bar',
  'Bike shop',
  'Bookstore',
  'Bowling Alley',
  'Cafe',
  'Car Dealer',
  'Car Rental',
  'Car Repair',
  'Car Wash',
  'Casino',
  'Cell Phone Store',
  'Charging Station',
  'Clinic',
  'Clothing Store',
  'Community Center',
  'Convenience Store',
  'Counseling',
  'Dentist',
  'Dessert Shop',
  'Discount Store',
  'Dog Park',
  'Dry Cleaning',
  'Eye Care',
  'Farmers Market',
  'Fast Food',
  'Fitness Center',
  'Florist',
  'Furniture Store',
  'Garden Store',
  'Gas Station',
  'Golf Course',
  'Hardware',
  'Jewelry Store',
  'Kindergarten',
  'Laundromat',
  'Library',
  'Liquor Store',
  'Marina',
  'Market',
  'Massage Therapy',
  'Maternity Center',
  'Mental Health',
  'Movie Theater',
  'Museum',
  'Nightclub',
  'Occupational Therapy',
  'Park',
  'Performing Arts',
  'Pet Store',
  'Pharmacy',
  'Physical Therapy',
  'Playground',
  'Post Office',
  'Recreation',
  'Restaurant',
  'Salon and Hair',
  'School',
  'Shoe Store',
  'Shopping Center',
  'Social Club',
  'Specialty Store',
  'Speech Therapist',
  'Sports Outlet',
  'Stadium',
  'Supermarket',
  'Swimming Pool',
  'Thrift Store',
  'Tire Store',
  'University',
  'Veterinarian',
  'Wholesale Store',
  // "Other" categories last, unchecked by default
  'Other Amenity',
  'Other Healthcare',
  'Other Shop',
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

// Stadia Maps Geocoding
export const STADIA_GEOCODING_URL =
  'https://api.stadiamaps.com/geocoding/v1/search'

// Initial map view — Times Square (fallback if geolocation is denied)
export const INITIAL_CENTER = [-73.9855, 40.758]
export const INITIAL_ZOOM = 18
