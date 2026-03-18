# Vue 3 + Vite

This template should help get you started developing with Vue 3 in Vite. The template uses Vue 3 `<script setup>` SFCs, check out the [script setup docs](https://v3.vuejs.org/api/sfc-script-setup.html#sfc-script-setup) to learn more.

Learn more about IDE Support for Vue in the [Vue Docs Scaling up Guide](https://vuejs.org/guide/scaling-up/tooling.html#ide-support).

## Maintenance

### Conflated category labels

The conflated data source filter uses a static list of `shared_label` values
defined in `src/constants.js` (`CONFLATED_LABELS`). This list is sourced from
`src/openpois/conflation/data/match_radii.csv`. If the taxonomy crosswalk adds
or removes labels, update both files to keep them in sync.

### S3 snapshot URLs

When a new snapshot is uploaded to S3, update the date strings in two places:

- `src/constants.js` — `OSM_S3_BASE`, `FSQ_S3_BASE`, and `CONFLATED_S3_BASE`
- `public/about.html` — hardcoded S3 paths and browse links in the data
  access section

The OSM/Foursquare and conflated snapshots may have different dates (e.g.
OSM `20260313`, conflated `20260318`), so update each URL individually.
