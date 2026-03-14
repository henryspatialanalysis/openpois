#   -------------------------------------------------------------
#   Copyright (c) Henry Spatial Analysis. All rights reserved.
#   Licensed under the MIT License. See LICENSE in project root for information.
#   -------------------------------------------------------------

"""
pyosmium utilities for collecting POI records from OSM PBF files.

``POIRecordBuilder`` — a plain class whose ``process_*`` methods accept
individual pyosmium objects and return a record dict (or ``None``).
Designed for use with ``osmium.FileProcessor``.
"""
from __future__ import annotations

import osmium
from shapely.geometry import LineString, MultiPolygon, Point, Polygon


class POIRecordBuilder:
    """
    Builds POI record dicts from individual pyosmium objects.

    Each ``process_*`` method returns a dict ready for a GeoDataFrame row,
    or ``None`` if the element should be skipped.
    """

    def __init__(
        self,
        source_label: str,
        filter_keys: list[str] | None = None,
        extract_keys: list[str] | None = None,
    ) -> None:
        """
        Args:
            source_label: Value for the 'source' column.
            filter_keys: Tag keys used to decide which elements to keep.
                An element is accepted if it has at least one of these keys.
                If None, all elements are accepted.
            extract_keys: Tag keys to include as output columns. Each key
                becomes a column (None for absent values). If None, all
                tags on the element are extracted.
        """
        self._filter_keys = (
            set(filter_keys) if filter_keys is not None else None
        )
        self._extract_keys = (
            set(extract_keys) if extract_keys is not None else None
        )
        self._source_label = source_label

    def _extract_tags(self, tags: osmium.osm.TagList) -> dict:
        """
        Return a dict of tag values.

        If extract_keys is set, only those keys are returned (None for absent
        keys); otherwise all tags are returned.

        Args:
            tags: The tag list from a pyosmium OSM object.

        Returns:
            Dict mapping tag key strings to their values.
        """
        if self._extract_keys is not None:
            return {key: tags.get(key) for key in self._extract_keys}
        return {tag.k: tag.v for tag in tags}

    def _has_target_tag(self, tags: osmium.osm.TagList) -> bool:
        """
        Return True if the element has at least one tag in filter_keys.

        If filter_keys was not set, always returns True.

        Args:
            tags: The tag list from a pyosmium OSM object.

        Returns:
            True if the element passes the tag filter.
        """
        if self._filter_keys is None:
            return True
        return any(key in tags for key in self._filter_keys)

    def process_node(self, n: osmium.osm.Node) -> dict | None:
        """
        Build a POI record dict from a pyosmium Node object.

        Args:
            n: A pyosmium Node object.

        Returns:
            A record dict for the node, or None if it should be skipped.
        """
        if not self._has_target_tag(n.tags):
            return None
        rec = self._extract_tags(n.tags)
        rec.update({
            "source": self._source_label,
            "osm_id": n.id,
            "osm_type": "node",
            "name": n.tags.get("name"),
            "geometry": Point(n.location.lon, n.location.lat),
        })
        return rec

    def process_way(self, w: osmium.osm.Way) -> dict | None:
        """
        Build a POI record dict from a pyosmium Way object.

        Closed ways (ring polygons) are stored as Polygon geometries;
        open ways use their centroid as a Point.

        Args:
            w: A pyosmium Way object.

        Returns:
            A record dict for the way, or None if it should be skipped or
            cannot be resolved due to missing node locations.
        """
        if not self._has_target_tag(w.tags):
            return None
        try:
            coords = [(nd.lon, nd.lat) for nd in w.nodes]
        except osmium.InvalidLocationError:
            return None
        if len(coords) < 2:
            return None

        if coords[0] == coords[-1] and len(coords) >= 4:
            geom = Polygon(coords)
        else:
            geom = LineString(coords).centroid

        rec = self._extract_tags(w.tags)
        rec.update({
            "source": self._source_label,
            "osm_id": w.id,
            "osm_type": "way",
            "name": w.tags.get("name"),
            "geometry": geom,
        })
        return rec

    def process_area(self, a: osmium.osm.Area) -> dict | None:
        """
        Build a POI record dict from a pyosmium Area object.

        Only processes relation-derived areas; closed-way areas are already
        captured by process_way() and are skipped here.

        Args:
            a: A pyosmium Area object.

        Returns:
            A record dict for the area, or None if it should be skipped or
            cannot be resolved due to missing node locations.
        """
        # Closed ways are already captured by process_way(); only handle
        # relation-derived areas.
        if a.from_way():
            return None
        if not self._has_target_tag(a.tags):
            return None
        try:
            polygons = []
            for outer in a.outer_rings():
                outer_coords = [(n.lon, n.lat) for n in outer]
                if len(outer_coords) < 4:
                    continue
                inner_rings = [
                    [(n.lon, n.lat) for n in inner]
                    for inner in a.inner_rings(outer)
                ]
                polygons.append(Polygon(outer_coords, inner_rings))
        except osmium.InvalidLocationError:
            return None
        if not polygons:
            return None
        geom = (
            polygons[0] if len(polygons) == 1 else MultiPolygon(polygons)
        )
        rec = self._extract_tags(a.tags)
        rec.update({
            "source": self._source_label,
            "osm_id": a.orig_id(),
            "osm_type": "relation",
            "name": a.tags.get("name"),
            "geometry": geom,
        })
        return rec
