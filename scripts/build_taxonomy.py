#!/usr/bin/env python3
"""Generate site/public/taxonomy.html from the conflation data CSVs.

Run from the repo root:
    python scripts/build_taxonomy.py
"""

import pandas as pd
from pathlib import Path

REPO_ROOT = Path(__file__).parent.parent
DATA_DIR = REPO_ROOT / "src/openpois/conflation/data"
OUTPUT = REPO_ROOT / "site/public/taxonomy.html"


def osm_cell(group):
    """Format OSM tags for one shared_label, grouped by key."""
    by_key = {}
    wildcard_keys = []
    for _, row in group.iterrows():
        if row["osm_value"] == "*":
            wildcard_keys.append(row["osm_key"])
        else:
            by_key.setdefault(row["osm_key"], []).append(row["osm_value"])
    parts = []
    for key, vals in by_key.items():
        vals_str = ", ".join(vals)
        wiki = f"https://wiki.openstreetmap.org/wiki/{key.capitalize()}"
        parts.append(
            f'<a href="{wiki}" target="_blank" rel="noopener noreferrer"'
            f' class="tx-key">{key}</a>={vals_str}'
        )
    for key in wildcard_keys:
        wiki = f"https://wiki.openstreetmap.org/wiki/{key.capitalize()}"
        parts.append(
            f'<a href="{wiki}" target="_blank" rel="noopener noreferrer"'
            f' class="tx-key">{key}</a>=* — all other {key} tags'
        )
    return "<br>".join(parts)


def overture_cell(group):
    """Format Overture categories for one shared_label."""
    parts = []
    for _, row in group.iterrows():
        l0 = row["overture_l0"]
        l1 = row.get("overture_l1", "")
        l2 = row.get("overture_l2", "")
        if l1 and l2:
            parts.append(
                f'<span class="tx-key">{l0} &rsaquo;'
                f' {l1}:</span> {l2}'
            )
        elif l1:
            parts.append(
                f'<span class="tx-key">{l0}:</span> {l1}'
            )
        elif l2:
            parts.append(
                f'<span class="tx-key">{l0}:</span> {l2}'
            )
        else:
            parts.append(
                f'<span class="tx-key">{l0}</span>'
            )
    return "<br>".join(parts)


def build_rows(radii, osm, overture):
    osm_html = (
        osm.groupby("shared_label")
        .apply(osm_cell, include_groups = False)
        .rename("osm_html")
    )
    overture_html = (
        overture.groupby("shared_label")
        .apply(overture_cell, include_groups = False)
        .rename("overture_html")
    )
    df = (
        radii.set_index("shared_label")
        .join(osm_html)
        .join(overture_html)
        .fillna("")
    )
    df = df.iloc[
        sorted(range(len(df)), key = lambda i: (df.index[i].startswith("Other "), df.index[i]))
    ]
    rows = []
    for label, row in df.iterrows():
        rows.append(
            f"""        <tr>
          <td>{label}</td>
          <td class="tx-tags">{row['osm_html']}</td>
          <td class="tx-tags">{row['overture_html']}</td>
        </tr>"""
        )
    return "\n".join(rows)


def render(rows):
    return f"""<!DOCTYPE html>
<html lang="en">

<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>Taxonomy – OpenPOIs</title>
  <link rel="icon" type="image/x-icon" href="/favicon.ico" />
  <style>
    * {{ margin: 0; padding: 0; box-sizing: border-box; }}

    body {{
      font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto,
        Oxygen, Ubuntu, Cantarell, sans-serif;
      font-size: 15px;
      color: #333;
      background: #f8f9fa;
      line-height: 1.6;
    }}

    .header {{
      background: #fff;
      border-bottom: 1px solid #ddd;
      padding: 10px 24px;
      display: flex;
      align-items: center;
      gap: 16px;
    }}

    .header-back {{
      color: #2563eb;
      text-decoration: none;
      font-size: 14px;
      white-space: nowrap;
      flex-shrink: 0;
    }}

    .header-back:hover {{ text-decoration: underline; }}

    .header-spacer {{ flex: 1; }}

    .header-link {{
      color: #555;
      text-decoration: none;
      font-size: 14px;
      white-space: nowrap;
      flex-shrink: 0;
      display: flex;
      align-items: center;
      gap: 6px;
    }}

    .header-link:hover {{ color: #333; text-decoration: underline; }}

    .brand-logo {{
      height: 28px;
      width: auto;
      display: block;
    }}

    .github-icon {{
      width: 20px;
      height: 20px;
      fill: #555;
      flex-shrink: 0;
    }}

    .header-link:hover .github-icon {{ fill: #333; }}

    .content {{
      max-width: 1100px;
      margin: 48px auto;
      padding: 0 24px;
    }}

    h1 {{
      font-size: 28px;
      font-weight: 700;
      margin-bottom: 12px;
      color: #111;
    }}

    .lead {{
      font-size: 16px;
      color: #555;
      margin-bottom: 32px;
    }}

    /* Taxonomy table */
    .tx-table {{
      width: 100%;
      border-collapse: collapse;
      font-size: 13px;
      background: #fff;
      border: 1px solid #e5e7eb;
      border-radius: 6px;
      overflow: hidden;
    }}

    .tx-table th {{
      background: #f3f4f6;
      padding: 10px 14px;
      text-align: left;
      font-weight: 600;
      font-size: 13px;
      border-bottom: 2px solid #e5e7eb;
      white-space: nowrap;
    }}

    .tx-table td {{
      padding: 8px 14px;
      vertical-align: top;
      border-bottom: 1px solid #f0f0f0;
    }}

    .tx-table tbody tr:last-child td {{ border-bottom: none; }}

    .tx-table tbody tr:hover {{ background: #fafafa; }}

    .tx-tags {{ line-height: 1.8; }}

    .tx-key {{
      color: #888;
      font-size: 12px;
    }}

    footer {{
      text-align: center;
      padding: 32px 24px;
      color: #999;
      font-size: 13px;
      border-top: 1px solid #e5e7eb;
      margin-top: 48px;
    }}

    footer a {{ color: #2563eb; text-decoration: none; }}
    footer a:hover {{ text-decoration: underline; }}
  </style>
</head>

<body>

  <header class="header">
    <a href="/about.html" class="header-back">&#8592; About</a>
    <div class="header-spacer"></div>
    <a href="https://github.com/henryspatialanalysis/openpois" target="_blank"
      rel="noopener noreferrer" class="header-link" aria-label="GitHub repository">
      <svg class="github-icon" viewBox="0 0 16 16" aria-hidden="true">
        <path d="M8 0C3.58 0 0 3.58 0 8c0 3.54 2.29 6.53 5.47 7.59.4.07.55-.17.55-.38
          0-.19-.01-.82-.01-1.49-2.01.37-2.53-.49-2.69-.94-.09-.23-.48-.94-.82-1.13
          -.28-.15-.68-.52-.01-.53.63-.01 1.08.58 1.23.82.72 1.21 1.87.87 2.33.66
          .07-.52.28-.87.51-1.07-1.78-.2-3.64-.89-3.64-3.95 0-.87.31-1.59.82-2.15
          -.08-.2-.36-1.02.08-2.12 0 0 .67-.21 2.2.82.64-.18 1.32-.27 2-.27.68 0
          1.36.09 2 .27 1.53-1.04 2.2-.82 2.2-.82.44 1.1.16 1.92.08 2.12.51.56.82
          1.27.82 2.15 0 3.07-1.87 3.75-3.65 3.95.29.25.54.73.54 1.48 0 1.07-.01
          1.93-.01 2.2 0 .21.15.46.55.38A8.013 8.013 0 0016 8c0-4.42-3.58-8-8-8z"/>
      </svg>
      GitHub
    </a>
    <a href="https://henryspatialanalysis.com/" target="_blank"
      rel="noopener noreferrer" class="header-link">
      <img src="/logo.png" alt="Henry Spatial Analysis" class="brand-logo" />
    </a>
  </header>

  <main class="content">
    <h1>POI Taxonomy</h1>
    <p class="lead">
      OpenPOIs maps OpenStreetMap and Overture Maps categories to a shared set
      of labels used for conflation and filtering. The match radius controls
      how close two POIs from different sources must be to be considered the
      same place.
    </p>

    <table class="tx-table">
      <thead>
        <tr>
          <th>Shared label</th>
          <th>OpenStreetMap tags</th>
          <th>Overture Maps categories</th>
        </tr>
      </thead>
      <tbody>
{rows}
      </tbody>
    </table>
  </main>

  <footer>
    OpenPOIs &mdash; by <a href="https://henryspatialanalysis.com/">Henry Spatial Analysis</a>
    &mdash; <a href="https://github.com/henryspatialanalysis/openpois">GitHub</a>
    &mdash; MIT License
  </footer>

</body>
</html>
"""


def main():
    osm = pd.read_csv(DATA_DIR / "taxonomy_crosswalk_openstreetmap.csv")
    overture = pd.read_csv(DATA_DIR / "taxonomy_crosswalk_overture_maps.csv")
    radii = pd.read_csv(DATA_DIR / "match_radii.csv")
    rows = build_rows(radii, osm, overture)
    OUTPUT.write_text(render(rows), encoding = "utf-8")
    print(f"Written: {OUTPUT}")


if __name__ == "__main__":
    main()
