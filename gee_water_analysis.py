"""
GEE Surface Water Proximity Analysis
=====================================
Uses the JRC Global Surface Water dataset (Landsat-derived, 1984–present)
to find which water points are within 500m of a detected water body.

For population impact analysis, use gee_population_impact.py instead.

Outputs
-------
  gee_surface_water_tiles.json  →  tile URLs for the dashboard map layers
  gee_proximity_results.json    →  per-point proximity flags + distance
  gee_summary.json              →  aggregate stats for the dashboard panel

Usage (notebook)
----------------
  import gee_water_analysis as gwa
  gwa.run(
      key_file=r'path/to/gee-key.json',
      csv_path=r'path/to/wpdx_enhanced.csv',
      out_dir=r'path/to/gdg_hackathon'
  )
"""

import ee
import json
import csv
import io
import argparse
from pathlib import Path
from datetime import datetime
from collections import defaultdict

# ── Config ────────────────────────────────────────────────────────────────────
DISTANCE_THRESHOLD_M = 500          # water point must be within this to count as "near water"
JRC_DATASET          = "JRC/GSW1_4/GlobalSurfaceWater"
WATER_OCCURRENCE_MIN = 50           # only count pixels that are water ≥50% of the time

KENYA_BOUNDS_COORDS  = (33.9, -4.7, 41.9, 4.6)   # west, south, east, north


def get_kenya_bounds():
    """Return Kenya bounding box — only call after ee.Initialize()."""
    return ee.Geometry.BBox(*KENYA_BOUNDS_COORDS)


# ── Authentication ────────────────────────────────────────────────────────────

def authenticate(key_file: str):
    """Initialise GEE with a service account JSON key file."""
    credentials = ee.ServiceAccountCredentials(email=None, key_file=key_file)
    ee.Initialize(credentials)
    print("✓ GEE authenticated")


def authenticate_personal():
    """Interactive auth with a personal Google account (alternative to service account)."""
    ee.Authenticate()
    ee.Initialize(project="your-cloud-project-id")   # replace with your project ID
    print("✓ GEE authenticated (personal)")


# ── JRC Surface Water images ──────────────────────────────────────────────────

def get_surface_water_mask() -> ee.Image:
    """
    JRC occurrence layer → binary mask.
    1 = pixel is water ≥ WATER_OCCURRENCE_MIN % of the time.
    """
    occurrence = ee.Image(JRC_DATASET).select("occurrence")
    return occurrence.gte(WATER_OCCURRENCE_MIN).selfMask()


def get_water_distance_image() -> ee.Image:
    """
    Distance (metres) from each pixel to the nearest permanent/seasonal water body.
    Clipped to Kenya bounds.
    """
    distance = get_surface_water_mask().distance(
        ee.Kernel.euclidean(radius=DISTANCE_THRESHOLD_M + 100, units="meters")
    ).rename("distance_to_water")
    return distance.clip(get_kenya_bounds())


# ── Tile URLs ─────────────────────────────────────────────────────────────────

def generate_tile_urls() -> dict:
    """
    Generate three XYZ tile URL layers for the dashboard map:
      1. surface_water     — blue overlay of detected water bodies
      2. water_occurrence  — blue gradient (darker = more permanent water)
      3. water_proximity   — green/red heatmap of distance to nearest water
    """
    print("Generating tile URLs...")

    # Layer 1: Surface water binary mask (blue)
    water_vis    = {"min": 0, "max": 1, "palette": ["#1565c0"]}
    water_map_id = get_surface_water_mask().getMapId(water_vis)

    # Layer 2: Occurrence frequency (blue gradient)
    gsw       = ee.Image(JRC_DATASET).select("occurrence")
    occ_mask  = gsw.gte(WATER_OCCURRENCE_MIN).selfMask().clip(get_kenya_bounds())
    occ_vis   = {
        "min": WATER_OCCURRENCE_MIN, "max": 100,
        "palette": ["#90caf9", "#42a5f5", "#1e88e5", "#1565c0", "#0d47a1"],
    }
    occ_map_id = gsw.updateMask(occ_mask).getMapId(occ_vis)

    # Layer 3: Proximity heatmap (green = near water, red = far)
    dist_image  = get_water_distance_image()
    prox_image  = ee.Image(DISTANCE_THRESHOLD_M).subtract(dist_image).max(ee.Image(0))
    prox_vis    = {
        "min": 0, "max": DISTANCE_THRESHOLD_M,
        "palette": ["#d32f2f", "#ff9800", "#ffeb3b", "#66bb6a", "#1b5e20"],
        "opacity": 0.6,
    }
    prox_map_id = prox_image.getMapId(prox_vis)

    tiles = {
        "generated_at":             datetime.now().isoformat(),
        "distance_threshold_m":     DISTANCE_THRESHOLD_M,
        "jrc_dataset":              JRC_DATASET,
        "water_occurrence_min_pct": WATER_OCCURRENCE_MIN,
        "layers": {
            "surface_water": {
                "label":       "Surface Water Bodies",
                "tile_url":    water_map_id["tile_fetcher"].url_format,
                "description": f"JRC water bodies detected ≥{WATER_OCCURRENCE_MIN}% of the time (1984–present)",
                "color":       "#1565c0",
            },
            "water_occurrence": {
                "label":       "Water Occurrence Frequency",
                "tile_url":    occ_map_id["tile_fetcher"].url_format,
                "description": "How often each pixel is classified as water. Darker = more permanent.",
                "color":       "#1e88e5",
            },
            "water_proximity": {
                "label":       f"Proximity to Water (≤{DISTANCE_THRESHOLD_M}m)",
                "tile_url":    prox_map_id["tile_fetcher"].url_format,
                "description": f"Green = within {DISTANCE_THRESHOLD_M}m of a water body, Red = farther away.",
                "color":       "#66bb6a",
            },
        },
    }

    print("✓ Tile URLs generated")
    return tiles


# ── Load water points from CSV ────────────────────────────────────────────────

def load_water_points(csv_path: str) -> list[dict]:
    """Load WPdx water points, skipping the HXL tag row if present."""
    with open(csv_path, encoding="utf-8") as fh:
        lines = fh.readlines()

    if len(lines) > 1 and lines[1].strip().startswith("#"):
        content = lines[0] + "".join(lines[2:])
    else:
        content = "".join(lines)

    w, s, e, n = KENYA_BOUNDS_COORDS
    points = []
    for row in csv.DictReader(io.StringIO(content)):
        try:
            lat = float(row.get("lat_deg") or row.get("lat") or 0)
            lon = float(row.get("lon_deg") or row.get("lon") or 0)
            if not (s <= lat <= n and w <= lon <= e):
                continue
            points.append({
                "wpdx_id":     row.get("wpdx_id", ""),
                "lat":         lat,
                "lon":         lon,
                "status":      row.get("status_clean") or row.get("functional_status", ""),
                "adm1":        row.get("clean_adm1", ""),
                "adm2":        row.get("clean_adm2", ""),
                "tech":        row.get("water_tech_category", ""),
                "pop":         row.get("local_population", ""),
                "criticality": row.get("criticality", ""),
            })
        except (ValueError, TypeError):
            continue

    print(f"✓ Loaded {len(points):,} water points from {csv_path}")
    return points


# ── Per-point proximity analysis ──────────────────────────────────────────────

def analyse_proximity_batch(points: list[dict], batch_size: int = 500) -> list[dict]:
    """
    For each water point, sample:
      - distance_to_water_m  : metres to nearest JRC water body
      - near_water           : True if distance ≤ DISTANCE_THRESHOLD_M
      - water_occurrence_pct : % of time that pixel is classified as water (0–100)

    Processes in batches to respect GEE rate limits.
    """
    combined = get_water_distance_image().addBands(
        ee.Image(JRC_DATASET).select("occurrence").clip(get_kenya_bounds())
    )

    results = []
    total   = len(points)

    for start in range(0, total, batch_size):
        batch    = points[start: start + batch_size]
        features = ee.FeatureCollection([
            ee.Feature(
                ee.Geometry.Point([p["lon"], p["lat"]]),
                {"wpdx_id": p["wpdx_id"]}
            )
            for p in batch
        ])

        sampled = combined.sampleRegions(
            collection=features,
            scale=30,         # JRC native resolution
            geometries=True,
            tileScale=4,
        )

        try:
            fc_info    = sampled.getInfo()
            sample_map = {
                f["properties"]["wpdx_id"]: f["properties"]
                for f in fc_info["features"]
            }
        except Exception as ex:
            print(f"  ⚠ Batch {start}–{start+batch_size} GEE error: {ex}")
            sample_map = {}

        for p in batch:
            props = sample_map.get(p["wpdx_id"], {})
            dist  = props.get("distance_to_water")
            occ   = props.get("occurrence")

            result = {**p}
            result["distance_to_water_m"]  = round(dist, 1) if dist is not None else None
            result["near_water"]           = dist is not None and dist <= DISTANCE_THRESHOLD_M
            result["water_occurrence_pct"] = round(occ, 1) if occ is not None else 0
            results.append(result)

        print(f"  Processed {min(start + batch_size, total):,} / {total:,} points...")

    return results


# ── Summary stats ─────────────────────────────────────────────────────────────

def compute_summary(results: list[dict]) -> dict:
    """Aggregate proximity stats for the dashboard Surface Water panel."""
    total    = len(results)
    near     = [r for r in results if r["near_water"]]
    not_near = [r for r in results if not r["near_water"] and r["distance_to_water_m"] is not None]
    no_data  = [r for r in results if r["distance_to_water_m"] is None]

    # Broken points near an existing water source = best rehab candidates
    rehab_candidates = [
        r for r in near
        if "Non-Functional" in r.get("status", "") or "needs repair" in r.get("status", "").lower()
    ]

    # Near-water counts by county
    county_near: dict[str, int] = defaultdict(int)
    for r in near:
        county_near[r.get("adm1", "Unknown")] += 1

    top_counties = sorted(county_near.items(), key=lambda x: x[1], reverse=True)[:10]

    return {
        "generated_at":            datetime.now().isoformat(),
        "distance_threshold_m":    DISTANCE_THRESHOLD_M,
        "total_points":            total,
        "near_water":              len(near),
        "near_water_pct":          round(len(near) / total * 100, 1) if total else 0,
        "not_near_water":          len(not_near),
        "no_data":                 len(no_data),
        "rehab_candidates":        len(rehab_candidates),
        "rehab_candidates_pct":    round(len(rehab_candidates) / len(near) * 100, 1) if near else 0,
        "top_counties_near_water": [{"county": c, "count": n} for c, n in top_counties],
        "note": (
            f"Rehabilitation candidates = non-functional or needs-repair points within "
            f"{DISTANCE_THRESHOLD_M}m of a JRC-detected water body. Water source exists "
            f"but infrastructure has failed. Run gee_population_impact.py for population analysis."
        ),
    }


# ── Main ──────────────────────────────────────────────────────────────────────

def run(key_file: str, csv_path: str, out_dir: str = "."):
    out = Path(out_dir)
    out.mkdir(exist_ok=True)

    # 1. Authenticate
    if key_file.lower() == "personal":
        ee.Initialize(project="hydrotrack-489711")
    else:
        authenticate(key_file)

    # 2. Tile URLs
    tiles = generate_tile_urls()
    (out / "gee_surface_water_tiles.json").write_text(json.dumps(tiles, indent=2))
    print("✓ Tile URLs → gee_surface_water_tiles.json")

    # 3. Load points
    points = load_water_points(csv_path)

    # 4. Proximity analysis
    print(f"\nRunning proximity analysis ({DISTANCE_THRESHOLD_M}m threshold)...")
    results = analyse_proximity_batch(points)
    (out / "gee_proximity_results.json").write_text(json.dumps(results, indent=2))
    print("✓ Per-point results → gee_proximity_results.json")

    # 5. Summary
    summary = compute_summary(results)
    (out / "gee_summary.json").write_text(json.dumps(summary, indent=2))
    print("✓ Summary → gee_summary.json")

    # 6. Report
    print("\n" + "=" * 52)
    print("  Surface Water Proximity — Results")
    print("=" * 52)
    print(f"  Total points analysed    : {summary['total_points']:>7,}")
    print(f"  Within {DISTANCE_THRESHOLD_M}m of water body : {summary['near_water']:>7,}  ({summary['near_water_pct']}%)")
    print(f"  Not near water           : {summary['not_near_water']:>7,}")
    print(f"  No satellite data        : {summary['no_data']:>7,}")
    print(f"  Rehab candidates         : {summary['rehab_candidates']:>7,}  ({summary['rehab_candidates_pct']}% of near-water)")
    print("\n  Top counties (near-water points):")
    for row in summary["top_counties_near_water"]:
        print(f"    {row['county']:<22} {row['count']:>4}")
    print("=" * 52)
    print("\n  ➜ For population impact, run gee_population_impact.py")

    return summary


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="GEE Surface Water Proximity Analysis")
    parser.add_argument("--key",   required=True,
                        help="Path to service account JSON key, or 'personal' for interactive auth")
    parser.add_argument("--input", required=True, help="Path to WPdx CSV (raw or cleaned)")
    parser.add_argument("--out",   default=".", help="Output directory (default: current dir)")
    args = parser.parse_args()
    run(key_file=args.key, csv_path=args.input, out_dir=args.out)