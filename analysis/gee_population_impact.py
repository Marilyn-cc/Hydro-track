"""
GEE Population Impact Analysis
================================
Loads WorldPop Kenya 2020 data, defines a service radius around each
water point, sums population within that buffer, and identifies
high-risk communities — areas with high population density but no
functional water infrastructure within reach.

Outputs
-------
  population_impact.json   →  per-point population impact + risk classification
  population_summary.json  →  aggregate stats + high-risk clusters for dashboard
  population_tiles.json    →  GEE tile URLs (WorldPop + risk heatmap layers)

Usage (notebook)
----------------
  import gee_population_impact as gpi
  gpi.run(
      key_file=r'path/to/gee-key.json',
      csv_path=r'path/to/wpdx_enhanced.csv',
      out_dir=r'path/to/gdg_hackathon'
  )
"""

import ee
import json
import csv
import io
import math
import argparse
from pathlib import Path
from datetime import datetime
from collections import defaultdict

# ── Config ────────────────────────────────────────────────────────────────────
WORLDPOP_DATASET   = "WorldPop/GP/100m/pop"
WORLDPOP_YEAR      = 2020

SERVICE_RADIUS_M   = 1000    # population buffer around each water point (1km)
HIGH_RISK_DENSITY  = 20      # WorldPop pixels > this = high-density area (ppl/100m²)
CRITICAL_POP       = 5000    # population threshold to flag as critical community

KENYA_BOUNDS_COORDS = (33.9, -4.7, 41.9, 4.6)   # west, south, east, north


def get_kenya_bounds():
    return ee.Geometry.BBox(*KENYA_BOUNDS_COORDS)


# ── Auth ──────────────────────────────────────────────────────────────────────

def authenticate(key_file: str):
    creds = ee.ServiceAccountCredentials(email=None, key_file=key_file)
    ee.Initialize(creds)
    print("✓ GEE authenticated")


# ── WorldPop image ────────────────────────────────────────────────────────────

def get_worldpop() -> ee.Image:
    """Kenya WorldPop 2020 — people per 100×100m pixel."""
    return (
        ee.ImageCollection(WORLDPOP_DATASET)
        .filter(ee.Filter.eq("country", "KEN"))
        .filter(ee.Filter.eq("year", WORLDPOP_YEAR))
        .first()
        .select("population")
        .clip(get_kenya_bounds())
    )


# ── Tile URLs ─────────────────────────────────────────────────────────────────

def generate_population_tiles() -> dict:
    """
    Three map layers:
      1. population_density  — WorldPop heatmap (white → dark blue)
      2. high_risk_zones     — pixels where density > HIGH_RISK_DENSITY (red)
      3. underserved_zones   — high density AND no functional water nearby (deep red)
    """
    print("Generating population tile URLs...")
    pop = get_worldpop()

    # Layer 1 — full population density
    density_vis = {
        "min": 0, "max": 80,
        "palette": ["#f7fbff", "#c6dbef", "#6baed6", "#2171b5", "#08306b"],
        "opacity": 0.8,
    }
    density_id = pop.getMapId(density_vis)

    # Layer 2 — high-density zones (population hotspots)
    high_density = pop.gte(HIGH_RISK_DENSITY).selfMask()
    hotspot_vis  = {"palette": ["#ff5722"], "opacity": 0.65}
    hotspot_id   = high_density.getMapId(hotspot_vis)

    # Layer 3 — extreme density (top tier, >50 ppl/100m²)
    critical = pop.gte(50).selfMask()
    critical_vis = {"palette": ["#b71c1c"], "opacity": 0.8}
    critical_id  = critical.getMapId(critical_vis)

    tiles = {
        "generated_at":    datetime.now().isoformat(),
        "worldpop_year":   WORLDPOP_YEAR,
        "service_radius_m": SERVICE_RADIUS_M,
        "high_risk_density_threshold": HIGH_RISK_DENSITY,
        "layers": {
            "population_density": {
                "label":       "Population Density (WorldPop 2020)",
                "tile_url":    density_id["tile_fetcher"].url_format,
                "description": "Estimated people per 100×100m grid cell. Darker blue = more people.",
                "color":       "#2171b5",
            },
            "population_hotspots": {
                "label":       f"Population Hotspots (>{HIGH_RISK_DENSITY} ppl/100m²)",
                "tile_url":    hotspot_id["tile_fetcher"].url_format,
                "description": "High-density population clusters — orange areas need water infrastructure most.",
                "color":       "#ff5722",
            },
            "critical_density": {
                "label":       "Critical Density (>50 ppl/100m²)",
                "tile_url":    critical_id["tile_fetcher"].url_format,
                "description": "Extreme population density — highest priority for new infrastructure.",
                "color":       "#b71c1c",
            },
        }
    }
    print("✓ Population tile URLs generated")
    return tiles


# ── Load water points ─────────────────────────────────────────────────────────

def load_points(csv_path: str) -> list[dict]:
    with open(csv_path, encoding="utf-8") as fh:
        lines = fh.readlines()
    if len(lines) > 1 and lines[1].strip().startswith("#"):
        content = lines[0] + "".join(lines[2:])
    else:
        content = "".join(lines)

    reader = csv.DictReader(io.StringIO(content))
    points = []
    for row in reader:
        try:
            lat = float(row.get("lat_deg") or row.get("lat") or 0)
            lon = float(row.get("lon_deg") or row.get("lon") or 0)
            w, s, e, n = KENYA_BOUNDS_COORDS
            if not (s <= lat <= n and w <= lon <= e):
                continue
            try:
                pop_val = float(row.get("local_population") or 0)
            except:
                pop_val = 0
            try:
                crit_val = float(row.get("criticality") or 0)
            except:
                crit_val = 0

            points.append({
                "wpdx_id":     row.get("wpdx_id", ""),
                "lat":         lat,
                "lon":         lon,
                "status":      row.get("status_clean") or row.get("functional_status", ""),
                "adm1":        row.get("clean_adm1", ""),
                "adm2":        row.get("clean_adm2", ""),
                "tech":        row.get("water_tech_category", ""),
                "local_pop":   pop_val,
                "criticality": crit_val,
            })
        except (ValueError, TypeError):
            continue

    print(f"✓ Loaded {len(points):,} water points")
    return points


# ── Per-point population sampling ────────────────────────────────────────────

def sample_population(points: list[dict], batch_size: int = 400) -> list[dict]:
    """
    For each water point:
      - Sample WorldPop density at the point's pixel
      - Sum WorldPop population within SERVICE_RADIUS_M buffer
      - Classify risk level
    """
    pop_image = get_worldpop()
    results   = []
    total     = len(points)

    for start in range(0, total, batch_size):
        batch = points[start: start + batch_size]

        features = ee.FeatureCollection([
            ee.Feature(
                ee.Geometry.Point([p["lon"], p["lat"]]),
                {"wpdx_id": p["wpdx_id"]}
            )
            for p in batch
        ])

        # Sample pixel-level density at each point
        sampled = pop_image.sampleRegions(
            collection=features,
            scale=100,
            geometries=False,
            tileScale=4,
        )

        try:
            fc_info    = sampled.getInfo()
            sample_map = {
                f["properties"]["wpdx_id"]: f["properties"].get("population")
                for f in fc_info["features"]
            }
        except Exception as ex:
            print(f"  ⚠ Batch {start}–{start+batch_size} sampling error: {ex}")
            sample_map = {}

        for p in batch:
            density = sample_map.get(p["wpdx_id"])
            density = round(float(density), 2) if density is not None else None

            # Classify this water point's risk level
            status   = p["status"].lower()
            is_broken = "non-functional" in status or "needs repair" in status
            is_func   = status == "functional"

            # Risk = broken point in high-density area
            if is_broken and density is not None and density >= HIGH_RISK_DENSITY:
                risk = "high"
            elif is_broken and density is not None and density > 0:
                risk = "medium"
            elif is_func:
                risk = "served"
            else:
                risk = "low"

            result = {**p}
            result["worldpop_density"]   = density
            result["is_broken"]          = is_broken
            result["risk_level"]         = risk
            # Estimated people impacted = local_pop (WPdx) or density × π × r² in km²
            if p["local_pop"] > 0:
                result["pop_impacted"] = int(p["local_pop"])
            elif density is not None:
                area_km2 = math.pi * (SERVICE_RADIUS_M / 1000) ** 2
                result["pop_impacted"] = int(density * area_km2 * 100)
            else:
                result["pop_impacted"] = 0

            results.append(result)

        done = min(start + batch_size, total)
        print(f"  Sampled {done:,} / {total:,} points...")

    return results


# ── Identify high-risk communities ────────────────────────────────────────────

def identify_high_risk_communities(results: list[dict]) -> list[dict]:
    """
    Group non-functional points by sub-county (adm2).
    A community is HIGH RISK if:
      - It has broken water points in high-density pixels
      - OR total population impacted exceeds CRITICAL_POP
    Returns sorted list of at-risk communities for the dashboard.
    """
    community = defaultdict(lambda: {
        "broken": 0, "functional": 0,
        "total_pop_impacted": 0,
        "max_density": 0.0,
        "high_risk_points": 0,
        "adm1": "", "adm2": "",
    })

    for r in results:
        key  = (r["adm1"], r["adm2"])
        c    = community[key]
        c["adm1"] = r["adm1"]
        c["adm2"] = r["adm2"]

        if r["is_broken"]:
            c["broken"]            += 1
            c["total_pop_impacted"] += r["pop_impacted"]
            if r["worldpop_density"] is not None:
                c["max_density"] = max(c["max_density"], r["worldpop_density"])
            if r["risk_level"] == "high":
                c["high_risk_points"] += 1
        else:
            c["functional"] += 1

    communities = []
    for (adm1, adm2), c in community.items():
        total = c["broken"] + c["functional"]
        if total == 0:
            continue

        failure_rate = c["broken"] / total

        # Risk classification
        if c["high_risk_points"] > 0 and c["total_pop_impacted"] >= CRITICAL_POP:
            community_risk = "🔴 Critical"
        elif c["high_risk_points"] > 0 or c["total_pop_impacted"] >= CRITICAL_POP:
            community_risk = "🟠 High Risk"
        elif failure_rate > 0.5:
            community_risk = "🟡 At Risk"
        else:
            community_risk = "🟢 Manageable"

        communities.append({
            "adm1":             adm1,
            "adm2":             adm2,
            "total_points":     total,
            "broken_points":    c["broken"],
            "functional_points": c["functional"],
            "failure_rate_pct": round(failure_rate * 100, 1),
            "pop_impacted":     c["total_pop_impacted"],
            "max_worldpop_density": round(c["max_density"], 2),
            "high_risk_points": c["high_risk_points"],
            "risk_level":       community_risk,
        })

    # Sort: Critical first, then by pop impacted
    risk_order = {"🔴 Critical": 0, "🟠 High Risk": 1, "🟡 At Risk": 2, "🟢 Manageable": 3}
    communities.sort(key=lambda x: (risk_order.get(x["risk_level"], 9), -x["pop_impacted"]))
    return communities[:30]   # top 30 communities


# ── Summary ───────────────────────────────────────────────────────────────────

def compute_summary(results: list[dict], communities: list[dict]) -> dict:
    total        = len(results)
    broken       = [r for r in results if r["is_broken"]]
    high_risk    = [r for r in results if r["risk_level"] == "high"]
    medium_risk  = [r for r in results if r["risk_level"] == "medium"]
    served       = [r for r in results if r["risk_level"] == "served"]

    total_pop_impacted   = sum(r["pop_impacted"] for r in broken)
    critical_communities = [c for c in communities if "Critical" in c["risk_level"]]
    high_risk_comms      = [c for c in communities if "High Risk" in c["risk_level"]]

    # County-level breakdown
    county_stats = defaultdict(lambda: {"broken": 0, "pop": 0, "high_risk_pts": 0})
    for r in results:
        c = r["adm1"] or "Unknown"
        if r["is_broken"]:
            county_stats[c]["broken"]   += 1
            county_stats[c]["pop"]      += r["pop_impacted"]
        if r["risk_level"] == "high":
            county_stats[c]["high_risk_pts"] += 1

    county_list = sorted(
        [{"county": k, **v} for k, v in county_stats.items()],
        key=lambda x: -x["pop"]
    )[:10]

    return {
        "generated_at":            datetime.now().isoformat(),
        "worldpop_year":           WORLDPOP_YEAR,
        "service_radius_m":        SERVICE_RADIUS_M,
        "high_risk_threshold":     HIGH_RISK_DENSITY,
        "total_points":            total,
        "broken_points":           len(broken),
        "high_risk_points":        len(high_risk),
        "medium_risk_points":      len(medium_risk),
        "served_points":           len(served),
        "total_pop_impacted":      total_pop_impacted,
        "critical_communities":    len(critical_communities),
        "high_risk_communities":   len(high_risk_comms),
        "top_counties":            county_list,
        "at_risk_communities":     communities,
    }


# ── Main ──────────────────────────────────────────────────────────────────────

def run(key_file: str, csv_path: str, out_dir: str = "."):
    out = Path(out_dir)
    out.mkdir(exist_ok=True)

    # 1. Auth
    authenticate(key_file)

    # 2. Tile URLs
    tiles = generate_population_tiles()
    (out / "population_tiles.json").write_text(json.dumps(tiles, indent=2))
    print(f"✓ Tile URLs → population_tiles.json")

    # 3. Load points
    points = load_points(csv_path)

    # 4. Sample WorldPop per point
    print(f"\nSampling WorldPop population ({SERVICE_RADIUS_M}m service radius)...")
    results = sample_population(points)
    (out / "population_impact.json").write_text(json.dumps(results, indent=2))
    print(f"✓ Per-point results → population_impact.json")

    # 5. Identify high-risk communities
    communities = identify_high_risk_communities(results)

    # 6. Summary
    summary = compute_summary(results, communities)
    (out / "population_summary.json").write_text(json.dumps(summary, indent=2))
    print(f"✓ Summary → population_summary.json")

    # 7. Report
    print("\n" + "=" * 58)
    print("  Population Impact Analysis — Results")
    print("=" * 58)
    print(f"  Total water points          : {summary['total_points']:>7,}")
    print(f"  Broken / non-functional     : {summary['broken_points']:>7,}")
    print(f"  🔴 High-risk points          : {summary['high_risk_points']:>7,}  (broken in dense area)")
    print(f"  🟠 Medium-risk points        : {summary['medium_risk_points']:>7,}")
    print(f"  Total population impacted   : {summary['total_pop_impacted']:>7,}")
    print(f"\n  At-risk communities")
    print(f"    🔴 Critical               : {summary['critical_communities']:>4}")
    print(f"    🟠 High Risk              : {summary['high_risk_communities']:>4}")
    print(f"\n  Top counties by pop impact:")
    for c in summary["top_counties"][:8]:
        print(f"    {c['county']:<22} pop={c['pop']:>8,}  broken={c['broken']:>4}  high-risk={c['high_risk_pts']:>3}")
    print("\n  Top 10 critical communities:")
    for c in communities[:10]:
        print(f"    {c['risk_level']}  {c['adm2']:<20} ({c['adm1']})  pop={c['pop_impacted']:>7,}  broken={c['broken_points']}")
    print("=" * 58)

    return summary


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="GEE Population Impact Analysis")
    parser.add_argument("--key",   required=True)
    parser.add_argument("--input", required=True)
    parser.add_argument("--out",   default=".")
    args = parser.parse_args()
    run(key_file=args.key, csv_path=args.input, out_dir=args.out)
