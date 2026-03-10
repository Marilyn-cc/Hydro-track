"""
GEE Depletion & Recharge Analysis
==================================
Analyzes long-term surface water transitions (1984–2022) and 
current vegetation/moisture stress (NDVI/NDWI) around water points.

Focus: Explaining 'Why' a point is failing based on recharge logic.
"""

import ee
import json
import csv
import io
import math
from pathlib import Path
from datetime import datetime

# -- Config --
BUFFER_RADIUS_M = 5000  # 5km buffer for recharge analysis
JRC_TRANSITIONS = "JRC/GSW1_4/GlobalSurfaceWater"
LANDSAT_COL     = "LANDSAT/LC08/C02/T1_L2"

def authenticate():
    try:
        ee.Initialize()
        print("✓ GEE initialized using local user credentials")
    except Exception as e:
        print(f"✗ GEE Initialization failed: {e}")
        print("Try running 'earthengine authenticate' in your terminal again.")

def get_depletion_metrics(lon, lat):
    """Calculate NDVI, NDWI and Lost Water Area for a single point."""
    point = ee.Geometry.Point([lon, lat])
    buffer = point.buffer(BUFFER_RADIUS_M)
    
    # 1. Surface Water Transitions (Lost Water)
    # Value 6 = 'Permanent water to land'
    gsw = ee.Image(JRC_TRANSITIONS).select('transition')
    lost_water = gsw.eq(6).clip(buffer)
    stats = lost_water.multiply(ee.Image.pixelArea()).reduceRegion(
        reducer=ee.Reducer.sum(),
        geometry=buffer,
        scale=30
    )
    lost_area_m2 = stats.get('transition').getInfo() or 0
    
    # 2. Landsat Indices (NDVI & NDWI)
    ls8 = ee.ImageCollection(LANDSAT_COL) \
        .filterBounds(buffer) \
        .filterDate('2025-01-01', datetime.now().strftime('%Y-%m-%d')) \
        .median()
    
    ndvi = ls8.normalizedDifference(['SR_B5', 'SR_B4']).reduceRegion(
        reducer=ee.Reducer.mean(), geometry=buffer, scale=30
    ).get('nd').getInfo() or 0
    
    ndwi = ls8.normalizedDifference(['SR_B3', 'SR_B5']).reduceRegion(
        reducer=ee.Reducer.mean(), geometry=buffer, scale=30
    ).get('nd').getInfo() or 0

    return {
        "lost_recharge_area_m2": round(lost_area_m2, 2),
        "current_ndvi": round(ndvi, 3),
        "current_ndwi": round(ndwi, 3),
        "recharge_risk": "High" if lost_area_m2 > 10000 else "Normal"
    }

def run(csv_path, project_id, out_dir="."):
    ee.Initialize(project=project_id)

    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    
    # Load points from CSV (Simplified loading logic)
    results = []
    with open(csv_path, 'r') as f:
        reader = csv.DictReader(f)
        for row in list(reader)[:50]: # Limit for demo
            if row.get('lat_deg') == '#geo+lat':
                continue
            try:
                lon = float(row['lon_deg'])
                lat = float(row['lat_deg'])
                metrics = get_depletion_metrics(lon, lat)
                results.append({**row, **metrics})
            except Exception as e:
                print(f"Error processing point {row.get('wpdx_id', 'unknown')}: {e}")
                continue
            
    (out / "depletion_results.json").write_text(json.dumps(results, indent=2))
    print("✓ Depletion analysis complete")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, help="Path to input CSV")
    parser.add_argument("--out", default=".", help="Output directory")
    parser.add_argument("--project", default="hydrotrack-489711", help="Google Cloud Project ID")
    args = parser.parse_args()
    
    run(csv_path=args.input, project_id=args.project, out_dir=args.out)