"""
GEE Climate Stress Analysis
============================
Integrates CHIRPS (Precipitation) and MODIS (Temperature) to find 
anomalies that stress local water infrastructure.
"""

import ee
import json
import csv
from pathlib import Path
from datetime import datetime, timedelta

# -- Config --
CHIRPS_DATA = "UCSB-CHG/CHIRPS/DAILY"
MODIS_LST   = "MODIS/061/MOD11A1"

def get_climate_anomalies(lon, lat):
    point = ee.Geometry.Point([lon, lat])
    today = datetime.now()
    last_month = today - timedelta(days=30)
    
    # 1. Rainfall Anomaly (CHIRPS)
    chirps = ee.ImageCollection(CHIRPS_DATA).filterBounds(point)
    recent_rain = chirps.filterDate(last_month.strftime('%Y-%m-%d'), today.strftime('%Y-%m-%d')).sum()
    hist_rain = chirps.filter(ee.Filter.calendarRange(today.month, today.month, 'month')).mean().multiply(30)
    
    rain_dict = recent_rain.reduceRegion(ee.Reducer.mean(), point, 5000).getInfo() or {}
    hist_dict = hist_rain.reduceRegion(ee.Reducer.mean(), point, 5000).getInfo() or {}
    
    rain_val = rain_dict.get('precipitation') or 0
    hist_val = hist_dict.get('precipitation') or 0
    
    rain_anomaly = ((rain_val - hist_val) / hist_val * 100) if hist_val > 0 else 0
    
    # 2. Temperature Stress (MODIS LST)
    lst = ee.ImageCollection(MODIS_LST).select('LST_Day_1km').filterBounds(point)
    temp_recent = lst.filterDate((today - timedelta(days=7)).strftime('%Y-%m-%d'), today.strftime('%Y-%m-%d')).mean()
    
    temp_dict = temp_recent.multiply(0.02).subtract(273.15).reduceRegion(ee.Reducer.mean(), point, 1000).getInfo() or {}
    temp_c = temp_dict.get('LST_Day_1km') or 0

    return {
        "monthly_rainfall_mm": round(rain_val, 1),
        "rainfall_anomaly_pct": round(rain_anomaly, 1),
        "surface_temp_c": round(temp_c, 1),
        "climate_stress": "Critical" if rain_anomaly < -50 or temp_c > 35 else "Stable"
    }

def run( csv_path,project_id ,out_dir="."):
    ee.Initialize(project=project_id)

    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    
    results = []
    with open(csv_path, 'r') as f:
        reader = csv.DictReader(f)
        for row in list(reader)[:50]:  # Limit for demo
            if row.get('lat_deg') == '#geo+lat':
                continue
            try:
                lon = float(row['lon_deg'])
                lat = float(row['lat_deg'])
                metrics = get_climate_anomalies(lon, lat)
                results.append({**row, **metrics})
            except Exception as e:
                print(f"Skipping row due to error: {e}")
                continue
            
    (out / "climate_results.json").write_text(json.dumps(results, indent=2))
    print("✓ Climate stress analysis complete")



if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, help="Path to input CSV")
    parser.add_argument("--out", default=".", help="Output directory")
    parser.add_argument("--project", default="hydrotrack-489711", help="Google Cloud Project ID")
    args = parser.parse_args()
    
    run(csv_path=args.input, project_id=args.project, out_dir=args.out)