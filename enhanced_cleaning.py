"""
WPdx Enhanced Kenya Dataset — Cleaning Script
==============================================
Input : wpdx_enhanced.csv  (WPdx format with HXL tags on row 2)
Output: wpdx_cleaned.csv   (clean, analysis-ready CSV)

Cleaning steps applied
----------------------
1.  Strip HXL tag row (row 2 in original file)
2.  Drop 100 %-empty columns + explicitly excluded columns (notes, clean_adm4,
    fecal_coliform_presence, fecal_coliform_value, last 7 prediction columns)
3.  Standardise column names to snake_case
4.  Parse & validate coordinates (Kenya bounding box)
5.  Parse report_date as proper date; derive report_year
6.  Clean install_year: null out implausible values (< 1950 or > current year)
7.  Standardise boolean columns (is_urban → True/False)
8.  Resolve status_id / status_clean mismatches (10 edge-case rows)
9.  Impute missing water_tech_category from water_tech_clean where possible
10. Impute missing water_source_category from water_source_clean where possible
11. Fix local_population / assigned_population <= 0 → NaN
12. Standardise free-text management, pay, subjective_quality to title-case
13. Cast numeric columns to correct dtypes
14. Add a derived column: functional_status (binary: Functional / Non-Functional)
15. Log a cleaning report to wpdx_cleaning_report.txt
"""

import csv
import io
import math
import re
import datetime
from pathlib import Path
from collections import defaultdict

# ── Config ────────────────────────────────────────────────────────────────────
INPUT_FILE  = Path("wpdx_enhanced.csv")
OUTPUT_FILE = Path("wpdx_cleaned.csv")
REPORT_FILE = Path("wpdx_cleaning_report.txt")

KENYA_BOUNDS = dict(lat_min=-4.7, lat_max=4.6, lon_min=33.9, lon_max=41.9)
INSTALL_YEAR_MIN = 1950
INSTALL_YEAR_MAX = datetime.date.today().year

# Columns to drop — either 100 % empty or explicitly excluded
EMPTY_COLS = {
    # 100 % empty
    "scheme_id",
    "rehab_year",
    "rehabilitator",
    "prediction_yes_0y",
    "prediction_yes_2y",
    "prediction_no_0y",
    "prediction_no_2y",
    "predicted_status_0y",
    "predicted_status_2y",
    "predicted_category",
    # Explicitly excluded
    "clean_adm4",
    "fecal_coliform_presence",
    "fecal_coliform_value",
    "notes",
}

# Columns that should be cast to float
FLOAT_COLS = [
    "lat_deg", "lon_deg",
    "local_population", "assigned_population",
    "usage_cap", "criticality", "pressure",
    "distance_to_primary", "distance_to_secondary", "distance_to_tertiary",
    "distance_to_city", "distance_to_town",
    "days_since_report", "staleness",
]

# Columns that should be cast to int (where non-null)
INT_COLS = ["install_year"]

# Free-text columns to title-case normalise
TITLECASE_COLS = ["management_clean", "pay_clean", "subjective_quality", "facility_type"]

# ── Helpers ───────────────────────────────────────────────────────────────────

def to_snake(name: str) -> str:
    """Convert a column header to snake_case."""
    name = name.strip().lower()
    name = re.sub(r"[\s\-]+", "_", name)
    name = re.sub(r"[^\w]", "", name)
    return name


def safe_float(val: str):
    """Return float or None."""
    try:
        v = float(val)
        return None if math.isnan(v) else v
    except (ValueError, TypeError):
        return None


def safe_int(val: str):
    """Return int or None."""
    try:
        return int(float(val))
    except (ValueError, TypeError):
        return None


def norm_bool(val: str) -> str:
    """Normalise boolean-ish strings to 'True'/'False'/''."""
    v = val.strip().lower()
    if v in ("true", "yes", "1"):
        return "True"
    if v in ("false", "no", "0"):
        return "False"
    return ""


# ── Water tech category inference map ────────────────────────────────────────
TECH_CATEGORY_MAP = {
    "motorized pump":     "Motorized Pump",
    "hand pump":          "Hand Pump",
    "public tapstand":    "Public Tapstand",
    "rope and bucket":    "Rope and Bucket",
    "gravity fed":        "Gravity Fed",
    "rainwater":          "Rainwater Harvesting",
    "borehole":           "Motorized Pump",
    "submersible":        "Motorized Pump",
    "solar":              "Motorized Pump",
    "standpipe":          "Public Tapstand",
    "tap":                "Public Tapstand",
}

SOURCE_CATEGORY_MAP = {
    "well":           "Well",
    "borehole":       "Well",
    "spring":         "Spring",
    "piped":          "Piped Water",
    "tap":            "Piped Water",
    "rain":           "Rainwater Harvesting",
    "rainwater":      "Rainwater Harvesting",
    "sand dam":       "Sand or Sub-surface Dam",
    "sub-surface":    "Sand or Sub-surface Dam",
    "delivered":      "Delivered Water",
    "river":          "Surface Water",
    "lake":           "Surface Water",
}


def infer_tech_category(tech_clean: str) -> str:
    t = tech_clean.strip().lower()
    for kw, cat in TECH_CATEGORY_MAP.items():
        if kw in t:
            return cat
    return ""


def infer_source_category(source_clean: str) -> str:
    s = source_clean.strip().lower()
    for kw, cat in SOURCE_CATEGORY_MAP.items():
        if kw in s:
            return cat
    return ""


# ── Functional status derivation ─────────────────────────────────────────────
def derive_functional_status(status_clean: str) -> str:
    s = status_clean.strip().lower()
    if s.startswith("functional") and "non" not in s:
        return "Functional"
    if "non-functional" in s or s == "abandoned/decommissioned":
        return "Non-Functional"
    return ""


# ── Status mismatch resolution ────────────────────────────────────────────────
# status_id = 'No' (water NOT available) but status_clean says Functional.
# Trust status_clean as the more granular field; correct status_id accordingly.
def resolve_status_mismatch(row: dict, log: list) -> dict:
    sid   = row["status_id"].strip()
    sclean = row["status_clean"].strip()
    functional = "Non" not in sclean and sclean not in ("", "Abandoned/Decommissioned")

    if sid == "No" and functional:
        log.append(
            f"  wpdx_id={row['wpdx_id']}: status_id 'No' → 'Yes' "
            f"(status_clean='{sclean}')"
        )
        row["status_id"] = "Yes"
    return row


# ── Main ──────────────────────────────────────────────────────────────────────

def clean(input_path: Path, output_path: Path, report_path: Path):
    log = []
    counters = defaultdict(int)

    # 1. Read raw lines; skip HXL tag row (index 1)
    with open(input_path, encoding="utf-8") as fh:
        raw_lines = fh.readlines()

    header_line = raw_lines[0]
    data_lines  = raw_lines[2:]          # row 0 = headers, row 1 = HXL tags
    total_input = len(data_lines)
    log.append(f"Input rows (excl. header + HXL row): {total_input}")

    reader = csv.DictReader(io.StringIO(header_line + "".join(data_lines)))
    original_cols = reader.fieldnames

    # 2. Determine kept columns (drop 100%-empty) and build snake_case name map
    kept_cols  = [c for c in original_cols if c not in EMPTY_COLS]
    col_rename = {c: to_snake(c) for c in kept_cols}
    log.append(f"Dropped {len(EMPTY_COLS)} fully-empty columns: {sorted(EMPTY_COLS)}")

    cleaned_rows = []

    for raw_row in reader:
        row = {col_rename[c]: raw_row[c] for c in kept_cols}
        counters["read"] += 1

        # ── 4. Coordinate validation ─────────────────────────────────────────
        lat = safe_float(row["lat_deg"])
        lon = safe_float(row["lon_deg"])
        if lat is None or lon is None:
            counters["dropped_no_coords"] += 1
            log.append(f"  DROPPED (no coords): wpdx_id={row.get('wpdx_id')}")
            continue
        if not (KENYA_BOUNDS["lat_min"] <= lat <= KENYA_BOUNDS["lat_max"] and
                KENYA_BOUNDS["lon_min"] <= lon <= KENYA_BOUNDS["lon_max"]):
            counters["dropped_oob_coords"] += 1
            log.append(
                f"  DROPPED (out-of-bounds coords {lat:.4f},{lon:.4f}): "
                f"wpdx_id={row.get('wpdx_id')}"
            )
            continue

        # ── 5. report_date ────────────────────────────────────────────────────
        rd = row.get("report_date", "").strip()
        if rd:
            try:
                dt = datetime.date.fromisoformat(rd)
                row["report_date"] = dt.isoformat()
                row["report_year"] = str(dt.year)
            except ValueError:
                counters["bad_dates"] += 1
                row["report_date"] = ""
                row["report_year"] = ""
        else:
            row["report_year"] = ""

        # ── 6. install_year ───────────────────────────────────────────────────
        iy = safe_int(row.get("install_year", ""))
        if iy is not None:
            if iy < INSTALL_YEAR_MIN:
                counters["install_year_too_old"] += 1
                row["install_year"] = ""
            elif iy > INSTALL_YEAR_MAX:
                counters["install_year_future"] += 1
                row["install_year"] = ""
            else:
                row["install_year"] = str(iy)
        else:
            row["install_year"] = ""

        # ── 7. Boolean columns ────────────────────────────────────────────────
        row["is_urban"] = norm_bool(row.get("is_urban", ""))

        # ── 8. Status mismatch resolution ────────────────────────────────────
        mismatch_log: list = []
        row = resolve_status_mismatch(row, mismatch_log)
        if mismatch_log:
            counters["status_mismatch_fixed"] += 1
            log.extend(mismatch_log)

        # ── 9. Impute water_tech_category ─────────────────────────────────────
        if not row.get("water_tech_category", "").strip():
            inferred = infer_tech_category(row.get("water_tech_clean", ""))
            if inferred:
                row["water_tech_category"] = inferred
                counters["tech_cat_imputed"] += 1

        # ── 10. Impute water_source_category ──────────────────────────────────
        if not row.get("water_source_category", "").strip():
            inferred = infer_source_category(row.get("water_source_clean", ""))
            if inferred:
                row["water_source_category"] = inferred
                counters["source_cat_imputed"] += 1

        # ── 11. Population sanity ─────────────────────────────────────────────
        for pop_col in ("local_population", "assigned_population"):
            pv = safe_float(row.get(pop_col, ""))
            if pv is not None and pv <= 0:
                counters["bad_population"] += 1
                row[pop_col] = ""
            elif pv is not None:
                row[pop_col] = str(pv)

        # ── 12. Title-case free-text fields ───────────────────────────────────
        for tc in TITLECASE_COLS:
            v = row.get(tc, "").strip()
            if v:
                row[tc] = v.title()

        # ── 13. Numeric dtypes (store as clean string for CSV) ────────────────
        for fc in FLOAT_COLS:
            if fc in row:
                fv = safe_float(row[fc])
                row[fc] = f"{fv:.6f}" if fv is not None else ""

        # ── 14. Derived: functional_status ────────────────────────────────────
        row["functional_status"] = derive_functional_status(row.get("status_clean", ""))

        cleaned_rows.append(row)

    # ── Write output ──────────────────────────────────────────────────────────
    output_cols = list(cleaned_rows[0].keys()) if cleaned_rows else []
    with open(output_path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=output_cols)
        writer.writeheader()
        writer.writerows(cleaned_rows)

    total_output = len(cleaned_rows)
    dropped_total = total_input - total_output

    # ── Build report ──────────────────────────────────────────────────────────
    report_lines = [
        "=" * 65,
        "  WPdx Kenya Dataset — Cleaning Report",
        f"  Run date : {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        "=" * 65,
        "",
        "── Row counts ──────────────────────────────────────────────────",
        f"  Input rows          : {total_input:>7,}",
        f"  Output rows         : {total_output:>7,}",
        f"  Dropped (total)     : {dropped_total:>7,}",
        f"    - No/invalid coords : {counters['dropped_no_coords']:>5,}",
        f"    - Out-of-bounds     : {counters['dropped_oob_coords']:>5,}",
        "",
        "── Column changes ──────────────────────────────────────────────",
        f"  Dropped cols        : {len(EMPTY_COLS):>7,}  (empty + explicitly excluded)",
        f"  Added cols          : report_year, functional_status",
        f"  All names → snake_case",
        "",
        "── Field-level fixes ───────────────────────────────────────────",
        f"  install_year nulled (too old <{INSTALL_YEAR_MIN})  : {counters['install_year_too_old']:>5,}",
        f"  install_year nulled (future >{INSTALL_YEAR_MAX}) : {counters['install_year_future']:>5,}",
        f"  Bad report_dates nulled             : {counters['bad_dates']:>5,}",
        f"  Population ≤ 0 nulled               : {counters['bad_population']:>5,}",
        f"  status_id mismatches corrected      : {counters['status_mismatch_fixed']:>5,}",
        f"  water_tech_category imputed         : {counters['tech_cat_imputed']:>5,}",
        f"  water_source_category imputed       : {counters['source_cat_imputed']:>5,}",
        "",
        "── Detail log ──────────────────────────────────────────────────",
    ] + log + [
        "",
        "── Output columns ──────────────────────────────────────────────",
    ] + [f"  {c}" for c in output_cols] + [""]

    with open(report_path, "w", encoding="utf-8") as fh:
        fh.write("\n".join(report_lines))

    print(f"✓ Cleaned {total_output:,} rows → {output_path}")
    print(f"✓ Dropped  {dropped_total:,} rows")
    print(f"✓ Report   → {report_path}")


if __name__ == "__main__":
    import sys
    inp = Path(sys.argv[1]) if len(sys.argv) > 1 else INPUT_FILE
    out = Path(sys.argv[2]) if len(sys.argv) > 2 else OUTPUT_FILE
    rpt = Path(sys.argv[3]) if len(sys.argv) > 3 else REPORT_FILE
    clean(inp, out, rpt)