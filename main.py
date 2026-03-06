import sqlite3
import pandas as pd
import random
from datetime import datetime, timedelta

# --- Connection ---
def create_connection(db_file):
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        print(f"Connected to database: {db_file}")
    except sqlite3.Error as e:
        print(e)
    return conn

# --- Create Tables ---
def create_tables(conn):
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS WaterPoints (
            water_point_id TEXT PRIMARY KEY,
            county TEXT,
            subcounty TEXT,
            sub_location TEXT,
            village TEXT,
            latitude REAL,
            longitude REAL
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS Reports (
            report_id INTEGER PRIMARY KEY,
            water_point_id TEXT,
            reported_status TEXT,
            reporter TEXT,
            date_reported TEXT,
            FOREIGN KEY (water_point_id) REFERENCES WaterPoints (water_point_id)
        )
    """)
    conn.commit()
    print("Tables ready.")

# --- Load Water Points from CSV ---
def load_water_points(conn, csv_file):
    df = pd.read_csv(csv_file)

    # Map CSV columns to table columns
    df = df[['i102_wpt_id', 'i013_county', 'i014_subcounty', 'i016_subloc', 'i017_vil', 'Latitude', 'Longitude']]
    df.columns = ['water_point_id', 'county', 'subcounty', 'sub_location', 'village', 'latitude', 'longitude']

    df.to_sql('WaterPoints', conn, if_exists='append', index=False)
    print(f"Inserted {len(df)} water points.")
    return df['water_point_id'].tolist()

# --- Simulate Reports ---
def simulate_reports(conn, water_point_ids, n=1000):
    statuses = ['functional', 'broken', 'verification_pending']
    reporters = ['resident']

    # Generate random dates between 2022 and 2024
    start_date = datetime(2022, 1, 1)
    end_date = datetime(2024, 12, 31)
    date_range = (end_date - start_date).days

    reports = []
    for _ in range(n):
        report = (
            random.choice(water_point_ids),
            random.choice(statuses),
            random.choice(reporters),
            (start_date + timedelta(days=random.randint(0, date_range))).strftime('%Y-%m-%d')
        )
        reports.append(report)

    cursor = conn.cursor()
    cursor.executemany("""
        INSERT INTO Reports (water_point_id, reported_status, reporter, date_reported)
        VALUES (?, ?, ?, ?)
    """, reports)
    conn.commit()
    print(f"Inserted {n} simulated reports.")

# --- Run Everything ---
conn = create_connection("HydroTrack.db")
if conn:
    create_tables(conn)
    water_point_ids = load_water_points(conn, "water_points.csv")
    simulate_reports(conn, water_point_ids, n=1000)
    conn.close()
    print("Done!")