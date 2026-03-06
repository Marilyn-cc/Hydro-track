import sqlite3
import pandas as pd
import folium

# 1. Connect to database
# -------------------------
conn = sqlite3.connect("HydroTrack.db")

# 2. Join Reports and WaterPoints
# -------------------------
query = """
SELECT r.report_id, r.water_point_id, r.reported_status, r.reporter, r.date_reported,
       w.county, w.subcounty, w.sub_location, w.village, w.latitude, w.longitude
FROM Reports r
JOIN WaterPoints w
ON r.water_point_id = w.water_point_id
"""
df_joined = pd.read_sql(query, conn)

print(f"Joined dataset shape: {df_joined.shape}")
print(df_joined.head())

# 3. Verification Layer

# a) Convert date to datetime
df_joined['date_reported'] = pd.to_datetime(df_joined['date_reported'])

# b) Remove duplicates (same water_point_id, same reporter, same date)
df_joined = df_joined.drop_duplicates(subset=['water_point_id','reporter','date_reported'])

# c) Pick latest report per water_point_id
df_verified = df_joined.sort_values('date_reported').groupby('water_point_id').last().reset_index()


print(f"Verified dataset shape: {df_verified.shape}")
print(df_verified.head())

# 4. Generate Folium Map and Center map around Kenya

m = folium.Map(location=[0.3, 37.9], zoom_start=6)

# Color logic based on status
status_colors = {
    'functional': 'green',
    'broken': 'red',
    'verification_pending': 'orange'
}

# Add markers
for _, row in df_verified.iterrows():
    folium.CircleMarker(
        location=[row['latitude'], row['longitude']],
        radius=5,
        color=status_colors.get(row['reported_status'], 'blue'),
        fill=True,
        fill_opacity=0.7,
        popup=f"{row['village']} ({row['sub_location']}) - {row['reported_status']} | Reporter: {row['reporter']} | Date: {row['date_reported'].date()}"
    ).add_to(m)

# Save map
m.save("water_points_map.html")
print("Map saved as water_points_map.html")

# Close connection
conn.close()