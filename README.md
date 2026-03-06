# Community Water Point Status Mapping - Hackathon Prototype

**Project Goal:**  
Create a community-powered map that allows residents to report the status of water points, enabling stakeholders to prioritize repairs and interventions in real time.

---

## Project Overview

This prototype demonstrates:

- **Spatial data modeling**: Water points loaded with latitude/longitude.  
- **Community reporting simulation**: Multiple reports per water point, with resident as the reporter.  
- **Verification layer**: Removes duplicates, selects latest report per water point.  
- **Interactive map**: Folium map showing water points color-coded by status (`functional`, `broken`, `verification_pending`) with clickable popups for details.

---

## Repository Structure
gdg_hackathon/

├─ main.py # Loads water points CSV and simulates reports into SQLite DB

├─ pipeline.py # Runs verification layer, joins tables, generates Folium map

├─ water_points.csv # CSV of water points (sample from Evidence Action Kenya)

├─ HydroTrack.db # SQLite database (optional, can be created by main.py)

├─ water_points_map.html # Generated interactive map (output of pipeline.py

├─ README.md

└─ .gitignore


## Steps to Run

#### python main.py - Load Water Points and Simulated Reports

  - Create HydroTrack.db

  - Load water points from water_points.csv

  - Simulate 1000 reports with reporter = resident

  - Generate Verified Map

#### python pipeline.py :  Join Reports and WaterPoints tables

  - Apply verification (latest report per water point)

  - Generate water_points_map.html

View Map : Accessible details 

- Open water_points_map.html in your browser
- Click on any water point to see:
- Village & Sublocation
- Reported Status
- Reporter
- Date Reported
