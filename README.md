# Sportradar NCAAFB Dashboard

<img src="pics/Police_officer_writing_a_ticket.jpg" width="200" alt="traffic violation image">

## Overview
A complete data analytics system that transforms a large, raw dataset of traffic violations (≈ 10 lakh rows) into actionable insights through Exploratory Data Analysis (EDA), data cleaning, preprocessing, and an interactive visualization dashboard built with Streamlit

## Dataset & Tech stack

### Dataset
- Link: [traffic_US_dataset](https://drive.google.com/drive/folders/1ZoS_lQQXKwJf-hfp--eLB-hPK5kKIC6k)

### Language / Runtime
- Python >= 3.11 (streamlit compatible version on linux)

### Package manager
- uv (astral)

### Libraries (requirements)
- mysql-connector-python>=9.5.0
- pandas>=2.3.3
- plotly>=6.5.0
- requests>=2.32.5
- ruff>=0.14.6
- sqlalchemy>=2.0.44
- streamlit>=1.51.0
- pyarrow


## Core pages
1. Summary Statistics
2. Temporal Trends
3. Vehicle Analysis
4. Demographics

| | |
|---|---|
| ![summary1](pics/summary.png) | ![summary2](pics/summary2.png) |
| ![temporal trends](pics/temporal_trends.png) | ![vehicle analysis](pics/vehicle_analysis.png) |
| ![demographics](pics/demographics.png) | ![sunburst_vehicle_analysis](pics/sunburst_vehicle.png)|

## Flow Diagram

<!-- ![project FD svg](pics/project_metadata.svg) -->
<img src="pics/project_metadata.svg" width="1300" alt="project_metadata svg">

## Getting started
1. Create virtual environment:  
     - `uv init`
2. Activate and install:  
     - copy paste './pyproject.toml`
     - `uv sync`
3. Run the app:  
     - `streamlit run ./app.py`



