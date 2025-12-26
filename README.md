# Sportradar NCAAFB Dashboard

<img src="pics/Police_officer_writing_a_ticket.eps" width="200" alt="traffic violation image">

## Overview
A complete data analytics system that transforms a large, raw dataset of traffic violations (≈ 10 lakh rows) into actionable insights through Exploratory Data Analysis (EDA), data cleaning, preprocessing, and an interactive visualization dashboard built with Streamlit

## Dataset & Tech stack
Link: [traffic_US_dataset](https://drive.google.com/drive/folders/1ZoS_lQQXKwJf-hfp--eLB-hPK5kKIC6k)

## Dataset
- 

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
| ![summary1](artifacts/home.png) | ![summary2](artifacts/ai_querier.png) |
| ![temporal trends](artifacts/teams.png) | ![vehicle analysis](artifacts/player_stats.png) |
| ![demographics](artifacts/seasons.png) | ![Rankings](artifacts/rankings.png) |

## Getting started
1. Create virtual environment:  
     - `uv init`
2. Activate and install:  
     - copy paste './pyproject.toml`
     - `uv sync`
3. Run the app:  
     - `streamlit run ./app.py`

