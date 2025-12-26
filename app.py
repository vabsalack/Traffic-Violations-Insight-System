import streamlit as st
from pages.summary import summary_page
from pages.temporal_trends import temporal_trends_page
from pages.vehicle_analysis import vehicle_analysis_page
from pages.demographics import demographics_page

st.set_page_config(
    page_title="Traffic Violations Insight System",
    layout="wide"
)

st.sidebar.title("Navigation")

page = st.sidebar.radio(
    "Go to",
    [
        "Summary Statistics",
        "Temporal Trends",
        "Vehicle Analysis",
        "Demographics"
    ]
)

# -----------------------------
# Page Routing
if page == "Summary Statistics":
    summary_page()

if page == "Temporal Trends":
    temporal_trends_page()

if page == "Vehicle Analysis":
    vehicle_analysis_page()

if page == "Demographics":
    demographics_page()
