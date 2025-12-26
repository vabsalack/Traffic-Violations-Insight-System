import streamlit as st
import pandas as pd
import plotly.express as px
from sqlalchemy import text
from db_utils import get_engine, DB_NAME


def vehicle_analysis_page():
    engine = get_engine(DB_NAME)

    st.title("Vehicle Analysis")

    # =============================
    # Vehicle Type Distribution
    # =============================
    type_query = text("""
        SELECT
            vehicle_type,
            COUNT(*) AS total
        FROM traffic_violations
        WHERE vehicle_type IS NOT NULL
        GROUP BY vehicle_type
        ORDER BY total DESC
        LIMIT 15
    """)

    type_df = pd.read_sql(type_query, engine)

    st.subheader("Violations by Vehicle Type")
    fig_type = px.bar(
        type_df,
        x="total",
        y="vehicle_type",
        orientation="h"
    )
    st.plotly_chart(fig_type, use_container_width=True)

    # =============================
    # Top Makes
    # =============================
    make_query = text("""
        SELECT
            make,
            COUNT(*) AS total
        FROM traffic_violations
        WHERE make IS NOT NULL
        GROUP BY make
        ORDER BY total DESC
        LIMIT 15
    """)

    make_df = pd.read_sql(make_query, engine)

    st.subheader("Top Vehicle Makes")
    fig_make = px.bar(
        make_df,
        x="make",
        y="total"
    )
    st.plotly_chart(fig_make, use_container_width=True)

    # =============================
    # Make → Model hierarchy
    # =============================
    model_query = text("""
        SELECT
            make,
            model,
            COUNT(*) AS total
        FROM traffic_violations
        WHERE make IS NOT NULL AND model IS NOT NULL
        GROUP BY make, model
        ORDER BY total DESC
        LIMIT 100
    """)

    model_df = pd.read_sql(model_query, engine)

    st.subheader("Make → Model Breakdown")
    fig_sun = px.sunburst(
        model_df,
        path=["make", "model"],
        values="total"
    )
    st.plotly_chart(fig_sun, use_container_width=True)
