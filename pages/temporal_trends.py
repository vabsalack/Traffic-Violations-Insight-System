import streamlit as st
import pandas as pd
import plotly.express as px
from sqlalchemy import text, bindparam
from datetime import date
from db_utils import get_engine, DB_NAME


def temporal_trends_page():
    engine = get_engine(DB_NAME)

    st.title("Temporal Trends Analysis")

    # =============================
    # Filters
    # =============================
    left, right = st.columns([1, 3])

    with left:
        start_date, end_date = st.date_input(
            "Date Range",
            value=(date(2016, 1, 1), date(2023, 12, 31))
        )

        violation_type = st.selectbox(
            "Violation Type",
            ["All", "CITATION", "WARNING", "ESERO"]
        )

    # =============================
    # SQL: Monthly trend
    # =============================
    where = ["stop_datetime BETWEEN :start AND :end"]
    params = {"start": start_date, "end": end_date}

    if violation_type != "All":
        where.append("violation_type = :vtype")
        params["vtype"] = violation_type

    where_sql = " AND ".join(where)

    monthly_query = text(f"""
        SELECT
            DATE_FORMAT(stop_datetime, '%Y-%m') AS month,
            COUNT(*) AS total
        FROM traffic_violations
        WHERE {where_sql}
        GROUP BY month
        ORDER BY month
    """)

    monthly_df = pd.read_sql(monthly_query, engine, params=params)

    # =============================
    # SQL: Hour × Weekday heatmap
    # =============================
    heatmap_query = text(f"""
        SELECT
            HOUR(stop_datetime) AS hour,
            DAYOFWEEK(stop_datetime) AS weekday,
            COUNT(*) AS total
        FROM traffic_violations
        WHERE {where_sql}
        GROUP BY hour, weekday
    """)

    heat_df = pd.read_sql(heatmap_query, engine, params=params)

    # =============================
    # Visuals
    # =============================
    with right:
        st.subheader("Monthly Violation Trend")

        fig_line = px.line(
            monthly_df,
            x="month",
            y="total",
            markers=True
        )
        st.plotly_chart(fig_line, use_container_width=True)

        st.subheader("Hourly vs Weekday Pattern")

        heat_df["weekday"] = heat_df["weekday"].map({
            1: "Sun", 2: "Mon", 3: "Tue",
            4: "Wed", 5: "Thu", 6: "Fri", 7: "Sat"
        })

        fig_heat = px.density_heatmap(
            heat_df,
            x="hour",
            y="weekday",
            z="total",
            color_continuous_scale="Viridis"
        )
        st.plotly_chart(fig_heat, use_container_width=True)
