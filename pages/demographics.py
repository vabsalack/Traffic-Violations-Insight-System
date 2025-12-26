import streamlit as st
import pandas as pd
import plotly.express as px
from sqlalchemy import text
from db_utils import get_engine, DB_NAME


def demographics_page():
    engine = get_engine(DB_NAME)

    st.title("Demographic Patterns (Exploratory)")

    st.caption(
        "This page presents observed distributions only. "
        "It does not imply causation or bias."
    )

    # =============================
    # Stops by Race
    # =============================
    race_query = text("""
        SELECT
            race,
            COUNT(*) AS total
        FROM traffic_violations
        WHERE race IS NOT NULL
        GROUP BY race
        ORDER BY total DESC
    """)

    race_df = pd.read_sql(race_query, engine)

    st.subheader("Stops by Race")
    fig_race = px.bar(
        race_df,
        x="race",
        y="total"
    )
    st.plotly_chart(fig_race, use_container_width=True)

    # =============================
    # Stops by Gender
    # =============================
    gender_query = text("""
        SELECT
            gender,
            COUNT(*) AS total
        FROM traffic_violations
        WHERE gender IS NOT NULL
        GROUP BY gender
    """)

    gender_df = pd.read_sql(gender_query, engine)

    st.subheader("Stops by Gender")
    fig_gender = px.pie(
        gender_df,
        names="gender",
        values="total"
    )
    st.plotly_chart(fig_gender, use_container_width=True)

    # =============================
    # Search Rate by Race
    # =============================
    search_query = text("""
        SELECT
            race,
            AVG(search_conducted) AS search_rate
        FROM traffic_violations
        WHERE race IS NOT NULL
        GROUP BY race
        ORDER BY search_rate DESC
    """)

    search_df = pd.read_sql(search_query, engine)

    st.subheader("Search Rate by Race")
    fig_search = px.bar(
        search_df,
        x="race",
        y="search_rate"
    )
    st.plotly_chart(fig_search, use_container_width=True)
