import streamlit as st
import pandas as pd
import plotly.express as px
from sqlalchemy import text, bindparam
from datetime import date
from db_utils import get_engine, DB_NAME

# =============================
# Cached metadata loaders
# =============================

@st.cache_data(ttl=3600)
def load_filter_values():
    engine = get_engine(DB_NAME)
    queries = {
        "state": "SELECT DISTINCT state FROM traffic_violations WHERE state IS NOT NULL",
        "charge": "SELECT DISTINCT charge FROM traffic_violations WHERE charge IS NOT NULL",
        "agency": "SELECT DISTINCT agency FROM traffic_violations WHERE agency IS NOT NULL",
        "subagency": "SELECT DISTINCT subagency FROM traffic_violations WHERE subagency IS NOT NULL",
    }

    values = {}
    for key, q in queries.items():
        df = pd.read_sql(q, engine)
        values[key] = sorted(df.iloc[:, 0].tolist())

    return values


def summary_page():
    engine = get_engine(DB_NAME)

    st.title("Traffic Violations – Summary Statistics")

    filter_values = load_filter_values()

    left, center, right = st.columns([1.2, 3, 1])

    # =============================
    # LEFT: Filters
    # =============================
    with left:
        st.subheader("Filters")

        start_date, end_date = st.date_input(
            "Date range",
            value=(date(2016, 1, 1), date(2023, 12, 31))
        )

        state = st.multiselect("State", filter_values["state"])
        charge = st.multiselect("Charge", filter_values["charge"])
        agency = st.multiselect("Agency", filter_values["agency"])
        subagency = st.multiselect("Sub-Agency", filter_values["subagency"])

        violation_type = st.selectbox(
            "Violation Type",
            ["All", "CITATION", "WARNING", "ESERO"]
        )

        alcohol = st.selectbox(
            "Alcohol Involved",
            ["All", "Yes", "No"]
        )

        search = st.selectbox(
            "Search Conducted",
            ["All", "Yes", "No"]
        )

    # =============================
    # WHERE clause construction
    # =============================
    where_clauses = [
        "stop_datetime BETWEEN :start_date AND :end_date",
        "latitude IS NOT NULL",
        "longitude IS NOT NULL"
    ]

    params = {
        "start_date": start_date,
        "end_date": end_date
    }

    if state:
        where_clauses.append("state IN :state")
        params["state"] = state

    if charge:
        where_clauses.append("charge IN :charge")
        params["charge"] = charge

    if agency:
        where_clauses.append("agency IN :agency")
        params["agency"] = agency

    if subagency:
        where_clauses.append("subagency IN :subagency")
        params["subagency"] = subagency

    if violation_type != "All":
        where_clauses.append("violation_type = :violation_type")
        params["violation_type"] = violation_type

    if alcohol != "All":
        where_clauses.append("alcohol = :alcohol")
        params["alcohol"] = alcohol == "Yes"

    if search != "All":
        where_clauses.append("search_conducted = :search")
        params["search"] = search == "Yes"

    where_sql = " AND ".join(where_clauses)

    # =============================
    # MAIN DATA QUERY
    # =============================
    query = text(f"""
            SELECT
                latitude,
                longitude,
                state,
                violation_type,
                charge
            FROM traffic_violations
            WHERE {where_sql}
            LIMIT 50000
            """)

    # Dynamically attach expanding bindparams
    binds = []

    if state:
        binds.append(bindparam("state", expanding=True))
    if charge:
        binds.append(bindparam("charge", expanding=True))
    if agency:
        binds.append(bindparam("agency", expanding=True))
    if subagency:
        binds.append(bindparam("subagency", expanding=True))

    if binds:
        query = query.bindparams(*binds)

    df = pd.read_sql(query, engine, params=params)
    # =============================
    # CENTER: Point Map
    # =============================
    with center:
        st.subheader("Violation Locations")

        if df.empty:
            st.warning("No data available for selected filters.")
        else:
            fig = px.scatter_geo(
                df,
                lat="latitude",
                lon="longitude",
                color="violation_type",
                hover_name="charge",
                scope="usa",
                opacity=0.4,
                height=650
            )
            st.plotly_chart(fig, use_container_width=True)

    # =============================
    # RIGHT: State Ranking
    # =============================
    with right:
        st.subheader("Violations by State")

        if not df.empty:
            state_counts = (
                df.groupby("state")
                  .size()
                  .sort_values(ascending=False)
                  .head(10)
            )

            max_val = state_counts.max()

            for state_code, count in state_counts.items():
                st.caption(f"{state_code} ({count:,})")
                st.progress(int((count / max_val) * 100))
        else:
            st.info("No data to display.")
