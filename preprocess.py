import pandas as pd
import numpy as np

# =====================================================
# Normalization helpers
# =====================================================

BOOLEAN_TRUE = {"yes", "y", "true", "1"}
BOOLEAN_FALSE = {"no", "n", "false", "0"}

def normalize_boolean(series: pd.Series) -> pd.Series:
    return (
        series.astype(str)
        .str.strip()
        .str.lower()
        .isin(BOOLEAN_TRUE)
    )

def normalize_text(series: pd.Series) -> pd.Series:
    return (
        series
        .astype(str)
        .str.strip()
        .str.upper()
        .replace({"": np.nan, "NAN": np.nan, "NONE": np.nan})
    )

def normalize_gender(series: pd.Series) -> pd.Series:
    return (
        series.astype(str)
        .str.strip()
        .str.upper()
        .replace({
            "M": "M",
            "MALE": "M",
            "F": "F",
            "FEMALE": "F"
        })
        .where(series.notna(), "UNKNOWN")
    )

def normalize_state(series: pd.Series) -> pd.Series:
    return (
        series.astype(str)
        .str.strip()
        .str.upper()
        .where(series.str.len() == 2)
    )

# =====================================================
# Date / time handling
# =====================================================

def build_stop_datetime(df: pd.DataFrame) -> pd.Series:
    date = pd.to_datetime(df["Date Of Stop"], errors="coerce")

    time = (
        df["Time Of Stop"]
        .astype(str)
        .str.replace(".", ":", regex=False)
    )
    time = pd.to_datetime(time, format="%H:%M:%S", errors="coerce").dt.time

    return pd.to_datetime(
        date.astype(str) + " " + time.astype(str),
        errors="coerce"
    )

# =====================================================
# Coordinate cleaning
# =====================================================

def clean_coordinates(df: pd.DataFrame) -> pd.DataFrame:
    df["Latitude"] = pd.to_numeric(df["Latitude"], errors="coerce")
    df["Longitude"] = pd.to_numeric(df["Longitude"], errors="coerce")

    df.loc[
        (df["Latitude"] == 0) | (df["Longitude"] == 0),
        ["Latitude", "Longitude"]
    ] = np.nan

    df.loc[~df["Latitude"].between(24, 50), "Latitude"] = np.nan
    df.loc[~df["Longitude"].between(-125, -65), "Longitude"] = np.nan

    return df

# =====================================================
# Main preprocessing function (chunk-safe)
# =====================================================

def preprocess_chunk(chunk: pd.DataFrame) -> pd.DataFrame:
    chunk = chunk.copy()

    # ---- datetime ----
    chunk["stop_datetime"] = build_stop_datetime(chunk)

    # ---- text normalization ----
    text_cols = [
        "Agency", "SubAgency", "Description", "Location",
        "Search Disposition", "Search Outcome", "Search Reason",
        "VehicleType", "Model", "Make", "Color",
        "Charge", "Violation Type"
    ]

    for col in text_cols:
        if col in chunk.columns:
            chunk[col] = normalize_text(chunk[col])

    # ---- demographics / codes ----
    chunk["Gender"] = normalize_gender(chunk["Gender"])
    chunk["Race"] = normalize_text(chunk["Race"])
    chunk["State"] = normalize_state(chunk["State"])
    chunk["DL State"] = normalize_state(chunk["DL State"])

    # ---- boolean columns ----
    boolean_cols = [
        "Accident",
        "Property Damage",
        "Alcohol",
        "Work Zone",
        "Search Conducted",
        "Personal Injury",
        "Fatal"
    ]

    for col in boolean_cols:
        if col in chunk.columns:
            chunk[col] = normalize_boolean(chunk[col])

    # ---- coordinates ----
    chunk = clean_coordinates(chunk)

    # ---- final column selection for DB / EDA ----
    chunk = chunk[
        [
            "SeqID",
            "Violation Type",
            "stop_datetime",

            "Agency",
            "SubAgency",
            "Location",
            "Description",

            "Latitude",
            "Longitude",

            "Accident",
            "Property Damage",
            "Alcohol",
            "Work Zone",
            "Personal Injury",
            "Fatal",

            "Search Conducted",
            "Search Disposition",
            "Search Outcome",
            "Search Reason",

            "VehicleType",
            "Make",
            "Model",
            "Color",

            "Charge",
            "Race",
            "Gender",
            "State",
            "DL State",
        ]
    ]


    # ---- rename to DB-friendly names ----
    chunk.columns = [
        "seq_id",
        "violation_type",
        "stop_datetime",

        "agency",
        "subagency",
        "location",
        "description",

        "latitude",
        "longitude",

        "accident",
        "property_damage",
        "alcohol",
        "work_zone",
        "personal_injury",
        "fatal",

        "search_conducted",
        "search_disposition",
        "search_outcome",
        "search_reason",

        "vehicle_type",
        "make",
        "model",
        "color",

        "charge",
        "race",
        "gender",
        "state",
        "dl_state",
    ]

    chunk = chunk.replace({np.nan: None})


    return chunk
