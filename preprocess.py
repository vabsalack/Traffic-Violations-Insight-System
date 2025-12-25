import pandas as pd
import numpy as np


def normalize_boolean(series: pd.Series) -> pd.Series:
    return (
        series.astype(str)
        .str.strip()
        .str.lower()
        .isin(["yes", "y", "true", "1"])
    )

def normalize_gender(series: pd.Series) -> pd.Series:
    return (
        series.astype(str)
        .str.upper()
        .replace({"M": "M", "F": "F"})
        .where(series.notna(), "UNKNOWN")
    )

def normalize_text(series: pd.Series) -> pd.Series:
    return series.astype(str).str.strip().str.upper()

def clean_coordinates(df: pd.DataFrame) -> pd.DataFrame:
    df["Latitude"] = pd.to_numeric(df["Latitude"], errors="coerce")
    df["Longitude"] = pd.to_numeric(df["Longitude"], errors="coerce")

    df.loc[(df["Latitude"] == 0) | (df["Longitude"] == 0), ["Latitude", "Longitude"]] = np.nan
    df.loc[~df["Latitude"].between(24, 50), "Latitude"] = np.nan
    df.loc[~df["Longitude"].between(-125, -65), "Longitude"] = np.nan

    return df

def build_stop_datetime(df: pd.DataFrame) -> pd.Series:
    date = pd.to_datetime(df["Date Of Stop"], errors="coerce")
    time = (
        df["Time Of Stop"]
        .astype(str)
        .str.replace(".", ":", regex=False)
    )
    time = pd.to_datetime(time, format="%H:%M:%S", errors="coerce").dt.time
    return pd.to_datetime(date.astype(str) + " " + time.astype(str), errors="coerce")

# -------------------------
# Main preprocessing entry
# -------------------------

def preprocess_chunk(chunk: pd.DataFrame) -> pd.DataFrame:
    chunk = chunk.copy()

    chunk["stop_datetime"] = build_stop_datetime(chunk)

    chunk["Agency"] = normalize_text(chunk["Agency"])
    chunk["SubAgency"] = normalize_text(chunk["SubAgency"])
    chunk["Race"] = normalize_text(chunk["Race"])
    chunk["Gender"] = normalize_gender(chunk["Gender"])
    chunk["VehicleType"] = normalize_text(chunk["VehicleType"])

    chunk["Accident"] = normalize_boolean(chunk["Accident"])
    chunk["Personal Injury"] = normalize_boolean(chunk["Personal Injury"])
    chunk["Fatal"] = normalize_boolean(chunk["Fatal"])

    chunk = clean_coordinates(chunk)

    # Select only columns needed for EDA & DB
    chunk = chunk[
        [
            "SeqID",
            "Violation Type",
            "stop_datetime",
            "Agency",
            "SubAgency",
            "Description",
            
            "Latitude",
            "Longitude",

            "Accident",
            "Personal Injury",
            "Fatal",
            "VehicleType",
            "Race",
            "Gender",
        ]
    ]


    # columns renaming
    chunk.columns = [
        "seq_id",
        "violation_type",
        "stop_datetime",
        "agency",
        "subagency",
        "description",
        "latitude",
        "longitude",
        "accident",
        "personal_injury",
        "fatal",
        "vehicle_type",
        "race",
        "gender",
    ]

    return chunk
