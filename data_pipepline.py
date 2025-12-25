import pandas as pd
from sqlalchemy import create_engine
from preprocess import preprocess_chunk

# =====================================================
# Configuration
# =====================================================

CSV_PATH = "traffic_violations.csv"
CHUNK_SIZE = 200_000

MYSQL_URI = (
    "mysql+mysqlconnector://username:password@localhost/traffic_db"
)

PARQUET_BACKUP = "traffic_cleaned.parquet"  # optional

# =====================================================
# Pipeline
# =====================================================

def run_pipeline():
    engine = create_engine(
        MYSQL_URI,
        pool_pre_ping=True,
        pool_recycle=3600
    )

    first_write = True

    for chunk_no, raw_chunk in enumerate(
        pd.read_csv(CSV_PATH, chunksize=CHUNK_SIZE, low_memory=False),
        start=1
    ):
        print(f"[INFO] Processing chunk {chunk_no}")

        # ---- preprocess ----
        clean_chunk = preprocess_chunk(raw_chunk)

        # ---- insert into MySQL ----
        # duplicates are rejected by PRIMARY KEY (seq_id, charge)
        clean_chunk.to_sql(
            "traffic_violations",
            con=engine,
            if_exists="append",
            index=False,
            method="multi",
            chunksize=10_000
        )

        # ---- optional parquet backup ----
        clean_chunk.to_parquet(
            PARQUET_BACKUP,
            engine="pyarrow",
            compression="snappy",
            append=not first_write
        )

        first_write = False

    print("[SUCCESS] Data pipeline completed successfully.")

# =====================================================
# Entry point
# =====================================================

if __name__ == "__main__":
    run_pipeline()
