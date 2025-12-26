import pandas as pd
# from sqlalchemy import create_engine
from sqlalchemy import text
from preprocess import preprocess_chunk

from db_utils import apply_schema_get_engine, get_engine

# =====================================================
# Configuration
# =====================================================

CSV_PATH = "Traffic_Violations.csv"
CHUNK_SIZE = 50_000


PARQUET_BACKUP = "traffic_cleaned.parquet"  # optional

# =====================================================
# Pipeline
# =====================================================

import math

def insert_ignore(engine, table_name, df, batch_size=5_000):
    cols = ",".join(df.columns)
    placeholders = ",".join([f":{col}" for col in df.columns])

    sql = f"""
    INSERT IGNORE INTO {table_name} ({cols})
    VALUES ({placeholders})
    """

    # ---- convert NaN → None (CRITICAL FIX) ----
    records = df.where(pd.notna(df), None).to_dict(orient="records")

    with engine.begin() as conn:
        for i in range(0, len(records), batch_size):
            batch = records[i : i + batch_size]
            conn.execute(text(sql), batch)



def run_pipeline():
    engine_server = get_engine()
    engine = apply_schema_get_engine(engine_server) # engine bound to specific database

    first_write = True
    
    print("=" * 60)
    print(f"reading {CSV_PATH} file in chunks")
    
    for chunk_no, raw_chunk in enumerate(
        pd.read_csv(CSV_PATH, chunksize=CHUNK_SIZE, low_memory=False),
        start=1
    ):
        print(f"[INFO] Processing chunk {chunk_no}")

        # ---- preprocess ----
        clean_chunk = preprocess_chunk(raw_chunk)

        # ---- insert into MySQL ----
        # duplicates are rejected by PRIMARY KEY (seq_id, charge)
        # clean_chunk.to_sql(
        #     "traffic_violations",
        #     con=engine,
        #     if_exists="append",
        #     index=False,
        #     method="multi",
        #     chunksize=10_000
        # )
        insert_ignore(engine, "traffic_violations", clean_chunk)


        # ---- optional parquet backup ----
        clean_chunk.to_parquet(
            PARQUET_BACKUP,
            engine="pyarrow",
            compression="snappy",
            # append=not first_write
        )

        first_write = False

    print("[SUCCESS] Data pipeline completed successfully.")

# =====================================================
# Entry point
# =====================================================

if __name__ == "__main__":
    run_pipeline()
