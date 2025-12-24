import pandas as pd
from sqlalchemy import create_engine
from preprocess import preprocess_chunk

# -------------------------
# Configuration
# -------------------------

CSV_PATH = "traffic_violations.csv"
CHUNK_SIZE = 200_000

MYSQL_URI = (
    "mysql+mysqlconnector://username:password@localhost/traffic_db"
)

PARQUET_BACKUP = "traffic_cleaned.parquet"

# -------------------------
# Main pipeline
# -------------------------

def run_pipeline():
    """
    Execute the data pipeline to process CSV data in chunks and load into MySQL database.
    
    This function reads a large CSV file in chunks, preprocesses each chunk, and writes
    the cleaned data to both a MySQL database and a Parquet backup file. The pipeline
    handles duplicate detection through primary key constraints.
    
    Process Flow:
        1. Establishes a connection pool to MySQL with pre-ping enabled
        2. Iterates through CSV file in configurable chunk sizes
        3. Preprocesses each chunk for data cleaning and validation
        4. Writes to MySQL with duplicate handling via primary key
        5. Creates/appends Parquet backup files for archival
    
    MySQL Configuration:
        - Uses connection pooling with pool_pre_ping=True to verify connections
          before execution, ensuring stale connections are recycled
        - Duplicate rows are automatically handled by primary key constraints
        - Multi-method insertion for optimized bulk data loading
    
    
    Note:
        - pool_pre_ping=True sends a SELECT 1 query before using each connection
          from the pool to detect disconnected connections, preventing "lost connection"
          errors and automatically replacing dead connections with new ones
    """
    engine = create_engine(MYSQL_URI, pool_pre_ping=True)

    first_write = True

    for chunk_no, chunk in enumerate(
        pd.read_csv(CSV_PATH, chunksize=CHUNK_SIZE, low_memory=False),
        start=1
    ):
        print(f"Processing chunk {chunk_no}")

        clean_chunk = preprocess_chunk(chunk)

        # ---- Save to MySQL (duplicates handled by PK) ----
        clean_chunk.to_sql(
            "traffic_violations",
            engine,
            if_exists="append",
            index=False,
            method="multi",
            chunksize=10_000
        )

        # ---- Optional Parquet backup ----
        clean_chunk.to_parquet(
            PARQUET_BACKUP,
            engine="pyarrow",
            compression="snappy",
            append=not first_write
        )

        first_write = False

    print("Pipeline completed successfully.")

if __name__ == "__main__":
    run_pipeline()
