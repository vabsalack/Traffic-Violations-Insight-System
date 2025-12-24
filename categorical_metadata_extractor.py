import pandas as pd
from collections import Counter, defaultdict
from typing import List, Optional

from tqdm import tqdm

CSV_PATH = "Traffic_Violations.csv"
CHUNK_SIZE = 200_000
TOP_N = 20   # top categories to keep per column


def profile_csv_columns(
    csv_path: str,
    columns: Optional[List[str]] = None,
    chunk_size: int = CHUNK_SIZE,
    top_n: int = TOP_N
):
    """
    Profiles categorical / nominal columns in a large CSV using chunking.

    Returns a dict with metadata per column.
    """

    value_counters = defaultdict(Counter)
    null_counts = Counter()
    total_rows = 0

    for chunk in tqdm(pd.read_csv(csv_path, chunksize=chunk_size, low_memory=False)):
        if columns:
            chunk = chunk[columns]

        total_rows += len(chunk)

        for col in chunk.columns:
            series = chunk[col]

            # count nulls
            null_counts[col] += series.isna().sum()

            # normalize text lightly for profiling
            cleaned = (
                series
                .dropna()
                .astype(str)
                .str.strip()
            )

            value_counters[col].update(cleaned)

    # build report
    report = {}

    for col, counter in value_counters.items():
        unique_count = len(counter)
        most_common = counter.most_common(top_n)
        duplicate_count = total_rows - unique_count
        report[col] = {
            "total_rows": total_rows,
            "null_count": null_counts[col],
            "null_percentage": round((null_counts[col] / total_rows) * 100, 2),
            "unique_values": unique_count,
            "duplicate_values": duplicate_count,
            "top_values": most_common,
            "possible_boolean": detect_boolean(counter),
            "sample_values": [val for val, _ in most_common[:5]]
        }

    return report


# -----------------------
# Helpers
# -----------------------

def detect_boolean(counter: Counter) -> bool:
    """
    Detects if a column is likely boolean-like.
    """
    boolean_tokens = {"yes", "no", "y", "n", "true", "false", "1", "0"}
    values = {str(v).lower() for v in counter.keys()}

    return values.issubset(boolean_tokens)


# -----------------------
# Output utilities
# -----------------------

def print_report(report: dict):
    for col, meta in report.items():
        print("=" * 60)
        print(f"Column: {col}")
        print(f"Total rows     : {meta['total_rows']}")
        print(f"Null count     : {meta['null_count']} ({meta['null_percentage']}%)")
        print(f"Unique values  : {meta['unique_values']}")
        print(f"Duplicate values: {meta['duplicate_values']}")
        print(f"Possible bool  : {meta['possible_boolean']}")
        print("Top values:")
        for val, cnt in meta["top_values"]:
            print(f"  {val} → {cnt}")
        print()


def save_report_to_csv(report: dict, output_path: str):
    rows = []

    for col, meta in report.items():
        for val, cnt in meta["top_values"]:
            rows.append({
                "column": col,
                "value": val,
                "count": cnt,
                "null_percentage": meta["null_percentage"],
                "unique_values": meta["unique_values"],
                "duplicate_values": meta["duplicate_values"],
                "possible_boolean": meta["possible_boolean"]
            })

    pd.DataFrame(rows).to_csv(output_path, index=False)


# -----------------------
# Main
# -----------------------

if __name__ == "__main__":
    report = profile_csv_columns(CSV_PATH)

    print_report(report)

    save_report_to_csv(report, "column_profile_summary.csv")
