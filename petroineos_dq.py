import os
import logging
import pandas as pd
from datetime import datetime
 
from petroineos import OUTPUT_DIR, MIN_ROWS_THRESHOLD, MAX_MISSING_VALUES
 
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
 
REQUIRED_COLUMNS = [
        "event_date",
        "event_year",
        "event_quarter",
        "series_name",
        "value"
]
 
 
def run_quality_checks(df: pd.DataFrame) -> None:
    """
    Runs basic integrity checks.
    Raises Exception if thresholds are breached.
    """
    logging.info("QUALITY: starting checks")
 
    # 1. Row count
    row_count = len(df)
    logging.info(f"QUALITY: row count = {row_count}")
    if row_count < MIN_ROWS_THRESHOLD:
        raise Exception(
            f"QUALITY FAILED: row count {row_count} below threshold {MIN_ROWS_THRESHOLD}"
        )
 
    # 2. Required columns
    missing_cols = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing_cols:
        raise Exception(
            f"QUALITY FAILED: missing required columns {missing_cols}"
        )
 
    # 3. Missing values threshold
    missing_total = df[REQUIRED_COLUMNS].isnull().sum().sum()
    logging.info(f"QUALITY: total missing values = {missing_total}")
    if missing_total > MAX_MISSING_VALUES:
        raise Exception(
            f"QUALITY FAILED: missing values {missing_total} exceed threshold {MAX_MISSING_VALUES}"
        )
 
    logging.info("QUALITY: all checks passed")
 
 
def write_quality_report(df: pd.DataFrame, source_file: str) -> str:
    """
    Writes a quality report file and returns its path.
    """
    report_path = os.path.join(
        OUTPUT_DIR,
        f"quality_report_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.txt"
    )
 
    with open(report_path, "w") as f:
        f.write("QUALITY CHECK: PASSED\n")
        f.write(f"Source file: {source_file}\n")
        f.write(f"Row count: {len(df)}\n")
        f.write(f"Checked at: {datetime.utcnow()}\n")
 
    logging.info(f"QUALITY: report written → {report_path}")
    return report_path
 
 
if __name__ == "__main__":
    logging.info("QUALITY: running standalone")
 
    files = [
        f for f in os.listdir(OUTPUT_DIR)
        if f.startswith("energy_supply_quarterly_") and f.endswith(".csv")
    ]
 
    if not files:
        raise FileNotFoundError("No output CSV found to validate")
 
    files.sort(reverse=True)
    csv_path = os.path.join(OUTPUT_DIR, files[0])
 
    logging.info(f"QUALITY: validating {csv_path}")
    df = pd.read_csv(csv_path)
 
    run_quality_checks(df)
    write_quality_report(df, csv_path)
 