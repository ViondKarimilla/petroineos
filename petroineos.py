import os
import re
import requests
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime
import logging


# CONFIG

BASE_URL = "https://www.gov.uk/government/statistics/oil-and-oil-products-section-3-energy-trends"
OUTPUT_DIR = "output"
SHEET_NAME = "Quarter"
MIN_ROWS_THRESHOLD = 10
MAX_MISSING_VALUES = 5

os.makedirs(OUTPUT_DIR, exist_ok=True)


# LOGGING

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)


# STEP 1: DOWNLOAD LATEST EXCEL FILE

def download_latest_excel():
    logging.info("Downloading webpage...")
    response = requests.get(BASE_URL)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "html.parser")

    excel_link = None
    for link in soup.find_all("a", href=True):
        if link["href"].endswith(".xlsx"):
            excel_link = link["href"]
            break

    if not excel_link:
        raise Exception("No Excel file found on the page")

    if excel_link.startswith("/"):
        excel_link = "https://www.gov.uk" + excel_link

    filename = excel_link.split("/")[-1]
    filepath = os.path.join(OUTPUT_DIR, filename)

    logging.info(f"Downloading Excel file: {filename}")
    file_response = requests.get(excel_link)
    file_response.raise_for_status()

    with open(filepath, "wb") as f:
        f.write(file_response.content)

    logging.info(f"File saved to: {filepath}")
    return filepath, filename


# TRANSFORMATION HELPERS

def _clean_series_name(s):
    s = "" if s is None else str(s)
    s = re.sub(r"\[note\s*\d+\]", "", s, flags=re.IGNORECASE)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def _series_name_from_name(name):
    return re.sub(r"[^a-z0-9]+", "_", name.lower()).strip("_")

def _quarter_label_to_event_date(label):
    if label is None:
        return None

    s = str(label).lower().replace("\n", " ")
    s = re.sub(r"\[.*?\]", "", s)

    year_match = re.search(r"(19|20)\d{2}", s)
    q_match = re.search(r"\b([1-4])(?:st|nd|rd|th)?\s+quarter\b", s)

    if not year_match or not q_match:
        return None

    year = int(year_match.group(0))
    quarter = int(q_match.group(1))
    month = {1: 1, 2: 4, 3: 7, 4: 10}[quarter]

    return f"{year}-{month:02d}-01"


# STEP 2: LOAD & TRANSFORM QUARTER DATA

def load_and_clean_quarter_sheet(excel_path, source_filename):
    logging.info(f"Reading Excel sheet: {SHEET_NAME}")

    df = pd.read_excel(excel_path, sheet_name=SHEET_NAME, header=4)
    df = df.rename(columns={df.columns[0]: "series_name"})
    df = df.dropna(how="all")

    df["series_name"] = df["series_name"].apply(_clean_series_name)
    df = df[df["series_name"] != ""]

    quarter_cols = [c for c in df.columns if c != "series_name"]

    long_df = df.melt(
        id_vars=["series_name"],
        value_vars=quarter_cols,
        var_name="quarter_label",
        value_name="value"
    )

    long_df["event_date"] = long_df["quarter_label"].apply(_quarter_label_to_event_date)
    long_df = long_df[long_df["event_date"].notna()].copy()

    long_df["value"] = pd.to_numeric(long_df["value"], errors="coerce")

    dt = pd.to_datetime(long_df["event_date"])
    long_df["event_year"] = dt.dt.year
    long_df["event_quarter"] = dt.dt.quarter

    long_df["series_id"] = long_df["series_name"].apply(_series_name_from_name)

    # Metadata
    long_df["source_filename"] = source_filename
    long_df["source_sheet"] = SHEET_NAME
    long_df["event_timestamp"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    long_df = long_df.drop_duplicates(
        subset=["event_date", "series_name"], keep="last"
    )

    out_cols = [
        "event_date",
        "event_year",
        "event_quarter",
        "series_name",
        "value",
        "source_filename",
        "source_sheet",
        "event_timestamp"
    ]

    long_df = long_df[out_cols]
    logging.info(f"Cleaned dataframe shape: {long_df.shape}")

    return long_df


# STEP 3: DATA QUALITY CHECKS

def data_quality_checks(df):
    logging.info("Running data quality checks...")

    if len(df) < MIN_ROWS_THRESHOLD:
        raise Exception("Row count below minimum threshold")

    required_cols = ["event_date", "series_name", "value"]
    for col in required_cols:
        if col not in df.columns:
            raise Exception(f"Missing required column: {col}")

    logging.info("Data quality checks passed")


# STEP 4: SAVE OUTPUT TO CSV

def save_to_csv(df):
    today = datetime.utcnow().strftime("%Y%m%d")
    output_file = f"energy_supply_quarterly_{today}.csv"
    output_path = os.path.join(OUTPUT_DIR, output_file)

    df.to_csv(output_path, index=False)
    logging.info(f"Saved final CSV to {output_path}")


# MAIN

def main():
    excel_path, filename = download_latest_excel()
    df = load_and_clean_quarter_sheet(excel_path, filename)
    data_quality_checks(df)
    save_to_csv(df)
    logging.info("Pipeline completed successfully")

if __name__ == "__main__":
    main()
