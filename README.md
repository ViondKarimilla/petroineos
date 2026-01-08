Energy Supply and USE Quarterly Data Pipeline
Overview
This project implements an automated Python data pipeline that:
Downloads the latest quarterly energy statistics Excel file from GOV.UK
Extracts the “Quarter” worksheet
Transforms the data from wide to long format
Cleans and standardizes values
Performs data quality (DQ) checks
Outputs an analytics-ready CSV file
The pipeline is designed to be idempotent, traceable, and scheduler-friendly.
Data Source
Website:
https://www.gov.uk/government/statistics/oil-and-oil-products-section-3-energy-trends
Dataset:
Supply and use of crude oil, natural gas liquids and feedstocks (ET 3.1 – quarterly)
Project Structure
project/
│
├── requirement.txt        # run for Install Dependencies
├── petroineos.py          # Main pipeline script
│── petroineos_dq.py       # DQ pipeline script
│── test_petroineos.py     # pytest script
├── README.md              # Project documentation
├── output/                # Generated outputs
│   ├── ET_3_1.xlsx        # Downloaded Excel file
│   └── debug_cleaned_data.csv
│   └── energy_supply_quarterly_YYYYMMDD.csv
│   └── quality_report_20260108_114137.txt
└── venv/                  # Python virtual environment (optional)


Prerequisites
Python 3.9+(Python 3.14.2 which I used)
Internet connection
Windows / macOS / Linux
Installation
1. Create and Activate Virtual Environment (Recommended)
Windows
python -m venv venv
venv\Scripts\activate

macOS / Linux
python3 -m venv venv
source venv/bin/activate

2. Install Dependencies
pip install pandas requests beautifulsoup4 openpyxl apscheduler

How to Run the Pipeline
From the project directory:
requirement.txt
python petroineos.py
python petroineos_dq.py - DQ checks
test_petroineos.py  - pytest

What the Pipeline Does
Step 1 – Download Latest Excel File
Scrapes the GOV.UK statistics page
Identifies the most recent .xlsx file
Downloads it into the output/ directory
Step 2 – Transform Quarterly Data
Reads the Quarter worksheet
Cleans series names (removes footnotes, normalizes text)
Converts wide quarterly columns into a long format
Parses quarter labels into standardized dates (YYYY-MM-DD)
Creates analytical fields:
event_date
event_year
event_quarter
Adds metadata:
source_filename
source_sheet
event_timestamp
Deduplicates records using (event_date, series_name)

Step 3 – Data Quality (DQ) Checks
The pipeline includes mandatory quality checks to ensure data integrity.
Implemented Checks
Check
Description
Row count
Dataset must contain at least MIN_ROWS_THRESHOLD rows
Required columns
Ensures presence of event_date, series_name, value
Type safety
Values converted to numeric where applicable
Deduplication
Removes duplicate (event_date, series_name) records

If any check fails, the pipeline raises an exception and stops execution.



Step 4 – Output
Saves a timestamped CSV file:
energy_supply_quarterly_YYYYMMDD.csv

Location:
output/

Configuration Parameters
Defined at the top of petroineos.py:
BASE_URL = "https://www.gov.uk/government/statistics/oil-and-oil-products-section-3-energy-trends"
OUTPUT_DIR = "output"
SHEET_NAME = "Quarter"
MIN_ROWS_THRESHOLD = 10
MAX_MISSING_VALUES = 5

Scheduling (Optional)
The script is as shown below and can be run daily using:
Install Dependencies
pip install apscheduler
Script:
from apscheduler.schedulers.blocking import BlockingScheduler
scheduler = BlockingScheduler()
scheduler.add_job(
    main,
    trigger="cron",
    hour=6,
    minute=0
)
logging.info("Scheduler started")
scheduler.start()

Logging
Uses Python’s built-in logging module
Logs:
Download status
Transformation steps
Data quality results
Output file location
Failure Handling
The pipeline fails fast when:
The Excel file cannot be found
The Quarter sheet is missing
Data quality checks fail
This ensures bad data is never silently propagated.

