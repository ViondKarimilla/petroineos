import pandas as pd
import pytest
from datetime import datetime

import petroineos


# -----------------------------
# UNIT TESTS – TRANSFORM HELPERS
# -----------------------------

def test_clean_series_name():
    raw = "Crude oil production [note 1]"
    cleaned = petroineos._clean_series_name(raw)
    assert cleaned == "Crude oil production"


def test_series_name_from_name():
    name = "Natural Gas Liquids (NGLs)"
    series_id = petroineos._series_name_from_name(name)
    assert series_id == "natural_gas_liquids_ngls"


def test_quarter_label_to_event_date():
    label = "2024\n2nd quarter [provisional]"
    date = petroineos._quarter_label_to_event_date(label)
    assert date == "2024-04-01"


def test_quarter_label_invalid():
    label = "Invalid Label"
    assert petroineos._quarter_label_to_event_date(label) is None

# DATA QUALITY TESTS



def test_data_quality_fails_row_count():
    df = pd.DataFrame({
        "event_date": ["2024-01-01"],
        "series_name": ["Series A"],
        "value": [10]
    })

    with pytest.raises(Exception, match="Row count below minimum threshold"):
        petroineos.data_quality_checks(df)

# -----------------------------
# TRANSFORMATION TEST (MOCKED)
# -----------------------------

def test_load_and_clean_quarter_sheet(monkeypatch, tmp_path):
    """
    Mock pd.read_excel to avoid real Excel dependency
    """

    mock_df = pd.DataFrame({
        "Series": ["Crude oil production"],
        "2024\n1st quarter": [100],
        "2024\n2nd quarter": [120]
    })

    def mock_read_excel(*args, **kwargs):
        return mock_df

    monkeypatch.setattr(pd, "read_excel", mock_read_excel)

    result = petroineos.load_and_clean_quarter_sheet(
        excel_path="dummy.xlsx",
        source_filename="dummy.xlsx"
    )

    assert not result.empty
    assert set(result.columns) == {
        "event_date",
        "event_year",
        "event_quarter",
        "series_name",
        "value",
        "source_filename",
        "source_sheet",
        "event_timestamp"
    }

    assert result["value"].sum() == 220
