"""Constructs daily time series of COVID-19 testing data for Norway.
Dashboard: https://www.fhi.no/en/id/infectious-diseases/coronavirus/daily-reports/daily-reports-COVID19/
"""
import re
import json
import requests
import pandas as pd
from cowidev.testing import CountryTestBase

COUNTRY = "Norway"
UNITS = "people tested"
TESTING_TYPE = "PCR only"
SOURCE_LABEL = "Norwegian Institute of Public Health"
SOURCE_URL = "https://www.fhi.no/en/id/infectious-diseases/coronavirus/daily-reports/daily-reports-COVID19"
DATA_URL = "https://www.fhi.no/api/chartdata/api/90789"


class Norway(CountryTestBase):
    location = "Norway"

    def export(self) -> None:
        df = get_data()
        df.sort_values("Date", inplace=True)
        df["Country"] = COUNTRY
        df["Units"] = UNITS
        df["Source URL"] = SOURCE_URL
        df["Source label"] = SOURCE_LABEL
        df["Notes"] = ""
        sanity_checks(df)
        df = df[
            [
                "Country",
                "Units",
                "Date",
                "Daily change in cumulative total",
                "Source URL",
                "Source label",
                "Notes",
            ]
        ]
        self.export_datafile(df)
        return None


def get_data() -> pd.DataFrame:
    res = requests.get(DATA_URL)
    assert res.ok
    json_data = json.loads(res.text)
    df = pd.DataFrame(json_data[1:], columns=json_data[0])
    df["Negative"] = df["Negative"].astype(int)
    df["Positive"] = df["Positive"].astype(int)
    df["Daily change in cumulative total"] = df["Negative"] + df["Positive"]
    df = df[df["Daily change in cumulative total"] != 0]
    return df


def sanity_checks(df: pd.DataFrame) -> None:
    """checks that there are no obvious errors in the scraped data."""
    # checks that the cumulative number of tests on date t is always greater than the figure for t-1:
    df_cp = df.copy()
    df_cp["Cumulative total"] = df_cp["Daily change in cumulative total"].cumsum()
    assert (
        df_cp["Cumulative total"].iloc[1:] >= df_cp["Cumulative total"].shift(1).iloc[1:]
    ).all(), "On one or more dates, `Cumulative total` is greater on date t-1."
    return None


def main():
    Norway().export()
