import re

from bs4 import BeautifulSoup
import pandas as pd

from cowidev.utils.clean import clean_date
from cowidev.utils.web.scraping import get_soup
from cowidev.vax.utils.incremental import enrich_data, increment


def read(source: str) -> pd.Series:
    soup = get_soup(source)
    return parse_data(soup)


def parse_data(soup: BeautifulSoup) -> pd.Series:

    numbers = soup.find_all(class_="odometer")

    date = re.search(r"[\d\.]{10}", soup.find(class_="counter").text).group(0)
    date = clean_date(date, "%d.%m.%Y")

    return pd.Series(
        data={
            "total_vaccinations": int(numbers[0]["data-count"]),
            "people_vaccinated": int(numbers[1]["data-count"]),
            "people_fully_vaccinated": int(numbers[2]["data-count"]),
            "total_boosters": int(numbers[3]["data-count"]),
            "date": date,
        }
    )


def enrich_location(ds: pd.Series) -> pd.Series:
    return enrich_data(ds, "location", "Northern Cyprus")


def enrich_vaccine(ds: pd.Series) -> pd.Series:
    return enrich_data(ds, "vaccine", "Oxford/AstraZeneca, Pfizer/BioNTech, Sinovac")


def enrich_source(ds: pd.Series, source: str) -> pd.Series:
    return enrich_data(ds, "source_url", source)


def pipeline(ds: pd.Series, source: str) -> pd.Series:
    return ds.pipe(enrich_location).pipe(enrich_vaccine).pipe(enrich_source, source)


def main():
    source = "https://asi.saglik.gov.ct.tr/"
    data = read(source).pipe(pipeline, source)
    increment(
        location=data["location"],
        total_vaccinations=data["total_vaccinations"],
        people_vaccinated=data["people_vaccinated"],
        people_fully_vaccinated=data["people_fully_vaccinated"],
        total_boosters=data["total_boosters"],
        date=data["date"],
        source_url=data["source_url"],
        vaccine=data["vaccine"],
    )


if __name__ == "__main__":
    main()
