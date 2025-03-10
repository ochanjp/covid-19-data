import pandas as pd

from cowidev.utils.web import request_json
from cowidev.vax.utils.incremental import enrich_data, increment


def read(source: str) -> pd.Series:
    data = request_json(source)
    return parse_data(data)


def parse_data(data: dict) -> pd.Series:

    people_vaccinated = int(data["data"][data["sheetNames"].index("1. doza")][0][0])
    people_fully_vaccinated = data["data"][data["sheetNames"].index("2. doza")]
    total_boosters = data["data"][data["sheetNames"].index("3. doza")]
    if len(people_fully_vaccinated) > 0:
        people_fully_vaccinated = int(people_fully_vaccinated[0][0])
    else:
        people_fully_vaccinated = 0
    if len(total_boosters) > 0:
        total_boosters = int(total_boosters[0][0])
    else:
        total_boosters = 0

    total_vaccinations = people_vaccinated + people_fully_vaccinated + total_boosters

    date = str(pd.to_datetime(data["refreshed"], unit="ms").date())

    data = {
        "date": date,
        "total_vaccinations": total_vaccinations,
        "people_vaccinated": people_vaccinated,
        "people_fully_vaccinated": people_fully_vaccinated,
        "total_boosters": total_boosters,
    }
    return pd.Series(data=data)


def enrich_location(ds: pd.Series) -> pd.Series:
    return enrich_data(ds, "location", "Montenegro")


def enrich_vaccine(ds: pd.Series) -> pd.Series:
    return enrich_data(
        ds,
        "vaccine",
        "Oxford/AstraZeneca, Pfizer/BioNTech, Sinopharm/Beijing, Sputnik V",
    )


def enrich_source(ds: pd.Series) -> pd.Series:
    return enrich_data(ds, "source_url", "https://www.covidodgovor.me/")


def pipeline(ds: pd.Series) -> pd.Series:
    return ds.pipe(enrich_location).pipe(enrich_vaccine).pipe(enrich_source)


def main():
    source = "https://atlas.jifo.co/api/connectors/520021dc-c292-4903-9cdb-a2467f64ed97"
    data = read(source).pipe(pipeline)
    increment(
        location=str(data["location"]),
        total_vaccinations=int(data["total_vaccinations"]),
        people_vaccinated=int(data["people_vaccinated"]),
        people_fully_vaccinated=int(data["people_fully_vaccinated"]),
        total_boosters=int(data["total_boosters"]),
        date=str(data["date"]),
        source_url=str(data["source_url"]),
        vaccine=str(data["vaccine"]),
    )


if __name__ == "__main__":
    main()
