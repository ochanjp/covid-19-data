import requests

import pandas as pd

from cowidev.vax.utils.utils import build_vaccine_timeline
from cowidev.vax.utils.base import CountryVaxBase
from cowidev.vax.utils.files import load_data
from cowidev.vax.utils.utils import make_monotonic


class Indonesia(CountryVaxBase):
    location = "Indonesia"
    source_url_ref = "https://data.covid19.go.id/public/index.html"
    source_url = "https://data.covid19.go.id/public/api/pemeriksaan-vaksinasi.json"
    # source with news, as images :(: https://covid19.go.id/p/berita?page=2&search=

    def read(self) -> pd.DataFrame:
        data = requests.get(self.source_url).json()
        assert set(data["vaksinasi"]["harian"][-1].keys()) == {
            "key_as_string",
            "key",
            "doc_count",
            "jumlah_vaksinasi_2",
            "jumlah_vaksinasi_1",
            "jumlah_jumlah_vaksinasi_1_kum",
            "jumlah_jumlah_vaksinasi_2_kum",
        }, f'New columns found! Check {data["vaksinasi"]["harian"][-1].keys()}'
        records = [
            {
                "date": record["key_as_string"],
                "dose_1": record["jumlah_jumlah_vaksinasi_1_kum"]["value"],
                "dose_2": record["jumlah_jumlah_vaksinasi_2_kum"]["value"],
            }
            for record in data["vaksinasi"]["harian"]
        ]
        df = pd.DataFrame(records)
        return df

    def pipe_metadata(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(location=self.location, source_url=self.source_url_ref)

    def pipe_vaccine(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.pipe(
            build_vaccine_timeline,
            {
                "Sinovac": "2020-12-01",
                "Oxford/AstraZeneca": "2021-03-22",
                "Sinopharm/Beijing": "2021-05-18",
                "Moderna": "2021-07-17",
                "Pfizer/BioNTech": "2021-08-29",
                "Johnson&Johnson": "2021-09-11",
                "Novavax": "2021-11-27",
            },
        )

    def pipe_merge_legacy(self, df: pd.DataFrame) -> pd.DataFrame:
        df_legacy = load_data(f"{self.location.lower()}-legacy")
        # df_legacy = df_legacy[~df_legacy.date.isin(df.date)]
        df = df[df.date > (df_legacy.date.max())]
        return pd.concat([df, df_legacy]).sort_values("date")

    def pipe_metrics(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.assign(
            people_vaccinated=df["dose_1"],
            total_vaccinations=df["dose_1"] + df["dose_2"],  # partial estimate (booster data missing)
            people_fully_vaccinated=df["dose_2"],  # single shot data missing
        )
        df.loc[df.date >= "2021-09-11", "people_fully_vaccinated"] = pd.NA  # single shot data missing from 2021-09-11
        return df

    def pipe_add_latest_who(self, df: pd.DataFrame) -> pd.DataFrame:
        who = pd.read_csv(
            "https://covid19.who.int/who-data/vaccination-data.csv",
            usecols=[
                "COUNTRY",
                "DATA_SOURCE",
                "DATE_UPDATED",
                "TOTAL_VACCINATIONS",
                "PERSONS_FULLY_VACCINATED",
                "PERSONS_VACCINATED_1PLUS_DOSE",
            ],
        )

        who = who[(who.COUNTRY == self.location) & (who.DATA_SOURCE == "REPORTING")]
        if len(who) == 0:
            return df

        last_who_report_date = who.DATE_UPDATED.values[0]
        df = df[-((df.date > last_who_report_date) & (df.total_vaccinations < who.TOTAL_VACCINATIONS.values[0]))]
        df.loc[df.date == last_who_report_date, "total_vaccinations"] = who.TOTAL_VACCINATIONS.values[0]
        df.loc[df.date == last_who_report_date, "people_vaccinated"] = who.PERSONS_VACCINATED_1PLUS_DOSE.values[0]
        df.loc[df.date == last_who_report_date, "people_fully_vaccinated"] = who.PERSONS_FULLY_VACCINATED.values[0]
        df.loc[df.date == last_who_report_date, "source_url"] = "https://covid19.who.int/"
        return df

    def pipeline(self, ds: pd.Series) -> pd.Series:
        return (
            ds.pipe(self.pipe_metadata)
            .pipe(self.pipe_metrics)
            .pipe(self.pipe_add_latest_who)
            .pipe(make_monotonic)
            .pipe(self.pipe_merge_legacy)
            .pipe(self.pipe_vaccine)[
                [
                    "location",
                    "date",
                    "vaccine",
                    "source_url",
                    "total_vaccinations",
                    "people_vaccinated",
                    "people_fully_vaccinated",
                ]
            ]
        )

    def export(self):
        df = self.read().pipe(self.pipeline)
        df.to_csv(self.output_path, index=False)


def main():
    Indonesia().export()
