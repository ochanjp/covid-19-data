import pandas as pd

from cowidev.utils import paths
from cowidev.utils.utils import check_known_columns
from cowidev.utils.web import request_json
from cowidev.vax.utils.checks import VACCINES_ONE_DOSE
from cowidev.vax.utils.files import export_metadata_manufacturer
from cowidev.vax.utils.utils import make_monotonic, build_vaccine_timeline


class Romania:
    source_url: str = "https://d35p9e4fm9h3wo.cloudfront.net/latestData.json"
    source_url_ref: str = "https://datelazi.ro/"
    location: str = "Romania"
    columns_rename: dict = {
        "total_administered": "total_vaccinations",
        "immunized": "people_fully_vaccinated",
    }
    vaccine_mapping: dict = {
        "pfizer": "Pfizer/BioNTech",
        "pfizer_pediatric": "Pfizer/BioNTech",
        "moderna": "Moderna",
        "astra_zeneca": "Oxford/AstraZeneca",
        "johnson_and_johnson": "Johnson&Johnson",
    }

    def read(self) -> pd.DataFrame:
        data = request_json(self.source_url)
        df = pd.DataFrame.from_dict(data["historicalData"], orient="index")
        check_known_columns(
            df,
            [
                "parsedOn",
                "parsedOnString",
                "fileName",
                "complete",
                "averageAge",
                "numberInfected",
                "numberCured",
                "numberDeceased",
                "percentageOfWomen",
                "percentageOfMen",
                "percentageOfChildren",
                "numberTotalDosesAdministered",
                "distributionByAge",
                "countyInfectionsNumbers",
                "incidence",
                "large_cities_incidence",
                "small_cities_incidence",
                "vaccines",
            ],
        )
        return df[["vaccines", "numberTotalDosesAdministered"]].reset_index().dropna().sort_values(by="index")

    def pipe_unnest_data(self, df: pd.DataFrame) -> pd.DataFrame:
        def _data_by_day(record):
            return (
                pd.DataFrame.from_records(record[1])
                .transpose()
                .reset_index()
                .rename(columns={"index": "vaccine"})
                .assign(date=record[0])
            )

        df = pd.concat(map(_data_by_day, df.values.tolist()))
        # Check vaccine names - Any new ones?
        vaccines_unknown = set(df.vaccine).difference(self.vaccine_mapping)
        if vaccines_unknown:
            raise ValueError(f"Unrecognized vaccine {vaccines_unknown}")
        df["vaccine"] = df.vaccine.replace(self.vaccine_mapping)
        return df

    def pipe_sum_vaccines(self, df: pd.DataFrame) -> pd.DataFrame:
        # Some vaccines are renamed to the same vaccine in our data e.g. 'pfizer' and 'pfizer_pediatric'
        return df.groupby(["date", "vaccine"], as_index=False).sum()

    def pipe_rename_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.rename(columns=self.columns_rename)

    def pipe_store_timeline(self, df: pd.DataFrame) -> pd.DataFrame:
        self.vaccine_timeline = df[df.total_vaccinations > 0].groupby("vaccine").date.min().to_dict()
        return df

    def pipe_location(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(location=self.location)

    def pipeline_base(self, df: pd.DataFrame) -> pd.DataFrame:
        return (
            df.pipe(self.pipe_unnest_data)
            .pipe(self.pipe_sum_vaccines)
            .pipe(self.pipe_rename_columns)
            .pipe(self.pipe_store_timeline)
            .pipe(self.pipe_location)
        )

    def pipe_metrics(self, df: pd.DataFrame) -> pd.DataFrame:

        # Calculate people_vaccinated
        df.loc[df.vaccine.isin(VACCINES_ONE_DOSE), "people_vaccinated"] = df.people_fully_vaccinated
        df.loc[-df.vaccine.isin(VACCINES_ONE_DOSE), "people_vaccinated"] = (
            df.total_vaccinations - df.people_fully_vaccinated
        )

        # Sum by day, then sum over time
        df = df.drop(columns="vaccine").groupby(["date", "location"], as_index=False).sum().sort_values("date")
        df[["total_vaccinations", "people_fully_vaccinated", "people_vaccinated"]] = (
            df[["total_vaccinations", "people_fully_vaccinated", "people_vaccinated"]].cumsum().astype(int)
        )

        # Starting on 2021-09-28 (start of the booster rollout) we can no longer use
        # people_vaccinated = total_vaccinations - people_fully_vaccinated
        df.loc[df.date >= "2021-09-28", "people_vaccinated"] = pd.NA

        return df

    def pipe_source(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(source_url=self.source_url_ref)

    def pipe_vaccines(self, df: pd.DataFrame) -> pd.DataFrame:
        return build_vaccine_timeline(df, self.vaccine_timeline)

    def pipe_add_latest_who(self, df: pd.DataFrame) -> pd.DataFrame:
        who = pd.read_csv(
            "https://covid19.who.int/who-data/vaccination-data.csv",
            usecols=["COUNTRY", "DATA_SOURCE", "DATE_UPDATED", "PERSONS_VACCINATED_1PLUS_DOSE"],
        )

        who = who[(who.COUNTRY == "Romania") & (who.DATA_SOURCE == "REPORTING")]
        if len(who) == 0:
            return df

        last_who_report_date = who.DATE_UPDATED.values[0]
        df = df[
            -((df.date > last_who_report_date) & (df.people_vaccinated < who.PERSONS_VACCINATED_1PLUS_DOSE.values[0]))
        ]
        df.loc[df.date == last_who_report_date, "total_vaccinations"] = pd.NA
        df.loc[df.date == last_who_report_date, "people_vaccinated"] = who.PERSONS_VACCINATED_1PLUS_DOSE.values[0]
        df.loc[df.date == last_who_report_date, "people_fully_vaccinated"] = pd.NA
        df.loc[df.date == last_who_report_date, "source_url"] = "https://covid19.who.int/"
        return df

    def pipeline(self, df: pd.DataFrame) -> pd.DataFrame:
        return (
            df.pipe(self.pipe_metrics)
            .pipe(self.pipe_source)
            .pipe(self.pipe_vaccines)
            .pipe(self.pipe_add_latest_who)
            .pipe(make_monotonic)
        )

    def pipe_filter_rows_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        return df[df.total_vaccinations != 0].drop(columns="people_fully_vaccinated")

    def pipe_manufacturer_cumsum(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.sort_values("date")
        df["total_vaccinations"] = df[["vaccine", "total_vaccinations"]].groupby("vaccine").cumsum()
        return df

    def pipeline_manufacturer(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.pipe(self.pipe_filter_rows_columns).pipe(self.pipe_manufacturer_cumsum)

    def export(self):
        df_base = self.read().pipe(self.pipeline_base)

        # Main vaccination data
        df = df_base.copy().pipe(self.pipeline)
        df.to_csv(paths.out_vax(self.location), index=False)

        # Manufacturer data
        df = df_base.copy().pipe(self.pipeline_manufacturer)
        df.to_csv(paths.out_vax(self.location, manufacturer=True), index=False)
        export_metadata_manufacturer(
            df,
            "Government of Romania via datelazi.ro",
            self.source_url,
        )


def main():
    Romania().export()


if __name__ == "__main__":
    main()
