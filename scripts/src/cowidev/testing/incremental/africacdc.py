import pandas as pd

from cowidev.vax.incremental.africacdc import AfricaCDC as AfricaCDCVax
from cowidev.testing.utils.incremental import increment


ACDC_COUNTRIES = {
    "Angola": {"name": "Angola", "notes": ""},  # New
    "Malawi": {  # Deprecate R script + change "samples tested" -> "tests performed"
        "name": "Malawi",
    },
    "Botswana": {"name": "Botswana"},  # Automate
    "Burundi": {  # New
        "name": "Burundi",
    },
    "Burkina Faso": {  # New
        "name": "Burkina Faso",
    },
    "Central African Republic": {  # New
        "name": "Central African Republic",
    },
    "Chad": {  # New
        "name": "Chad",
    },
    "Cote d'Ivoire": {"name": "Cote d'Ivoire"},  # Automate
    # "Democratic Republic of the Congo": {"name": "Democratic Republic of Congo"},  # Manual
    "Ethiopia": {"name": "Ethiopia"},  # Change source to Africa CDC  # Change source to Africa CDC
    "Gabon": {"name": "Gabon"},  # Automate
    "Gambia": {"name": "Gambia"},  # Automate
    "Ghana": {"name": "Ghana"},  # Automate
    "Kenya": {
        "name": "Kenya",
    },
    "Madagascar": {"name": "Madagascar"},  # Automate
    "Mauritania": {
        "name": "Mauritania",
    },  # Deprecate R script
    "Morocco": {"name": "Morocco"},  # Automate
    "Mozambique": {"name": "Mozambique"},  # Automate
    "Namibia": {"name": "Namibia"},  # Automate
    "South Sudan": {  # Deprecate R script
        "name": "South Sudan",
    },
    "Uganda": {  # Automate
        "name": "Uganda",
    },
    "Zimbabwe": {"name": "Zimbabwe"},  # Automate
}
country_mapping = {country: metadata["name"] for country, metadata in ACDC_COUNTRIES.items()}


class AfricaCDC(AfricaCDCVax):
    _base_url = (
        "https://services8.arcgis.com/vWozsma9VzGndzx7/ArcGIS/rest/services/"
        "DailyCOVIDDashboard_5July21_1/FeatureServer/0/"
    )
    source_url_ref = "https://africacdc.org/covid-19/"
    source_label = "Africa Centres for Disease Control and Prevention"
    columns_use = [
        "Country",
        "Tests_Conducted",
        "Date",
    ]
    columns_rename = {
        "Country": "location",
        "Tests_Conducted": "Cumulative total",
        "Date": "date",
    }
    units = "tests performed"
    notes = ""

    def pipeline(self, df: pd.DataFrame) -> pd.DataFrame:
        return (
            df.pipe(self.pipe_filter_columns)
            .pipe(self.pipe_rename)
            .pipe(self.pipe_filter_countries, country_mapping)
            .pipe(self.pipe_date)
        )

    def increment_countries(self, df: pd.DataFrame):
        for row in df.sort_values("location").iterrows():
            row = row[1]
            country = row["location"]
            # print(country, row["Cumulative total"])
            notes = ACDC_COUNTRIES[country].get("notes", self.notes)
            units = ACDC_COUNTRIES[country].get("units", self.units)
            increment(
                count=row["Cumulative total"],
                sheet_name=country,
                country=country,
                units=units,
                date=row["date"],
                source_url=self.source_url_ref,
                source_label=self.source_label,
                notes=notes,
            )

    def export(self):
        df = self.read().pipe(self.pipeline)
        self.increment_countries(df)


def main():
    AfricaCDC().export()
