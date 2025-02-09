import tempfile
import re

import pandas as pd
from bs4 import BeautifulSoup
from pdfminer.high_level import extract_text

from cowidev.utils import clean_date, clean_count, get_soup
from cowidev.utils.web.download import download_file_from_url
from cowidev.vax.utils.incremental import enrich_data, increment


class Azerbaijan:
    location = "Azerbaijan"
    source_url = "https://koronavirusinfo.az"
    regex = {
        "title": r"Vaksinasiya",
        "date": r"(\d{2}\.\d{2}\.20\d{2})",
        "total": r"ümumi sayı (\d+) Gün",
        "doses": r"vaksinlərin sayı (\d+) (\d+) (\d+) 1\-ci",
    }

    def read(self) -> pd.Series:
        """Read data from source."""
        soup = get_soup(self.source_url)
        data = self._parse_data(soup)
        return pd.Series(data)

    def _parse_data(self, soup: BeautifulSoup) -> dict:
        """get data from the source page."""
        # Get pdf url
        url = self._parse_pdf_link(soup)

        if not url.endswith(".pdf"):
            raise ValueError(f"File reporting metrics is not a PDF: {url}!")
        # Extract pdf text
        text = self._parse_pdf_text(url)
        # Extract date from text
        date = self._parse_date(text)
        # Extract metrics from text
        total_vaccinations, people_vaccinated, people_fully_vaccinated, total_boosters = self._parse_metrics(text)
        record = {
            "total_vaccinations": total_vaccinations,
            "people_vaccinated": people_vaccinated,
            "people_fully_vaccinated": people_fully_vaccinated,
            "total_boosters": total_boosters,
            "source_url": url,
            "date": date,
        }
        return record

    def _parse_pdf_link(self, soup: BeautifulSoup) -> str:
        """Parse pdf link from source page."""
        href = soup.find("a", string=self.regex["title"]).get("href")
        return f"{self.source_url}{href}"

    def _parse_pdf_text(self, url: str) -> str:
        """Parse pdf text from url."""
        with tempfile.NamedTemporaryFile() as tmp:
            download_file_from_url(url, tmp.name)
            with open(tmp.name, "rb") as f:
                text = extract_text(f)
        text = re.sub(r"(\d)\s(\d)", r"\1\2", text)
        text = re.sub(r"\s+", " ", text)
        return text

    def _parse_date(self, text: str) -> str:
        """Parse date from text."""
        date_str = re.search(self.regex["date"], text).group(1)
        return clean_date(date_str, "%d.%m.%Y")

    def _parse_metrics(self, text: str) -> tuple:
        """Parse metrics from text."""
        total_vaccinations = re.search(self.regex["total"], text).group(1)
        people_vaccinated = re.search(self.regex["doses"], text).group(1)
        people_fully_vaccinated = re.search(self.regex["doses"], text).group(2)
        total_boosters = re.search(self.regex["doses"], text).group(3)
        return (
            clean_count(total_vaccinations),
            clean_count(people_vaccinated),
            clean_count(people_fully_vaccinated),
            clean_count(total_boosters),
        )

    def enrich_vaccine(self, ds: pd.Series) -> pd.Series:
        """Enrich data with vaccine names."""
        return enrich_data(ds, "vaccine", "Oxford/AstraZeneca, Pfizer/BioNTech, Sinovac, Sputnik V")

    def pipeline(self, df: pd.DataFrame) -> pd.DataFrame:
        """Pipeline for data."""
        return df.pipe(self.enrich_vaccine)

    def export(self):
        """Export data to csv."""
        data = self.read().pipe(self.pipeline)
        increment(
            location=self.location,
            total_vaccinations=data["total_vaccinations"],
            people_vaccinated=data["people_vaccinated"],
            people_fully_vaccinated=data["people_fully_vaccinated"],
            total_boosters=data["total_boosters"],
            date=data["date"],
            source_url=data["source_url"],
            vaccine=data["vaccine"],
        )


def main():
    Azerbaijan().export()


if __name__ == "__main__":
    main()
