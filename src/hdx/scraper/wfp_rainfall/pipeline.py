#!/usr/bin/python
"""wfp-rainfall scraper"""

import csv
import logging
from datetime import datetime, timedelta
from math import ceil
from pathlib import Path

from hdx.api.configuration import Configuration
from hdx.api.utilities.hdx_error_handler import HDXErrorHandler
from hdx.data.dataset import Dataset
from hdx.data.resource import Resource
from hdx.location.adminlevel import AdminLevel
from hdx.location.country import Country
from hdx.pipelineutils.hapi_admins import complete_admins
from hdx.utilities.dateparse import iso_string_from_datetime, parse_date
from hdx.utilities.downloader import DownloadError
from hdx.utilities.retriever import Retrieve
from kalendar import Dekad

logger = logging.getLogger(__name__)


_AGGREGATION_PERIODS = {
    "f": "dekad",
    "1": "1-month",
    "3": "3-month",
}
_VERSIONS = {
    "final": "final",
    "forecast": "forecast",
    "prelim": "preliminary",
}


class Pipeline:
    def __init__(
        self,
        configuration: Configuration,
        retriever: Retrieve,
        temp_dir: str,
        error_handler: HDXErrorHandler,
        today: datetime,
    ):
        self._configuration = configuration
        self._retriever = retriever
        self._temp_dir = temp_dir
        self._error_handler = error_handler
        self._today = today
        self._admins = []
        self.data: dict[int, Path] = {}
        self.dates: set = set()
        self._csv_handles: dict = {}
        self._csv_writers: dict[int, csv.DictWriter] = {}

    def _write_hapi_row(self, ytd: int, hapi_row: dict) -> None:
        if ytd not in self._csv_handles:
            filepath = Path(self._temp_dir) / f"hdx_hapi_rainfall_global_{ytd}yr.csv"
            fh = open(filepath, "w", newline="", encoding="utf-8-sig")
            writer = csv.DictWriter(
                fh,
                fieldnames=self._configuration["headers"],
                extrasaction="ignore",
            )
            writer.writeheader()
            self._csv_handles[ytd] = fh
            self._csv_writers[ytd] = writer
            self.data[ytd] = filepath
        clean = {k: ("" if v is None else v) for k, v in hapi_row.items()}
        self._csv_writers[ytd].writerow(clean)

    def get_pcodes(self) -> None:
        for admin_level in [1, 2]:
            admin = AdminLevel(admin_level=admin_level, retriever=self._retriever)
            admin.setup_from_url()
            admin.load_pcode_formats()
            self._admins.append(admin)

    def download_data(self, countryiso3s: list | None = None) -> None:
        self.get_pcodes()
        if not countryiso3s:
            countryiso3s = [
                key for key in Country.countriesdata()["countries"] if key != "JPN"
            ]
        try:
            for countryiso3 in countryiso3s:
                dataset_name = f"{countryiso3.lower()}-rainfall-subnational"
                dataset = Dataset.read_from_hdx(dataset_name)
                if not dataset:
                    continue
                dataset_id = dataset["id"]
                hrp = "Y" if Country.get_hrp_status_from_iso3(countryiso3) else "N"
                gho = "Y" if Country.get_gho_status_from_iso3(countryiso3) else "N"
                pcode_lookup = {}

                resources = [r for r in dataset.get_resources() if "5ytd" in r["name"]]
                if len(resources) == 0:
                    self._error_handler.add_message(
                        "Rainfall",
                        dataset_name,
                        "Could not find resource",
                        message_type="warning",
                    )
                    continue
                resource = resources[0]
                resource_id = resource["id"]
                try:
                    headers, rows = self._retriever.get_tabular_rows(
                        resource["url"], dict_form=True
                    )
                except DownloadError:
                    self._error_handler.add_message(
                        "Rainfall",
                        dataset_name,
                        "Could not download resource",
                    )
                    continue
                pcode_header = "PCODE" if "PCODE" in headers else "ADM2_PCODE"
                wfp_id_header = "adm_id" if "adm_id" in headers else "adm2_id"
                for row in rows:
                    row_non_null = [r for r in row if r]
                    if "#" in row_non_null[0]:
                        continue

                    admin_level = int(row.get("adm_level", 2))
                    if admin_level == 2 and (
                        countryiso3 == "BRA" or (hrp == "N" and gho == "N")
                    ):
                        continue

                    start_date = parse_date(row["date"])
                    days_ago = (self._today - start_date).days
                    ytd = ceil(days_ago / 365)
                    if days_ago > 365 and admin_level > 1:
                        continue

                    pcode = row[pcode_header]
                    if admin_level == 1:
                        provider_names = ["Not provided", ""]
                        provider_codes = [str(row[wfp_id_header]), ""]
                        adm_codes = [pcode, ""]
                    else:
                        provider_names = ["Not provided", "Not provided"]
                        provider_codes = ["", str(row[wfp_id_header])]
                        adm_codes = ["", pcode]
                    adm_names = ["", ""]
                    if pcode in pcode_lookup:
                        adm_codes = pcode_lookup[pcode][0]
                        adm_names = pcode_lookup[pcode][1]
                        warnings = pcode_lookup[pcode][2]
                    else:
                        try:
                            adm_level, warnings = complete_admins(
                                self._admins,
                                countryiso3,
                                ["", ""],
                                adm_codes,
                                adm_names,
                                fuzzy_match=False,
                            )
                        except IndexError:
                            warnings = [f"Pcode unknown {adm_codes[1]}"]
                            adm_codes = ["", ""]
                        pcode_lookup[pcode] = (adm_codes, adm_names, warnings)
                    for warning in warnings:
                        self._error_handler.add_message(
                            "Rainfall",
                            dataset_name,
                            warning,
                            message_type="warning",
                        )

                    version = _VERSIONS.get(row["version"])
                    dekad = Dekad.fromdatetime(start_date)
                    end_date = (dekad + 1).todate() - timedelta(days=1)
                    end_date = parse_date(str(end_date))
                    self.dates.add(start_date)
                    self.dates.add(end_date)
                    start_date_iso = iso_string_from_datetime(start_date)
                    end_date_iso = iso_string_from_datetime(end_date)

                    for agg_header, aggregation_period in _AGGREGATION_PERIODS.items():
                        errors = []
                        if not version:
                            errors.append(f"Version unknown {row['version']}")
                            self._error_handler.add_message(
                                "Rainfall",
                                dataset_name,
                                f"Version unknown {row['version']}",
                            )
                        rainfall = row[f"r{agg_header}h"]
                        rainfall_long_term_average = row[f"r{agg_header}h_avg"]
                        rainfall_anomaly_pct = row[f"r{agg_header}q"]
                        if None in [
                            rainfall,
                            rainfall_long_term_average,
                            rainfall_anomaly_pct,
                        ]:
                            errors.append("Missing rainfall value")
                        hapi_row = {
                            "location_code": countryiso3,
                            "has_hrp": hrp,
                            "in_gho": gho,
                            "provider_admin1_name": provider_names[0],
                            "provider_admin2_name": provider_names[1],
                            "admin1_code": adm_codes[0],
                            "admin1_name": adm_names[0],
                            "admin2_code": adm_codes[1],
                            "admin2_name": adm_names[1],
                            "admin_level": admin_level,
                            "provider_admin1_code": provider_codes[0],
                            "provider_admin2_code": provider_codes[1],
                            "aggregation_period": aggregation_period,
                            "rainfall": rainfall,
                            "rainfall_long_term_average": rainfall_long_term_average,
                            "rainfall_anomaly_pct": rainfall_anomaly_pct,
                            "number_pixels": int(float(row["n_pixels"])),
                            "version": version,
                            "reference_period_start": start_date_iso,
                            "reference_period_end": end_date_iso,
                            "dataset_hdx_id": dataset_id,
                            "resource_hdx_id": resource_id,
                            "warning": "|".join(warnings),
                            "error": "|".join(errors),
                        }
                        self._write_hapi_row(ytd, hapi_row)
        finally:
            for fh in self._csv_handles.values():
                fh.close()
            self._csv_handles.clear()
            self._csv_writers.clear()

    def generate_global_dataset(self) -> Dataset:
        dataset = Dataset(
            {
                "name": "hdx-hapi-rainfall",
                "title": "HDX HAPI - Climate: Rainfall",
            }
        )
        dataset.add_tags(self._configuration["tags"])
        dataset.add_other_location("world")
        start_date = min(self.dates)
        end_date = max(self.dates)
        dataset.set_time_period(start_date, end_date)

        for ytd in sorted(self.data.keys()):
            resourcedata = {
                "name": self._configuration["resource_name"].format(ytd=ytd),
                "description": self._configuration["resource_description"].format(
                    ytd=ytd
                ),
                "p_coded": True,
            }
            resource = Resource(resourcedata)
            resource.set_format("csv")
            resource.set_file_to_upload(self.data[ytd])
            dataset.add_update_resource(resource)

        return dataset
