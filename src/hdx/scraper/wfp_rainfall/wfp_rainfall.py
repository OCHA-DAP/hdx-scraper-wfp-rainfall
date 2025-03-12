#!/usr/bin/python
"""wfp-rainfall scraper"""

import logging
from datetime import timedelta
from typing import List, Optional

from hdx.api.configuration import Configuration
from hdx.api.utilities.hdx_error_handler import HDXErrorHandler
from hdx.data.dataset import Dataset
from hdx.location.adminlevel import AdminLevel
from hdx.location.country import Country
from hdx.scraper.framework.utilities.hapi_admins import complete_admins
from hdx.utilities.dateparse import iso_string_from_datetime, parse_date
from hdx.utilities.dictandlist import dict_of_lists_add
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


class WFPRainfall:
    def __init__(
        self,
        configuration: Configuration,
        retriever: Retrieve,
        temp_dir: str,
        error_handler: HDXErrorHandler,
    ):
        self._configuration = configuration
        self._retriever = retriever
        self._temp_dir = temp_dir
        self._error_handler = error_handler
        self._admins = []
        self.data = {}
        self.dates = set()

    def get_pcodes(self) -> None:
        for admin_level in [1, 2]:
            admin = AdminLevel(admin_level=admin_level, retriever=self._retriever)
            dataset = admin.get_libhxl_dataset(retriever=self._retriever)
            admin.setup_from_libhxl_dataset(dataset)
            admin.load_pcode_formats()
            self._admins.append(admin)

    def download_data(self, countryiso3s: Optional[List] = None) -> None:
        self.get_pcodes()
        if not countryiso3s:
            countryiso3s = [key for key in Country.countriesdata()["countries"]]
        for countryiso3 in countryiso3s:
            dataset_name = f"{countryiso3.lower()}-rainfall-subnational"
            dataset = Dataset.read_from_hdx(dataset_name)
            if not dataset:
                continue
            dataset_id = dataset["id"]
            hrp = "Y" if Country.get_hrp_status_from_iso3(countryiso3) else "N"
            gho = "Y" if Country.get_gho_status_from_iso3(countryiso3) else "N"

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
            headers, rows = self._retriever.get_tabular_rows(
                resource["url"], dict_form=True
            )
            pcode_header = "PCODE" if "PCODE" in headers else "ADM2_PCODE"
            wfp_id_header = "adm_id" if "adm_id" in headers else "adm2_id"
            for row in rows:
                if "#" in row[pcode_header]:
                    continue

                errors = []

                admin_level = int(row.get("adm_level", 2))
                if admin_level == 1:
                    provider_codes = [str(row[wfp_id_header]), ""]
                    adm_codes = [row["PCODE"], ""]
                else:
                    provider_codes = ["", str(row[wfp_id_header])]
                    adm_codes = ["", row[pcode_header]]
                adm_names = ["", ""]
                try:
                    adm_level, warnings = complete_admins(
                        self._admins,
                        countryiso3,
                        ["", ""],
                        adm_codes,
                        adm_names,
                    )
                except IndexError:
                    warnings = [f"Pcode unknown {adm_codes[1]}"]
                    adm_codes = ["", ""]
                for warning in warnings:
                    self._error_handler.add_message(
                        "Rainfall",
                        dataset_name,
                        warning,
                        message_type="warning",
                    )

                version = _VERSIONS.get(row["version"])
                if not version:
                    errors.append(f"Version unknown {row['version']}")
                    self._error_handler.add_message(
                        "Rainfall",
                        dataset_name,
                        f"Version unknown {row['version']}",
                    )

                start_date = parse_date(row["date"])
                dekad = Dekad.fromdatetime(start_date)
                end_date = (dekad + 1).todate() - timedelta(days=1)
                end_date = parse_date(str(end_date))
                self.dates.add(start_date)
                self.dates.add(end_date)
                start_date = iso_string_from_datetime(start_date)
                end_date = iso_string_from_datetime(end_date)

                for agg_header, aggregation_period in _AGGREGATION_PERIODS.items():
                    hapi_row = {
                        "location_code": countryiso3,
                        "has_hrp": hrp,
                        "in_gho": gho,
                        "admin1_code": adm_codes[0],
                        "admin1_name": adm_names[0],
                        "admin2_code": adm_codes[1],
                        "admin2_name": adm_names[1],
                        "admin_level": admin_level,
                        "provider_admin1_code": provider_codes[0],
                        "provider_admin2_code": provider_codes[1],
                        "aggregation_period": aggregation_period,
                        "rainfall": row[f"r{agg_header}h"],
                        "rainfall_long_term_average": row[f"r{agg_header}h_avg"],
                        "rainfall_anomaly_pct": row[f"r{agg_header}q"],
                        "number_pixels": row["n_pixels"],
                        "version": version,
                        "reference_period_start": start_date,
                        "reference_period_end": end_date,
                        "dataset_hdx_id": dataset_id,
                        "resource_hdx_id": resource_id,
                        "warning": "|".join(warnings),
                        "error": "|".join(errors),
                    }
                    dict_of_lists_add(self.data, countryiso3, hapi_row)

    def generate_dataset(self) -> Dataset:
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

        hxl_tags = self._configuration["hxl_tags"]
        headers = list(hxl_tags.keys())
        countryiso3s = sorted(list(self.data.keys()))
        for countryiso3 in countryiso3s:
            country_name = Country.get_country_name_from_iso3(countryiso3)
            resourcedata = {
                "name": self._configuration["resource_name"].replace("iso", countryiso3),
                "description": self._configuration["resource_description"].replace(
                    "country", country_name
                ),
            }
            dataset.generate_resource_from_iterable(
                headers,
                self.data[countryiso3],
                hxl_tags,
                self._temp_dir,
                f"hdx_hapi_rainfall_{countryiso3.lower()}.csv",
                resourcedata,
                encoding="utf-8-sig",
            )

        return dataset
