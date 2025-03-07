#!/usr/bin/python
"""wfp-rainfall scraper"""

import logging

from hdx.api.configuration import Configuration
from hdx.api.utilities.hdx_error_handler import HDXErrorHandler
from hdx.data.dataset import Dataset
from hdx.location.adminlevel import AdminLevel
from hdx.scraper.framework.utilities.hapi_admins import complete_admins
from hdx.utilities.dateparse import iso_string_from_datetime, parse_date_range
from hdx.utilities.retriever import Retrieve

logger = logging.getLogger(__name__)


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
        self.dates = []

    def get_pcodes(self) -> None:
        for admin_level in [1, 2]:
            admin = AdminLevel(admin_level=admin_level, retriever=self._retriever)
            dataset = admin.get_libhxl_dataset(retriever=self._retriever)
            admin.setup_from_libhxl_dataset(dataset)
            admin.load_pcode_formats()
            self._admins.append(admin)

    def download_data(self) -> None:
        self.get_pcodes()

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
        dataset.generate_resource_from_iterable(
            headers,
            self.data,
            hxl_tags,
            self._temp_dir,
            "hdx_hapi_rainfall_global.csv",
            self._configuration["resource_info"],
            encoding="utf-8-sig",
        )

        return dataset
