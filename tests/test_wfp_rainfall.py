from os.path import join

import pytest
from hdx.api.configuration import Configuration
from hdx.api.utilities.hdx_error_handler import HDXErrorHandler
from hdx.data.dataset import Dataset
from hdx.utilities.compare import assert_files_same
from hdx.utilities.dateparse import parse_date
from hdx.utilities.downloader import Download
from hdx.utilities.path import temp_dir
from hdx.utilities.retriever import Retrieve
from hdx.utilities.useragent import UserAgent

from hdx.scraper.wfp_rainfall.wfp_rainfall import WFPRainfall


class TestWFPRainfall:
    @pytest.fixture(scope="function")
    def configuration(self, config_dir):
        UserAgent.set_global("test")
        Configuration._create(
            hdx_read_only=True,
            hdx_site="prod",
            project_config_yaml=join(config_dir, "project_configuration.yaml"),
        )
        return Configuration.read()

    @pytest.fixture(scope="class")
    def fixtures_dir(self):
        return join("tests", "fixtures")

    @pytest.fixture(scope="class")
    def input_dir(self, fixtures_dir):
        return join(fixtures_dir, "input")

    @pytest.fixture(scope="function")
    def read_dataset(self, monkeypatch, input_dir):
        def read_from_hdx(dataset_name):
            return Dataset.load_from_json(join(input_dir, f"dataset-{dataset_name}.json"))

        monkeypatch.setattr(Dataset, "read_from_hdx", staticmethod(read_from_hdx))

    @pytest.fixture(scope="class")
    def config_dir(self, fixtures_dir):
        return join("src", "hdx", "scraper", "wfp_rainfall", "config")

    def test_wfp_rainfall(
        self, configuration, fixtures_dir, input_dir, config_dir, read_dataset
    ):
        with HDXErrorHandler() as error_handler:
            with temp_dir(
                "Test_wfp_rainfall",
                delete_on_success=True,
                delete_on_failure=False,
            ) as tempdir:
                with Download(user_agent="test") as downloader:
                    retriever = Retrieve(
                        downloader=downloader,
                        fallback_dir=tempdir,
                        saved_dir=input_dir,
                        temp_dir=tempdir,
                        save=False,
                        use_saved=True,
                    )
                    today = parse_date("2025-03-11")
                    wfp_rainfall = WFPRainfall(
                        configuration, retriever, tempdir, error_handler
                    )
                    wfp_rainfall.download_data(today, ["MOZ"])
                    dataset = wfp_rainfall.generate_country_dataset("MOZ")
                    dataset.update_from_yaml(
                        path=join(config_dir, "hdx_dataset_static.yaml")
                    )
                    assert dataset == {
                        "name": "hdx-hapi-rainfall-moz",
                        "title": "HDX HAPI - Climate: Rainfall (Mozambique)",
                        "tags": [
                            {
                                "name": "climate-weather",
                                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                            },
                            {
                                "name": "hxl",
                                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                            },
                        ],
                        "groups": [{"name": "moz"}],
                        "dataset_date": "[2021-01-01T00:00:00 TO 2025-03-10T23:59:59]",
                        "license_id": "cc-by",
                        "methodology": "Registry",
                        "caveats": "This dataset is refreshed every week, but the source "
                        "datasets may have different update schedules. Please refer to "
                        "the [source datasets](https://data.humdata.org/dataset/?"
                        "dataseries_name=WFP+-+Rainfall+Indicators+at+Subnational+Level) "
                        "to verify their specific update frequency.",
                        "dataset_source": "Climate Hazards Center UC Santa Barbara & WFP",
                        "package_creator": "HDX Data Systems Team",
                        "private": False,
                        "maintainer": "aa13de36-28c5-47a7-8d0b-6d7c754ba8c8",
                        "owner_org": "hdx-hapi",
                        "data_update_frequency": 7,
                        "notes": "This dataset contains data obtained from the\n[HDX "
                        "Humanitarian API](https://hapi.humdata.org/) (HDX HAPI),\nwhich "
                        "provides standardized humanitarian indicators designed\nfor "
                        "seamless interoperability from multiple sources.\nThe data "
                        "facilitates automated workflows and visualizations\nto support "
                        "humanitarian decision making.\nFor more information, please see "
                        "the HDX HAPI\n[landing page](https://data.humdata.org/hapi)\nand"
                        "\n[documentation](https://hdx-hapi.readthedocs.io/en/latest/)."
                        "\n\nWarnings typically indicate corrections have been made to\n"
                        "the data or show things to look out for. Rows with only warnings"
                        "\nare considered complete, and are made available via the API.\n"
                        "Errors usually mean that the data is incomplete or unusable.\n"
                        "Rows with any errors are not present in the API but are included"
                        "\nhere for transparency.\n",
                        "subnational": "1",
                        "dataset_preview": "no_preview",
                    }

                    resources = dataset.get_resources()
                    assert len(resources) == 5
                    assert resources[0] == {
                        "name": "Climate: Rainfall (2025)",
                        "description": "Rainfall data (2025) from HDX HAPI, "
                        "please see [the documentation](https://hdx-hapi.readthedocs."
                        "io/en/latest/data_usage_guides/climate/#rainfall) for more "
                        "information",
                        "format": "csv",
                        "resource_type": "file.upload",
                        "url_type": "upload",
                    }

                    assert_files_same(
                        join(fixtures_dir, "hdx_hapi_rainfall_moz_2024.csv"),
                        join(tempdir, "hdx_hapi_rainfall_moz_2024.csv"),
                    )

                    dataset = wfp_rainfall.generate_global_dataset()
                    dataset.update_from_yaml(
                        path=join(config_dir, "hdx_dataset_static.yaml")
                    )
                    assert dataset == {
                        "name": "hdx-hapi-rainfall",
                        "title": "HDX HAPI - Climate: Rainfall",
                        "tags": [
                            {
                                "name": "climate-weather",
                                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                            },
                            {
                                "name": "hxl",
                                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                            },
                        ],
                        "groups": [{"name": "world"}],
                        "dataset_date": "[2025-02-11T00:00:00 TO 2025-03-10T23:59:59]",
                        "license_id": "cc-by",
                        "methodology": "Registry",
                        "caveats": "This dataset is refreshed every week, but the source "
                        "datasets may have different update schedules. Please refer to "
                        "the [source datasets](https://data.humdata.org/dataset/?"
                        "dataseries_name=WFP+-+Rainfall+Indicators+at+Subnational+Level) "
                        "to verify their specific update frequency.",
                        "dataset_source": "Climate Hazards Center UC Santa Barbara & WFP",
                        "package_creator": "HDX Data Systems Team",
                        "private": False,
                        "maintainer": "aa13de36-28c5-47a7-8d0b-6d7c754ba8c8",
                        "owner_org": "hdx-hapi",
                        "data_update_frequency": 7,
                        "notes": "This dataset contains data obtained from the\n[HDX "
                        "Humanitarian API](https://hapi.humdata.org/) (HDX HAPI),\nwhich "
                        "provides standardized humanitarian indicators designed\nfor "
                        "seamless interoperability from multiple sources.\nThe data "
                        "facilitates automated workflows and visualizations\nto support "
                        "humanitarian decision making.\nFor more information, please see "
                        "the HDX HAPI\n[landing page](https://data.humdata.org/hapi)\nand"
                        "\n[documentation](https://hdx-hapi.readthedocs.io/en/latest/)."
                        "\n\nWarnings typically indicate corrections have been made to\n"
                        "the data or show things to look out for. Rows with only warnings"
                        "\nare considered complete, and are made available via the API.\n"
                        "Errors usually mean that the data is incomplete or unusable.\n"
                        "Rows with any errors are not present in the API but are included"
                        "\nhere for transparency.\n",
                        "subnational": "1",
                        "dataset_preview": "no_preview",
                    }
                    resources = dataset.get_resources()
                    assert len(resources) == 2
                    assert resources[0] == {
                        "name": "Global Climate: Rainfall",
                        "description": "30-day Rainfall data from HDX HAPI, "
                        "please see [the documentation](https://hdx-hapi.readthedocs."
                        "io/en/latest/data_usage_guides/climate/#rainfall) for more "
                        "information",
                        "format": "csv",
                        "resource_type": "file.upload",
                        "url_type": "upload",
                    }
                    assert_files_same(
                        join(fixtures_dir, "hdx_hapi_rainfall_global_30_day.csv"),
                        join(tempdir, "hdx_hapi_rainfall_global_30_day.csv"),
                    )
