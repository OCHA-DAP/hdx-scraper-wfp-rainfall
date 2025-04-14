#!/usr/bin/python
"""
Top level script. Calls other functions that generate datasets that this
script then creates in HDX.

"""

import logging
from os.path import dirname, expanduser, join

from hdx.api.configuration import Configuration
from hdx.api.utilities.hdx_error_handler import HDXErrorHandler
from hdx.data.user import User
from hdx.facades.infer_arguments import facade
from hdx.utilities.downloader import Download
from hdx.utilities.path import temp_dir
from hdx.utilities.retriever import Retrieve

from hdx.scraper.wfp_rainfall.wfp_rainfall import WFPRainfall

logger = logging.getLogger(__name__)

_USER_AGENT_LOOKUP = "hdx-scraper-wfp-rainfall"
_SAVED_DATA_DIR = "saved_data"  # Keep in repo to avoid deletion in /tmp
_UPDATED_BY_SCRIPT = "HDX Scraper: WFP Rainfall"


def main(
    save: bool = True,
    use_saved: bool = False,
    err_to_hdx: bool = False,
) -> None:
    """Generate datasets and create them in HDX

    Args:
        save (bool): Save downloaded data. Defaults to True.
        use_saved (bool): Use saved data. Defaults to False.
        err_to_hdx (bool): Whether to write any errors to HDX metadata. Defaults to False.

    Returns:
        None
    """
    logger.info(f"##### {_USER_AGENT_LOOKUP} ####")
    configuration = Configuration.read()
    if not User.check_current_user_organization_access("hdx-hapi", "create_dataset"):
        raise PermissionError(
            "API Token does not give access to HDX-HAPI organisation!"
        )

    with HDXErrorHandler(write_to_hdx=err_to_hdx) as error_handler:
        with temp_dir(folder=_USER_AGENT_LOOKUP) as temp_folder:
            with Download() as downloader:
                retriever = Retrieve(
                    downloader=downloader,
                    fallback_dir=temp_folder,
                    saved_dir=_SAVED_DATA_DIR,
                    temp_dir=temp_folder,
                    save=save,
                    use_saved=use_saved,
                )

                wfp_rainfall = WFPRainfall(
                    configuration, retriever, temp_folder, error_handler
                )
                wfp_rainfall.download_data()
                years = reversed(wfp_rainfall.data.keys())
                for year in years:
                    dataset = wfp_rainfall.generate_global_dataset(year)
                    dataset.update_from_yaml(
                        path=join(
                            dirname(__file__), "config", "hdx_dataset_static.yaml"
                        )
                    )
                    dataset.create_in_hdx(
                        remove_additional_resources=False,
                        match_resource_order=False,
                        hxl_update=False,
                        updated_by_script=_UPDATED_BY_SCRIPT,
                    )


if __name__ == "__main__":
    facade(
        main,
        user_agent_config_yaml=join(expanduser("~"), ".useragents.yaml"),
        user_agent_lookup=_USER_AGENT_LOOKUP,
        project_config_yaml=join(
            dirname(__file__), "config", "project_configuration.yaml"
        ),
    )
