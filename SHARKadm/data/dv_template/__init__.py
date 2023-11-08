import pathlib

from .dv_template_data_holder import DvTemplateDataHolder


def get_dv_template_data_holder(path: str | pathlib.Path) -> DvTemplateDataHolder:
    return DvTemplateDataHolder(template_path=path)
