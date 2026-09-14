import pathlib

from nodc_config import Config

from .dv_template_data_holder import PolarsDvTemplateDataHolder


def get_polars_dv_template_data_holder(
    nodc_conf: Config,
    path: str | pathlib.Path,
) -> PolarsDvTemplateDataHolder:
    path = pathlib.Path(path)
    if not path.suffix == ".xlsx":
        raise NotImplementedError(f"Invalid template path: {path}")
    return PolarsDvTemplateDataHolder(template_path=path)
