import pathlib

from .dv_template_data_holder import PolarsDvTemplateDataHolder


def get_polars_dv_template_data_holder(
    path: str | pathlib.Path,
) -> PolarsDvTemplateDataHolder:
    path = pathlib.Path(path)
    if not path.suffix == ".xlsx":
        raise NotImplementedError(f"Invalid template path: {path}")
    return PolarsDvTemplateDataHolder(template_path=path)
