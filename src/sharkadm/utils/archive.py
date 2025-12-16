import datetime

from sharkadm.data import PolarsDataHolder


def _get_year_range_str(data_holder: PolarsDataHolder) -> str:
    all_years = sorted(set(data_holder.data["visit_year"]))
    if len(all_years) == 1:
        return all_years[0]
    return f"{all_years[0]}_{all_years[-1]}"


def get_zip_archive_base(data_holder: PolarsDataHolder) -> str:
    parts = ["SHARK", data_holder.data_type, _get_year_range_str(data_holder)]
    if hasattr(data_holder, "delivery_note"):
        parts.append(data_holder.delivery_note.sample_orderer_code)
    return "_".join(parts)


def get_zip_archive_file_base(data_holder: PolarsDataHolder) -> str:
    today = datetime.datetime.now().strftime("%Y-%m-%d")
    return f"{get_zip_archive_base(data_holder)}_version_{today}"
