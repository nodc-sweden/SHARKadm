from sharkadm.config.data_type import data_type_handler
from sharkadm.data.data_holder import PolarsDataHolder


class PolarsProfileDataHolder(PolarsDataHolder):
    _data_type_obj = data_type_handler.get_data_type_obj("physicalchemical")
    _data_structure = "profile"
    _dataset_name = "profile"

    _header_mapper = None

    @property
    def date_column(self) -> str:
        date_col = "SDATE"
        if self._header_mapper:
            date_col = self._header_mapper.get_internal_name(date_col)
        return date_col

    @property
    def time_column(self) -> str:
        time_col = "STIME"
        if self._header_mapper:
            time_col = self._header_mapper.get_internal_name(time_col)
        return time_col

    @property
    def metadata(self) -> dict:
        meta = {}
        for (date, time), df in self.data.group_by(self.date_column, self.time_column):
            key = (date, time)
            meta[key] = {}
            for col in df.columns:
                values = set(df[col])
                if len(values) != 1:
                    continue
                meta[key][col] = values.pop()
        return meta

    @property
    def metadata_original_columns(self) -> dict:
        if not self._header_mapper:
            return self.metadata
        meta = {}
        for key, data in self.metadata.items():
            meta[key] = {}
            for col, value in data.items():
                meta[key][self._header_mapper.get_external_name(col)] = value
        return meta

    @staticmethod
    def get_data_holder_description() -> str:
        return """"""

    @property
    def dataset_name(self) -> str:
        return self._dataset_name

    @property
    def columns(self) -> list[str]:
        return sorted(self.data.columns)
