import qc_lib

from ..base import DataHolderProtocol, Transformer


class QCRange(Transformer):
    QC0_index = 0
    datatype_column_name = "REPLACE_WITH_VALUE"

    def __init__(self, columns: list[str], **kwargs):
        super().__init__(**kwargs)
        self._columns = columns

    @staticmethod
    def get_transformer_description() -> str:
        return "Flags data if out of range"

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        header = list(data_holder.data.columns)
        for col in self._columns:
            if col not in header:
                self._log(f"Missing column for range check: {col}", level="warning")
                continue
            qc0_col = f"QC0_{col}"
            if qc0_col not in header:
                # Maybe it should be possible to add this column here?
                self._log(
                    f"Missing QC0 column for range check: {qc0_col}", level="warning"
                )
                continue
        for key, vdata in data_holder.data.groupby("sharkadm_visit_id"):
            lat = next(iter(set(vdata["sample_latitude_dd"])))
            lon = next(iter(set(vdata["sample_longitude_dd"])))
            date = next(iter(set(vdata["visit_date"])))
            for col in self._columns:
                ranges = qc_lib.Ranges()
                low, high = ranges.get_ranges(par=col, lat=lat, lon=lon, date=date)
                low_obj = qc_lib.CheckLowerRange(low)
                low_obj.get_boolean()

        data_holder.data[self.datatype_column_name] = data_holder.data_type
