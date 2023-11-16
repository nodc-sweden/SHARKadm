from ..base import Transformer, DataHolderProtocol

import qc_lib
from SHARKadm import adm_logger


class QCRange(Transformer):
    QC0_index = 0

    def __init__(self, columns: list[str], **kwargs):
        super().__init__(**kwargs)
        self._columns = columns

    @staticmethod
    def get_transformer_description() -> str:
        return f'Flags data if out of range'

    def _transform(self, data_holder: DataHolderProtocol) -> None:

        header = list(data_holder.data.columns)
        for col in self._columns:
            if col not in header:
                adm_logger.log_transformation(f'Missing column for range check: {col}', level='warning')
                continue
            qc0_col = f'QC0_{col}'
            if qc0_col not in header:
                # Maybe it should be possible to add this column here?
                adm_logger.log_transformation(f'Missing QC0 column for range check: {qc0_col}', level='warning')
                continue
        for key, vdata in data_holder.data.groupby('sharkadm_visit_id'):
            lat = list(set(vdata['sample_latitude_dd']))[0]
            lon = list(set(vdata['sample_longitude_dd']))[0]
            date = list(set(vdata['visit_date']))[0]
            for col in self._columns:
                ranges = qc_lib.Ranges()
                low, high = ranges.get_ranges(par=col, lat=lat, lon=lon, date=date)
                low_obj = qc_lib.CheckLowerRange(low)
                low_obj.get_boolean()


        data_holder.data[self.datatype_column_name] = data_holder.data_type