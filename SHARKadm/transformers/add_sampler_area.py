from .base import Transformer, DataHolderProtocol
from SHARKadm import adm_logger


class AddCalculatedSamplerArea(Transformer):
    sampler_area_par = 'sampler_area_m2'
    section_distance_start_par = 'section_distance_start_m'
    section_distance_end_par = 'section_distance_end_m'
    transect_width_par = 'transect_width_m'

    @property
    def transformer_description(self) -> str:
        cols = '\n'.join([
            self.sampler_area_par,
            self.section_distance_start_par,
            self.section_distance_end_par,
            self.transformer_name
        ])
        return f"Calculates sampler area using columns:\n{cols}"

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        data_holder.data[self.sampler_area_par] = data_holder.data.apply(lambda row: self.calculate(row), axis=1)

    def calculate(self, row) -> str:
        if row.get(self.sampler_area_par):
            return row[self.sampler_area_par]
        can_calculate = True
        for par in [self.section_distance_start_par, self.section_distance_end_par, self.transect_width_par]:
            if par not in row:
                adm_logger.log_transformation(f'Missing key: {par}', level='warning')
                can_calculate = False
            elif not row[par]:
                adm_logger.log_transformation(f'Missing value for parameter: {par}', level='warning')
                can_calculate = False
        if not can_calculate:
            return ''
        print(row[self.section_distance_start_par], row[self.section_distance_end_par], row[self.transect_width_par])
        return str((float(self.section_distance_end_par) - float(self.section_distance_start_par)) *
                   float(self.transect_width_par))


