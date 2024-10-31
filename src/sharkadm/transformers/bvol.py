import functools

from sharkadm import adm_logger
from .base import Transformer, DataHolderProtocol

try:
    import nodc_bvol
    bvol_nomp = nodc_bvol.get_bvol_nomp_object()
    translate_bvol_name = nodc_bvol.get_translate_bvol_name_object()
    translate_bvol_name_and_size = nodc_bvol.get_translate_bvol_name_size_object()
except ModuleNotFoundError as e:
    module_name = str(e).split("'")[-2]
    adm_logger.log_workflow(f'Could not import package "{module_name}" in module {__name__}. You need to install this dependency if you want to use this module.', level=adm_logger.WARNING)


class AddBvolScientificNameOriginal(Transformer):
    valid_data_types = ['Phytoplankton']
    source_col = 'reported_scientific_name'
    col_to_set = 'bvol_scientific_name_original'

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    @staticmethod
    def get_transformer_description() -> str:
        return f'Adds {AddBvolScientificNameOriginal.col_to_set} from {AddBvolScientificNameOriginal.source_col}'

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        for name, df in data_holder.data.groupby(self.source_col):
            new_name = translate_bvol_name.get(str(name))
            if new_name:
                adm_logger.log_transformation(f'Translating {name} -> {new_name} ({len(df)} places)',
                                              level=adm_logger.INFO)
            else:
                adm_logger.log_transformation(f'Adding {name} to {self.col_to_set} ({len(df)} places)',
                                              level=adm_logger.DEBUG)
                new_name = name

            boolean = data_holder.data[self.source_col] == name
            data_holder.data.loc[boolean, self.col_to_set] = new_name


class AddBvolScientificNameAndSizeClass(Transformer):
    valid_data_types = ['Phytoplankton']

    source_name_col = 'bvol_scientific_name_original'
    source_size_class_col = 'size_class'
    col_to_set_name = 'bvol_scientific_name'
    col_to_set_size = 'bvol_size_class'

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    @staticmethod
    def get_transformer_description() -> str:
        return f'Adds {AddBvolScientificNameAndSizeClass.col_to_set_name} and {AddBvolScientificNameAndSizeClass.col_to_set_size}'

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        data_holder.data[self.col_to_set_name] = ''
        data_holder.data[self.col_to_set_size] = ''
        for (name, size), df in data_holder.data.groupby([self.source_name_col, self.source_size_class_col]):
            info = translate_bvol_name_and_size.get(name, size)
            new_name = info.get('name') or  name
            new_size_class = info.get('size_class') or size
            if new_name != name:
                adm_logger.log_transformation(f'Translate bvol name: {name} -> {new_name} ({len(df)} places)', level=adm_logger.INFO)
            data_holder.data.loc[df.index, self.col_to_set_name] = new_name
            if new_size_class != size:
                adm_logger.log_transformation(f'Translate bvol size_class: {name} -> {new_name} ({len(df)} places)', level=adm_logger.INFO)
            data_holder.data.loc[df.index, self.col_to_set_size] = new_size_class


class AddBvolRefList(Transformer):
    valid_data_types = ['Phytoplankton']

    source_col = 'bvol_scientific_name'
    col_to_set = 'bvol_ref_list'

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    @staticmethod
    def get_transformer_description() -> str:
        return f'Adds {AddBvolRefList.col_to_set} from {AddBvolRefList.source_col}'

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        data_holder.data[self.col_to_set] = ''
        for name, df in data_holder.data.groupby(self.source_col):
            lst = bvol_nomp.get_info(Species=name)
            if not lst:
                continue
            if type(lst) == list:
                text = ', '.join(sorted(set([item['List'] for item in lst if item['List']])))
            else:
                text = lst['List']
            adm_logger.log_transformation(f'Setting {self.col_to_set} for {name}: {text} ({len(df)} places)',
                                          level=adm_logger.INFO)
            data_holder.data.loc[df.index, self.col_to_set] = text


class AddBvolAphiaId(Transformer):
    valid_data_types = ['Phytoplankton']

    source_col = 'bvol_scientific_name'
    col_to_set = 'bvol_aphia_id'

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    @staticmethod
    def get_transformer_description() -> str:
        return f'Adds {AddBvolAphiaId.col_to_set} from {AddBvolAphiaId.source_col}'

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        data_holder.data[self.col_to_set] = ''
        for name, df in data_holder.data.groupby(self.source_col):
            lst = bvol_nomp.get_info(Species=name)
            if not lst:
                continue
            if type(lst) == list:
                text = ', '.join(sorted(set([item['AphiaID'] for item in lst if item['AphiaID']])))
            else:
                text = lst['AphiaID']
            adm_logger.log_transformation(f'Setting {self.col_to_set} for {name}: {text} ({len(df)} places)',
                                          level=adm_logger.INFO)
            data_holder.data.loc[df.index, self.col_to_set] = text


# class AddBvolCalculatedVolume(Transformer):
#     valid_data_types = ['Phytoplankton']
#
#     col_to_set = 'bvol_calculated_volume_um3'
#     source_col = 'bvol_scientific_name'
#
#     def __init__(self, **kwargs):
#         super().__init__(**kwargs)
#
#     @staticmethod
#     def get_transformer_description() -> str:
#         return f'Adds {AddBvolCalculatedVolume.col_to_set}'
#
#     def _transform(self, data_holder: DataHolderProtocol) -> None:
#         data_holder.data[self.col_to_set] = data_holder.data[self.source_col].apply(self._translate)
#
#     @functools.cache
#     def _translate(self, x: str) -> str:
#         lst = bvol_nomp.get_info(Species=x)
#         if not lst:
#             return ''
#         if type(lst) == list:
#             return ', '.join(sorted(set([item['Calculated_volume_um3'] for item in lst if item['Calculated_volume_um3']])))
#         return lst['Calculated_volume_um3']



