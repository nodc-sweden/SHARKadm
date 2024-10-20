import functools

from sharkadm import adm_logger
from .base import Transformer, DataHolderProtocol

try:
    import nodc_bvol
except ModuleNotFoundError as e:
    module_name = str(e).split("'")[-2]
    adm_logger.log_workflow(f'Could not import package "{module_name}" in module {__name__}. You need to install this dependency if you want to use this module.', level=adm_logger.WARNING)


class AddBvolScientificName(Transformer):
    valid_data_types = ['Phytoplankton']
    col_to_set = 'bvol_scientific_name'
    # source_col = 'scientific_name'
    source_col = 'scientific_name'

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.translate_bvol_name = nodc_bvol.get_translate_bvol_name_object()

    @staticmethod
    def get_transformer_description() -> str:
        return 'Adds the bivol scientific name'

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        data_holder.data[self.col_to_set] = data_holder.data[self.source_col].apply(self._translate)

    @functools.cache
    def _translate(self, x: str) -> str:
        name = self.translate_bvol_name.get(x)
        if not name:
            return x
        adm_logger.log_transformation(f'bvol_scientific_name translated: {x} -> {name}', level='info')
        return name


class AddBvolSizeClass(Transformer):
    valid_data_types = ['Phytoplankton']

    col_to_set_name = 'bvol_scientific_name'
    col_to_set_size = 'bvol_size_class'
    # source_col = 'scientific_name'
    source_name_col = 'scientific_name'
    source_size_class_col = 'size_class'

    cash = dict()

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.translate_bvol_name = nodc_bvol.get_translate_bvol_name_size_object()

    @staticmethod
    def get_transformer_description() -> str:
        return 'Adds the bivol scientific name'

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        for i in data_holder.data.index:
            from_name = data_holder.data.at[i, self.source_name_col]
            from_size = data_holder.data.at[i, self.source_size_class_col]
            to_name = from_name
            to_size = from_size
            result = self.cash.setdefault(f'{from_name}_{from_size}', self.translate_bvol_name.get(from_name,
                                                                                                   from_size))
            if result:
                to_name, to_size = result
            data_holder.data.at[i, self.col_to_set_name] = to_name
            data_holder.data.at[i, self.col_to_set_size] = to_size


class AddBvolRefList(Transformer):
    valid_data_types = ['Phytoplankton']

    col_to_set = 'bvol_ref_list'
    # source_col = 'scientific_name'
    source_col = 'scientific_name'

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.translate_bvol_nomp = nodc_bvol.get_translate_bvol_nomp_object()

    @staticmethod
    def get_transformer_description() -> str:
        return 'Adds the bivol reference list name'

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        data_holder.data[self.col_to_set] = data_holder.data[self.source_col].apply(self._translate)

    @functools.cache
    def _translate(self, x: str) -> str:
        lst = self.translate_bvol_nomp.get_info(Species=x)
        if not lst:
            return ''
        if type(lst) == list:
            return ', '.join(sorted(set([item['List'] for item in lst if item['List']])))
        return lst['List']


class AddBvolAphiaId(Transformer):
    valid_data_types = ['Phytoplankton']

    col_to_set = 'bvol_aphia_id'
    source_col = 'scientific_name'

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.translate_bvol_nomp = nodc_bvol.get_translate_bvol_nomp_object()

    @staticmethod
    def get_transformer_description() -> str:
        return 'Adds the bivol reference list name'

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        data_holder.data[self.col_to_set] = data_holder.data[self.source_col].apply(self._translate)

    @functools.cache
    def _translate(self, x: str) -> str:
        lst = self.translate_bvol_nomp.get_info(Species=x)
        if not lst:
            return ''
        if type(lst) == list:
            return ', '.join(sorted(set([item['AphiaID'] for item in lst if item['AphiaID']])))
        return lst['AphiaID']



