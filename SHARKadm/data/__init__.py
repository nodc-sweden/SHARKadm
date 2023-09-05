import pathlib

from SHARKadm import config
from SHARKadm.data import data_holder
from SHARKadm.data import data_source
from SHARKadm.data import delivery_note


def load_dataset(
            data_source_directory: str | pathlib.Path = None,
            data_file_name: str = None,
            data_source_class: data_source.DataFile = None
        ) -> data_holder.DataHolder | None:
    """Loads a txt based dataset"""
    dataset_directory = pathlib.Path(data_source_directory)

    delivery_note_path = pathlib.Path(dataset_directory, 'processed_data', 'delivery_note.txt')
    data_file_path = pathlib.Path(dataset_directory, 'processed_data', data_file_name)

    if not data_file_path.exists():
        return

    d_note = delivery_note.DeliveryNote(delivery_note_path)

    column_info = config.get_column_info_config()
    id_handler = config.get_sharkadm_id_handler()
    print(f'{d_note.data_type=}')
    import_matrix = config.get_import_matrix_config(data_type=d_note.data_type)

    import_header_mapper = import_matrix.get_mapper(d_note.import_matrix_key)

    data_file = data_source_class(path=data_file_path, data_type=d_note.data_type)
    data_file.map_header(import_header_mapper)

    d_holder = data_holder.DataHolder(data_type=d_note.data_type,
                                     column_info=column_info,
                                     id_handler=id_handler,
                                     dataset_name=dataset_directory.name)

    d_holder.add_data_source(data_file)
    return d_holder


def load_xml_dataset(directory: str | pathlib.Path) -> data_holder.DataHolder | None:
    return load_dataset(
        data_source_directory=directory,
        data_file_name='data.xml',
        data_source_class=data_source.XmlDataFile
    )


def load_txt_dataset(directory: str | pathlib.Path) -> data_holder.DataHolder | None:
    return load_dataset(
        data_source_directory=directory,
        data_file_name='data.txt',
        data_source_class=data_source.TxtColumnDataFile
    )

# def load_xml_dataset(directory: str | pathlib.Path) -> data_holder.DataHolder | None:
#     """Loads a xml based dataset"""
#     dataset_directory = pathlib.Path(directory)
#
#     delivery_note_path = pathlib.Path(dataset_directory, 'processed_data', 'delivery_note.txt')
#     data_file_path = pathlib.Path(dataset_directory, 'processed_data', 'data.xml')
#
#     if not data_file_path.exists():
#         return
#
#     d_note = delivery_note.DeliveryNote(delivery_note_path)
#
#     column_info = config.get_column_info_config()
#     id_handler = config.get_sharkadm_id_handler()
#     import_matrix = config.get_import_matrix_config(data_type=d_note.data_type)
#
#     import_header_mapper = import_matrix.get_mapper(d_note.import_matrix_key)
#
#     data_file = data_source.XmlDataFile(path=data_file_path, data_type=d_note.data_type)
#     data_file.map_header(import_header_mapper)
#
#     d_holder = data_holder.DataHolder(data_type=d_note.data_type,
#                              column_info=column_info,
#                              id_handler=id_handler,
#                              dataset_name=dataset_directory.name)
#
#     d_holder.add_data_source(data_file)
#     return d_holder
#
#
# def load_txt_dataset(directory: str | pathlib.Path) -> data_holder.DataHolder | None:
#     """Loads a txt based dataset"""
#     dataset_directory = pathlib.Path(directory)
#
#     delivery_note_path = pathlib.Path(dataset_directory, 'processed_data', 'delivery_note.txt')
#     data_file_path = pathlib.Path(dataset_directory, 'processed_data', 'data.txt')
#
#     if not data_file_path.exists():
#         return
#
#     d_note = delivery_note.DeliveryNote(delivery_note_path)
#
#     column_info = config.get_column_info_config()
#     id_handler = config.get_sharkadm_id_handler()
#     import_matrix = config.get_import_matrix_config(data_type=d_note.data_type)
#
#     import_header_mapper = import_matrix.get_mapper(d_note.import_matrix_key)
#
#     data_file = data_source.TxtColumnDataFile(path=data_file_path, data_type=d_note.data_type)
#     data_file.map_header(import_header_mapper)
#
#     d_holder = data_holder.DataHolder(data_type=d_note.data_type,
#                              column_info=column_info,
#                              id_handler=id_handler,
#                              dataset_name=dataset_directory.name)
#
#     d_holder.add_data_source(data_file)
#     return d_holder






