import yaml
import pathlib


def convert_import_matrix_to_yaml(input_path, output_path, **kwargs):
    data = {}
    header = []
    with open(input_path, encoding=kwargs.get('encoding', 'cp1252')) as fid:
        for r, line in enumerate(fid):
            if not line.strip():
                continue
            split_line = [item.strip() for item in line.split('\t')]
            if r == 0:
                header = split_line
                data = {key: {} for key in header[1:]}
                continue

            for inst, par_str in zip(header[1:], split_line[1:]):
                if not par_str:
                    continue
                for par in par_str.split('<or>'):
                    data[inst][par.split('.', 1)[-1]] = split_line[0].split('.', 1)[-1]

    with open(output_path, 'w') as fid:
        yaml.safe_dump(data, fid)


def convert_column_info_to_yaml(input_path, output_path, **kwargs):
    data = {}
    header = []
    with open(input_path, encoding=kwargs.get('encoding', 'cp1252')) as fid:
        for r, line in enumerate(fid):
            if not line.strip():
                continue
            split_line = [item.strip() for item in line.split('\t')]
            if r == 0:
                header = split_line
                continue
            line_dict = dict(zip(header[1:], split_line[1:]))
            key = split_line[0]
            if not key:
                continue
            data[key] = line_dict

    with open(output_path, 'w') as fid:
        yaml.safe_dump(data, fid)


def convert_translate_codes(input_path, output_path, **kwargs):
    """Translates the old translate_codes.txt file to the new yaml format"""
    data = {}
    header = []
    with open(input_path, encoding=kwargs.get('encoding', 'cp1252')) as fid:
        for r, line in enumerate(fid):
            if not line.strip():
                continue
            split_line = [item.strip() for item in line.split('\t')]
            if r == 0:
                header = split_line
                continue
            line_dict = dict(zip(header, split_line))
            internal_key = line_dict['internal_key']
            internal_value = line_dict['internal_value']
            data.setdefault(internal_key, {})
            data[internal_key][internal_value] = {}
            data[internal_key][internal_value]['synonyms'] = \
                [item.strip() for item in line_dict['synonyms'].split('<or>')]
            for key in ['short_name', 'swedish_name', 'english_name']:
                data[internal_key][internal_value][key] = line_dict[key]

    with open(output_path, 'w') as fid:
        yaml.safe_dump(data, fid, encoding=kwargs.get('encoding', 'cp1252'))

