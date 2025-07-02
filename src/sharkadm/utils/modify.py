import pathlib


def remove_depth_info_in_standard_format(path: pathlib.Path,
                                         export_directory: pathlib.Path | None = None,
                                         overwrite: bool = False,
                                         encoding: str = "cp1252",
                                         replace_value: str | int | float = "999") -> None:
    if not export_directory:
        export_directory = path.parent

    lines = []
    with open(path, encoding=encoding) as fid:
        for line in fid:
            if "METADATA;WADEP" in line:
                line = f"//METADATA;WADEP;{replace_value}\n"
            lines.append(line)
    export_path = export_directory / path.name
    if export_path.exists() and not overwrite:
        raise FileExistsError(export_path)
    with open(export_path, "w", encoding=encoding) as fid:
        fid.write("".join(lines))


def remove_wadep_in_metadata_file(path: pathlib.Path,
                                        export_directory: pathlib.Path | None = None,
                                        replace_value: str | int | float = "999",
                                        overwrite=False,
                                        encoding="cp1252",
                                        sep="\t") -> None:
    if not export_directory:
        export_directory = path.parent
    index = None
    lines = []
    with open(path, encoding=encoding) as fid:
        for r, line in enumerate(fid):
            split_line = line.split(sep)
            if r == 0:
                header = split_line
                index = header.index("WADEP")
                lines.append(line)
                continue
            split_line[index] = str(replace_value)
            lines.append(sep.join(split_line))
    export_path = export_directory / path.name
    if export_path.exists() and not overwrite:
        raise FileExistsError(export_path)
    with open(export_path, "w", encoding=encoding) as fid:
        fid.write("".join(lines))
