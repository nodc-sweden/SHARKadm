import pathlib

import rich
import typer

from sharkadm import transformers, utils, validators, write_operations_description_to_file
from sharkadm.workflow import SHARKadmWorkflow

app = typer.Typer()


def _write_operations(directory: str, file_name: str, open_file: bool) -> None:
    if not directory:
        directory = utils.get_export_directory(date_directory=False)
    path = directory / file_name
    write_operations_description_to_file(path)
    if open_file:
        utils.open_file_with_default_program(path)


@app.command()
def list_validators(filter_string: str | None = None):
    info_lines = ["", "=" * 100]
    if filter_string:
        info_lines.append(f'VALIDATORS filtered on "{filter_string}":')
    else:
        info_lines.append("VALIDATORS (all)")
    for name, obj in validators.get_validators().items():
        desc = obj.get_validator_description()
        if filter_string:
            in_name = filter_string.lower() in name.lower()
            in_desc = filter_string.lower() in desc.lower()
            if not (in_name or in_desc):
                continue
        info_lines.append(f"{name.ljust(60)}: {desc}")
    info_lines.append("-" * 100)
    rich.print("\n".join(info_lines))


@app.command()
def list_transformers(filter_string: str | None = None):
    info_lines = ["", "=" * 100]
    if filter_string:
        info_lines.append(f'TRANSFORMERS filtered on "{filter_string}":')
    else:
        info_lines.append("TRANSFORMERS (all)")
    for name, obj in transformers.get_transformers().items():
        desc = obj.get_transformer_description()
        if filter_string:
            in_name = filter_string.lower() in name.lower()
            in_desc = filter_string.lower() in desc.lower()
            if not (in_name or in_desc):
                continue
        info_lines.append(f"{name.ljust(60)}: {desc}")
    info_lines.append("-" * 100)
    rich.print("\n".join(info_lines))


@app.command()
def operations(
    write: bool = False,
    directory: str = typer.Option(None, "--directory", "--dir"),
    file_name: str = typer.Option("sharkadm_operators.txt", "--file_name", "--name"),
    open_file: bool = typer.Option(False, "--open_file", "--open"),
):
    if write:
        _write_operations(directory, file_name, open_file)


@app.command()
def workflow(config_path: str, source: str):
    config_file = pathlib.Path(config_path)
    if not config_file.exists():
        raise FileNotFoundError(config_file)
    wf = SHARKadmWorkflow.from_yaml_config(config_file)
    if source:
        wf.set_data_sources(source)
    wf.start_workflow()
    rich.print("Workflow DONE!")


def main():
    app()


if __name__ == "__main__":
    main()
