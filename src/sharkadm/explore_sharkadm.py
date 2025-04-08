import rich

from sharkadm import (
    exporters,
    transformers,
    utils,
    validators,
    write_operations_description_to_file,
)

ROW_LENGTH = 79


class ExploreSHARKadm:
    def __init__(self):
        self._title = "Explore SHARKadm"
        self._print_main_menu()

    def _new_page(self):
        rich.print("\n" * 40)

    def _print_main_menu(self):
        rich.print("=" * ROW_LENGTH)
        rich.print(f"{self._title:^{ROW_LENGTH}}")
        rich.print("=" * ROW_LENGTH)
        rich.print(f"{'W or 1'.ljust(40)}: Write all OPERATIONS to file")
        rich.print(f"{'V or 2 (filter)'.ljust(40)}: List VALIDATORS")
        rich.print(f"{'T or 3 (filter)'.ljust(40)}: List TRANSFORMERS")
        rich.print(f"{'E or 4 (filter)'.ljust(40)}: List EXPORTERS")
        rich.print(f"{'Q'.ljust(40)}: Quit")
        rich.print("-" * ROW_LENGTH)
        rich.print()
        ans = input("Gör ett val:")
        self._handle_main_menu_ans(ans)

    def _handle_main_menu_ans(self, ans: str):
        self._new_page()
        if not ans.strip():
            rich.print("Invalid option given...try again!")
            self._print_main_menu()
            return
        filt = None
        parts = ans.split()
        sel = parts[0].upper()
        if len(parts) > 1:
            filt = parts[1]
        if sel == "Q":
            return
        elif sel in ["W", "1"]:
            write_operations(open_file=True)
            self._print_main_menu()
        elif sel in ["V", "2"]:
            rich.print(f"{parts=}")
            rich.print(f"{filt=}")
            list_validators(filt)
        elif sel in ["T", "3"]:
            rich.print(f"{parts=}")
            rich.print(f"{filt=}")
            list_transformers(filt)
        elif sel in ["E", "4"]:
            rich.print(f"{parts=}")
            rich.print(f"{filt=}")
            list_exporters(filt)
        else:
            rich.print("Invalid option given...try again!")
            self._print_main_menu()
            return


def write_operations(open_file: bool) -> None:
    directory = utils.get_export_directory(date_directory=False)
    path = directory / "all_sharkadm_operations.txt"
    write_operations_description_to_file(path)
    if open_file:
        utils.open_file_with_default_program(path)


def list_validators(filter_string: str | None = None):
    info_lines = ["", "=" * ROW_LENGTH]
    if filter_string:
        info_lines.append(f'VALIDATORS filtered on "{filter_string}":')
    else:
        info_lines.append("VALIDATORS (all)")
    for name, obj in validators.get_validators().items():
        desc = obj.get_validator_description()
        if not desc:
            continue
        if filter_string:
            in_name = filter_string.lower() in name.lower()
            in_desc = filter_string.lower() in desc.lower()
            if not (in_name or in_desc):
                continue
        info_lines.append(f"{name.ljust(60)}: {desc}")
    info_lines.append("-" * ROW_LENGTH)
    rich.print("\n".join(info_lines))


def list_transformers(filter_string: str | None = None):
    info_lines = ["", "=" * ROW_LENGTH]
    if filter_string:
        info_lines.append(f'TRANSFORMERS filtered on "{filter_string}":')
    else:
        info_lines.append("TRANSFORMERS (all)")
    for name, obj in transformers.get_transformers().items():
        desc = obj.get_transformer_description()
        if not desc:
            continue
        if filter_string:
            in_name = filter_string.lower() in name.lower()
            in_desc = filter_string.lower() in desc.lower()
            if not (in_name or in_desc):
                continue
        info_lines.append(f"{name.ljust(60)}: {desc}")
    info_lines.append("-" * ROW_LENGTH)
    rich.print("\n".join(info_lines))


def list_exporters(filter_string: str | None = None):
    info_lines = ["", "=" * ROW_LENGTH]
    if filter_string:
        info_lines.append(f'EXPORTERS filtered on "{filter_string}":')
    else:
        info_lines.append("EXPORTERS (all)")
    for name, obj in exporters.get_exporters().items():
        desc = obj.get_exporter_description()
        if not desc:
            continue
        if filter_string:
            in_name = filter_string.lower() in name.lower()
            in_desc = filter_string.lower() in desc.lower()
            if not (in_name or in_desc):
                continue
        info_lines.append(f"{name.ljust(60)}: {desc}")
    info_lines.append("-" * ROW_LENGTH)
    rich.print("\n".join(info_lines))


if __name__ == "__main__":
    e = ExploreSHARKadm()
