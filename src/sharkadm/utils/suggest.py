from difflib import get_close_matches


def suggest_commands(command: str, valid_commands: list[str], nr: int = 3) -> list[str]:
    return get_close_matches(
        command,
        valid_commands,
        n=nr,
        cutoff=0.5,
    )
