import enum


class Events(enum.StrEnum):
    LOG = "log"
    LOG_WORKFLOW = "log_workflow"
    LOG_TRANSFORMATION = "log_transformation"
    LOG_VALIDATION = "log_validation"
    LOG_EXPORT = "log_export"
    LOG_PROGRESS = "progress"


# _subscribers = dict(
#     log=dict(),
#     log_workflow=dict(),
#     log_transformation=dict(),
#     log_validation=dict(),
#     log_export=dict(),
#     progress=dict(),
# )

_subscribers = dict((str(ev), dict()) for ev in Events)


class EventNotFound(Exception):
    pass


def get_events() -> list[str]:
    return sorted(_subscribers)


def subscribe(event: str | Events, func, prio: int = 50) -> None:
    event = str(event)
    if event not in _subscribers:
        raise EventNotFound(event)
    _subscribers[event].setdefault(prio, [])
    if func in _subscribers[event][prio]:
        return
    _subscribers[event][prio].append(func)


def post_event(event: str | Events, data: dict | str) -> None:
    event = str(event)
    if type(data) is str:
        data = dict(msg=data)
    if event not in _subscribers:
        raise EventNotFound(event)
    for prio in sorted(_subscribers[event]):
        for func in _subscribers[event][prio]:
            print(f"{func=}")
            func(data)
