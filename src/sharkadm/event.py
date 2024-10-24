_subscribers = dict(
    log=dict(),
    log_workflow=dict(),
    log_transformation=dict(),
    log_validation=dict(),
    log_export=dict(),
)


class EventNotFound(Exception):
    pass


def get_events() -> list[str]:
    return sorted(_subscribers)


def subscribe(event: str, func, prio: int = 50) -> None:
    if event not in _subscribers:
        raise EventNotFound(event)
    _subscribers[event].setdefault(prio, [])
    _subscribers[event][prio].append(func)


def post_event(event: str, data: dict | str) -> None:
    if type(data) is str:
        data = dict(msg=data)
    if event not in _subscribers:
        raise EventNotFound(event)
    for prio in sorted(_subscribers[event]):
        for func in _subscribers[event][prio]:
            func(data)

