subscribers = dict(
    log=dict(),
    workflow=dict()
)


class EventNotFound(Exception):
    pass


def get_events() -> list[str]:
    return sorted(subscribers)


def subscribe(event: str, func, prio: int = 50) -> None:
    if event not in subscribers:
        raise EventNotFound(event)
    subscribers[event].setdefault(prio, [])
    subscribers[event][prio].append(func)


def post_event(event: str, data=None) -> None:
    if event not in subscribers:
        raise EventNotFound(event)
    for prio in sorted(subscribers[event]):
        for func in subscribers[event][prio]:
            func(data)

