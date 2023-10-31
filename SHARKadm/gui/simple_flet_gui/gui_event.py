_gui_subscribers = dict(

)


class GuiEventNotFound(Exception):
    pass


def get_events():
    return sorted(_gui_subscribers)


def subscribe(event: str, func, prio=50):
    if event not in _gui_subscribers:
        raise GuiEventNotFound(event)
    _gui_subscribers[event].setdefault(prio, [])
    _gui_subscribers[event][prio].append(func)


def post_event(event: str, data=None):
    if event not in _gui_subscribers:
        raise GuiEventNotFound(event)
    for prio in sorted(_gui_subscribers[event]):
        for func in _gui_subscribers[event][prio]:
            func(data)

