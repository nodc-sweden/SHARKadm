import inspect


def get_kwargs_for_class(cls: type):
    kw = dict()
    for key, value in inspect.signature(cls.__init__).parameters.items():
        if key in ['self', 'kwargs']:
            continue
        val = value.default
        if type(val) == type:
            val = None
        kw[key] = val
    return kw