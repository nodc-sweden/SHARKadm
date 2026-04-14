from importlib.metadata import entry_points

PLUGINS = dict()
for discovered_plugin in entry_points(group="sharkadm.plugin_operators"):
    name = discovered_plugin.value
    module = discovered_plugin.load()
    PLUGINS[name] = module
