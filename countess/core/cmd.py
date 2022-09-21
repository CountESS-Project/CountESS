import configparser
import importlib
import re
import sys

import dask.dataframe as dd

from .plugins import BasePlugin


def progress_callback(a, b, s="Running"):
    if b > 0:
        print(
            "%-20s %4d/%4d %-50s" % (s, a, b, "*" * int(a * 50 / b)),
            end="\r",
            flush=True,
        )
    else:
        print("%-20s %d" % (s, a), end="\r", flush=True)


def process_ini(config_filenames):
    config = configparser.ConfigParser()
    config.read(config_filenames)

    obj = None
    previous_plugin = None

    # XXX should use Pipeline instead

    for section_name in config.sections():
        module_name, class_name = section_name.split(":")
        plugin_class = getattr(importlib.import_module(module_name), class_name)
        assert issubclass(plugin_class, BasePlugin)

        print(plugin_class.name)

        plugin = plugin_class()
        plugin.set_previous_plugin(previous_plugin)
        plugin.update()

        for k, v in config[section_name].items():
            # XXX cheesy but this way of handling files will be replaced soon
            if k.startswith("file.") and k.endswith(".filename"):
                plugin.add_file(v)
            else:
                plugin.parameters[k].set_value(v)

        obj = plugin.run(obj, progress_callback)

        print()
        print()

        previous_plugin = plugin

    if isinstance(obj, dd.DataFrame):
        print(obj.compute())


def main():
    process_ini(["./countess.ini"] + sys.argv[1:])


if __name__ == "__main__":
    main()
