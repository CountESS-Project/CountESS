import configparser
import sys
import re
import importlib

from .plugins import BasePlugin

def progress_callback(a, b, s='Running'):
    if b > 0:
        print("%-20s %4d/%4d %-50s" % (s, a, b, '*'*int(a*50/b)), end='\r', flush=True)
    else:
        print("%-20s %d" % (s, a), end='\r', flush=True)

def process_ini(config_filenames):
    config = configparser.ConfigParser()
    config.read(config_filenames)

    obj = None

    for section_name in config.sections():
        module_name, class_name = section_name.split(':')
        plugin_class = getattr(importlib.import_module(module_name), class_name)
        assert issubclass(plugin_class, BasePlugin)

        print(plugin_class.name)

        plugin = plugin_class(**config[section_name])
        obj = plugin.run_with_progress_callback(obj, progress_callback)

        print()
        print()

def main():
    process_ini(['./countess.ini'] + sys.argv[1:])

if __name__ == '__main__':
    main()
