import configparser
import importlib
import re
import sys

import dask.dataframe as dd
import pandas as pd

from .pipeline import Pipeline
from .plugins import BasePlugin
from functools import partial

def progress_callback(n, a, b, s="Running"):
    if b > 0:
        print(
            "%-20s %4d/%4d %-50s" % (s, a, b, "*" * int(a * 50 / b)),
            end="\r",
            flush=True,
        )
    else:
        print("%-20s %d" % (s, a), end="\r", flush=True)


def process_ini(config_filenames):
    config = configparser.ConfigParser(strict=False)
    config.read(config_filenames)

    pipeline = Pipeline()
    for section_name in config.sections():
        pipeline.load_plugin_config(section_name, config[section_name])

    for num, item in enumerate(pipeline.items):

        print(f"{num+1}: {item.plugin.name}\n")

        pipeline.run(num, partial(progress_callback, num))

        if isinstance(item.result, dd.DataFrame):
            print(item.result.compute())
        else:
            print(item.result)
        
        print("\n==========\n")

def main():
    process_ini(["./countess.ini"] + sys.argv[1:])


if __name__ == "__main__":
    main()
