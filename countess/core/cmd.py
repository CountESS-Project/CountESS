import sys
import time

from .config import read_config

# import dask
# dask.config.set(scheduler='processes')

start_time = time.time()


def progress_callback(name, a, b, s=""):
    if a == 1 and b != 1:
        print()
    elapsed = time.time() - start_time
    print(f"{name:40s} {a:4d}/{b:4d} {elapsed:9.3f} {s}", end="\r", flush=True)


def output_callback(output):
    print()
    print(output)


def process_ini(config_filename):
    graph = read_config(
        config_filename,
        progress_callback=progress_callback,
        output_callback=output_callback,
    )
    graph.run(progress_callback, output_callback)


def main():
    for config_filename in sys.argv[1:]:
        process_ini(config_filename)


if __name__ == "__main__":
    main()
