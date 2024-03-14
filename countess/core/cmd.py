import sys
import time

from .config import read_config
from .logger import ConsoleLogger

start_time = time.time()


def process_ini(config_filename):
    logger = ConsoleLogger()

    graph = read_config(
        config_filename,
        logger=logger,
    )
    graph.run(logger)


def run(argv):
    for config_filename in argv:
        process_ini(config_filename)


def main():
    run(sys.argv[1:])


if __name__ == "__main__":
    main()  # pragma: no cover
