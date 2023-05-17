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


def main():
    for config_filename in sys.argv[1:]:
        process_ini(config_filename)


if __name__ == "__main__":
    main()
