import logging
import logging.handlers
import multiprocessing
import sys
import time

from .config import read_config

logging_queue: multiprocessing.Queue = multiprocessing.Queue()
logging.getLogger().addHandler(logging.handlers.QueueHandler(logging_queue))
logging.getLogger().setLevel(logging.INFO)

logging.handlers.QueueListener(logging_queue, logging.StreamHandler())

start_time = time.time()


def process_ini(config_filename):
    graph = read_config(config_filename)
    graph.run()


def run(argv):
    for config_filename in argv:
        process_ini(config_filename)


def main():
    run(sys.argv[1:])


if __name__ == "__main__":
    main()  # pragma: no cover
