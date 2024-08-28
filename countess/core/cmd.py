import logging
import logging.handlers
import multiprocessing
import sys

from .config import read_config


def process_ini(config_filename) -> None:
    graph = read_config(config_filename)
    graph.run()


def run(argv) -> None:
    for config_filename in argv:
        process_ini(config_filename)


def main() -> None:
    logging_queue: multiprocessing.Queue = multiprocessing.Queue()
    logging.getLogger().addHandler(logging.handlers.QueueHandler(logging_queue))
    logging.getLogger().setLevel(logging.INFO)
    logging_handler = logging.handlers.QueueListener(logging_queue, logging.StreamHandler())
    logging_handler.start()

    run(sys.argv[1:])

    # tidy up just in case we're not just about to exit
    logging.getLogger().handlers.clear()
    logging_handler.stop()
    logging_queue.close()


if __name__ == "__main__":
    main()  # pragma: no cover
