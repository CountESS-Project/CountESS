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
    # set up a default stderr StreamHandler for logs
    logging_handler = logging.StreamHandler()

    # set up a QueueHandler/QueueListener to forward the logs between
    # processes and send them to the logging_handler
    logging_queue: multiprocessing.Queue = multiprocessing.Queue()
    logging_queue_handler = logging.handlers.QueueHandler(logging_queue)
    logging_queue_listener = logging.handlers.QueueListener(logging_queue, logging_handler)
    logging_queue_listener.start()

    # set up all loggers to be handled by the QueueHandler.
    root_logger = logging.getLogger()
    root_logger.addHandler(logging_queue_handler)
    root_logger.setLevel(logging.INFO)

    run(sys.argv[1:])

    # shut down the logging subsystem, in case this function is being
    # called as part of something else (eg: tests)
    root_logger.handlers.clear()
    logging_queue_listener.stop()
    logging_queue.close()


if __name__ == "__main__":
    main()  # pragma: no cover
