import ast
import getopt
import logging
import logging.handlers
import multiprocessing
import re
import sys

from countess import VERSION
from countess.core.config import read_config

logger = logging.getLogger(__name__)


def run(args: list[str]) -> None:
    options, args = getopt.getopt(args, "", ["version", "set=", "log="])
    config: list[tuple[str, str, str]] = []

    for opt_key, opt_val in options:
        if opt_key == "--version":
            print(f"CountESS {VERSION}")  # pylint: disable=bad-builtin
        elif opt_key == "--set":
            if m := re.match(r"([^.]+)\.([^=]+)=(.*)", opt_val):
                config.append((m.group(1), m.group(2), m.group(3)))
            else:
                logger.warning("Bad --set option: %s", opt_val)
        elif opt_key == "--log":
            try:
                if re.match(r"\d+$", opt_val):
                    logging.getLogger().setLevel(int(opt_val))
                else:
                    logging.getLogger().setLevel(opt_val.upper())
            except ValueError:
                logger.error("Bad --log level: %s", opt_val)
        else:
            logger.warning("Unknown option %s", opt_key)

    for filename in args:
        graph = read_config(filename)
        for node_name, config_key, config_val in config:
            node = graph.find_node(node_name)
            if node:
                try:
                    node.set_config(config_key, ast.literal_eval(config_val), ".")
                except (TypeError, KeyError, ValueError):
                    logger.warning("Bad --set option: %s.%s=%s", node_name, config_key, config_val)
            else:
                logger.warning("Bad --set node name: %s", node_name)

        graph.run()


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
