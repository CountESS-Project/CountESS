import pandas as pd
import pytest

from countess.core.logger import MultiprocessLogger
from countess.plugins.mutagenize import MutagenizePlugin

logger = MultiprocessLogger()


def test_mutagenize_mutate():
    plugin = MutagenizePlugin()
    plugin.set_parameter("sequence", "GATTACA")
    plugin.set_parameter("mutate", True)

    output_df = next(plugin.load_file(0, logger))
    output = list(output_df["sequence"])

    assert len(output) == 21


def test_mutagenize_insert():
    plugin = MutagenizePlugin()
    plugin.set_parameter("sequence", "GATTACA")
    plugin.set_parameter("mutate", False)
    plugin.set_parameter("insert", True)

    output_df = next(plugin.load_file(0, logger))
    output = list(output_df["sequence"])

    assert len(output) == 32


def test_mutagenize_insert3():
    plugin = MutagenizePlugin()
    plugin.set_parameter("sequence", "GATTACA")
    plugin.set_parameter("mutate", False)
    plugin.set_parameter("ins3", True)

    output_df = next(plugin.load_file(0, logger))
    output = list(output_df["sequence"])

    assert len(output) == 512


def test_mutagenize_insert_dedup():
    plugin = MutagenizePlugin()
    plugin.set_parameter("sequence", "GATTACA")
    plugin.set_parameter("mutate", False)
    plugin.set_parameter("insert", True)
    plugin.set_parameter("remove", True)

    output_df = next(plugin.load_file(0, logger))

    assert len(output_df) == 25


def test_mutagenize_insert3_dedup():
    plugin = MutagenizePlugin()
    plugin.set_parameter("sequence", "GATTACA")
    plugin.set_parameter("mutate", False)
    plugin.set_parameter("ins3", True)
    plugin.set_parameter("remove", True)

    output_df = next(plugin.load_file(0, logger))

    assert len(output_df) == 400


def test_mutagenize_delete():
    plugin = MutagenizePlugin()
    plugin.set_parameter("sequence", "GATTACA")
    plugin.set_parameter("mutate", False)
    plugin.set_parameter("delete", True)

    output_df = next(plugin.load_file(0, logger))
    output = list(output_df["sequence"])

    assert len(output) == 7


def test_mutagenize_del3():
    plugin = MutagenizePlugin()
    plugin.set_parameter("sequence", "GATTACA")
    plugin.set_parameter("mutate", False)
    plugin.set_parameter("del3", True)

    output_df = next(plugin.load_file(0, logger))
    output = list(output_df["sequence"])

    assert len(output) == 5


def test_mutagenize_delete_dedup():
    plugin = MutagenizePlugin()
    plugin.set_parameter("sequence", "GATTACA")
    plugin.set_parameter("mutate", False)
    plugin.set_parameter("delete", True)
    plugin.set_parameter("remove", True)

    output_df = next(plugin.load_file(0, logger))

    assert len(output_df) == 6


def test_mutagenize_del3_dedup():
    plugin = MutagenizePlugin()
    plugin.set_parameter("sequence", "GATTACA")
    plugin.set_parameter("mutate", False)
    plugin.set_parameter("del3", True)
    plugin.set_parameter("remove", True)

    output_df = next(plugin.load_file(0, logger))

    assert len(output_df) == 4


def test_num_files():
    plugin = MutagenizePlugin()
    assert plugin.num_files() == 1
