import bz2
import gzip
import time

import duckdb
import pandas as pd

from countess.plugins.csv import LoadCsvPlugin, SaveCsvPlugin

ddbc = duckdb.connect()


def test_load_csv():
    plugin = LoadCsvPlugin()
    plugin.set_parameter("files.0.filename", "tests/input1.csv")
    output = plugin.execute(ddbc, None)
    assert output.columns == ["thing", "count"]
    assert len(output) == 4


def test_load_csv_gz():
    plugin = LoadCsvPlugin()
    plugin.set_parameter("files.0.filename", "tests/input1.csv.gz")
    output = plugin.execute(ddbc, None)
    assert output.columns == ["thing", "count"]
    assert len(output) == 4


def test_load_csv_bz2():
    plugin = LoadCsvPlugin()
    plugin.set_parameter("files.0.filename", "tests/input1.csv.bz2")
    output = plugin.execute(ddbc, None)
    assert output.columns == ["thing", "count"]
    assert len(output) == 4


def test_load_tsv():
    plugin = LoadCsvPlugin()
    plugin.set_parameter("files.0.filename", "tests/input1.tsv")
    plugin.set_parameter("delimiter", "TAB")
    output = plugin.execute(ddbc, None)
    assert output.columns == ["thing", "count"]
    assert len(output) == 4


def test_load_txt():
    plugin = LoadCsvPlugin()
    plugin.set_parameter("files.0.filename", "tests/input1.txt")
    plugin.set_parameter("delimiter", "SPACE")
    output = plugin.execute(ddbc, None)
    assert output.columns == ["thing", "count"]
    assert len(output) == 4


def test_filename_column():
    plugin = LoadCsvPlugin()
    plugin.set_parameter("files.0.filename", "tests/input1.csv")
    plugin.set_parameter("filename_column", True)
    output = plugin.execute(ddbc, None)
    assert "filename" in output.columns
    assert output.df()["filename"].iloc[1] == "tests/input1.csv"


df = pd.DataFrame([[1, 2.01, "three"], [4, 5.5, "six"], [7, 8.8, "nine"]], columns=["a", "b", "c"])
ddbc.from_df(df).create("n_0")


def test_save_csv(tmp_path):
    plugin = SaveCsvPlugin()
    plugin.set_parameter("header", True)
    # an absolute filename is written where it says, not in the output directory
    plugin.set_parameter("filename", str(tmp_path / "output1.csv"))
    plugin.set_parameter("metadata_header", "No")
    source = ddbc.table("n_0")
    plugin.prepare(ddbc, source)
    assert plugin.execute(ddbc, source) is None

    with open(tmp_path / "output1.csv", "r", encoding="utf-8") as fh:
        text = fh.read()
        assert text == '"a","b","c"\n1,2.01,"three"\n4,5.5,"six"\n7,8.8,"nine"\n'


def test_save_csv_metadata_header(tmp_path):
    plugin = SaveCsvPlugin()
    plugin.set_parameter("filename", str(tmp_path / "output1.csv"))
    source = ddbc.table("n_0")
    plugin.prepare(ddbc, source)
    plugin.execute(ddbc, source)

    lines = (tmp_path / "output1.csv").read_text().splitlines()
    assert lines[0].startswith("# CountESS ")
    assert "# node: CSV Save (CSV Save " in lines[5]
    assert [line for line in lines if not line.startswith("#")][0] == '"a","b","c"'

    load = LoadCsvPlugin()
    load.set_parameter("files.0.filename", str(tmp_path / "output1.csv"))
    assert load.execute(ddbc, None).columns == ["a", "b", "c"]


def test_save_csv_gz(tmp_path):
    plugin = SaveCsvPlugin()
    plugin.set_parameter("header", True)
    # an absolute filename is written where it says, not in the output directory
    plugin.set_parameter("filename", str(tmp_path / "output1.csv.gz"))
    plugin.set_parameter("metadata_header", "No")
    source = ddbc.table("n_0")
    plugin.prepare(ddbc, source)
    assert plugin.execute(ddbc, source) is None

    with gzip.open(tmp_path / "output1.csv.gz", "rt") as fh:
        text = fh.read()
        assert text == '"a","b","c"\n1,2.01,"three"\n4,5.5,"six"\n7,8.8,"nine"\n'


def test_save_csv_bz2(tmp_path):
    plugin = SaveCsvPlugin()
    plugin.set_parameter("header", True)
    # an absolute filename is written where it says, not in the output directory
    plugin.set_parameter("filename", str(tmp_path / "output1.csv.bz2"))
    plugin.set_parameter("metadata_header", "No")
    source = ddbc.table("n_0")
    plugin.prepare(ddbc, source)
    assert plugin.execute(ddbc, source) is None
    time.sleep(0.5)

    with bz2.open(tmp_path / "output1.csv.bz2", "rt") as fh:
        text = fh.read()
        assert text == '"a","b","c"\n1,2.01,"three"\n4,5.5,"six"\n7,8.8,"nine"\n'
