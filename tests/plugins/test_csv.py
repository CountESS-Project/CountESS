import bz2
import gzip

import pandas as pd

from countess.plugins.csv import LoadCsvPlugin, SaveCsvPlugin


def test_load_csv():
    plugin = LoadCsvPlugin()
    plugin.set_parameter("files.0.filename", "tests/input1.csv")
    output_df = next(plugin.load_file(0))
    assert list(output_df.columns) == ["thing", "count"]
    assert len(output_df) == 4


def test_load_csv_index():
    plugin = LoadCsvPlugin()
    plugin.set_parameter("files.0.filename", "tests/input1.csv")
    plugin.set_parameter("columns.0.name", "whatever")
    plugin.set_parameter("columns.0.type", "string")
    plugin.set_parameter("columns.0.index", True)
    plugin.set_parameter("columns.1.name", "stuff")
    plugin.set_parameter("columns.1.type", "integer")
    plugin.set_parameter("columns.1.index", False)
    output_df = next(plugin.load_file(0))
    assert output_df.index.name == "whatever"
    assert list(output_df.columns) == ["stuff"]
    assert len(output_df) == 4


def test_load_csv_gz():
    plugin = LoadCsvPlugin()
    plugin.set_parameter("files.0.filename", "tests/input1.csv.gz")
    output_df = next(plugin.load_file(0))
    assert list(output_df.columns) == ["thing", "count"]
    assert len(output_df) == 4


def test_load_csv_bz2():
    plugin = LoadCsvPlugin()
    plugin.set_parameter("files.0.filename", "tests/input1.csv.bz2")
    output_df = next(plugin.load_file(0))
    assert list(output_df.columns) == ["thing", "count"]
    assert len(output_df) == 4


def test_load_tsv():
    plugin = LoadCsvPlugin()
    plugin.set_parameter("files.0.filename", "tests/input1.tsv")
    plugin.set_parameter("delimiter", "TAB")
    output_df = next(plugin.load_file(0))
    assert list(output_df.columns) == ["thing", "count"]
    assert len(output_df) == 4


def test_load_txt():
    plugin = LoadCsvPlugin()
    plugin.set_parameter("files.0.filename", "tests/input1.txt")
    plugin.set_parameter("delimiter", "WHITESPACE")
    output_df = next(plugin.load_file(0))
    assert list(output_df.columns) == ["thing", "count"]
    assert len(output_df) == 4


def test_load_quoting_double():
    plugin = LoadCsvPlugin()
    plugin.set_parameter("files.0.filename", "tests/input2.csv")
    plugin.set_parameter("quoting", "Double-Quote")
    output_df = next(plugin.load_file(0))
    assert output_df["y"].iloc[0] == 'this line has a comma, and a double " quote'


def test_load_quoting_escaped():
    plugin = LoadCsvPlugin()
    plugin.set_parameter("files.0.filename", "tests/input2.csv")
    plugin.set_parameter("quoting", "Quote with Escape")
    output_df = next(plugin.load_file(0))
    assert output_df["y"].iloc[1] == 'this line has a comma, and an escaped " quote'


def test_load_comment():
    plugin = LoadCsvPlugin()
    plugin.set_parameter("files.0.filename", "tests/input2.csv")
    plugin.set_parameter("quoting", "Double-Quote")
    plugin.set_parameter("comment", "#")
    output_df = next(plugin.load_file(0))
    assert output_df["y"].iloc[2] == "this line has a comment "


def test_filename_column():
    plugin = LoadCsvPlugin()
    plugin.set_parameter("files.0.filename", "tests/input1.csv")
    plugin.set_parameter("filename_column", "filename")
    output_df = next(plugin.load_file(0))
    assert "filename" in output_df.columns
    assert output_df["filename"].iloc[1] == "input1"


df = pd.DataFrame([[1, 2, 3], [4, 5, 6], [7, 8, 9]], columns=["a", "b", "c"])


def test_save_csv():
    plugin = SaveCsvPlugin()
    plugin.set_parameter("header", True)
    plugin.set_parameter("filename", "tests/output1.csv")
    plugin.prepare(["test"], None)
    plugin.process(df, "test")
    plugin.finalize()

    with open("tests/output1.csv", "r", encoding="utf-8") as fh:
        text = fh.read()
        assert text == "a,b,c\n1,2,3\n4,5,6\n7,8,9\n"


def test_save_csv_gz():
    plugin = SaveCsvPlugin()
    plugin.set_parameter("header", True)
    plugin.set_parameter("filename", "tests/output1.csv.gz")
    plugin.prepare(["test"], None)
    plugin.process(df, "test")
    list(plugin.finalize())

    with gzip.open("tests/output1.csv.gz", "rt") as fh:
        text = fh.read()
        assert text == "a,b,c\n1,2,3\n4,5,6\n7,8,9\n"


def test_save_csv_bz2():
    plugin = SaveCsvPlugin()
    plugin.set_parameter("header", True)
    plugin.set_parameter("filename", "tests/output1.csv.bz2")
    plugin.prepare(["test"], None)
    plugin.process(df, "test")
    list(plugin.finalize())

    with bz2.open("tests/output1.csv.bz2", "rt") as fh:
        text = fh.read()
        assert text == "a,b,c\n1,2,3\n4,5,6\n7,8,9\n"


df2 = pd.DataFrame([[10, 11, 12]], columns=["a", "b", "d"])


def test_save_csv_multi(caplog):
    plugin = SaveCsvPlugin()
    plugin.set_parameter("header", True)
    plugin.set_parameter("filename", "tests/output2.csv")
    plugin.prepare(["test"], None)
    plugin.process(df, "test")
    plugin.process(df2, "test2")
    plugin.finalize()

    with open("tests/output2.csv", "r", encoding="utf-8") as fh:
        text = fh.read()
        assert text == "a,b,c\n1,2,3\n4,5,6\n7,8,9\n10,11,,12\n"
    assert "Added CSV Column" in caplog.text
