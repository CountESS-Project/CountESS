from countess.plugins.csv import LoadCsvPlugin


def test_load_csv():
    plugin = LoadCsvPlugin()
    plugin.set_parameter("files.0.filename", "tests/input1.csv")
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
