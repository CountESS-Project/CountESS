import pandas as pd

from countess.plugins.regex import RegexReaderPlugin, RegexToolPlugin


def test_tool():
    plugin = RegexToolPlugin()
    plugin.set_parameter("column", "stuff")
    plugin.set_parameter("regex", ".*?([a]+).*")
    plugin.set_parameter("output.0.name", "foo")
    plugin.prepare(["fake"])

    input_df = pd.DataFrame([["hello"], ["backwards"], ["noaardvark"]], columns=["stuff"])
    output_df = plugin.process_dataframe(input_df)

    assert output_df is not None
    assert "foo" in output_df.columns
    assert output_df.foo[0] is None
    assert output_df.foo[1] == "a"
    assert output_df.foo[2] == "aa"


def test_reader():
    plugin = RegexReaderPlugin()
    plugin.set_parameter("files.0.filename", "tests/input2.txt")
    plugin.set_parameter("regex", r"(\w+) (\d+)\.(\d+)-(\d+)_(\d+)")
    plugin.set_parameter("output.0.name", "thing")
    plugin.set_parameter("output.1.name", "foo")
    plugin.set_parameter("output.1.datatype", "integer")
    plugin.set_parameter("output.2.name", "bar")
    plugin.set_parameter("output.2.datatype", "integer")
    plugin.set_parameter("output.3.name", "baz")
    plugin.set_parameter("output.3.datatype", "integer")
    plugin.set_parameter("output.4.name", "qux")
    plugin.set_parameter("output.4.datatype", "integer")
    plugin.prepare([])

    output_df = next(plugin.load_file(0))

    assert list(output_df.columns) == ["thing", "foo", "bar", "baz", "qux"]
    assert list(output_df["thing"]) == ["foo", "bar", "baz", "qux"]
    assert list(output_df["foo"]) == [9, 10, 11, 12]
    assert list(output_df["bar"]) == [7, 2, 3, 9]
    assert list(output_df["baz"]) == [6, 1, 2, 8]
    assert list(output_df["qux"]) == [3, 4, 1, 7]
