from countess.plugins.data_table import DataTablePlugin


def test_data_table():
    plugin = DataTablePlugin()
    assert plugin.num_files() == 1

    plugin.set_parameter("columns.0.name", "foo")
    plugin.set_parameter("columns.0.type", "string")
    plugin.set_parameter("columns.0.index", True)
    plugin.set_parameter("columns.1.name", "bar")
    plugin.set_parameter("columns.1.type", "integer")
    plugin.set_parameter("columns.2.name", "baz")
    plugin.set_parameter("columns.2.type", "number")
    plugin.set_parameter("rows.0.foo", "a")
    plugin.set_parameter("rows.0.bar", "1")
    plugin.set_parameter("rows.0.baz", "4.5")
    plugin.set_parameter("rows.1.foo", "b")
    plugin.set_parameter("rows.1.bar", "2")
    plugin.set_parameter("rows.1.baz", "5.6")
    plugin.set_parameter("rows.2.foo", "c")
    plugin.set_parameter("rows.2.bar", "3")
    plugin.set_parameter("rows.2.baz", "6.7")

    df = next(plugin.load_file(0, None))

    assert len(df) == 3
    assert df["bar"].iloc[0] == 1
    assert df["baz"].iloc[2] == 6.7
    assert df.index[0] == "a"

    plugin.set_parameter("columns.1.type", "number")
    plugin.set_parameter("columns.2.type", "integer")
    plugin.set_parameter("columns.3.name", "hello")
    plugin.set_parameter("columns.3.type", "string")
    plugin.fix_columns()
    plugin.set_parameter("rows.2.hello", "world")
    df = next(plugin.load_file(0, None))

    assert df["bar"].iloc[0] == 1.0
    assert df["baz"].iloc[1] == 5
    assert df["hello"].iloc[2] == "world"

    plugin.set_parameter("columns.3.type", "number")
    plugin.fix_columns()
    df = next(plugin.load_file(0, None))

    assert df["hello"].iloc[2] is None

    plugin.columns.del_row(1)
    plugin.fix_columns()

    df = next(plugin.load_file(0, None))
    assert "bar" not in df.columns
