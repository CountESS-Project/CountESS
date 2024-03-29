from countess.core.logger import MultiprocessLogger
from countess.plugins.data_table import DataTablePlugin

logger = MultiprocessLogger()


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
    plugin.set_parameter("rows.0.0", "a")
    plugin.set_parameter("rows.0.1", "1")
    plugin.set_parameter("rows.0.2", "4.5")
    plugin.set_parameter("rows.1.0", "b")
    plugin.set_parameter("rows.1.1", "2")
    plugin.set_parameter("rows.1.2", "5.6")
    plugin.set_parameter("rows.2.0", "c")
    plugin.set_parameter("rows.2.1", "3")
    plugin.set_parameter("rows.2.2", "6.7")

    df = next(plugin.load_file(0, logger, None))

    assert len(df) == 3
    assert df["bar"].iloc[0] == 1
    assert df["baz"].iloc[2] == 6.7
    assert df.index[0] == "a"

    plugin.set_parameter("columns.1.type", "number")
    plugin.set_parameter("columns.2.type", "integer")
    plugin.set_parameter("columns.3.name", "hello")
    plugin.set_parameter("columns.3.type", "string")
    plugin.fix_columns()
    plugin.set_parameter("rows.2.3", "world")
    df = next(plugin.load_file(0, logger, None))

    assert df["bar"].iloc[0] == 1.0
    assert df["baz"].iloc[1] == 5
    assert df["hello"].iloc[2] == "world"

    plugin.set_parameter("columns.3.type", "number")
    plugin.fix_columns()
    df = next(plugin.load_file(0, logger, None))

    assert df["hello"].iloc[2] is None

    plugin.parameters["columns"].del_row(1)
    plugin.fix_columns()

    df = next(plugin.load_file(0, logger, None))
    assert "bar" not in df.columns
