from countess.plugins.data_table import DataTablePlugin

import duckdb

def test_data_table():
    plugin = DataTablePlugin()

    plugin.set_parameter("columns.0.name", "foo")
    plugin.set_parameter("columns.0.type", "STRING")
    plugin.set_parameter("columns.1.name", "bar")
    plugin.set_parameter("columns.1.type", "INTEGER")
    plugin.set_parameter("columns.2.name", "baz")
    plugin.set_parameter("columns.2.type", "FLOAT")
    plugin.set_parameter("rows.0.foo", "a")
    plugin.set_parameter("rows.0.bar", "1")
    plugin.set_parameter("rows.0.baz", "4.5")
    plugin.set_parameter("rows.1.foo", "b")
    plugin.set_parameter("rows.1.bar", "2")
    plugin.set_parameter("rows.1.baz", "5.6")
    plugin.set_parameter("rows.2.foo", "c")
    plugin.set_parameter("rows.2.bar", "3")
    plugin.set_parameter("rows.2.baz", "6.7")

    output = plugin.execute(duckdb, None)
    assert output.fetchall() == [('a', 1, 4.5), ('b', 2, 5.6), ('c', 3, 6.7)]

    plugin.set_parameter("columns.1.type", "FLOAT")
    plugin.set_parameter("columns.2.type", "INTEGER")
    plugin.set_parameter("columns.3.name", "hello")
    plugin.set_parameter("columns.3.type", "STRING")
    plugin.fix_columns()
    plugin.set_parameter("rows.2.hello", "world")

    output = plugin.execute(duckdb, None)
    assert output.fetchall() == [('a', 1.0, 4, ''), ('b', 2.0, 5, ''), ('c', 3.0, 6, 'world')]

    plugin.set_parameter("columns.3.type", "FLOAT")
    plugin.fix_columns()

    output = plugin.execute(duckdb, None)
    assert output.fetchall() == [('a', 1.0, 4, None), ('b', 2.0, 5, None), ('c', 3.0, 6, None)]

    plugin.columns.del_row(1)
    plugin.fix_columns()

    output = plugin.execute(duckdb, None)
    assert output.fetchall() == [('a', 4, None), ('b', 5, None), ('c', 6, None)]
