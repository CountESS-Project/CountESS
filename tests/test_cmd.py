import csv
import sys
import time
from unittest.mock import patch

import pytest

import countess.core.cmd
from countess import VERSION
from countess.core.cmd import configure_graphs, main, run

expected_output = """"thing","foo","bar","baz","qux","number","zz"
"bar",10,2,1,4,232,0.08620689655172414
"baz",11,3,2,1,565,0.0584070796460177
"qux",12,9,8,7,999,0.10810810810810811
"""


@pytest.mark.slow
def test_command_invocation():
    run(["tests/simple.ini"])
    time.sleep(0.5)
    with open("tests/output.csv", "r", encoding="utf-8") as fh:
        output = fh.read()
        assert output == expected_output


def test_main():
    with patch.object(countess.core.cmd, "run") as p:
        main()
        p.assert_called_once()


def test_run_version(capsys):
    with pytest.raises(SystemExit) as excinfo:
        run(["--version"])
    assert excinfo.value.code == 0
    out, _ = capsys.readouterr()
    assert f"CountESS {VERSION}" in out


def test_run_help(capsys):
    with pytest.raises(SystemExit) as excinfo:
        run(["--help"])
    assert excinfo.value.code == 0
    out, _ = capsys.readouterr()
    assert out.startswith("usage:")


def test_run_log(caplog):
    run(["--log", "debug"])
    assert "Log level set to DEBUG" in caplog.text


def test_run_log_bad(caplog):
    run(["--log", "FNORD"])
    assert "Bad --log level" in caplog.text


def test_run_bad_option(caplog):
    with pytest.raises(SystemExit) as excinfo:
        run(["--bad-option"])
    assert excinfo.value.code == 1
    assert "option --bad-option" in caplog.text


def test_missing_config():
    with pytest.raises(SystemExit) as excinfo:
        run(["doesnt_exist.ini"])
    assert excinfo.value.code == 2


def test_override_config():
    pg = next(
        configure_graphs(
            ["--set", "DataTable.columns.0.name='zzz'", "--set", "DataTable.rows.1.bbb=107", "tests/test_datatable.ini"]
        )
    )
    pg.run()
    plugin = pg.find_node("DataTable").plugin
    assert plugin.columns[0].name == "zzz"
    assert plugin.rows[1].bbb == 107


def test_override_config_no_node(caplog):
    next(configure_graphs(["--set", "DataTuble.columns.0.name='zzz'", "tests/test_datatable.ini"]))
    assert "Bad --set node name" in caplog.text


def test_override_config_bad_key(caplog):
    pg = next(configure_graphs(["--set", "DataTable.coulombs.0.name='zzz'", "tests/test_datatable.ini"]))
    pg.run()
    assert "Not Found" in caplog.text


def test_override_config_bad_value():
    # XXX this probably should actually throw an error
    pg = next(configure_graphs(["--set", "DataTable.rows.1.bbb='zzz'", "tests/test_datatable.ini"]))
    pg.run()
    plugin = pg.find_node("DataTable").plugin
    assert plugin.rows[1].bbb.value is None
