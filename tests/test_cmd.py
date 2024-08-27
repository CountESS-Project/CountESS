import csv
from unittest.mock import patch

import pytest

import countess.core.cmd
from countess.core.cmd import main, run

expected_output = """"thing","foo","bar","baz","qux","number","zz"
"bar",10,2,1,4,232,0.08620689655172414
"baz",11,3,2,1,565,0.0584070796460177
"qux",12,9,8,7,999,0.10810810810810811
"""


@pytest.mark.slow
def test_command_invocation():
    run(["countess_cmd", "tests/simple.ini"])

    with open("tests/output.csv", "r", encoding="utf-8") as fh:
        output = fh.read()
        assert output == expected_output


def test_main():
    with patch.object(countess.core.cmd, "run") as p:
        main()
        p.assert_called_once()
