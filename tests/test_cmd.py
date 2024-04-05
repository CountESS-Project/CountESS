import csv

import pytest

from countess.core.cmd import run as cmd_run

expected_output = """"thing","foo","bar","baz","qux","number","zz"
"bar",10,2,1,4,232,0.08620689655172414
"baz",11,3,2,1,565,0.0584070796460177
"qux",12,9,8,7,999,0.10810810810810811
"""


@pytest.mark.slow
def test_command_invocation():
    cmd_run(["countess_cmd", "tests/simple.ini"])

    with open("tests/output.csv", "r") as fh:
        output = fh.read()
        assert output == expected_output
