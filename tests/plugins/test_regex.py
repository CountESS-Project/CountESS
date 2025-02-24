import pandas as pd

from countess.plugins.regex import RegexToolPlugin


def test_tool():
    plugin = RegexToolPlugin()
    plugin.set_parameter("regex", ".*?([a]+).*")
    plugin.prepare("fake", None)
    plugin.set_parameter("column", "stuff")
    plugin.set_parameter("output.0.name", "foo")

    assert plugin.transform({"stuff": "hello"}) == {"stuff": "hello"}
    assert plugin.transform({"stuff": "backwards"})["foo"] == "a"
    assert plugin.transform({"stuff": "noaardvark"})["foo"] == "aa"

    plugin.set_parameter("drop_unmatch", True)
    assert plugin.transform({"stuff": "hello"}) is None
