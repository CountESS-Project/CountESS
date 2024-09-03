import io
from unittest.mock import patch

from countess.core.config import export_config_graphviz, read_config_dict, write_config, write_config_node_string
from countess.core.parameters import IntegerParam
from countess.core.pipeline import PipelineGraph, PipelineNode
from countess.core.plugins import BasePlugin


class NothingPlugin(BasePlugin):
    version = "0"
    param = IntegerParam("param", 0)


def test_read_config_dict():
    pn = read_config_dict("node", ".", {"_module": __name__, "_class": "NothingPlugin", "foo": '"bar"'})
    assert pn
    assert isinstance(pn.plugin, NothingPlugin)
    assert list(pn.config[0]) == ["foo", "bar", "."]


def test_read_config_dict_no_plugin():
    pn = read_config_dict("node", ".", {"foo": '"bar"'})
    assert pn.plugin is None
    assert list(pn.config[0]) == ["foo", "bar", "."]


def test_write_config():
    pn = PipelineNode("node", plugin=NothingPlugin("node"))
    pn.set_config("foo", "bar", "baz")
    pg = PipelineGraph([pn])

    buf = io.StringIO()
    buf.close = lambda: None
    with patch("builtins.open", lambda *_, **__: buf):
        write_config(pg, "whatever")

    s = buf.getvalue()
    assert s.startswith("[node]")
    assert "foo = 'bar'" in s


def test_write_config_node_string():
    pn = PipelineNode("node", plugin=NothingPlugin("node"))
    pn.plugin.param = 12
    pn.notes = "hello"

    s = write_config_node_string(pn)

    assert "[node]" in s
    assert "_module = %s" % __name__ in s
    assert "_class = NothingPlugin" in s
    assert "_notes = hello" in s
    assert "param = 12" in s


def test_export_graphviz():
    pn1 = PipelineNode("node 1")
    pn2 = PipelineNode("node 2")
    pn3 = PipelineNode("node 3")
    pn3.add_parent(pn2)
    pn2.add_parent(pn1)
    pg = PipelineGraph([pn1, pn2, pn3])

    buf = io.StringIO()
    buf.close = lambda: None
    with patch("builtins.open", lambda *_, **__: buf):
        export_config_graphviz(pg, "filename")

    s = buf.getvalue()
    assert s.startswith("digraph {")
    assert '"node 1" -> "node 2";' in s
    assert '"node 2" -> "node 3";' in s
