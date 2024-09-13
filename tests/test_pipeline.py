import pytest

from countess.core.parameters import IntegerParam
from countess.core.pipeline import PipelineGraph, PipelineNode
from countess.core.plugins import ProcessPlugin


@pytest.fixture(name="pg")
def fixture_pg():
    pn0 = PipelineNode("node")
    pn1 = PipelineNode("node")
    pn2 = PipelineNode("node")
    pn3 = PipelineNode("node")
    pn4 = PipelineNode("node")

    pg = PipelineGraph([pn0, pn1, pn2, pn3, pn4])

    pn4.add_parent(pn2)
    pn4.add_parent(pn3)
    pn3.add_parent(pn1)
    pn2.add_parent(pn1)
    pn1.add_parent(pn0)
    pn1.add_parent(pn0)

    return pg


def test_ancestor_descendant(pg):
    pns = list(pg.traverse_nodes())
    for pn in pns[1:]:
        assert pns[0].is_ancestor_of(pn)
        assert not pn.is_ancestor_of(pns[0])

    for pn in pns[:-1]:
        assert pns[-1].is_descendant_of(pn)
        assert not pn.is_descendant_of(pns[-1])


def test_pipeline_graph_tidy(pg):
    pg.tidy()

    pns = list(pg.traverse_nodes())

    # check that all nodes have different positions
    assert len(set(pn.position for pn in pns)) == len(pns)

    # check that first coordinate is monotonic increasing
    xs = [pn.position[0] for pn in pns]
    assert sorted(xs) == xs


def test_pipeline_del_node(pg):
    pns = list(pg.traverse_nodes())
    pg.del_node(pns[2])

    assert not pns[2].is_descendant_of(pns[0])
    assert not pns[2].is_ancestor_of(pns[-1])


def test_pipeline_del_parent(pg):
    pns = list(pg.traverse_nodes())
    pns[2].del_parent(pns[1])

    assert not pns[1].is_ancestor_of(pns[2])
    assert pns[2].is_ancestor_of(pns[-1])


def test_pipeline_graph_reset_node_name(pg):
    pns = list(pg.traverse_nodes())
    pg.reset_node_name(pns[1])
    assert pns[1].name == "node 2"

    pg.reset_node_name(pns[3])
    assert pns[3].name == "node 3"


def test_pipeline_graph_reset_node_names(pg):
    pg.reset_node_names()
    names = [pn.name for pn in pg.traverse_nodes()]
    assert sorted(set(names)) == names

    pn = PipelineNode("node")
    pg.add_node(pn)
    assert pn.name == "node 5"


def test_pg_reset(pg):
    pg.reset()

    assert all(pn.result is None for pn in pg.traverse_nodes())
    assert all(pn.is_dirty for pn in pg.traverse_nodes())


class DoesNothingPlugin(ProcessPlugin):
    version = "0"
    param = IntegerParam("param", 0)

    def process(self, data, source):
        yield data

    def finished(self, source):
        yield 107


def test_plugin_config(caplog):
    dnp = DoesNothingPlugin()
    dnn = PipelineNode("node", plugin=dnp)
    dnn.set_config("param", "1", ".")
    dnn.set_config("noparam", "whatever", ".")
    dnn.load_config()

    assert "noparam=whatever" in caplog.text
    assert dnp.param == 1


def test_noplugin_prerun():
    pn = PipelineNode("node")

    with pytest.raises(AssertionError):
        pn.load_config()

    pn.prerun()


def test_mark_dirty():
    pn1 = PipelineNode("node1", plugin=DoesNothingPlugin())
    pn2 = PipelineNode("node2", plugin=DoesNothingPlugin())
    pn2.add_parent(pn1)

    pn2.prerun()

    assert not pn1.is_dirty
    assert not pn2.is_dirty

    pn1.configure_plugin("param", 2)

    assert pn1.is_dirty
    assert pn2.is_dirty
