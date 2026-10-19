import datetime
import re

import pytest

from countess.core.cmd import run
from countess.core.config import read_config, write_config
from countess.core.pipeline import PipelineGraph, PipelineNode
from countess.core.workflow import RunContext, WorkflowError, WorkflowInfo, default_filename, fill_filename

STARTED = datetime.datetime(2026, 10, 19, 14, 5, 9)
NAMED = RunContext(WorkflowInfo("FWD Saturation (pilot)", "1.2.0-rc.1"), "Save Annotated", STARTED)
UNNAMED = RunContext(WorkflowInfo(), "Save Annotated", STARTED)


@pytest.mark.parametrize(
    "template,context,expected",
    [
        (
            "{date}_{workflow}_{version}_{node}.csv",
            NAMED,
            "2026-10-19_fwd-saturation-pilot_v1-2-0-rc-1_save-annotated.csv",
        ),
        ("{date}_{workflow}_{version}_{node}.csv", UNNAMED, "2026-10-19_save-annotated.csv"),
        ("out/{workflow}_{node}.csv", UNNAMED, "out/save-annotated.csv"),
        ("{date}T{time}.tsv.gz", NAMED, "2026-10-19T14-05-09.tsv.gz"),
        ("literal {{braces}}_{node}.csv", NAMED, "literal {braces}_save-annotated.csv"),
        ("my--file_{version}.csv", UNNAMED, "my--file.csv"),
        ("results.csv", NAMED, "results.csv"),
    ],
)
def test_fill_filename(template, context, expected):
    assert fill_filename(template, context) == expected


@pytest.mark.parametrize("template", ["{nope}.csv", "{date:%Y}.csv", "{date!r}.csv", "{unclosed.csv"])
def test_fill_filename_refuses_a_field_it_does_not_have(template):
    with pytest.raises(WorkflowError):
        fill_filename(template, NAMED)


def test_fill_filename_refuses_a_template_that_leaves_no_name():
    with pytest.raises(WorkflowError, match="no file name"):
        fill_filename("{workflow}-{version}.csv", UNNAMED)


def test_default_filename_adds_a_suffix():
    assert default_filename(NAMED, "log", "txt") == "2026-10-19_fwd-saturation-pilot_v1-2-0-rc-1_save-annotated-log.txt"


@pytest.mark.parametrize(
    "name,version",
    [
        ("x", "1.2"),
        ("x", "v1.2.3"),
        ("x", "01.2.3"),
        ("   ", None),
        ("two\nlines", None),
        ("!!!", None),
        ("x" * 101, None),
    ],
)
def test_workflow_info_refuses_what_cannot_be_used(name, version):
    with pytest.raises(WorkflowError):
        WorkflowInfo(name, version)


def test_workflow_info_from_strings_reads_empty_as_unset():
    assert WorkflowInfo.from_strings("  ", "") == WorkflowInfo()


def test_workflow_metadata_round_trips_through_a_config_file(tmp_path):
    graph = PipelineGraph()
    graph.workflow = WorkflowInfo("FWD saturation", "1.2.0")
    path = tmp_path / "workflow.ini"

    write_config(graph, str(path))

    assert read_config([str(path)]).workflow == WorkflowInfo("FWD saturation", "1.2.0")


def test_a_config_file_without_metadata_has_none(tmp_path):
    path = tmp_path / "plain.ini"
    write_config(PipelineGraph(), str(path))

    assert "_workflow" not in path.read_text()
    assert read_config([str(path)]).workflow == WorkflowInfo()


def test_no_node_takes_the_name_of_the_metadata_section():
    graph = PipelineGraph()
    node = PipelineNode(name="_workflow")

    graph.add_node(node)

    assert node.name != "_workflow"


CSV_PIPELINE = """
[_workflow]
name = FWD test
version = 1.0.0

[Load]
_module = countess.plugins.csv
_class = LoadCsvPlugin
files.0.filename = 'input.csv'

[Save Results]
_module = countess.plugins.csv
_class = SaveCsvPlugin
_parent.0 = Load
"""


def test_csv_save_names_its_file_from_the_workflow_by_default(tmp_path):
    (tmp_path / "input.csv").write_text("a,b\n1,2\n")
    (tmp_path / "pipeline.ini").write_text(CSV_PIPELINE)

    run([str(tmp_path / "pipeline.ini")])

    [written] = [p.name for p in tmp_path.glob("*.csv") if p.name != "input.csv"]
    assert re.fullmatch(r"\d{4}-\d{2}-\d{2}_fwd-test_v1-0-0_save-results\.csv", written)


def test_csv_save_fills_a_template_given_as_its_filename(tmp_path):
    (tmp_path / "input.csv").write_text("a,b\n1,2\n")
    (tmp_path / "pipeline.ini").write_text(CSV_PIPELINE + "filename = 'out/{workflow}_{version}.csv'\n")

    run([str(tmp_path / "pipeline.ini")])

    assert (tmp_path / "out" / "fwd-test_v1-0-0.csv").read_text().splitlines() == ['"a","b"', "1,2"]


def test_a_run_names_its_files_from_the_start_time_it_is_given(tmp_path):
    (tmp_path / "input.csv").write_text("a,b\n1,2\n")
    (tmp_path / "pipeline.ini").write_text(CSV_PIPELINE + "filename = '{date}T{time}.csv'\n")

    read_config([str(tmp_path / "pipeline.ini")]).run(started=STARTED)

    assert (tmp_path / "2026-10-19T14-05-09.csv").exists()
