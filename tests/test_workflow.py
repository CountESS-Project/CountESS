import datetime
import gzip
import os

import pytest

from countess.core.cmd import run
from countess.core.config import read_config, write_config
from countess.core.output import count_leading_comment_lines, preview_order, save_preview_csv
from countess.core.pipeline import PipelineGraph, PipelineNode
from countess.core.workflow import (
    NodeLineage,
    RunContext,
    WorkflowError,
    WorkflowInfo,
    fill_output_dir,
    preview_filename,
)

STARTED = datetime.datetime(2026, 10, 19, 14, 5, 9)
NAMED = WorkflowInfo("FWD Saturation (pilot)", "1.2.0-rc.1")
UNNAMED = WorkflowInfo()


@pytest.mark.parametrize(
    "template,workflow,expected",
    [
        ("{date}_{time}_{workflow}_{version}", NAMED, "2026-10-19_14-05-09_fwd-saturation-pilot_v1-2-0-rc-1"),
        ("{date}_{time}_{workflow}_{version}", UNNAMED, "2026-10-19_14-05-09"),
        ("results/{workflow}/{date}", NAMED, "results/fwd-saturation-pilot/2026-10-19"),
        ("{version}-{date}", UNNAMED, "2026-10-19"),
        ("literal {{braces}}_{date}", NAMED, "literal {braces}_2026-10-19"),
        ("output", NAMED, "output"),
    ],
)
def test_fill_output_dir(template, workflow, expected):
    assert fill_output_dir(template, STARTED, workflow) == expected


@pytest.mark.parametrize("template", ["{nope}", "{node}", "{date:%Y}", "{date!r}", "{unclosed", " "])
def test_an_output_dir_with_a_field_it_does_not_have_is_refused(template):
    with pytest.raises(WorkflowError):
        WorkflowInfo(output_dir=template)


def test_fill_output_dir_refuses_a_template_that_leaves_no_directory():
    with pytest.raises(WorkflowError, match="no directory"):
        fill_output_dir("{workflow}-{version}", STARTED, UNNAMED)


def test_preview_filename_is_the_node_name_in_kebab_case():
    assert preview_filename("Collapse by Variant (2)") == "collapse-by-variant-2_preview.csv"


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


def test_workflow_info_from_strings_reads_empty_as_unset_or_default():
    assert WorkflowInfo.from_strings("  ", "", " ") == WorkflowInfo()


def test_a_run_context_puts_its_output_dir_in_its_base_dir(tmp_path):
    context = RunContext(NAMED, NodeLineage("Save", "CSV Save", "1.0"), STARTED, base_dir=str(tmp_path))

    assert context.output_dir() == str(tmp_path / "2026-10-19_14-05-09_fwd-saturation-pilot_v1-2-0-rc-1")


def test_metadata_lines_describe_the_run_and_the_nodes_upstream():
    context = RunContext(
        NAMED,
        NodeLineage("Save", "CSV Save", "1.0"),
        STARTED,
        upstream=(NodeLineage("Load", "CSV Load", "1.0"),),
    )

    lines = context.metadata_lines(STARTED + datetime.timedelta(minutes=1), "a note")

    assert all(line.startswith("# ") for line in lines)
    assert lines[1:] == [
        "# written: 2026-10-19 14:06:09",
        "# run started: 2026-10-19 14:05:09",
        "# workflow: FWD Saturation (pilot)",
        "# workflow version: 1.2.0-rc.1",
        "# node: Save (CSV Save 1.0)",
        "# upstream: Load (CSV Load 1.0)",
        "# a note",
    ]


def test_workflow_metadata_round_trips_through_a_config_file(tmp_path):
    graph = PipelineGraph()
    graph.workflow = WorkflowInfo("FWD saturation", "1.2.0", "out/{date}", metadata_header=False)
    path = tmp_path / "workflow.ini"

    write_config(graph, str(path))

    assert read_config([str(path)]).workflow == graph.workflow


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


def run_csv_pipeline(tmp_path, extra="", started=STARTED):
    (tmp_path / "input.csv").write_text("a,b\n1,2\n")
    (tmp_path / "pipeline.ini").write_text(CSV_PIPELINE + extra)
    read_config([str(tmp_path / "pipeline.ini")]).run(started=started)


def data_lines(path):
    return [line for line in path.read_text().splitlines() if not line.startswith("#")]


def test_csv_save_writes_results_csv_in_the_run_output_dir_with_a_metadata_header(tmp_path):
    run_csv_pipeline(tmp_path)

    written = tmp_path / "2026-10-19_14-05-09_fwd-test_v1-0-0" / "results.csv"
    lines = written.read_text().splitlines()
    assert "# workflow: FWD test" in lines
    assert "# node: Save Results (CSV Save " + lines[0].split()[-1] + ")" in lines
    assert any(line.startswith("# upstream: Load (CSV Load ") for line in lines)
    assert data_lines(written) == ['"a","b"', "1,2"]


def test_each_run_writes_to_a_directory_of_its_own(tmp_path):
    (tmp_path / "input.csv").write_text("a,b\n1,2\n")
    (tmp_path / "pipeline.ini").write_text(CSV_PIPELINE)

    run([str(tmp_path / "pipeline.ini")])

    [output_dir] = [p for p in tmp_path.iterdir() if p.is_dir()]
    assert output_dir.name.endswith("_fwd-test_v1-0-0")
    assert (output_dir / "results.csv").exists()


def test_csv_save_writes_an_absolute_filename_where_it_says(tmp_path):
    target = tmp_path / "elsewhere" / "absolute.csv.gz"
    run_csv_pipeline(tmp_path, f"filename = {str(target)!r}\n")

    assert gzip.open(target, "rt").read().splitlines()[-1] == "1,2"
    assert not (tmp_path / "2026-10-19_14-05-09_fwd-test_v1-0-0").exists()


@pytest.mark.parametrize(
    "workflow_setting,node_setting,expected",
    [("", "", True), ("metadata_header = false\n", "", False), ("metadata_header = false\n", "Yes", True)],
)
def test_the_metadata_header_follows_the_workflow_unless_the_node_says(
    tmp_path, workflow_setting, node_setting, expected
):
    extra = f"metadata_header = {node_setting!r}\n" if node_setting else ""
    pipeline = CSV_PIPELINE.replace("version = 1.0.0\n", "version = 1.0.0\n" + workflow_setting)
    (tmp_path / "input.csv").write_text("a,b\n1,2\n")
    (tmp_path / "pipeline.ini").write_text(pipeline + extra)

    read_config([str(tmp_path / "pipeline.ini")]).run(started=STARTED)

    text = (tmp_path / "2026-10-19_14-05-09_fwd-test_v1-0-0" / "results.csv").read_text()
    assert text.startswith("#") == expected


def test_a_relative_output_dir_is_relative_to_the_config_file(tmp_path, monkeypatch):
    pipeline = CSV_PIPELINE.replace("version = 1.0.0\n", "version = 1.0.0\noutput_dir = out/{version}\n")
    (tmp_path / "input.csv").write_text("a,b\n1,2\n")
    (tmp_path / "pipeline.ini").write_text(pipeline)
    monkeypatch.chdir(os.path.dirname(tmp_path))

    read_config([str(tmp_path / "pipeline.ini")]).run(started=STARTED)

    assert (tmp_path / "out" / "v1-0-0" / "results.csv").exists()


def test_csv_load_skips_a_metadata_header(tmp_path):
    run_csv_pipeline(tmp_path)
    written = tmp_path / "2026-10-19_14-05-09_fwd-test_v1-0-0" / "results.csv"
    pipeline = CSV_PIPELINE.replace("'input.csv'", repr(str(written)))
    (tmp_path / "again.ini").write_text(pipeline)

    graph = read_config([str(tmp_path / "again.ini")])
    graph.run(started=STARTED + datetime.timedelta(seconds=1))

    again = tmp_path / "2026-10-19_14-05-10_fwd-test_v1-0-0" / "results.csv"
    assert data_lines(again) == ['"a","b"', "1,2"]


@pytest.mark.parametrize("suffix", ["", ".gz", ".bz2", ".xz"])
def test_count_leading_comment_lines(tmp_path, suffix):
    from countess.core.output import open_text  # pylint: disable=import-outside-toplevel

    path = str(tmp_path / f"f.csv{suffix}")
    with open_text(path, "w") as fh:
        fh.write("# one\n# two\na,b\n# not a header\n")

    assert count_leading_comment_lines(path) == 2


def test_preview_order_sorts_by_the_chosen_column_then_the_rest():
    assert preview_order(["a", "b", "c"], 1, True) == '"b" DESC, "a" DESC, "c" DESC'
    assert preview_order([], 0, False) is None


def preview_graph(tmp_path):
    (tmp_path / "input.csv").write_text("a,b\n1,2\n3,1\n")
    (tmp_path / "pipeline.ini").write_text(CSV_PIPELINE)
    graph = read_config([str(tmp_path / "pipeline.ini")])
    graph.session_started = STARTED
    load = graph.find_node("Load")
    load.sort_column, load.sort_descending = 1, False
    graph.start_thread()
    graph.thread.join()
    return graph, load


def test_save_preview_csv_writes_the_preview_in_its_order_and_refuses_to_overwrite(tmp_path):
    graph, load = preview_graph(tmp_path)

    path = save_preview_csv(graph, load)

    assert path == str(tmp_path / "2026-10-19_14-05-09_fwd-test_v1-0-0" / "load_preview.csv")
    text = open(path, encoding="utf-8").read()
    assert "# preview: the 2 rows the node's preview held" in text
    assert [line for line in text.splitlines() if not line.startswith("#")] == ["a,b", "3,1", "1,2"]
    with pytest.raises(FileExistsError):
        save_preview_csv(graph, load)
    assert save_preview_csv(graph, load, overwrite=True) == path
