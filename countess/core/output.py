"""Writing CSV files: the CSV Save plugin's, and a node's preview saved from a GUI.

A CSV file CountESS writes can start with a metadata header, lines starting '#', saying what wrote it:
the CountESS version, when, the workflow's name and version, and the nodes the data came through.
CSV Load skips such lines, and pandas (``comment='#'``) and R (``comment.char = "#"``) can be told to.
"""

import bz2
import csv
import datetime
import gzip
import logging
import lzma
import os
import shutil
from io import TextIOBase
from typing import IO, Optional, Sequence, Union, cast

from duckdb import DuckDBPyRelation

from countess.core.pipeline import PipelineGraph, PipelineNode, PipelineNodeStatus
from countess.core.workflow import preview_filename
from countess.utils.duckdb import duckdb_escape_identifier

logger = logging.getLogger(__name__)


def open_text(filename: str, mode: str) -> IO[str]:
    """Opens a text file, compressed as its name says (.gz, .bz2, .xz)."""
    if filename.endswith(".gz"):
        return cast(IO[str], gzip.open(filename, mode + "t", encoding="utf-8"))
    if filename.endswith(".bz2"):
        return cast(IO[str], bz2.open(filename, mode + "t", encoding="utf-8"))
    if filename.endswith(".xz"):
        return cast(IO[str], lzma.open(filename, mode + "t", encoding="utf-8"))
    return open(filename, mode, encoding="utf-8")  # pylint: disable=consider-using-with


def count_leading_comment_lines(filename: str) -> int:
    """The number of lines starting '#' at the top of a (possibly compressed) text file: its
    metadata header, which a reader should skip."""
    count = 0
    with open_text(filename, "r") as handle:
        for line in handle:
            if not line.startswith("#"):
                break
            count += 1
    return count


def write_to_csv(source: DuckDBPyRelation, filename: str, sep: str, quoting: int) -> None:
    """Writes ``source`` to ``filename`` as delimited text, compressed as its name says."""
    logger.debug("writing file %s", filename)

    def _write(fh: Union[TextIOBase, gzip.GzipFile, bz2.BZ2File, lzma.LZMAFile], **kwargs):
        # the type check supression is because the first parameter to
        # to_csv is:
        #     path_or_buf: str | PathLike[str] | WriteBuffer[bytes] | WriteBuffer[str]
        # but bz2.BZFile|lzma.LZMAFile don't seem to be compatible.
        chunk = source.fetch_df_chunk()
        chunk.to_csv(fh, header=True, index=False, **kwargs)  # type: ignore
        while len(chunk) > 0:
            chunk = source.fetch_df_chunk()
            chunk.to_csv(fh, header=False, index=False, **kwargs)  # type: ignore

    # If it's one of the formats that duckdb doesn't (yet) support, write via pandas
    # See also https://github.com/nickzoic/duckdb-compression/

    if filename.endswith(".bz2"):
        with bz2.open(filename, "w") as fh_bz2:
            _write(fh_bz2, sep=sep, quoting=quoting)
    elif filename.endswith(".xz"):
        with lzma.open(filename, "w") as fh_xz:
            _write(fh_xz, sep=sep, quoting=quoting)
    elif quoting not in (csv.QUOTE_MINIMAL, csv.QUOTE_ALL):
        # duckdb doesn't support this properly
        # https://github.com/duckdb/duckdb-python/discussions/491
        if filename.endswith(".gz"):
            with gzip.open(filename, "w") as fh_gz:
                _write(fh_gz, sep=sep, quoting=quoting)
        else:
            with open(filename, "w", encoding="utf-8") as fh_open:
                _write(fh_open, sep=sep, quoting=quoting)
    elif quoting == csv.QUOTE_ALL:
        # otherwise write it directly
        source.to_csv(filename, sep=sep, quoting=quoting)
    else:
        source.to_csv(filename, sep=sep)


def write_csv(
    source: DuckDBPyRelation, filename: str, sep: str, quoting: int, header: Optional[Sequence[str]] = None
) -> None:
    """Writes ``source`` to ``filename``, after the metadata ``header`` lines if any, creating the
    folder it goes in."""
    os.makedirs(os.path.dirname(os.path.abspath(filename)), exist_ok=True)
    if not header:
        write_to_csv(source, filename, sep, quoting)
        return
    # The table is written as usual to a plain file beside the target, then copied after the
    # header into the target, compressed as its name says.
    partial = filename + ".partial"
    try:
        write_to_csv(source, partial, sep, quoting)
        with open_text(filename, "w") as out, open(partial, encoding="utf-8") as table:
            out.writelines(f"{line}\n" for line in header)
            shutil.copyfileobj(table, out)
    finally:
        if os.path.exists(partial):
            os.remove(partial)


def preview_order(columns: Sequence[str], sort_column: int, descending: bool) -> Optional[str]:
    """The ORDER BY of a node's preview, as the GUI sorts it: by column number ``sort_column``, then
    by every other column for a stable order, all in the one direction."""
    if not columns:
        return None
    key = columns[sort_column] if 0 <= sort_column < len(columns) else columns[0]
    direction = "DESC" if descending else "ASC"
    ordered = [key] + [c for c in columns if c != key]
    return ", ".join(f"{duckdb_escape_identifier(c)} {direction}" for c in ordered)


def preview_csv_path(graph: PipelineGraph, node: PipelineNode) -> str:
    """Where ``node``'s preview is saved: ``<output directory>/<node-name>_preview.csv``, the output
    directory filled in for this session, so that saves made in one session go to one place."""
    return os.path.join(graph.context_for(node, graph.session_started).output_dir(), preview_filename(node.name))


def save_preview_csv(graph: PipelineGraph, node: PipelineNode, overwrite: bool = False) -> str:
    """Writes the rows ``node``'s preview holds, in its sort order, to ``preview_csv_path``.

    Returns:
        The path written.

    Raises:
        FileExistsError: If the file is already there and ``overwrite`` is not set.
        ValueError: If the node has no preview table.
    """
    if node.status != PipelineNodeStatus.RESULT or not node.table_name:
        raise ValueError(f"{node.name} has no preview to save")
    path = preview_csv_path(graph, node)
    if os.path.exists(path) and not overwrite:
        raise FileExistsError(path)
    cursor = graph.ddbc.cursor()
    try:
        table = cursor.table(node.table_name)
        [(rows,)] = cursor.table(node.table_name).count("*").fetchall()
        if order := preview_order(table.columns, node.sort_column, node.sort_descending):
            table = table.order(order)
        header = None
        if graph.workflow.metadata_header:
            limit = graph.preview_row_limit
            note = f"preview: the {rows} rows the node's preview held" + (
                f" (a preview holds at most {limit} rows)" if limit else ""
            )
            header = graph.context_for(node, graph.session_started).metadata_lines(
                datetime.datetime.now(), note, started_label="session started"
            )
        write_csv(table, path, sep=",", quoting=csv.QUOTE_MINIMAL, header=header)
    finally:
        cursor.close()
    logger.info("%s: preview saved to %s", node.name, path)
    return path
