"""Workflow metadata: a workflow's name and version, where its output goes, and how its files describe
themselves.

A workflow (a pipeline) can carry a short name and a semantic version. Its output directory is a
template, ``{date}_{time}_{workflow}_{version}`` by default, so that each run writes to a folder of
its own while the files in it keep fixed names that downstream scripts can rely on. A plugin learns
all of this, along with its node's name and the run's start, from the RunContext it is given before
it runs.
"""

import dataclasses
import datetime
import os
import re
import string
from typing import Optional

from countess import VERSION

# Semantic Versioning 2.0.0, https://semver.org/#is-there-a-suggested-regular-expression-regex-to-check-a-semver-string
SEMVER_RE = re.compile(
    r"^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)"
    r"(?:-((?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*)(?:\.(?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*))*))?"
    r"(?:\+([0-9a-zA-Z-]+(?:\.[0-9a-zA-Z-]+)*))?$"
)

MAX_NAME_LENGTH = 100

# The config file section holding a workflow's metadata; it is not a node.
CONFIG_SECTION = "_workflow"

# The fields an output directory template can use, and what each becomes.
OUTPUT_DIR_FIELDS = {
    "date": "the date the run started, YYYY-MM-DD",
    "time": "the time the run started, HH-MM-SS",
    "workflow": "the workflow's name, in kebab-case",
    "version": "the workflow's version, as v1-2-3",
}

DEFAULT_OUTPUT_DIR = "{date}_{time}_{workflow}_{version}"

# The separators an empty field takes with it.
_SEPARATORS = "_-"


class WorkflowError(ValueError):
    """A workflow name, version or output directory that cannot be used."""


def kebab_case(text: str) -> str:
    """'FWD Saturation (pilot)' -> 'fwd-saturation-pilot'."""
    return re.sub(r"[^a-z0-9]+", "-", text.lower()).strip("-")


def preview_filename(node_name: str) -> str:
    """The file a node's preview is saved as: 'Collapse by Variant' -> 'collapse-by-variant_preview.csv'."""
    return f"{kebab_case(node_name) or 'node'}_preview.csv"


def check_output_dir(template: str) -> None:
    """Raises WorkflowError if ``template`` is not an output directory template that can be filled."""
    if not template.strip():
        raise WorkflowError("A workflow needs an output directory")
    try:
        parsed = list(string.Formatter().parse(template))
    except ValueError as exc:
        raise WorkflowError(f"{template!r}: {exc}; write a literal brace as {{{{ or }}}}") from exc
    for _literal, field, spec, conversion in parsed:
        if field is not None and (field not in OUTPUT_DIR_FIELDS or spec or conversion):
            raise WorkflowError(
                f"{template!r}: unknown field {{{field}}}; the fields are "
                + ", ".join(f"{{{name}}}" for name in OUTPUT_DIR_FIELDS)
            )


@dataclasses.dataclass(frozen=True)
class WorkflowInfo:
    """A workflow's short name and semantic version (either may be unset), its output directory
    template, and whether the CSV files it writes start with a metadata header."""

    name: Optional[str] = None
    version: Optional[str] = None
    output_dir: str = DEFAULT_OUTPUT_DIR
    metadata_header: bool = True

    def __post_init__(self):
        if self.name is not None:
            if not self.name.strip():
                raise WorkflowError("A workflow name cannot be blank")
            if len(self.name) > MAX_NAME_LENGTH:
                raise WorkflowError(f"A workflow name is at most {MAX_NAME_LENGTH} characters")
            if any(not c.isprintable() for c in self.name):
                raise WorkflowError("A workflow name is a single line of text")
            if not kebab_case(self.name):
                raise WorkflowError("A workflow name needs at least one letter or digit")
        if self.version is not None and not SEMVER_RE.match(self.version):
            raise WorkflowError(
                f"{self.version!r} is not a semantic version: use MAJOR.MINOR.PATCH, such as 1.0.0 or 2.1.0-rc.1"
            )
        check_output_dir(self.output_dir)

    @classmethod
    def from_strings(
        cls,
        name: Optional[str],
        version: Optional[str],
        output_dir: Optional[str] = None,
        metadata_header: bool = True,
    ) -> "WorkflowInfo":
        """From user input, where an empty name or version means unset and an empty output
        directory means the default."""
        name = name.strip() if name else None
        version = version.strip() if version else None
        output_dir = output_dir.strip() if output_dir else None
        return cls(name or None, version or None, output_dir or DEFAULT_OUTPUT_DIR, metadata_header)

    @property
    def name_for_files(self) -> str:
        return kebab_case(self.name) if self.name else ""

    @property
    def version_for_files(self) -> str:
        return "v" + kebab_case(self.version) if self.version else ""

    def label(self) -> str:
        """'FWD saturation 1.2.0', or as much of it as is set."""
        return " ".join(part for part in (self.name, self.version) if part)


@dataclasses.dataclass(frozen=True)
class NodeLineage:
    """One node a file's data passed through: its name, and its plugin's name and version."""

    node: str
    plugin: str
    version: str

    def describe(self) -> str:
        return f"{self.node} ({self.plugin} {self.version})"


@dataclasses.dataclass(frozen=True)
class RunContext:
    """What a plugin is told about the run it is part of. ``base_dir`` is the folder a relative
    output directory is in: the pipeline's config file's. ``upstream`` is the nodes the plugin's
    data passes through before it, in pipeline order."""

    workflow: WorkflowInfo
    node: NodeLineage
    started: datetime.datetime
    base_dir: str = "."
    upstream: tuple[NodeLineage, ...] = ()

    @property
    def node_name(self) -> str:
        return self.node.node

    def output_dir(self) -> str:
        """The absolute path of this run's output directory, its template filled in."""
        return os.path.abspath(
            os.path.join(self.base_dir, fill_output_dir(self.workflow.output_dir, self.started, self.workflow))
        )

    def metadata_lines(
        self, written: datetime.datetime, note: Optional[str] = None, started_label: str = "run started"
    ) -> list[str]:
        """The metadata header of a CSV file this run writes at ``written``, each line starting '#'.
        ``started_label`` names what ``started`` is the start of: a preview's is the session's."""
        lines = [
            f"CountESS {VERSION}",
            f"written: {written.isoformat(sep=' ', timespec='seconds')}",
            f"{started_label}: {self.started.isoformat(sep=' ', timespec='seconds')}",
            f"workflow: {self.workflow.name or '(unnamed)'}",
            f"workflow version: {self.workflow.version or '(none)'}",
            f"node: {self.node.describe()}",
            *(f"upstream: {node.describe()}" for node in self.upstream),
        ]
        if note:
            lines.append(note)
        return [f"# {line}" for line in lines]


def fill_output_dir(template: str, started: datetime.datetime, workflow: WorkflowInfo) -> str:
    """The path an output directory template names for a run started at ``started``.

    Fields are written ``{date}``, ``{workflow}`` and so on (see OUTPUT_DIR_FIELDS); ``{{`` and
    ``}}`` are literal braces. A field with no value (a workflow with no version, say) is left out
    with the separator next to it, so ``{date}_{time}_{workflow}_{version}`` for an unnamed workflow
    gives ``2026-10-28_14-05-09``.

    Raises:
        WorkflowError: If the template names a field that does not exist, or is malformed.
    """
    check_output_dir(template)
    fields = {
        "date": started.strftime("%Y-%m-%d"),
        "time": started.strftime("%H-%M-%S"),
        "workflow": workflow.name_for_files,
        "version": workflow.version_for_files,
    }
    pieces: list[str] = []
    drop_leading_separator = False
    for literal, field, _spec, _conversion in string.Formatter().parse(template):
        if drop_leading_separator and literal[:1] in _SEPARATORS and literal:
            literal = literal[1:]
        drop_leading_separator = False
        pieces.append(literal)
        if field is None:
            continue
        if fields[field]:
            pieces.append(fields[field])
        elif pieces[-1][-1:] in _SEPARATORS and pieces[-1]:
            # An empty field takes the separator before it with it, or failing that the one after.
            pieces[-1] = pieces[-1][:-1]
        else:
            drop_leading_separator = True
    filled = "".join(pieces)
    if not filled.strip("/"):
        raise WorkflowError(f"{template!r} gives no directory here: set the workflow's name and version, or add text")
    return filled
