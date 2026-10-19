"""Workflow metadata, and the names it gives the files a run writes.

A workflow (a pipeline) can carry a short name and a semantic version. A plugin that writes files
learns them, along with its node's name and the run's date, from the RunContext it is given before
it runs, and can name its files from a template such as ``{date}_{workflow}_{version}_{node}.csv``.
"""

import dataclasses
import datetime
import re
import string
from typing import Optional

# Semantic Versioning 2.0.0, https://semver.org/#is-there-a-suggested-regular-expression-regex-to-check-a-semver-string
SEMVER_RE = re.compile(
    r"^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)"
    r"(?:-((?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*)(?:\.(?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*))*))?"
    r"(?:\+([0-9a-zA-Z-]+(?:\.[0-9a-zA-Z-]+)*))?$"
)

MAX_NAME_LENGTH = 100

# The config file section holding a workflow's metadata; it is not a node.
CONFIG_SECTION = "_workflow"

# The fields a file name template can use, and what each becomes.
FILENAME_FIELDS = {
    "date": "the date of the run, YYYY-MM-DD",
    "time": "the time the run started, HH-MM-SS",
    "workflow": "the workflow's name, in kebab-case",
    "version": "the workflow's version, as v1-2-3",
    "node": "the name of the node writing the file, in kebab-case",
}

DEFAULT_FILENAME_TEMPLATE = "{date}_{workflow}_{version}_{node}"

# The separators an empty field takes with it.
_SEPARATORS = "_-"


class WorkflowError(ValueError):
    """A workflow name or version that cannot be used, or a file name template that cannot be filled."""


def kebab_case(text: str) -> str:
    """'FWD Saturation (pilot)' -> 'fwd-saturation-pilot'."""
    return re.sub(r"[^a-z0-9]+", "-", text.lower()).strip("-")


@dataclasses.dataclass(frozen=True)
class WorkflowInfo:
    """A workflow's short name and semantic version; either may be unset."""

    name: Optional[str] = None
    version: Optional[str] = None

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

    @classmethod
    def from_strings(cls, name: Optional[str], version: Optional[str]) -> "WorkflowInfo":
        """From user input, where an empty string means unset."""
        name = name.strip() if name else None
        version = version.strip() if version else None
        return cls(name or None, version or None)

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
class RunContext:
    """What a plugin is told about the run it is part of."""

    workflow: WorkflowInfo
    node_name: str
    started: datetime.datetime

    def filename_fields(self) -> dict[str, str]:
        return {
            "date": self.started.strftime("%Y-%m-%d"),
            "time": self.started.strftime("%H-%M-%S"),
            "workflow": self.workflow.name_for_files,
            "version": self.workflow.version_for_files,
            "node": kebab_case(self.node_name),
        }


def fill_filename(template: str, context: RunContext) -> str:
    """The path ``template`` names in ``context``.

    Fields are written ``{date}``, ``{workflow}`` and so on (see FILENAME_FIELDS); ``{{`` and
    ``}}`` are literal braces. A field with no value (a workflow with no version, say) is left
    out of the file name with the separator next to it, so ``{date}_{workflow}_{version}_{node}``
    with no version set gives ``2026-10-19_my-workflow_save-results``.

    Raises:
        WorkflowError: If the template names a field that does not exist, or is malformed.
    """
    fields = context.filename_fields()
    try:
        parsed = list(string.Formatter().parse(template))
    except ValueError as exc:
        raise WorkflowError(f"{template!r}: {exc}; write a literal brace as {{{{ or }}}}") from exc
    for _literal, field, spec, conversion in parsed:
        if field is None:
            continue
        if field not in fields or spec or conversion:
            raise WorkflowError(
                f"{template!r}: unknown field {{{field}}}; the fields are "
                + ", ".join(f"{{{name}}}" for name in FILENAME_FIELDS)
            )
    pieces: list[str] = []
    drop_leading_separator = False
    for literal, field, _spec, _conversion in parsed:
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
    name = filled.rpartition("/")[2]
    if not name or name.startswith("."):
        raise WorkflowError(f"{template!r} gives no file name here: set the workflow's name and version, or add text")
    return filled


def default_filename(context: RunContext, suffix: str, extension: str) -> str:
    """A file name in the house style, ``{date}_{workflow}_{version}_{node}``, with a suffix.

    For files a tool makes outside any save node: ``default_filename(context, "log", "txt")``
    gives ``2026-10-19_my-workflow_v1-2-0_run-log.txt`` for a context whose node is "run".
    """
    template = DEFAULT_FILENAME_TEMPLATE + (f"-{suffix}" if suffix else "") + f".{extension}"
    return fill_filename(template, context)
