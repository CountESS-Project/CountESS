# CountESS Configuration File Format

CountESS configuration is stored in a .INI file with a pretty
simple format.  [.INI Files](https://en.wikipedia.org/wiki/INI_file)
are simple and human readable/writable.

While CountESS is intended to be used via the GUI, the config file 
is designed to be editable if necessary and to be compatible with
revision control systems.

## Sections & Modules

.INI files are grouped into sections.
Each section corresponds to a node of the data flow graph, and
has the label of a node of the data flow graph.  All nodes must
have a distinct name.

    [Load FASTQ]

Each section contains key/value pairs which configure that node.
Keys are hierarchical, with subkeys separated by '.'

Keys and subkeys starting with `_` are reserved for CountESS.
Several reserved key/value pairs exist to define the plugin module being
used for this node:

    _module = countess.plugins.fastq
    _class = LoadFastqPlugin
    _version = 0.0.37

`_uuid` and `_hash` are used to keep the configuration file order stable
under name changes and to track changes to input files, respectively:

    _uuid = 0a0a9f2a6772942557ab5355d76a
    _hash = dffd6021bb2bd5b0af676290809ec3a53191dd81c7f70a4b28688a362182986f

CountESS also uses a reserved key/value pair to store graphical layout
information.  Currently this is stored as a major (flow-wards) and minor (sideways)
coordinate, each between 0 and 1000:

    _position = 55 500

## Connections

The connections between nodes are stored as reserved key/value pairs too:

    [Join on "Run"]
    _parent.0 = Read File Descriptions
    _parent.1 = Break out "Run"

Plugins are written to the file "top down" so that a node never appears in
the config file before all of its parents do.

## Plugin Configuration

All other keys are defined by the plugin modules.  Keys may not contain
`.` or `=` or other values used in the .INI format.

Values are stored in
Python [`repr()`](https://docs.python.org/3/library/functions.html#repr)
format and interpreted using
[`ast.literal_eval`](https://docs.python.org/3/library/ast.html#ast.literal_eval)
which avoids problems with special characters & also preserves types.

     min_avg_quality = 10.0
     greeting = 'Hello, World!'

Arrays of values are stored using numeric subkeys:

     files.0.filename = 'first_file.txt'
     files.1.filename = 'second_file.txt'
     files.2.filename = 'third_file.txt'

When loading, subkeys are reinstated in numeric order but any missing values
are skipped, so if you remove `files.1.filename` above everything will just work.
When re-writing config files from the GUI the array elements will be given 
contiguous numbers again.

## Per-Column Arrays

Where an array of values represents columns of a datatable, an additional `_label`
subkey records this mapping so it can be restored:

    columns.5._label = 'count'
    columns.5.sum = True
    columns.5.count = True
    columns.7._label = 'variant'
    columns.7.index = True

## Workflow Name, Version and Output Directory

A configuration can give the workflow a short name and a
[semantic version](https://semver.org), and say where its output goes, in a section of
its own at the top of the file:

    [_workflow]
    name = FWD saturation
    version = 1.2.0
    output_dir = {date}_{time}_{workflow}_{version}
    metadata_header = false

All are optional, and are set from the GUI's "Workflow" button. Unlike plugin
settings these are plain text, not Python literals. `_workflow` is reserved: no node
can be given that name.

`output_dir` is the folder each run writes its files into; a relative one is relative to
the configuration file. It can use the fields `{date}` (YYYY-MM-DD) and `{time}`
(HH-MM-SS) of the run's start, `{workflow}` (the name, in kebab-case) and `{version}`
(as `v1-2-0`). The default, `{date}_{time}_{workflow}_{version}`, gives each run a folder of
its own such as `2026-10-19_14-05-09_fwd-saturation_v1-2-0`, while the files in it keep fixed
names (a [CSV Writer](../included-plugins/#csv-writer) writes `results.csv` unless told
otherwise) that downstream scripts can rely on. A field with no value, such as an unset
version, is left out along with the separator next to it; write `{{` or `}}` for a literal brace.

The GUI's "Save Preview CSV" button writes the rows a node's preview holds, in the order
shown, to `<node-name>_preview.csv` in the output directory, its `{date}` and `{time}` being
when the configuration was opened.

`metadata_header` says whether CSV files start with a metadata header (see the
[CSV Writer](../included-plugins/#csv-writer)); it is on unless set to `false`, and a
CSV Writer can override it.

## Working with Revision Control

This file format should be well-behaved with revision control systems such as Git,
with some small caveats:

* Duplicate section names are not allowed:

     [New Counter 1]
     config = True

     [New Counter 1]
     config = False

  To fix this, manually rename the sections.

* Duplicate keys are not allowed, so revision control operations may end up with an
  invalid state such as:

     files.0.filename = 'Alice\'s File'
     files.0.filename = 'Bob\'s File'

  To fix this, manually renumber the files.

* The order of nodes is generally preserved, but parents are always written before 
  their children, which means that changes to the topology may lead to large changes
  in revision control.

These issues are all potentially addressable in future versions of the software.
