# CountESS Configuration File Format

CountESS configuration is stored in a .INI file with a pretty
simple format.  [.INI Files](https://en.wikipedia.org/wiki/INI_file)
are simple and human readable/writable.

While CountESS is intended to be used via the GUI, the config file 
is designed to be editable if necessary and to be compatible with
revision control systems.

# Sections & Modules

Each section of the file has the label of a node of the data flow graph.

    [Load FASTQ]

The section contains key/value pairs which configure that node.
Keys are hierarchical, with subkeys separated by '.'

Keys and subkeys starting with `_` are reserved for CountESS.
Several key/value pairs exist to define the plugin module being
used for this node:

    _module = countess.plugins.fastq
    _class = LoadFastqPlugin
    _version = 0.0.37

CountESS also stores graphical layout information in the .INI file.
Currently this is stored as a major (flow-wards) and minor (sideways)
coordinate between 0 and 1000:

    _position = 55 500

The connetions between nodes are stored as key/value pairs too:

    [Join on "Run"]
    _parent.0 = Read File Descriptions
    _parent.1 = Break out "Run"

# Plugin Configuration

All other keys are defined by the plugin modules.  Keys may not contain
`.` or `=` or probably some other values used in the .INI format.

For now, values are stored in
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

# Per-Column Arrays

Where an array of values represents columns of a datatable, an additional
`_label` subkey records this mapping so it can be restored:

    columns.5._label = 'count'
    columsn.5.sum = True
    columns.7._label = 'variant'
    columns.7.index = True


