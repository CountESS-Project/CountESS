# CountESS 0.0.20

This is CountESS, a modular, Python 3 reimplementation of Enrich2.

## License

BSD 3-clause.  See [LICENSE.txt](LICENSE.txt)

Source code is available at [https://github.com/CountESS-Project/CountESS](https://github.com/CountESS-Project/CountESS) and contributions are welcomed.

## Installing

CountESS can be installed from pypi:
```
pip install CountESS
```

... or install the latest development version directly from github:
```
pip install git+https://github.com/CountESS-Project/CountESS.git
```

... or download and install for development:
```
git clone https://github.com/CountESS-Project/CountESS.git
cd CountESS
pip install -e .
```

... or run with nix:
```
nix run github:CountESS-Project/CountESS
```

## Counting with CountESS

To run the CountESS GUI, use `countess_gui`.

A CountESS pipeline consists of a bunch of components called 'nodes',
connected in an acyclic graph.
Some components read or write files, others perform basic data operations, 
others perform more specialized operations relevant to specific fields.

The graph is displayed on the left of the window (or at the top of the window,
on portrait monitors).  To add a node, click on the graph window and a node
called "NEW" will be added to the graph.  Pick a plugin for this node from
the menu.

To select a node for configuration, left-click it.  You can also move it
around on the scren with left-click and drag.  To remove a node, select it
and then press Delete.

To link nodes, right-click and drag a new link between them.  Nodes cannot be
linked to themselves, or in cycles.  To remove a link between nodes, right-click
the link.

Only the first few hundred rows of input files are read at configuration time.
To perform a complete run using the entire input file, click 'Run'.

Plugin configurations can be saved and loaded in .INI file format.
CountESS can also be run in headless mode with `countess_cmd your_config.ini`.

## Writing Plugins

CountESS is really just a pipeline of plugins, which are run sequentially to 
process your data.  Some of these are bundled as part of CountESS or 
available as packages on PyPI, but it is easy to create your own plugin packages.

To write a new plugin:

* make a Python class which inherits from `countess.core.plugins.BasePlugin` or 
  one of its subclasses.
* Fill in `name`, `title`, `description` and `parameters` attributes.
* Implement the appropriate methods (see the examples under core/plugins)
* set you package's `entry_points` to include `countess_plugins` pointing to
  your plugin class.
* install your package locally with `pip install -e .` 

... and your plugin will appear in CountESS.

For more details, see [Writing CountESS Plugins](doc/writing_plugins.md)

## Enquiries

Please contact Alan Rubin `alan@rubin.id.au` for further information or
Nick Moore `nick@zoic.org` for code-related matters.
