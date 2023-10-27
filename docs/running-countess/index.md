# Running CountESS

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

Only the first few thousand rows of input files are read at configuration time.
To perform a complete run using the entire input file, click 'Run'.

Plugin configurations can be saved and loaded in .INI file format.
CountESS can also be run in headless mode with `countess_cmd your_config.ini`.

