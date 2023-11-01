# Running CountESS

## GUI

To run the CountESS GUI, type `countess_gui` or find "CountESS" in
your application launcher.

## Cross-platform Compatibility

CountESS runs on multiple platforms, which have slightly different 
names for keys and mouse buttons. To avoid confusion, CountESS
accepts multiple ways of using the UI.

CountESS

In this document, we're using the following terms:

Primary Mouse Button 
: The first (most frequently used) button

Secondary Mouse Button
: Any other button, **OR** the first button while holding "Control"

Delete Key
: Either "Backspace" or "Delete" or similar key.

## Loading and saving configurations

At the top of the CountESS window are buttons to load and save configurations.

New Config
: Clear all configuration and start from nothing

Load Config
: Load a configuration from an .INI file

Save Config
: Save the current configuration to an .INI file

## The dataflow graph

A CountESS pipeline consists of multiple components called 'nodes',
connected in an acyclic graph.

The graph is displayed on the left of the window (or at the top of the window,
on portrait monitors/windows).

### Highlighting nodes & connections

Nodes and connections can be highlighted (in red) by hovering over them.
Multiple connections can be highlighted at once where they overlap.

### Selecting Nodes

Primary click on a node to select it and make its configuration visible
in the configuration pane.

### Adding new nodes 

To add a new node, secondary click anywhere on the graph window and a node
called "NEW" will be added to the graph and selected for configuration.

If connection(s) were highlighted, the new node will be inserted into those
connections in the graph automatically. 

To add a new connection between nodes, highlight a node, secondary click
and drag to the node you want to connect it to, or to an empty area to 
create a new node.  Note that cyclic connections are disallowed.

### Deleting Nodes & Connections

To delete connection(s), highlight them and press the Delete Key.

To delete a node, highlight it and press the Delete Key.
Connections will be retained between the parents and children of the
deleted node.  To prevent this, hold Shift and press the Delete Key.


To select a node for configuration, primary click it.  You can also move it
around on the scren with primary click and drag.  To remove a node, select it
and then press the Delete key.

To link nodes, secondary click and drag a new link between them.  Nodes cannot be
linked to themselves, or in cycles.  To remove a link between nodes, hover over 
the link and press the Delete key.

Only the first few thousand rows of input files are read at configuration time.
To perform a complete run using the entire input file, click 'Run'.

## CLI

Plugin configurations can be saved and loaded in .INI file format.
CountESS can also be run in headless mode with `countess_cmd your_config.ini`.

