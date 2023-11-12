# Running CountESS

## GUI

To run the CountESS GUI, type `countess_gui` or find "CountESS" in
your application launcher.

## Cross-platform Compatibility

CountESS runs on multiple platforms, which have slightly different 
names for keys and mouse buttons. To avoid confusion, CountESS
accepts multiple ways of using the UI.  In this document, we're using
the following terms in the hopes of avoiding confusion:

Primary Mouse Button 
: The first (most frequently used, or only) mouse button

Secondary Mouse Button
: Any other mouse button, **OR** the first mouse button while holding the "Control" key.

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

The graph is displayed on the left of the window (or at the top of the window, on portrait monitors/windows).

### Highlighting nodes & connections

Nodes and connections can be highlighted (in red) by hovering over them.
Multiple connections can be highlighted at once where they overlap.

### Selecting Nodes

Primary click on a node to select it and make its configuration visible
in the configuration pane.  You can also primary click-and-drag to move nodes
within the graph.

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

### Tidying the Graph

You can primary click-and-drag nodes to rearrange them in the graph.

Rearranging nodes by hand can be tedious, so there's a "Tidy Graph"
button at the top of the screen which realigns nodes in a tidy way.
Node positions are stored in the configuration file in a way which
is independent of the display dimensions and orientation.

## Configuring Nodes

At the top of the configuration pane is a field for the node "name", which is displayed on the graph and in diagnostic messages. Nodes must have distinct names and CountESS will add numbers to the end of the name to distinguish them.

Under the node name is the version number and a brief description of the purpose of the plugin, which may also include an **ℹ** button which links to a description of the plugin on the web.

Under that is an optional notes section to outline to purpose of this node.
This text isn't used by CountESS but is saved in the configuration file and exported.

Beneath that are all the configuration options allowed by this plugin.

## Previewing Results

Beneath the configuration pane is the results pane, which shows a preview of the output from the node.

Only the first few thousand rows of input files are read at preview time, which may lead to misleading preview results.

To perform a complete run using the entire input file, click 'Run'.

## CLI

Plugin configurations can be saved and loaded in .INI file format.
CountESS can also be run in headless mode with `countess_cmd your_config.ini`.
In headless mode, all output is to the console.
