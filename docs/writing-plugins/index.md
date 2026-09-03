---
layout: default
---

# Writing CountESS Plugins

CountESS is a framework which connects together many components which do the actual
work, these components are called Plugins.  

There are many plugins supplied as part of CountESS but it is also very easy to 
write your own to perform specialized tasks.

## Included Plugins

See [Included CountESS Plugins](../included-plugins/) for a list of plugins which are
bundled with CountESS.

## Declaring Plugins

Plugins are provided by Python classes, either within the CountESS module or 
in other modules which may be part of the
[CountESS Project](https://github.com/CountESS-Project/) or external.

CountESS detects installed plugins using the 
[Python `importlib.metadata.entry_points`](https://docs.python.org/3/library/importlib.metadata.html#entry-points)
mechanism.  So to declare your new class as a CountESS plugin, you
just need to add a clause like this to your `pyproject.toml` or
equivalent:

```
[project.entry-points.countess_plugins]
myplugin = "myproject:MyPluginClass"
```

CountESS will then detect the existence of your plugin class and make 
it available to the user.

## Types of Plugin

All CountESS plugin classes are required to be subclasses of
`countess.core.plugins.BasePlugin`.

Several abstract subclassess are provided in `countess.core.plugins`,
which do most of the complex work of implementing different types of
plugin, leaving your plugin class to fill in the final details.

### DuckdbPlugin 

**Details TBD**

* DuckdbPlugin
* DuckdbSimplePlugin
* DuckdbSqlPlugin
* DuckdbInputPlugin
* DuckdbStatementPlugin
* DuckdbLoadFilePlugin
* DuckdbLoadFileWithTheLotPlugin
* DuckdbSaveFilePlugin
* DuckdbTransformPlugin
* DuckdbThreadedTransformPlugin
* DuckdbParallelTransformPlugin

## Configuration Parameters

Plugins have "configuration parameters" which are declared in the plugin
class declaration using subclasses of `countess.core.parameters.BaseParameter`.

Configuration values are provided by a [single configuration file](../config-file-format/)
which is read and written by the CountESS GUI and read by the CountESS CLI.

The file can be edited by hand if necessary and should be amenable to storage
in a revision control system.

## Testing Plugins

Tests for included plugins are included under
[tests/plugins/](https://github.com/CountESS-Project/CountESS/tree/main/tests/plugins)
and should provide some guidance on how to write tests for plugins.

## Example Plugins

Under [example-plugins](../example-plugins/) are files showing the 
suggested layout for Python code in a plugin.
