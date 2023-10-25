# Writing CountESS Plugins

CountESS is a framework which connects together many components which do the actual
work, these components are called Plugins.  

There are many plugins supplied as part of CountESS but it is also very easy to 
write your own.

## General Structure

Plugins are provided by Python classes.
All CountESS plugin classes are required to be subclasses of
`countess.core.plugins.BasePlugin`.

CountESS detects installed plugins using the
[Python `importlib.metadata.entry_points`](https://docs.python.org/3/library/importlib.metadata.html#entry-points)
mechanism.  So to declare your new class as a CountESS plugin, you
just need to add a clause like this to your `pyproject.toml` or
equivalent:

```
[project.entry-points.countess_plugins]
myplugin = "myproject:MyPluginClass"
```

## Types of Plugin

The most common kind of plugin takes a pandas dataframe, transforms it,
and passes the transformed dataframe along to the next plugin.

Examples include the Expression plugin, which creates a new column by
applying a formula to the existing columns, or the Regex Tool plugin which 
uses a regular expression to break an existing column into multiple
parts.

Other types of plugin include readers, which read from a file in
some particular format, and writers, which write formatted data.

## Configuration Parameters

Plugins have "configuration parameters" which are declared in the plugin
class declaration using subclasses of `countess.core.parameters.BaseParameter`.

Configfuration values are provided by a [single configuration file which
is read and written by CountESS](../config-file-format/)

## Subclassing `BasePlugin`

To make life easier, there are several subclasses of `BasePlugin`
under `countess.core.plugins` ... each of these provides common 
features used in different types of plugins.

### PandasSimplePlugin

### PandasTransform**X**To**Y**Plugin

There are several classes of the form `PandasTransform`**X**`To`**Y**`Plugin`.

The most obviously useful ones are `PandasTransformSingleToSinglePlugin`, 
which translates a single value into another single value, and 
`PandasTransformDictToDictPlugin`, which translates an entire table row.

To use these classes, subclass the appropriate one for your plugin and 
override `process_value`, `process_row` or `process_dict` as appropriate.


## Built-in Plugins

### `LoadCSVPlugin`

### `LoadHdfPlugin`

### `LoadFastqPlugin`

### `LogScorePlugin`

### `GroupByPlugin`

## Example Plugins

Under [example-plugins](../example-plugins/) are files showing the 
suggested layout for Python code in a plugin.
