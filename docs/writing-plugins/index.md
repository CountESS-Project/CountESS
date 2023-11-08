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

### PandasBasePlugin

Plugins which subclass PandasBasePlugin pass iterables of Pandas dataframes.
This lets CountESS process data sets larger than RAM.  Where possible each
dataframe is processed separately.  Where this isn't possible, a map/reduce/finalize
strategy can be used.

### PandasInputFilesPlugin

Many plugins exist to read specific file formats, both generic formats like CSV and
bioinformatics specific formats like FASTQ.  To implement a reader for a new file format,
subclass PandasInputFilesPlugin and override the function `read_file_to_dataframe`.

Where multiple files are selected, several processes can be run in parallel.

*Examples: countess.core.plugins.csv.LoadCsvPlugin, countess.core.plugins.fastq.LoadFastqPlugin*

### PandasTransform**...**Plugin

Many plugins just take tabular data and transform each row by
altering or deleting columns, or adding columns based on the existing ones.
As processing is entirely row-local, it can be distributed over many CPUs
and caching can be performed to increase efficiency.

Some plugins may operate on a single input value, others on an entire row.
Some plugins may generate a single output value, others several.
For convenience, several subclasses are provided to transform either a 
single value or a whole row into one or several new columns.

These include (not all combintions are listed here):

* PandasTransformSingleToSinglePlugin

  Subclasses provide a function `process_value` which takes a single value and returns a single value.
  Input value is extracted from a column whose name is provided in a parameter 'column'.
  Output value is written to a column whose name is provided in a parameter 'output'.

  *Example: countess.core.plugins.sequence.SequencePlugin*

<!--
* PandasTransformSingleToTuplePlugin

  Subclasses provide a function `process_value` which takes a single value and returns a tuple of values.
  Input value is extracted from a column whose name is provided in a parameter 'column'.
  Output values are written to columns whose names are provided in subparameter 'name' of an array parameter 'output'.
-->
  
* PandasTransformSingleToDictPlugin

  Subclasses provide a function `process_value` which takes a single value and returns a dictionary.
  Input value is extracted from a column whose name is provided in a parameter 'column'.
  Output values are written to columns whose names are provided by the dictionary keys.
  
  *Example: countess.core.plugins.regex.RegexToolPlugin*

* PandasTransformRowToSinglePlugin

  Subclasses provide a function `process_row` which takes a row (pd.Series) and returns a single value.
  Marshalling rows into pd.Series incurs significant overhead, so you might be better off using PandasTransformDictToSinglePlugin.
  Output value is written to a column whose name is provided in a parameter 'output'.

<!--
* PandasTransformRowToTuplePlugin

  Subclasses provide a function `process_row` which takes a row (pd.Series) and returns a tuple of values.
  Marshalling rows into pd.Series incurs significant overhead, so you might be better off using PandasTransformDictToTuplePlugin.
  Output values are written to columns whose names are provided in subparameter 'name' of an array parameter 'output'.

* PandasTransformRowToDictPlugin

  Subclasses provide a function `process_row` which takes a row (pd.Series) and returns a dictionary.
  Marshalling rows into pd.Series incurs significant overhead, so you might be better off using PandasTransformDictToDictPlugin.
  Output values are written to columns whose names are provided by the dictionary keys.
-->

* PandasTransformDictToSinglePlugin

  Subclasses provide a function `process_dict` which takes a dictionary and returns a single value.
  Output value is written to a column whose name is provided in a parameter 'output'.

  *Example: countess.core.plugins.variant.VariantPlugin*

<!--
* PandasTransformDictToTuplePlugin

  Subclasses provide a function `process_dict` which takes a dictionary and returns a tuple of values.
  Output values are written to columns whose names are provided in subparameter 'name' of an array parameter 'output'.
-->

* PandasTransformDictToDictPlugin

  Subclasses provide a function `process_dict` which takes a dictionary and returns a dictionary.
  Output values are written to columns whose names are provided by the dictionary keys.

  *Examples: countess.core.plugins.python.PythonPlugin, countess.core.plugins.hgvs\_parser.HgvsParserPlugin*

## Configuration Parameters

Plugins have "configuration parameters" which are declared in the plugin
class declaration using subclasses of `countess.core.parameters.BaseParameter`.

Configuration values are provided by a [single configuration file](../config-file-format/)
which is read and written by the CountESS GUI and read by the CountESS CLI.

## Testing Plugins

Tests for included plugins are included under
[tests/plugins/](https://github.com/CountESS-Project/CountESS/tree/main/tests/plugins)
and should provide some guidance on how to write tests for plugins.

## Example Plugins

Under [example-plugins](../example-plugins/) are files showing the 
suggested layout for Python code in a plugin.
