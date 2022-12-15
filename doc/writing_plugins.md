# Writing CountESS Plugins

TBC

## General Structure

## Configuration Parameters


## Subclassing `BasePlugin`

All CountESS plugins are required to be subclasses of `countess.core.plugins.BasePlugin`.

To make life easier, there are several subclasses of `BasePlugin` under `countess.core.plugins` ...

### `DaskBasePlugin`

### `DaskInputPlugin`

* `read_files_to_dataframe`

* `combine_dfs`

### `DaskTransformPlugin`

### `DaskScoringPlugin`


## Built-in Plugins

### `LoadCSVPlugin`

### `LoadHdfPlugin`

### `LoadFastqPlugin`

### `LogScorePlugin`

### `GroupByPlugin`

## Example Plugins

Under [example-plugins](../example-plugins/) are files showing the 
suggested layout for Python code in a plugin.
