# CountESS Documentation: Plugins

CountESS pipelines are constructed from plugins.
There are several built-in plugins and 

## Built-in Plugins: File Formats

### CSV Reader

Loads data from CSV and similar delimited text formats into a dataframe.

### CSV Writer

Writes dataframe data out to CSV (or TSV, etc)

### Regex Reader

If the CSV Reader isn't flexible enough for your file format, the Regex
Reader can be used to parse each line of your file with a [python regular 
expression](https://docs.python.org/3/howto/regex.html#regex-howto)

## Built-in Plugins: Data Manipulation

### Regex Tool

### Pivot Tool


## Other Plugins

Anyone can write and publish CountESS plugins.

* [Pebbles](https://github.com/genomematt/pebbles/) for reading SAM and BAM files

