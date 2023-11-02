# Included Plugins

CountESS is designed to be easy to write plugins for, but plugins are included for common file formats and simple data transformations.

Source code for the included plugins can be found in the repository at
[countess/plugins](https://github.com/CountESS-Project/CountESS/tree/main/countess/plugins)

## Built-in Plugins: File Formats

### CSV Reader

Loads data from CSV (and similar delimited text formats) into a dataframe.
Unfortunately CSV is not well standardized so there are quite a few options.

*Note that pandas can't represent null values in integer columns, so columns containing null values and set to integer data type will be promoted to float columns.*

#### Parameters

delimiter
: whether to use `,`, `;`, `|`, tab or whitespace as the column delimiter

comment
: are comments allowed, and if so is `#` or `;` used to delimit them?

header
: is the first row a header row? If so it will be used to populate default column names

filename_column
: if this is set, the filename will be added as a separate column. This is useful when reading many CSV files at once

columns
: for each column, specify a name and data type, and optionally index this column

### CSV Writer

Writes dataframe data out to CSV (or TSV, etc). 
Unfortunately CSV is not well standardized so there are quite a few options.

#### Parameters

header
: include a header row with column headers

filename
: the filename to write (if this ends with ".gz", data will be gzipped as it is written)

delimiter
: whether to use `,`, `;`, `|`, tab or whitespace as the column delimiter

quoting
: whether to quote all string values, even if they don't appear to need it.  This can reduce the chance of data corruption with software which can otherwise confuse string values for dates or numbers.

### Regex Reader

If the CSV Reader isn't flexible enough for your file format, the Regex
Reader can be used to parse each line of your file with a
[python regular expression](https://docs.python.org/3/howto/regex.html#regex-howto), splitting each line into multiple columns.

*Note that pandas can't represent null values in integer columns, so columns containing null values and set to integer data type will be promoted to float columns.*

#### Parameters

regex
: a Python Regular Expression which describes the expected format

skip
: how many lines to skip at the start of reading, useful for header rows.

output
: for each regex group, what colum name and data type should it be stored as.

## Built-in Plugins: Data Manipulation

### Regex Tool

Splits a string value into multiple parts using a regular expression.

Specify a pattern to match and columns to output the matching parts to.

In "multi" mode, the regex can match multiple times, each of which will generate its own output row.

### Pivot Tool

Performs a "pivot" operation on the data table.

Each column can be specified as an "Index", "Pivot" or "Expand" column, or dropped.

The result will have one row per Index value, with each of the "Expand" columns expanded to include each distinct value of the "Pivot" column(s).  Duplicate values are summed and missing values are set to `0`.

For example this data set:

| `x` | `y` | `z` |
|---|---|---|
| `1` | `2` | `3` |
| `1` | `4` | `5` |
| `2` | `2` | `6` |
| `2` | `7` | `8` |
| `3` | `7` | `9` |
| `3` | `7` | `10` |

when pivoted with Index X, Pivot Y, Expand Z becomes:

| `x` | `z__y_2` | `z__y_4` | `z__y_7` |
|---|---|---|---|
| `1` | `3` | `5` | `0` |
| `2` | `6` | `0` | `8` |
| `3` | `0` | `0` | `19` | 

### Variant Caller


## Other Plugins

Anyone can write and publish CountESS plugins.

* [Pebbles](https://github.com/genomematt/pebbles/) for reading SAM and BAM files

## Writing your own Plugins

See [Writing CountESS Plugins](../writing-plugins/)
