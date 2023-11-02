# Included Plugins

CountESS is designed to be easy to write plugins for, but plugins are included for common file formats and simple data transformations.

Source code for the included plugins can be found in the repository at
[countess/plugins](https://github.com/CountESS-Project/CountESS/tree/main/countess/plugins)

CountESS is not limited to these included plugins: anyone can write and publish CountESS plugins. See [Other Plugins](../other-plugins/) for some examples or  [Writing CountESS Plugins](../writing-plugins/) to write your own.

## File Formats

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

## Data Manipulation

### Regex Tool

Splits a string value into multiple parts using a regular expression.

Specify a pattern to match and columns to output the matching parts to.

In "multi" mode, the regex can match multiple times, each of which will generate its own output row.

### Pivot Tool

Performs a "pivot" operation on the data table.

Each column can be specified as an "Index", "Pivot" or "Expand" column, or dropped.

The result will have one row per Index value, with each of the "Expand" columns expanded to include each distinct value of the "Pivot" column(s).  Duplicate values are summed and missing values are set to `0`.

For example this data set:

| `variant` | `replicate` | `count` |
|---|---|---|
| `1` | `2` | `3` |
| `1` | `4` | `5` |
| `2` | `2` | `6` |
| `2` | `7` | `8` |
| `3` | `7` | `9` |
| `3` | `7` | `10` |

when pivoted with index on Variant, pivot on Replicate and expanding Count becomes:

| `variant` | `count__replicate_2` | `count__replicate_4` | `count__replicate_7` |
|---|---|---|---|
| `1` | `3` | `5` | `0` |
| `2` | `6` | `0` | `8` |
| `3` | `0` | `0` | `19` | 

## Bioinformatics

### FASTQ Load

Read one (or more) [FASTQ](https://maq.sourceforge.net/fastq.shtml) files, optionally gzipped.
Returns a datatable with columns:

sequence
: the sequence, as a DNA or RNA string of A, C, G, T.

header
: the header string from the FASTQ read (the line starting with `@`)

A minimum average quality filter can be applied.

If "Group by sequence?" is selected, an additional column "count" is added and the sequences are grouped.  The header is reduced to the common prefix of all headers in the FASTQ file.

### Mutagenize

Takes a sequence in configuration and returns all possible mutations of that sequence of various types.  Only one kind is applied at a time.

#### Parameters

All Single Mutations?
: Include all SNVs of the sequence

All Single Deletes?
: Include all single base deletions of the sequence

All Triple Deletes?
: Include all triple base deletions, eg: aligned deletions, of the sequence

All Single Inserts?
: Include all single base insertions of the sequence

All Triple Inserts?
: Include all triple base insertions, eg: aligned insertions, of the sequence

Remove Duplicates?
: Deletions and Insertions into the sequence can end up with the same sequence from multiple operations, for example inserting `A` into `CAAT` can produce `CAAAT` in three different ways, and single deletions can produce `CAT` in two different ways.  If "Remove Dupliates" is selected, duplicates are removed but an additional column "count" is added to indicate how many ways this sequence can have been produced.

### Variant Caller

Turns a DNA sequence into an HGVS variant code by comparing it to a reference sequence.
The reference sequence can either be provided directly as a configuration parameter or in a selected column.

*See also: [countess-minimap2 plugin](https://github.com/CountESS-Project/countess-minimap2), a variant caller which uses 'minimap2' to find sequences within a genome.*

