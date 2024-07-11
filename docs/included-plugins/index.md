---
layout: default
---

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

### Data Table

Sometimes you need a little bit of extra data joined into your tables but don't really want to have a separate CSV file.  Data Table lets you embed small amounts of data into the configuration directly.

#### Parameters

Columns
: Add columns, giving them names and types. Columns may also be indexed to improve join performance.

Rows
: Add rows to the data table.

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

### Collate 

Group and sort records, keeping only the first N records in each group.

### Filter

Filter records using simple expressions.
Multiple filters can be chained together.
Rows which match a filter can have column values set before being passed on to the next plugin.
Rows which do not match the filter are sent to the next filter.
Rows which do not match any filter are dropped.

Each filter consists of one or more expressions, each of which compares a column 
to a value, using the following operators:

| operator | type(s) |
|---|---|
| `equals` | numbers, strings, booleans |
| `greater than` | numbers, strings |
| `less than` | numbers, strings |
| `starts with` | strings |
| `ends with` | strings |
| `matches regex` | strings |

Each operator can also be negated, so for example a filter can select values which do 
not start with a substring by negating the `starts with` operator.

Operators are then combined with either `All` or `Any` filters matching.

### Group By

Groups data by one or more columns, and aggregates the rest of the columns.

For each column of the input, select either "Index" or zero or more aggregation functions.

| `name` | `score` |
|:---:|:---:|
| `A` | `7` |
| `A` | `8` |
| `B` | `9` |

For example the above can be indexed by `name` and both count and mean evaluated for `score`, resulting in:

| `name` | `score__count` | `score__mean` |
|:---:|:---:|:---:|
| `A` | `2` | `7.5` |
| `B` | `1` | `9` |

### Join

Join two datatables on indexes or a column.

#### Parameters

For each input datatable, there are three parameters:

Join On
: Select index to use the table index, or a column

Required
: Is a match on this table required to output the row?

Drop Column
: Select if you no longer need the joining column and want to drop it.

The "required" flag on the input datatables lets you select the type of join:

| Input 1 Required | Input 2 Required | Type of Join |
|:---:|:---:|:---:|
| ✓ | ✓ | Inner |
| ✓ | ✗ | Left |
| ✗ | ✓ | Right | 
| ✗ | ✗ | Full / Outer |

### Regex Tool

> "Some people, when confronted with a problem, think "I know, I'll use regular expressions." Now they have two problems."
> -- Jamie Zawinski

Splits a string value into multiple parts using a regular expression.

Specify a pattern to match and columns to output the matching parts to.

In "multi" mode, the regex can match multiple times, each of which will generate its own output row.

### Pivot Tool

Performs a "pivot" operation on the data table.

Each column can be specified as an "Index", "Pivot" or "Expand" column, or dropped.

The result will have one row per Index value, with each of the "Expand" columns expanded to include each distinct value of the "Pivot" column(s).  Duplicate values are summed and missing values are set to `0`.

For example this data set:

| `variant` | `replicate` | `count` |
|:---:|:---:|:---:|
| `1` | `1` | `1` |
| `1` | `2` | `2` |
| `2` | `1` | `3` |
| `2` | `2` | `4` |
| `2` | `2` | `5` |
| `3` | `2` | `6` |

when pivoted with index on Variant, pivot on Replicate and expanding Count becomes:

| `variant` | `count__replicate_1` | `count__replicate_2` |
|:---:|:---:|:---:|
| `1` | `1` | `2` |
| `2` | `3` | `9` |
| `3` | `0` | `6` |

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
: Deletions and Insertions into the sequence can end up with the same sequence from multiple operations, for example inserting `A` into `CAAT` can produce `CAAAT` in three different ways, and single deletions can produce `CAT` in two different ways.  If "Remove Duplicates" is selected, duplicates are removed but an additional column "count" is added to indicate how many ways this sequence can have been produced.

### Variant Caller

Turns a DNA sequence into an HGVS variant code by comparing it to a reference sequence.
The reference sequence can either be provided directly as a configuration parameter or in a selected column.

*See also: [countess-minimap2 plugin](https://github.com/CountESS-Project/countess-minimap2), a variant caller which uses 'minimap2' to find sequences within a genome.*

#### Parameters

Input Column
: the input column with the variant sequence

Reference
: (optional) select column which contains the reference sequence ...

Sequence
: (optional) ... or supply a reference sequence as a value

Output Column
: Column name for HGVS string

Max Mutations
: Maximum number of mutations, if no variant with this number or less mutations is found then return a null value for the output

Drop
: Drop rows which would have null values for output
