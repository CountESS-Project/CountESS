---
layout: default
---

# What is CountESS?

CountESS is a visual, interactive programming environment for numerical computing, focused on bioinformatics.

CountESS programs are acyclic graphs of processing nodes, each of which reads data, transforms data or writes data out to files. Nodes can be rearranged and reconfigured through the graphical user interface, with a live preview of their output visible to the user. Programs can have multiple inputs and multiple outputs, with multiple branches of calculations performed in parallel.

This provides many of the advantages of spreadsheets while avoiding their well-known pitfalls. It also avoids the need for science staff to learn several text-based programming tools before proceeding with data analysis.

## Applications in Bioinformatics

CountESS has been developed as a tool for bioinformatics research, with support for file-formats and operations needed for processing biological data. Support is built in for FASTA and FASTQ file formats, as well as for operations such as DNA and protein variant calling.

CountESS is especially suitable for processing data from Multiplexed Assays of Variant Effect (MAVEs) such as Saturation Genome Editing (SGE) or Variant Abundance by Massively Parallel Sequencing (VAMP-seq) experiments.

## Pipeline

CountESS constructs an internal "pipeline" of internal processing nodes to allow multiple operations to proceed in parallel. Data processing is limited by system resources:

* CPU Cores
* Available Memory
* Input/Output Operations

CountESS attempts to utilize these resources as effectively as possible by executing multiple nodes' operations in parallel across multiple CPU cores.

## Nodes and Plugins

CountESS uses a "plugin" architecture to enable new features to be added by third parties.  Plugins are implemented as Python classes which inherit from supplied generic plugin parent classes.  Once the plugins are installed, they automatically become available for selection and configuration through CountESS's user interface.

## Technologies

We use Python's built-in TkInter library for cross-platform GUI development, and Pandas for numerical computing.
