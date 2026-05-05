---
layout: default
---

# Further Examples

Continuing on from the examples in [Getting Started with CountESS](../getting-started/)
let's look at a more realistic experiment.

## Example 5: Deep Mutational Scan of BRCA1

This example is based on a small subset of a real deep mutational scan of BRCA1, testing
for E3 ubiquitin ligase activity ([Starita *et al.* 2015](http://dx.doi.org/10.1534/genetics.115.175802)).

The data is taken from the [Enrich2-Example](https://github.com/FowlerLab/Enrich2-Example) project to
make it easy to compare CountESS to [Enrich2](https://github.com/FowlerLab/Enrich2).

Load the example by running `countess_gui example_5.ini`.  Note that this 
data set is bigger than the default limit of rows loaded in the GUI, so only a
subset of the data is used until you click 'Run'.

This is example is quite complex so let's walk through it one step at a time:

### Loading Files 

The first thing to do is to load the data files.  

#### Barcode Map

The barcode map is a simple .CSV file so we load that with
the CSV loader:

[![Screenshot 1](img/s5_1.jpg)](img/s5_1.png)

We then feed the 'sequence' column to the variant caller to
translate the raw sequences into HGVS strings:

[![Screenshot 6](img/s5_6.jpg)](img/s5_6.png)

#### Replicates

There are also four .FASTQ files representing two replicates
at two time points.  The replicate number and time point are
encoded in the filename, so we enable the 'Filename Column?'
option to capture this data:

[![Screenshot 2](img/s5_2.jpg)](img/s5_2.png)

... and then the Regex plugin can split these out into separate
columns called 'rep' and 'time'.

[![Screenshot 4](img/s5_4.jpg)](img/s5_4.png)

#### Library

There's a single .FASTQ representing a library shared between the
replicates:

[![Screenshot 3](img/s5_3.jpg)](img/s5_3.png)

Because this library file is part of two replicates, we use 
two Expression plugins to make a copy of the library for each
replicate, both at time 0:

[![Screenshot 5](img/s5_5.jpg)](img/s5_5.png)

[![Screenshot 20](img/s5_20.jpg)](img/s5_20.png)

### Pivoting and Joining

Now we have all our data loaded, we need to pivot it
and join it to bring it into a more useful form.

First we combine the four replicate files and the two
copies of the library into a single table, and pivot it
by time so that for each replicate, each row relates a barcode
to three counts:

[![Screenshot 7](img/s5_7.jpg)](img/s5_7.png)

Then we can join that to the barcode map:

[![Screenshot 8](img/s5_8.jpg)](img/s5_8.png)

### Scoring by Barcode

We can calculate a score for each barcode using the 
Score plugin.  This is done using the same least-squares
regression model as Enrich2:

[![Screenshot 9](img/s5_9.jpg)](img/s5_9.png)

Each barcode now has a score and sigma (standard error)
for each replicate.  We can combine those scores into
a final score by pivoting and then using a Random Effects Model
to combine the individual scores and sigmas into a 
combined version:

[![Screenshot 10](img/s5_10.jpg)](img/s5_10.png)

[![Screenshot 11](img/s5_11.jpg)](img/s5_11.png)

### Scoring by Variant

We can also choose to collate by DNA variant 
before calculating scores as above:

[![Screenshot 12](img/s5_12.jpg)](img/s5_12.png)

[![Screenshot 13](img/s5_13.jpg)](img/s5_13.png)

[![Screenshot 14](img/s5_14.jpg)](img/s5_14.png)

[![Screenshot 16](img/s5_16.jpg)](img/s5_16.png)

[![Screenshot 17](img/s5_17.jpg)](img/s5_17.png)

### Scoring by Protein

... Or to collate by protein variant:

[![Screenshot 21](img/s5_21.jpg)](img/s5_21.png)

[![Screenshot 22](img/s5_22.jpg)](img/s5_22.png)

[![Screenshot 15](img/s5_15.jpg)](img/s5_15.png)

[![Screenshot 18](img/s5_18.jpg)](img/s5_18.png)

[![Screenshot 19](img/s5_19.jpg)](img/s5_19.png)




