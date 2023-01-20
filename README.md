# CSV bag of random tools
Some odds and ends to help working with CSV files.

## Contents
### Python
- CSV Splitter - Helps break down big files into smaller ones to loading or sampling
- CSV Sampler -  Get the first n rows of data for sampling

### Databricks - PySpark
- Loading CSV files
- Exporting CSV files
- Find column lengths

## Pyhton
### CSV Splitter
Enter source and destination locations, and if needed set the number of rows in each section with the row_size parameter

```
# Parameters
import math as m
row_size = 50000
source_location = 'D:/Test/test.csv'
destination_location = 'D:/Test/test_out_'

# Def to write the rows
def write_rows(part, lines):
    with open(destination_location + str(part) + '.csv', 'w') as f_out:
        f_out.write(header)
        f_out.writelines(lines)
        print('Writing file ' + str(part) + ' of ' + str(total_files))

# Counts the rows in the file
def count_rows():
   f = open(source_location,'r')
   line_count = len(f.readlines())
   return line_count

# Code that does the stuff
with open(source_location, 'r') as f:
    ## Display file info
    total_lines = count_rows()
    print('Csv file lines: ' +  str("{:,}".format(total_lines)))
    total_files = m.ceil(total_lines/row_size)
    print('Process will output ' + str("{:,}".format(total_files)) + ' files')
# Split out the file    
    count = 0
    header = f.readline()
    lines = []
    for line in f:
        count += 1
        lines.append(line)
        if count % row_size == 0:
            write_rows(count // row_size, lines)
            lines = []
    ## output the remainder of the rows
    if len(lines) > 0:
        write_rows((count // row_size) + 1, lines)
```

### CSV Sampler

TBA

## Databricks
The examples here use the auto-mpg machine learning datset from the website https://archive.ics.uci.edu/ml/datasets/auto+mpg

### Loading CSV Files

```
df = spark.read.format("csv").option("header", True).load("data/auto-mpg.csv")
```
### Profile the dataframe
Handy if you need to create tables else where like Azure SQL/Synapse and you need to find the max length of the string. Otherwise some lazy dev, will just VARCHAR(MAX) the hell out the external table, and make me want to punch them!

```
from pyspark.sql import functions as f
df1 = df.select([f.length(col).alias(col) for col in df.columns])
df1.groupby().max().show()
```

