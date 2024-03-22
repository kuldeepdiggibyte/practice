<title>DataFrame and RDD Operations</title>
For DataFrame:
Created a DataFrame:
Utilized spark.read.csv to read a CSV file into a DataFrame.
Used spark.read.option to specify options while reading the CSV file.
Described the data:
Invoked df_spark1.describe() to generate descriptive statistics of the DataFrame.
Manipulated DataFrame:
Employed withColumn to add or replace a column in the DataFrame with a new column.
For RDD:
Worked with RDDs:
Utilized RDDs for lower-level transformation and actions compared to DataFrames.
Applied transformations:
Utilized map to apply a function to each element of the RDD.
Employed flatMap to produce multiple output elements for each input element.
Used filter to selectively retain elements in the RDD based on a condition.
Performed custom transformations:
Utilized mapPartitions to apply a function to each partition of the RDD.
Leveraged glom within mapPartitions to gather all elements of each partition into a single list.
