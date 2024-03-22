<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>DataFrame and RDD Operations</title>
</head>
<body>
  <h1>For DataFrame:</h1>
  <h2>Created a DataFrame:</h2>
  <ul>
    <li>Utilized <code>spark.read.csv</code> to read a CSV file into a DataFrame.</li>
    <li>Used <code>spark.read.option</code> to specify options while reading the CSV file.</li>
  </ul>
  <h2>Described the data:</h2>
  <ul>
    <li>Invoked <code>df_spark1.describe()</code> to generate descriptive statistics of the DataFrame.</li>
  </ul>
  <h2>Manipulated DataFrame:</h2>
  <ul>
    <li>Employed <code>withColumn</code> to add or replace a column in the DataFrame with a new column.</li>
  </ul>

  <h1>For RDD:</h1>
  <h2>Worked with RDDs:</h2>
  <ul>
    <li>Utilized RDDs for lower-level transformation and actions compared to DataFrames.</li>
  </ul>
  <h2>Applied transformations:</h2>
  <ul>
    <li>Utilized <code>map</code> to apply a function to each element of the RDD.</li>
    <li>Employed <code>flatMap</code> to produce multiple output elements for each input element.</li>
    <li>Used <code>filter</code> to selectively retain elements in the RDD based on a condition.</li>
  </ul>
  <h2>Performed custom transformations:</h2>
  <ul>
    <li>Utilized <code>mapPartitions</code> to apply a function to each partition of the RDD.</li>
    <li>Leveraged <code>glom</code> within <code>mapPartitions</code> to gather all elements of each partition into a single list.</li>
  </ul>
</body>
</html>
