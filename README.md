<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
 
</head>
<body>
  <h1>Databricks Utilities:</h1>
  <h2>Widgets:</h2>
  <p>Used <code>dbutils.widgets</code> to create interactive widgets for parameterizing notebooks.</p>
  <h2>Credentials:</h2>
  <p>Accessed and managed secrets, tokens, and other sensitive information securely using <code>dbutils.credentials</code>.</p>
  <h2>Data Summarization:</h2>
  <p>Summarized data efficiently using <code>dbutils.data.summarize</code> and <code>dbutils.data.summarize(precise=True)</code> for detailed summaries.</p>

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
    <li>Renamed columns using <code>withColumnRenamed</code> to change column names in the DataFrame.</li>
    <li>Used <code>select</code> to select specific columns from the DataFrame.</li>
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
  <h2>Worked with RDD actions:</h2>
  <ul>
    <li>Used <code>count()</code> action to count the number of elements in the RDD.</li>
    <li>Applied <code>min()</code> action to find the minimum element in the RDD.</li>
    <li>Utilized <code>max()</code> action to find the maximum element in the RDD.</li>
    <li>Used <code>mean()</code> action to calculate the mean (average) of the elements in the RDD.</li>
    <li>Applied <code>countByKey()</code> action to count the number of occurrences of each key in an RDD of key-value pairs.</li>
    <li>Utilized <code>countByValue()</code> action to count the number of occurrences of each unique value in the RDD.</li>
    <li>Used <code>fold()</code> action to aggregate the elements of the RDD using a given associative function and a neutral "zero value".</li>
  </ul>
  <h2>Worked with DataFrame actions:</h2>
  <ul>
    <li>Used <code>collect()</code> to retrieve all elements of the DataFrame to the driver program.</li>
    <li>Applied <code>show()</code> to display the contents of the DataFrame in a tabular format.</li>
    <li>Utilized <code>take()</code> to return an array with the first n elements of the DataFrame.</li>
    <li>Used <code>head()</code> to return the first element of the DataFrame.</li>
  </ul>
  <h2>Difference between collect, show, take, head:</h2>
  <p><strong>collect():</strong> Retrieves all elements of the DataFrame to the driver program. It should be used cautiously as it collects all data to the driver and may cause out-of-memory errors if the dataset is large.</p>
  <p><strong>show():</strong> Displays the contents of the DataFrame in a tabular format, typically limited to a default number of rows. It provides a more human-readable view of the data and is suitable for exploration and visualization.</p>
  <p><strong>take():</strong> Returns an array with the first n elements of the DataFrame. It can be useful for quickly inspecting a subset of the data.</p>
  <p><strong>head():</strong> Returns the first element of the DataFrame. It is often used to quickly peek at the structure of the data or to check the first record.</p>

  <h1>Read and Write Operations:</h1>
  <h2>JSON:</h2>
  <ul>
    <li>Read JSON files using <code>spark.read.json()</code>.</li>
    <li>Write DataFrame to JSON using <code>df.write.json()</code> with options for mode (overwrite, append, etc.).</li>
  </ul>
  <h2>CSV:</h2>
  <ul>
    <li>Read CSV files using <code>spark.read.csv()</code>.</li>
    <li>Write DataFrame to CSV using <code>df.write.csv()</code> with options for mode (overwrite, append, etc.).</li>
  </ul>
  <h2>Parquet:</h2>
  <ul>
    <li>Read Parquet files using <code>spark.read.parquet()</code>.</li>
    <li>Write DataFrame to Parquet using <code>df.write.parquet()</code> with options for mode (overwrite, append, etc.).</li>
  </ul>
</body>
</html>
