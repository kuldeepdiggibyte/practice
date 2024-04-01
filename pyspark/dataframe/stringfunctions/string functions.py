from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("string").getOrCreate()
import pyspark.sql.functions as F

# Sample DataFrame
data = [("John  ", "Doe", "123-456-7890"),
        ("Jane", "Doe", "987-654-3210"),
        ("   Alice   ", "Smith", "555-555-1234")]

columns = ["first_name", "last_name", "phone_number"]

df = spark.createDataFrame(data, columns)

# Applying string functions
df = df.withColumn("first_name_upper", F.upper(df["first_name"]))
df = df.withColumn("last_name_trim", F.trim(df["last_name"]))
df = df.withColumn("first_name_ltrim", F.ltrim(df["first_name"]))
df = df.withColumn("last_name_rtrim", F.rtrim(df["last_name"]))
df = df.withColumn("phone_number_translate", F.translate(df["phone_number"], "-", " "))
df = df.withColumn("first_name_substring_index", F.substring_index(df["first_name"], " ", 1))
df = df.withColumn("last_name_substring", F.substring(df["last_name"], 2, 3))
df = df.withColumn("first_name_split", F.split(df["first_name"], " "))
df = df.withColumn("last_name_repeat", F.repeat(df["last_name"], 2))
df = df.withColumn("first_name_rpad", F.rpad(df["first_name"], 10, "X"))
df = df.withColumn("last_name_lpad", F.lpad(df["last_name"], 10, "Y"))
df = df.withColumn("last_name_regex_replace", F.regexp_replace(df["last_name"], "[aeiou]", "X"))
df = df.withColumn("first_name_lower", F.lower(df["first_name"]))
df = df.withColumn("last_name_regex_extract", F.regexp_extract(df["last_name"], "(.)", 1))

# Showing the DataFrame
df.show(truncate=False)
