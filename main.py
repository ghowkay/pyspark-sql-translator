from pyspark.sql import SparkSession
from translator import dataframe_to_sql
from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.dataframe import DataFrame

# Initialize Spark session
spark = SparkSession.builder.appName("example").getOrCreate()

# Create a DataFrame
df = spark.createDataFrame([(1, "foo"), (2, "bar")], ["id", "value"])

# Perform some operations
df_transformed = df.select("id", "value").where("value = 'foo'")


data = [
    ('test1', 'value1', 'value2'),
    ('no_test', 'value3', 'value4'),
    ('something_test', 'value5', 'value6')
    # Add more rows as needed
]

# Define the schema, if needed
schema = StructType([
    StructField("a", StringType(), True),
    StructField("b", StringType(), True),
    StructField("c", StringType(), True)
])

# Create DataFrame
df = spark.createDataFrame(data, schema)

# Select and filter operations
data = df.select('a', 'b', 'c').where(f.col('a').like('%test%'))

# Translate to SQL
sql_query = dataframe_to_sql(df_transformed)
print(sql_query)
