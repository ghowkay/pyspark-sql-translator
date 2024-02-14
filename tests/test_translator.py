import pytest
from pyspark.sql import SparkSession
from pyspark_sql_translator.translator import dataframe_to_sql

# Initialize Spark session for testing
@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.appName("PySparkSQLTranslatorTest").getOrCreate()

def test_select_single_column(spark):
    df = spark.createDataFrame([(1, "foo"), (2, "bar")], ["id", "value"])
    df_transformed = df.select("id")
    sql_query = dataframe_to_sql(df_transformed)
    assert sql_query == "SELECT id FROM table"  # Replace with actual table name

def test_select_multiple_columns(spark):
    df = spark.createDataFrame([(1, "foo"), (2, "bar")], ["id", "value"])
    df_transformed = df.select("id", "value")
    sql_query = dataframe_to_sql(df_transformed)
    assert sql_query == "SELECT id, value FROM table"

def test_filter_condition(spark):
    df = spark.createDataFrame([(1, "foo"), (2, "bar")], ["id", "value"])
    df_transformed = df.where(df.value == "foo")
    sql_query = dataframe_to_sql(df_transformed)
    assert sql_query == "SELECT * FROM table WHERE value = 'foo'"

# Add more tests here to cover different cases like joins, group by, etc.
