from pyspark.sql import SparkSession

# List of tables to process
TABLES = [
    "categories",
    "cities",
    "countries",
    "customers",
    "employees",
    "products",
    "sales"
]


def create_spark_session():
    """Create and return a Spark session."""
    return SparkSession.builder \
        .config("spark.driver.memory", "12g") \
        .appName("CSVtoParquetToPostgres") \
        .getOrCreate()


def create_dataframe(spark, file_name):
    """Create a Spark DataFrame from a CSV file."""
    try:
        if file_name.endswith(".csv"):
            return spark.read \
                .option("header", "true") \
                .option("inferSchema", True) \
                .csv(file_name)
        elif file_name.endswith(".parquet"):
            return spark.read.parquet(file_name)
    except Exception as e:
        print(f"Error reading file {file_name}: {e}")