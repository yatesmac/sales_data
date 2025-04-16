import os
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
        .appName("CSVtoParquet") \
        .getOrCreate()


def create_directory(directory):
    """Create directory if it doesn't exist."""
    if not os.path.exists(directory):
        os.makedirs(directory)


def create_dataframe(spark, file_name):
    """Create a Spark DataFrame from a CSV file."""
    return spark.read \
        .option("header", "true") \
        .option("inferSchema", True) \
        .csv(file_name)


def main():
    # Initialize Spark session
    spark = create_spark_session()
    
    # Process each table
    for table in TABLES:
        print(f"Processing {table} data...")
        
        # Extract: Read CSV and create DataFrame
        csv_path = f"data/raw/{table}.csv"
        df = create_dataframe(spark, csv_path)
        
        # Transform: Save to parquet
        parquet_dir = f"data/datalake/{table}"
        create_directory(parquet_dir)
        df.write.mode("overwrite").parquet(f"{parquet_dir}/{table}.parquet")

    # Close spark session
    spark.stop()
    

if __name__ == "__main__":
    main() 