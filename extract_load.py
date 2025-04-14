import os
import itertools
import pandas as pd
from pyspark.sql import SparkSession
from sqlalchemy import create_engine

# Database configuration
USER = "postgres"
PASSWORD = "postgres"
HOST = "localhost"
PORT = 5432
DB = "sales_data"

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

def chunk_iterator(iterator, chunk_size):
    """Yield successive chunks of a given size from an iterator."""
    while True:
        chunk = list(itertools.islice(iterator, chunk_size))
        if not chunk:
            break
        yield chunk

def load_to_postgres_in_chunks(spark_df, table_name, engine, chunk_size=100000):
    """Write data from a Spark DataFrame to PostgreSQL in chunks."""
    total_rows = 0
    try:
        iterator = spark_df.toLocalIterator()
        for chunk_index, chunk in enumerate(chunk_iterator(iterator, chunk_size), start=1):
            chunk_dicts = [row.asDict() for row in chunk]
            pdf = pd.DataFrame(chunk_dicts)
            pdf.to_sql(name=table_name, con=engine, if_exists="append", index=False)
            total_rows += len(pdf)
            print(f"Inserted chunk {chunk_index} with {len(pdf)} rows; Total inserted rows: {total_rows}")
        print(f"Successfully wrote data to PostgreSQL table '{table_name}'.")
    except Exception as e:
        print(f"Error processing table {table_name}: {e}")

def load_to_postgres(df_spark, table_name, engine):
    """Load smaller datasets directly to PostgreSQL."""
    try:
        df_pandas = df_spark.toPandas()
        df_pandas.to_sql(table_name, engine, if_exists="replace", index=False)
        print(f"Successfully wrote data to PostgreSQL table '{table_name}'.")
    except Exception as e:
        print(f"Error processing table {table_name}: {e}")

def main():
    # Initialize Spark session
    spark = create_spark_session()
    
    # Initialize database connection
    engine = create_engine(f'postgresql://{USER}:{PASSWORD}@{HOST}:{PORT}/{DB}')
    
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
        
        # Load: Write to PostgreSQL
        print(f"Loading {table} data into PostgreSQL...")
        if df.count() > 100000:
            load_to_postgres_in_chunks(df, table, engine)
        else:
            load_to_postgres(df, table, engine)

if __name__ == "__main__":
    main() 