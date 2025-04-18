import os
from resources import create_spark_session, create_dataframe, TABLES


def create_directory(directory):
    """Create directory if it doesn't exist."""
    if not os.path.exists(directory):
        os.makedirs(directory)


def main():
    """Extract data from CSV files and save as parquet format."""
    # Initialize Spark session
    spark = create_spark_session()
    
    # Process each table
    for table in TABLES:
        print(f"Processing {table} data...")
        
        # Read CSV and create DataFrame
        csv_path = f"../data/raw/{table}.csv"
        df = create_dataframe(spark, csv_path)
        
        # Save to parquet format
        parquet_dir = f"../data/datalake/{table}"
        create_directory(parquet_dir)
        df.write.mode("overwrite").parquet(f"{parquet_dir}/{table}.parquet")

    # Clean up
    spark.stop()
    

if __name__ == "__main__":
    main() 