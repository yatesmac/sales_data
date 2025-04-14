# Sales Data ELT Pipeline

This project implements an ELT (Extract, Load, Transform) pipeline for sales data using Apache Airflow, PySpark, and PostgreSQL. The pipeline processes sales data from various sources, performs data quality checks, and loads it into a PostgreSQL database for analysis.

## Project Structure

```
.
├── dags/                    :   Airflow DAG definitions
│   └── sales_data_elt.py    :   Main ELT workflow DAG
├── data/                    :   Data directory
│   ├── raw/                 :  Raw CSV files
│   └── datalake/            :  Processed parquet files
│       ├── categories/      :  Category data in parquet format
│       ├── cities/          :  City data in parquet format
│       ├── countries/       :  Country data in parquet format
│       ├── customers/       :  Customer data in parquet format
│       ├── employees/       :  Employee data in parquet format
│       ├── products/        :  Product data in parquet format
│       └── sales/           :  Sales data in parquet format
├── extract/                 :  Extraction related files
│   └── extract.ipynb        :   Jupyter notebook for data extraction
├── load/                    :  Loading related files
│   ├── load.ipynb           :   Jupyter notebook for data loading
│   └── schema.sql           :   Database schema definition
├── transform/               :  Transformation related files
│   └── models/              :   Data transformation models
│       └── src_sales.sql    :   Sales data transformation
├── explore/                 :   Data exploration files
│   └── explore.ipynb        :  Jupyter notebook for data analysis
├── extract_load.py          :  Main ELT script
├── requirements.txt         :  Python dependencies
└── README.md                :   Project documentation
```

## Dataset Information

The dataset consists of seven interconnected tables that form a complete sales database:

1. **categories.csv**
   - CategoryID (INT, PRIMARY KEY)
   - CategoryName (VARCHAR)

2. **cities.csv**
   - CityID (INT, PRIMARY KEY)
   - CityName (VARCHAR)
   - Zipcode (NUMERIC)
   - CountryID (INT, FOREIGN KEY)

3. **countries.csv**
   - CountryID (INT, PRIMARY KEY)
   - CountryName (VARCHAR)
   - CountryCode (VARCHAR)

4. **customers.csv**
   - CustomerID (INT, PRIMARY KEY)
   - FirstName (VARCHAR)
   - MiddleInitial (VARCHAR)
   - LastName (VARCHAR)
   - CityID (INT, FOREIGN KEY)
   - Address (VARCHAR)

5. **employees.csv**
   - EmployeeID (INT, PRIMARY KEY)
   - FirstName (VARCHAR)
   - MiddleInitial (VARCHAR)
   - LastName (VARCHAR)
   - BirthDate (DATE)
   - Gender (VARCHAR)
   - CityID (INT, FOREIGN KEY)
   - HireDate (DATE)

6. **products.csv**
   - ProductID (INT, PRIMARY KEY)
   - ProductName (VARCHAR)
   - Price (DECIMAL)
   - CategoryID (INT, FOREIGN KEY)
   - Class (VARCHAR)
   - ModifyDate (DATE)
   - Resistant (VARCHAR)
   - IsAllergic (VARCHAR)
   - VitalityDays (NUMERIC)

7. **sales.csv**
   - SalesID (INT, PRIMARY KEY)
   - SalesPersonID (INT, FOREIGN KEY)
   - CustomerID (INT, FOREIGN KEY)
   - ProductID (INT, FOREIGN KEY)
   - Quantity (INT)
   - Discount (NUMERIC)
   - TotalPrice (DECIMAL)
   - SalesDate (DATETIME)
   - TransactionNumber (VARCHAR)

## Data Quality Checks

The pipeline includes several data quality checks:

1. **Schema Validation**
   - Verifies data types match expected schema
   - Checks for required fields
   - Validates foreign key relationships

2. **Data Completeness**
   - Checks for missing values
   - Validates required fields are populated
   - Ensures referential integrity

3. **Data Consistency**
   - Validates date formats
   - Checks numeric ranges
   - Ensures categorical values match expected domains

4. **Business Rules**
   - Validates discount calculations
   - Ensures price consistency
   - Checks transaction uniqueness

## Prerequisites

- Python 3.8+
- Docker and Docker Compose
- Java 8+ (required for PySpark)
- Apache Airflow 2.7+
- 8GB+ RAM (recommended for processing large datasets)
- 20GB+ disk space

## Setup

1. Create a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Initialize Airflow:
```bash
airflow db init
airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password admin
```

4. Start Airflow:
```bash
airflow webserver -p 8080
airflow scheduler
```

5. Start the services using Docker Compose:
```bash
docker-compose up -d
```

6. Configure Airflow connections:
- Open Airflow UI (http://localhost:8080)
- Go to Admin > Connections
- Add a new PostgreSQL connection:
  - Connection Id: postgres_default
  - Connection Type: Postgres
  - Host: postgres (Docker service name)
  - Schema: sales_data
  - Login: postgres
  - Password: postgres
  - Port: 5432

## Usage

1. Place your CSV files in the `data/raw` directory with the following names:
   - categories.csv
   - cities.csv
   - countries.csv
   - customers.csv
   - employees.csv
   - products.csv
   - sales.csv

2. The DAG will run automatically on a daily schedule, or you can trigger it manually from the Airflow UI.

## DAG Tasks

1. `create_directories`: Creates necessary data directories
2. `create_database`: Creates the PostgreSQL database in the Docker container
3. `create_schema`: Creates database tables using the schema.sql file mounted in the container
4. `run_extract_load`: Runs the main ELT process

## Monitoring

- Access the Airflow UI at http://localhost:8080
- Monitor DAG runs, task status, and logs
- View the PostgreSQL database to verify data loading
- Check Docker container logs: `docker logs postgres`

## Performance Considerations

1. **Memory Management**
   - Large datasets are processed in chunks
   - PySpark configuration optimized for available memory
   - Batch processing for database operations
   - Docker container resource limits can be adjusted in docker-compose.yml

2. **Error Handling**
   - Automatic retries for failed tasks
   - Detailed error logging
   - Transaction rollback on failure
   - Docker container health checks

3. **Scalability**
   - Parallel processing of independent tables
   - Configurable batch sizes
   - Efficient data partitioning
   - Docker container scaling options

## Troubleshooting

Common issues and solutions:

1. **Memory Errors**
   - Increase Spark driver memory
   - Reduce batch size
   - Check available system resources
   - Adjust Docker container memory limits

2. **Database Connection Issues**
   - Verify PostgreSQL container is running: `docker ps`
   - Check container logs: `docker logs postgres`
   - Verify network connectivity between Airflow and PostgreSQL
   - Ensure proper Docker networking configuration

3. **Data Quality Issues**
   - Review error logs
   - Check data validation rules
   - Verify source data format
   - Check container volume mounts

## Contributing

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details.
