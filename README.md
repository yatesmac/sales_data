# 🚀 Sales Data ELT Pipeline

A modern data pipeline for processing and analyzing sales data, built with Apache Airflow, PySpark, and PostgreSQL.

[![Python Version](https://img.shields.io/badge/python-3.8%2B-blue)](https://www.python.org/downloads/)
[![Apache Airflow](https://img.shields.io/badge/Airflow-2.7%2B-orange)](https://airflow.apache.org/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-13%2B-blue)](https://www.postgresql.org/)
[![License](https://img.shields.io/badge/license-MIT-green)](LICENSE)

## 📋 Table of Contents

- [Overview](#-overview)
- [Technology Stack](#-technology-stack)
- [Getting Started](#-getting-started)
- [Project Structure](#-project-structure)
- [Data Flow](#-data-flow)
- [Contributing](#-contributing)
- [License](#-license)

## 🌟 Overview

### Challenges and Solutions

Modern businesses face significant challenges in managing and analyzing their sales data. This project provides comprehensive solutions through a robust ELT pipeline:

1. **Data Processing and Integration**
   - **Challenge**: Sales data comes from multiple sources in various formats, making consolidation and analysis difficult
   - **Solution**: 
     - Automated data extraction from multiple sources
     - Efficient data transformation using PySpark
     - Parquet-based storage for optimal performance
     - Automated data quality validation

2. **Data Quality and Consistency**
   - **Challenge**: Ensuring data accuracy and consistency across different systems
   - **Solution**:
     - Schema validation during extraction
     - Data integrity checks during loading
     - Business rule validation in dbt transformations
     - Automated testing of transformed data

3. **Scalability and Performance**
   - **Challenge**: Processing large datasets efficiently as data volumes grow
   - **Solution**:
     - Distributed processing with PySpark
     - Smart loading strategies for large datasets
     - Optimized storage with Parquet format
     - Efficient query performance

4. **Analysis and Insights**
   - **Challenge**: Providing timely access to sales data for decision-making
   - **Solution**:
     - Interactive data exploration with Jupyter notebooks
     - SQL-based transformations with dbt
     - Visualization capabilities with Metabase
     - Custom reporting and dashboards

5. **Operational Management**
   - **Challenge**: Managing complex data pipeline dependencies and monitoring
   - **Solution**:
     - DAG-based workflow orchestration with Airflow
     - Automated scheduling and monitoring
     - Error handling and retry mechanisms
     - Dependency management

## 🛠 Technology Stack

|Component |Tool |Description |
|----------|-----|------------|
|Source |CSV |Raw data files in CSV format|
|Storage |Parquet |Optimized columnar storage format|
|Destination |PostgreSQL |Production database for transformed data|
|Processing |Apache Spark |Distributed data processing|
|Transformation |dbt |SQL-based data transformations|
|Quality Checks |dbt tests |Automated data quality validation|
|Orchestration |Airflow |Workflow scheduling and monitoring|
|Analysis |Jupyter |Interactive data exploration|
|Visualization |Metabase |Business intelligence and dashboards|

## 🚀 Getting Started

### Prerequisites

- Python 3.8 or higher
- Docker and Docker Compose (optional)
- Java 8 or higher (for PySpark)
- 8GB+ RAM
- 20GB+ disk space

### Option 1: Setup with Airflow (Full Pipeline)

1. **Clone the repository**

   ```bash
   git clone https://github.com/yourusername/sales-data-elt.git
   cd sales-data-elt
   ```

2. **Set up the environment**

   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   pip install -r requirements.txt
   ```

3. **Start the services**

   ```bash
   docker-compose up -d
   ```

4. **Initialize Airflow**

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

5. **Start Airflow services**

   ```bash
   airflow webserver -p 8080
   airflow scheduler
   ```

### Option 2: Setup without Airflow (Standalone)

If you prefer to run the pipeline without Airflow, you can use the standalone Python scripts:

1. **Clone and setup environment**

   ```bash
   git clone https://github.com/yourusername/sales-data-elt.git
   cd sales-data-elt
   python -m venv venv
   source venv/bin/activate
   pip install -r requirements.txt
   ```

2. **Start PostgreSQL (if needed)**

   ```bash
   docker-compose up -d postgres
   ```

3. **Run the pipeline manually**

   ```bash
   # Create necessary directories
   python src/create_data_dir.py

   # Run extraction
   python src/extract.py

   # Run loading
   python src/load.py
   ```

4. **Run transformations (optional)**

   ```bash
   # Using dbt
   cd dbt
   dbt run
   ```

## 📁 Project Structure

```mermaid
├── README.md
├── dags
│   └── pipeline.py
├── data
│   ├── datalake
│   ├── metabase_data
│   ├── postgres
│   │   ├── vol-pgadmin_data
│   │   └── vol-pgdata
│   └── raw
│       ├── categories.csv
│       ├── cities.csv
│       ├── countries.csv
│       ├── customers.csv
│       ├── employees.csv
│       ├── products.csv
│       └── sales.csv
├── dbt
│   ├── dbt_project.yml
│   ├── models
│   │   ├── sales_total_data_schema.yml
│   │   ├── sales_total_price.sql
│   │   ├── src_products.sql
│   │   ├── src_sales.sql
│   │   ├── src_sales_data.yml
│   │   └── src_sales_data_schema.yml
│   ├── packages.yml 
│   └── profiles.yml
├── docker-compose.yml
├── notebooks
│   ├── exploration.ipynb
│   ├── extract.ipynb
│   ├── load.ipynb
│   ├── pipeline.ipynb
│   └── visualization.ipynb
├── requirements.txt
└── src
    ├── create_data_dir.py
    ├── extract.py
    ├── load.py
    ├── resources.py
    └── schema.sql
```

## 🔄 Data Flow

### 1. Data Extraction (`src/extract.py`)

- Reads raw CSV files from `data/raw/` directory
- Uses PySpark for efficient data processing
- Converts and saves data in Parquet format
- Stores processed files in `data/datalake/{table_name}/`

### 2. Data Loading (`src/load.py`)

- Reads Parquet files from the datalake
- Implements smart loading strategy:
  - Direct loading for small datasets
  - Chunked loading for large datasets (>100,000 rows)
- Loads data into PostgreSQL tables
- Handles error logging and data validation

### 3. Data Transformation (dbt)

- Applies business logic using dbt models
- Performs data quality checks
- Creates transformed views and tables
- Maintains data lineage

### 4. Data Analysis (Jupyter)

- Interactive data exploration in `notebooks/exploration.ipynb`
- Generates insights and visualizations
- Supports ad-hoc analysis
- Connects to both raw and transformed data

### Data Quality Checks

Throughout the pipeline, several data quality checks are performed:

- Schema validation during extraction
- Data integrity checks during loading
- Business rule validation in dbt transformations
- Automated testing of transformed data

## 🤝 Contributing

We welcome contributions! Please follow these steps:

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- Apache Airflow for workflow orchestration
- PySpark for data processing
- PostgreSQL for data storage
- Metabase for visualization
- The open-source community for their contributions
