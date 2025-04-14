# Sales Data

## About Dataset

Grocery Sales Database - Data Card

### Download Dataset

The dataset can be accessed on [Kaggle](https://www.kaggle.com/datasets/andrexibiza/grocery-sales-dataset/data). 
A copy of the dataset has been added to the repo (in the `data/raw` directory). If not present the dataset can be downloaded as follows:

From the project's root directory:

```bash
curl -L -o data/raw/grocery-sales-dataset.zip\
  https://www.kaggle.com/api/v1/datasets/download/andrexibiza/grocery-sales-dataset
```

### Overview

The Grocery Sales Database is a structured relational dataset designed for analyzing sales transactions, customer demographics, product details, employee records, and geographical information across multiple cities and countries. This dataset is ideal for data analysts, data scientists, and machine learning practitioners looking to explore sales trends, customer behaviors, and business insights.

### Database Schema

The dataset consists of seven interconnected tables:

|File Name |Description |
|----------|------------|
|categories.csv |Defines the categories of the products. |
|cities.csv |Contains city-level geographic data. |
|countries.csv |Stores country-related metadata. |
|customers.csv |Contains information about the customers who make purchases. |
|employees.csv |Stores details of employees handling sales transactions. |
|products.csv |Stores details about the products being sold. |
|sales.csv |Contains transactional data for each sale. |
