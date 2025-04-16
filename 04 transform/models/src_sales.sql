{{ config(materialized='view') }}

SELECT
    "SalesID",
    "SalesPersonID",
    "CustomerID",
    "ProductID",
    "Quantity",
    "Discount",
    "TotalPrice",
    "SalesDate",
    "TransactionNumber"
FROM {{ source('sales_data', 'sales') }}
