{{ config(materialized='view') }}

SELECT
    "ProductID",
    "ProductName",
    "Price",
    "CategoryID",
    "ModifyDate",
    "Resistant",
    "IsAllergic",
    "VitalityDays"
FROM {{ source('sales_data', 'products') }}
