{{ config(materialized='view') }}

SELECT
    ProductID,
    ProductName,
    Price,
    CategoryID,
    ModifyDate,
    Resistant,
    IsAllergic,
    VitalityDays
FROM {{ source('total_sales', 'products') }}
