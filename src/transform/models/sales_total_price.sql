{{ config(materialized='table') }}

SELECT
    s."SalesID",
    s."SalesPersonID",
    s."CustomerID",
    s."ProductID",
    s."Quantity",
    s."Discount",
    -- Calculate new total price:
    (p."Price" * s."Quantity" * s."Discount") AS TotalPrice,
    s."SalesDate",
    s."TransactionNumber"
FROM {{ ref('src_sales') }} AS s
LEFT JOIN {{ ref('src_products') }} AS p
    ON s."ProductID" = p."ProductID"