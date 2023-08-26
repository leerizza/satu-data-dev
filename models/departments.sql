{{
    config(materialized='table')
}}

WITH x AS (
    SELECT 
        * 
    FROM `databasesuperstore.superstore.departments`
)
SELECT
    *, CURRENT_DATE('Asia/Jakarta') AS created_at
FROM x