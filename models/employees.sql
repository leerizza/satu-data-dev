{{
    config(materialized='table', description='merupakan data master employees')
}}

WITH x AS (
SELECT 
    * 
FROM `databasesuperstore.superstore.employees`)

SELECT
    *, CURRENT_DATE('Asia/Jakarta') AS created_at
FROM x