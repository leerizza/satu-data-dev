{{
    config(materialized='table', description='merupakan data master order')
}}

WITH x AS (
SELECT 
    * 
FROM `databasesuperstore.superstore.ref_order`)

SELECT
    *, CURRENT_DATE('Asia/Jakarta') AS created_at
FROM x