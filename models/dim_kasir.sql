{{ config(materialized='table', alias='dim_kasir') }}

WITH x AS (
SELECT 
    * 
FROM data-engineering-riza.satu_data_master.dim_kasir)

SELECT
    *, CURRENT_DATE('Asia/Jakarta') AS created_at
FROM x

