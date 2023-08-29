{{ config(materialized='table', alias='dim_status') }}

WITH x AS (
SELECT 
    * 
FROM data-engineering-riza.satu_data_master.dim_status)

SELECT
    *, CURRENT_DATE('Asia/Jakarta') AS created_at
FROM x

