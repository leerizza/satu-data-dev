{{ config(materialized='table', alias='dim_tipe_pesanan') }}

WITH x AS (
SELECT 
    * 
FROM data-engineering-riza.satu_data_master.dim_tipe_pesanan)

SELECT
    *, CURRENT_DATE('Asia/Jakarta') AS created_at
FROM x

