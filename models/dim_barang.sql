{{ config(materialized='table', alias='dim_barang') }}

WITH x AS (
SELECT 
    * 
FROM `data-engineering-riza.satu_data_master.dim_barang`)

SELECT
    *, CURRENT_DATE('Asia/Jakarta') AS created_at
FROM x
WHERE x.id_barang IS NOT NULL

