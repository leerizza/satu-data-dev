{{
    config(materialized='table', alias='dim_transaksi', description='merupakan data master transaksi')
}}

WITH x AS (
SELECT 
    * 
FROM data-engineering-riza.satu_data_master.fact_transaksi_all)

SELECT
    *, CURRENT_DATE('Asia/Jakarta') AS created_at
FROM x
WHERE x.transaksi_id IS NOT NULL