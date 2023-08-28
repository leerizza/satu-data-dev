{{
    config(materialized='table', description='merupakan data transaksi bulan mei')
}}

WITH x AS (
SELECT 
    * 
FROM `data-engineering-riza.satu_data_master.fact_transaksi_20230501`)

SELECT
    *, CURRENT_DATE('Asia/Jakarta') AS created_at
FROM x
WHERE x.transaksi_id IS NOT NULL