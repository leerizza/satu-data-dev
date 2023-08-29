{{
    config(materialized='table', alias='fact_transaksi_20230502', description='merupakan data transaksi bulan mei')
}}

WITH x AS (
SELECT 
    * 
FROM data-engineering-riza.satu_data_master.fact_transaksi_20230502)

SELECT
    *, CURRENT_DATE('Asia/Jakarta') AS created_at
FROM x
WHERE x.transaksi_id IS NOT NULL


-- {% set kategori_barang = ["Tea and Chocolate", "Cake", "Chilled","Baked","Coffee","Other","Package","Drinks"] %}

-- select
--     transaksi_id,
--     {% for kategori_barang in kategori_barangs %}
--     sum(case when kategori_barang = '{{kategori_barang}}' then total end) as {{kategori_barang}}_total,
--     {% endfor %}
--     sum(total) as total_amount
-- from data-engineering-riza.satu_data_master.fact_transaksi_20230502
-- group by 1