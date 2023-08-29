{{
    config(materialized='table', alias='fact_transaksi_all', description='merupakan data fact transaksi bulan mei juni juli')
}}


WITH t AS (
  SELECT
    *
  FROM
    --`satu-data-staging-dev.dbt_project.dim_transaksi`
    {{ ref('dim_transaksi') }}
  ORDER BY 1  
),
b AS (
  SELECT
    id_barang,
    nama_barang,
    kategori,
    harga
  FROM
    --`satu-data-staging-dev.dbt_project.dim_barang`  
    {{ ref('dim_barang') }}
),
k AS (
  SELECT
    id_kasir,
    nama_kasir
  FROM
    --`satu-data-staging-dev.dbt_project.dim_kasir`  
    {{ ref('dim_kasir') }}
),
s AS (
  SELECT
    id_status,
    status
  FROM
    --`satu-data-staging-dev.dbt_project.dim_status`  
    {{ ref('dim_status') }}
),
p AS (
  SELECT
    id_tipe_pesanan,
    tipe_pesanan
  FROM
    --`satu-data-staging-dev.dbt_project.dim_tipe_pesanan`  
    {{ ref('dim_tipe_pesanan') }}
)


SELECT
  t.transaksi_id,
  b.nama_barang,
  b.kategori AS kategori_barang,
  p.tipe_pesanan,
  t.satuan,
  b.harga,
  t.ongkir,
  t.total,
  t.profit,
  PARSE_DATETIME('%d-%m-%Y %H:%M:%S', t.tanggal_transaksi) AS tanggal_transaksi,
  k.nama_kasir,
  s.status,
  t.keterangan
FROM 
  t
LEFT JOIN b ON t.id_barang = b.id_barang
LEFT JOIN k ON t.id_kasir = k.id_kasir
LEFT JOIN s ON t.id_status = s.id_status
LEFT JOIN p ON t.id_tipe_pesanan = p.id_tipe_pesanan  
ORDER BY 1 DESC


