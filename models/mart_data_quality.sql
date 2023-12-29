{{
    config(materialized='table', alias='mart_data_quality', description='merupakan data mart data quality transaksi')
}}


WITH x AS (
  SELECT
    DISTINCT
    transaksi_id,
    tanggal_transaksi,
    id_kasir,
    nama_kasir,
    keterangan
  FROM
    --satu-data-staging-dev.dbt_project.dim_transaksi
    {{ ref('dim_transaksi') }}  
),
y AS (
  SELECT transaksi_id, COUNT(1) AS jumlah
  FROM 
    --satu-data-staging-dev.dbt_project.dim_transaksi
    {{ ref('dim_transaksi') }}
  GROUP BY 1
),
z AS (
  SELECT
  DISTINCT
  transaksi_id, 
  nama_barang,
  kategori_barang
  FROM 
    --satu-data-staging-dev.dbt_project.dim_transaksi
    {{ ref('dim_transaksi') }}
),
a AS (
  SELECT
  DISTINCT
  transaksi_id, 
  id_tipe_pesanan,
  tipe_pesanan,
  id_status,
  status
  FROM 
    --satu-data-staging-dev.dbt_project.dim_transaksi
    {{ ref('dim_transaksi') }}
),
agg AS (
SELECT
  'completeness' AS criteria,
  'transaksi_id null' AS metrics,
  COUNT(DISTINCT x.transaksi_id) AS total_data,
  COUNT(
    CASE
      WHEN transaksi_id IS NOT NULL THEN 1
    END ) 
    AS good_data,
  COUNT(
    CASE
      WHEN transaksi_id IS NULL THEN 1
    END)
    AS bad_data,
  'transaksi_id' AS validasi_kolom
FROM x
UNION ALL
SELECT
  'uniqueness' AS criteria,
  'transaksi_id duplikasi' AS metrics,
  COUNT(y.transaksi_id) AS total_data,
  COUNT(
    CASE
      WHEN y.jumlah = 1 THEN 1
    END )
    AS good_data,
  COUNT(
    CASE
      WHEN y.jumlah >1 THEN 1
    END)
    AS bad_data,
  'transaksi_id' AS validasi_kolom
FROM y
UNION ALL
SELECT
  'completeness' AS criteria,
  'nama_barang null' AS metrics,
  COUNT(z.transaksi_id) AS total_data,
  COUNT(
    CASE
      WHEN z.nama_barang IS NOT NULL THEN 1
    END ) 
    AS good_data,
  COUNT(
    CASE
      WHEN z.nama_barang IS NULL THEN 1
    END)
    AS bad_data,
  'nama_barang' AS validasi_kolom
FROM z
UNION ALL
SELECT
  'completeness' AS criteria,
  'kategori_barang null' AS metrics,
  COUNT(z.transaksi_id) AS total_data,
  COUNT(
    CASE
      WHEN z.kategori_barang IS NOT NULL THEN 1
    END ) 
    AS good_data,
  COUNT(
    CASE
      WHEN z.kategori_barang IS NULL THEN 1
    END)
    AS bad_data,
  'kategori_barang' AS validasi_kolom
FROM z
UNION ALL
SELECT
  'validty' AS criteria,
  'validasi id_tipe_pesanan' AS metrics,
  COUNT(a.transaksi_id) AS total_data,
  COUNT(
    CASE
      WHEN(a.id_tipe_pesanan = 1 AND a.tipe_pesanan = 'Dine-in') THEN 1  
      WHEN(a.id_tipe_pesanan = 2 AND a.tipe_pesanan = 'Takeaway') THEN 1
    END ) 
    AS good_data,
  COUNT(
    CASE
      WHEN(a.id_tipe_pesanan = 1 AND a.tipe_pesanan <> 'Dine-in') THEN 1
      WHEN(a.id_tipe_pesanan = 2 AND a.tipe_pesanan <> 'Takeaway') THEN 1
      WHEN(a.id_tipe_pesanan <> 1 AND a.tipe_pesanan = 'Dine-in') THEN 1
      WHEN(a.id_tipe_pesanan <> 2 AND a.tipe_pesanan = 'Takeaway') THEN 1
      WHEN a.tipe_pesanan IS NULL THEN 1
    END)
    AS bad_data,
  'id_tipe_pesanan' AS validasi_kolom
FROM a
UNION ALL
SELECT
  'completeness' AS criteria,
  'tipe_pesanan null' AS metrics,
  COUNT(a.transaksi_id) AS total_data,
  COUNT(
    CASE
      WHEN a.tipe_pesanan IS NOT NULL THEN 1
    END ) 
    AS good_data,
  COUNT(
    CASE
      WHEN a.tipe_pesanan IS NULL THEN 1
    END)
    AS bad_data,
  'tipe_pesanan' AS validasi_kolom
FROM a
UNION ALL
SELECT
  'completeness' AS criteria,
  'tanggal_transaksi null' AS metrics,
  COUNT(x.transaksi_id) AS total_data,
  COUNT(
    CASE
      WHEN x.tanggal_transaksi IS NOT NULL THEN 1
    END ) 
    AS good_data,
  COUNT(
    CASE
      WHEN x.tanggal_transaksi IS NULL THEN 1
    END)
    AS bad_data,
  'tanggal_transaksi' AS validasi_kolom
FROM x
UNION ALL
SELECT
  'completeness' AS criteria,
  'id_kasir null' AS metrics,
  COUNT(x.transaksi_id) AS total_data,
  COUNT(
    CASE
      WHEN x.id_kasir IS NOT NULL THEN 1
    END ) 
    AS good_data,
  COUNT(
    CASE
      WHEN x.id_kasir IS NULL THEN 1
    END)
    AS bad_data,
  'id_kasir' AS validasi_kolom
FROM x
UNION ALL
SELECT
  'completeness' AS criteria,
  'nama_kasir null' AS metrics,
  COUNT(x.transaksi_id) AS total_data,
  COUNT(
    CASE
      WHEN x.nama_kasir IS NOT NULL THEN 1
    END ) 
    AS good_data,
  COUNT(
    CASE
      WHEN x.nama_kasir IS NULL THEN 1
    END)
    AS bad_data,
  'nama_kasir' AS validasi_kolom
FROM x
UNION ALL
SELECT
  'validty' AS criteria,
  'validasi id_status' AS metrics,
  COUNT(a.transaksi_id) AS total_data,
  COUNT(
    CASE
      WHEN(a.id_status = 200 AND a.status = 'sukses') THEN 1  
      WHEN(a.id_status = 201 AND a.status = 'dikembalikan') THEN 1
      WHEN(a.id_status = 202 AND a.status = 'dibatalkan') THEN 1
    END ) 
    AS good_data,
  COUNT(
    CASE
      WHEN(a.id_status = 200 AND a.status <> 'sukses') THEN 1  
      WHEN(a.id_status = 201 AND a.status <> 'dikembalikan') THEN 1
      WHEN(a.id_status = 202 AND a.status <> 'dibatalkan') THEN 1
      WHEN(a.id_status = 400 AND a.status = 'dikembalikan') THEN 1
      WHEN(a.id_status <> 200 AND a.status = 'sukses') THEN 1  
      WHEN(a.id_status <> 201 AND a.status = 'dikembalikan') THEN 1
      WHEN(a.id_status <> 202 AND a.status = 'dibatalkan') THEN 1
      WHEN a.status IS NULL THEN 1
      WHEN a.id_status IS NULL THEN 1
    END) + 1
    AS bad_data,
  'id_status' AS validasi_kolom
FROM a
UNION ALL
SELECT
  'completeness' AS criteria,
  'status null' AS metrics,
  COUNT(a.transaksi_id) AS total_data,
  COUNT(
    CASE
      WHEN a.status IS NOT NULL THEN 1
    END ) 
    AS good_data,
  COUNT(
    CASE
      WHEN a.status IS NULL THEN 1
    END)
    AS bad_data,
  'status' AS validasi_kolom
FROM a
UNION ALL
SELECT
  'completeness' AS criteria,
  'keterangan null' AS metrics,
  COUNT(x.transaksi_id) AS total_data,
  COUNT(
    CASE
      WHEN x.keterangan IS NOT NULL THEN 1
    END ) 
    AS good_data,
  COUNT(
    CASE
      WHEN x.keterangan IS NULL THEN 1
    END)
    AS bad_data,
  'keterangan' AS validasi_kolom
FROM x
)

SELECT
  criteria,
  metrics,
  IFNULL(total_data, 0) AS total_data,
  IFNULL(total_data, 0) - IFNULL(bad_data, 0) AS good_data,
  IFNULL(bad_data, 0) AS bad_data,
  SAFE_CAST(
    (IFNULL(total_data, 0) - IFNULL(bad_data, 0)) * 100
      /
    (CASE WHEN IFNULL(total_data, 0) = 0 THEN 1 ELSE IFNULL(total_data, 0) END) AS NUMERIC
  ) AS percentage_good_data,
  100 - SAFE_CAST(
    (IFNULL(total_data, 0) - IFNULL(bad_data, 0)) * 100
      /
    (CASE WHEN IFNULL(total_data, 0) = 0 THEN 1 ELSE IFNULL(total_data, 0) END) AS NUMERIC
  ) AS percentage_bad_data,
  validasi_kolom,
  DATETIME(CURRENT_TIMESTAMP(), 'Asia/Jakarta') AS time_execution
FROM
  agg
