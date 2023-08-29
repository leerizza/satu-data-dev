SELECT
  * EXCEPT(keterangan)
FROM
  --`satu-data-staging-dev.dbt_project.fact_transaksi_all`
  {{ ref('fact_transaksi_all') }}
WHERE
  transaksi_id IN (
  SELECT
    transaksi_id
  FROM
    --`satu-data-staging-dev.dbt_project.fact_transaksi_all`
    {{ ref('fact_transaksi_all') }}
  GROUP BY
    1
  HAVING
    COUNT(1)=1)
  AND tanggal_transaksi IS NOT NULL
  AND nama_barang IS NOT NULL
  AND tipe_pesanan IS NOT NULL
  AND nama_kasir IS NOT NULL
  AND kategori_barang <> 'Extra Charges'
ORDER BY 1 DESC