-- GENERATED FOR 1GB SCALE

-- Unsupported features for this query
--   ORDER BY (ignored)
--   LIMIT    (ignored)

INCLUDE '../alpha5/test/queries/tpcds/schemas.sql';

SELECT dt.d_year, item.i_brand_id AS brand_id, item.i_brand AS brand, sum(ss_ext_sales_price) AS sum_agg
  FROM date_dim dt, store_sales, item
 WHERE dt.d_date_sk = store_sales.ss_sold_date_sk
   AND store_sales.ss_item_sk = item.i_item_sk
   AND item.i_manufact_id = 436
   AND dt.d_moy=12
 GROUP BY dt.d_year, item.i_brand, item.i_brand_id;
