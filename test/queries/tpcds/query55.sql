-- GENERATED FOR 100GB SCALE

-- Unsupported features for this query
--   ORDER BY (ignored)
--   LIMIT    (ignored)

INCLUDE './test/queries/tpcds/schemas.sql';

SELECT i_brand_id AS brand_id, i_brand AS brand, 
       sum(ss_ext_sales_price) AS ext_price
FROM date_dim, store_sales, item
WHERE d_date_sk = ss_sold_date_sk
    AND ss_item_sk = i_item_sk
    AND i_manager_id=36
    AND d_moy=12
    AND d_year=2001
GROUP BY i_brand, i_brand_id;
