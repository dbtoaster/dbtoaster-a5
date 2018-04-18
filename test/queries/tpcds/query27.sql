-- GENERATED FOR 100GB SCALE

-- Unsupported features for this query
--   ROLLUP and GROUPING (ignored)
--   ORDER BY (ignored)
--   LIMIT    (ignored)

INCLUDE './test/queries/tpcds/schemas.sql';

SELECT i_item_id, s_state, 
       avg(ss_quantity) AS agg1, 
       avg(ss_list_price) AS agg2, 
       avg(ss_coupon_amt) AS agg3, 
       avg(ss_sales_price) AS agg4
  FROM store_sales, customer_demographics, date_dim, store, item
 WHERE ss_sold_date_sk = d_date_sk 
   AND ss_item_sk = i_item_sk 
   AND ss_store_sk = s_store_sk 
   AND ss_cdemo_sk = cd_demo_sk 
   AND cd_gender = 'M' 
   AND cd_marital_status = 'W' 
   AND cd_education_status = 'College' 
   AND d_year = 2002 
   AND s_state IN LIST ('MO','LA', 'GA', 'MI', 'SC', 'OH')
GROUP BY i_item_id, s_state;
