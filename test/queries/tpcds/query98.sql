-- GENERATED FOR 1GB SCALE

-- Unsupported features for this query
--   ORDER BY (ignored)
--   LIMIT    (ignored)

INCLUDE '../alpha5/test/queries/tpcds/schemas.sql';

SELECT i_item_desc, i_category, i_class, i_current_price, 
       sum(ss_ext_sales_price) AS itemrevenue 
  FROM  store_sales, item, date_dim
 WHERE ss_item_sk = i_item_sk 
   AND i_category IN LIST ('Jewelry', 'Sports', 'Books')
   AND ss_sold_date_sk = d_date_sk
   AND d_date BETWEEN DATE('2001-01-01') AND DATE('2001-01-31')
GROUP BY i_item_id, i_item_desc, i_category, i_class, i_current_price;