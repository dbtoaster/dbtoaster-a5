-- GENERATED FOR 100GB SCALE

-- Unsupported features for this query
--   AVG OVER (OVER ignored)
--   ABS      (ignored)      FIX THIS!!!
--   ORDER BY (ignored)
--   LIMIT    (ignored)

INCLUDE '../alpha5/test/queries/tpcds/schemas.sql';

SELECT *
FROM (
   SELECT item.i_category, item.i_class, item.i_brand, store.s_store_name, store.s_company_name, date_dim.d_moy, 
          sum(store_sales.ss_sales_price) AS sum_sales, 
          avg(store_sales.ss_sales_price) AS avg_sales
     FROM item, store_sales, date_dim, store
    WHERE ss_item_sk = i_item_sk 
      AND ss_sold_date_sk = d_date_sk 
      AND ss_store_sk = s_store_sk 
      AND d_year IN LIST (2000) 
      AND (
             (i_category IN LIST ('Home','Books','Electronics') 
             AND i_class IN LIST ('wallpaper','parenting','musical'))
          OR
             (i_category IN LIST ('Shoes','Jewelry','Men') 
             AND i_class IN LIST ('womens','birdal','pants') 
          ))
   GROUP BY i_category, i_class, i_brand, s_store_name, s_company_name, d_moy
) tmp1
where (CASE WHEN (avg_sales <> 0) 
       THEN ((sum_sales - avg_sales) / avg_sales) 
       ELSE 0 END) > 0.1;