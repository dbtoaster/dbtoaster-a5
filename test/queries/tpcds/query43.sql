-- GENERATED FOR 100GB SCALE

-- Unsupported features for this query
--   ORDER BY (ignored)
--   LIMIT    (ignored)

INCLUDE './test/queries/tpcds/schemas.sql';

SELECT s_store_name, s_store_id,
       sum(case when (d_day_name='Sunday') then ss_sales_price else 0 end) AS sun_sales,
       sum(case when (d_day_name='Monday') then ss_sales_price else 0 end) AS mon_sales,
       sum(case when (d_day_name='Tuesday') then ss_sales_price else 0 end) AS tue_sales,
       sum(case when (d_day_name='Wednesday') then ss_sales_price else 0 end) AS wed_sales,
       sum(case when (d_day_name='Thursday') then ss_sales_price else 0 end) AS thu_sales,
       sum(case when (d_day_name='Friday') then ss_sales_price else 0 end) AS fri_sales,
       sum(case when (d_day_name='Saturday') then ss_sales_price else 0 end) AS sat_sales
  FROM date_dim, store_sales, store
 WHERE d_date_sk = ss_sold_date_sk 
   AND s_store_sk = ss_store_sk 
   AND s_gmt_offset = -6 
   AND d_year = 1998 
 GROUP BY s_store_name, s_store_id;
