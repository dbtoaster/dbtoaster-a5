-- GENERATED FOR 1GB SCALE

-- Unsupported features for this query
--   AVG OVER (OVER ignored)
--   ABS     (ignored)      FIX THIS!!!
--   ORDER BY (ignored)
--   LIMIT    (ignored)

INCLUDE '../alpha5/test/queries/tpcds/schemas.sql';

SELECT * 
  FROM (
    SELECT i_manufact_id, sum(ss_sales_price) AS sum_sales, avg(ss_sales_price) AS avg_sales
    FROM item, store_sales, date_dim, store
   WHERE ss_item_sk = i_item_sk 
     AND ss_sold_date_sk = d_date_sk 
     AND ss_store_sk = s_store_sk 
     AND d_month_seq in (1212,1213,1214,1215,1216,1217,1218,1219,1220,1221,1222,1223 
     AND (
        (i_category IN LIST ('Books','Children','Electronics') 
        AND i_class IN LIST ('personal','portable','reference','self-help') 
        AND i_brand IN LIST ('scholaramalgamalg #14','scholaramalgamalg #7', 'exportiunivamalg #9','scholaramalgamalg #9')) 
        OR
        (i_category IN LIST ('Women','Music','Men') 
        AND i_class IN LIST ('accessories','classical','fragrances','pants')
        AND i_brand IN LIST ('amalgimporto #1','edu packscholar #1','exportiimporto #1', 'importoamalg #1'))
      )
    GROUP BY i_manufact_id, d_qoy 
  ) tmp1
WHERE (CASE WHEN avg_quarterly_sales > 0 
       THEN (sum_sales - avg_quarterly_sales) / avg_quarterly_sales 
       ELSE 0 END) > 0.1;
