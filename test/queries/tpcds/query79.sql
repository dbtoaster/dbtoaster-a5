-- GENERATED FOR 100GB SCALE

-- Unsupported features for this query
--   ORDER BY (ignored)
--   LIMIT    (ignored)
--   LIST VALUES (inlined)

INCLUDE '../alpha5/test/queries/tpcds/schemas.sql';

SELECT c_last_name, c_first_name, SUBSTRING(s_city, 1, 30), ss_ticket_number, amt, profit
  FROM
   (SELECT store_sales.ss_ticket_number, store_sales.ss_customer_sk, store.s_city, 
           sum(store_sales.ss_coupon_amt) AS amt, 
           sum(store_sales.ss_net_profit) AS profit
      FROM store_sales, date_dim, store, household_demographics
     WHERE store_sales.ss_sold_date_sk = date_dim.d_date_sk
       AND store_sales.ss_store_sk = store.s_store_sk  
       AND store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk
       AND (household_demographics.hd_dep_count = 8 OR household_demographics.hd_vehicle_count > 0)
       AND date_dim.d_dow = 1
       AND date_dim.d_year IN LIST (1998,1999,2000) 
       AND store.s_number_employees BETWEEN 200 AND 295
    GROUP BY ss_ticket_number, ss_customer_sk, ss_addr_sk, store.s_city
  ) ms, customer
WHERE ss_customer_sk = c_customer_sk;