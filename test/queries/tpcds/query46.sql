-- GENERATED FOR 100GB SCALE

-- Unsupported features for this query
--   ORDER BY (ignored)
--   LIMIT    (ignored)
--   LIST VALUES (inlined)

INCLUDE '../alpha5/test/queries/tpcds/schemas.sql';

SELECT c_last_name, c_first_name, current_addr.ca_city, bought_city, ss_ticket_number, amt, profit 
  FROM
    (SELECT store_sales.ss_ticket_number, store_sales.ss_customer_sk, customer_address.ca_city AS bought_city, 
        sum(store_sales.ss_coupon_amt) AS amt, 
        sum(store_sales.ss_net_profit) AS profit
       FROM store_sales, date_dim, store, household_demographics, customer_address 
      WHERE store_sales.ss_sold_date_sk = date_dim.d_date_sk
    AND store_sales.ss_store_sk = store.s_store_sk  
    AND store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk
    AND store_sales.ss_addr_sk = customer_address.ca_address_sk
    AND (household_demographics.hd_dep_count = 6 OR
         household_demographics.hd_vehicle_count= 3)
    AND date_dim.d_dow IN LIST (6,0)
    AND date_dim.d_year IN LIST (1999,2000,2001) 
    AND store.s_city IN LIST ('Oakland','Riverside','Union','Salem','Greenwood') 
    GROUP BY ss_ticket_number, ss_customer_sk, ss_addr_sk, customer_address.ca_city
  ) dn, customer, customer_address AS current_addr
WHERE ss_customer_sk = c_customer_sk
  AND customer.c_current_addr_sk = current_addr.ca_address_sk
  AND current_addr.ca_city <> bought_city;