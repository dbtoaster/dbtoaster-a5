-- GENERATED FOR 100GB SCALE

-- Unsupported features for this query
--   ORDER BY (ignored)
--   LIMIT    (ignored)
--   LIST VALUES (inlined)

INCLUDE '../alpha5/test/queries/tpcds/schemas.sql';

SELECT c_last_name, c_first_name, c_salutation, c_preferred_cust_flag, ss_ticket_number, cnt 
  FROM
   (SELECT store_sales.ss_ticket_number, store_sales.ss_customer_sk, count(*) AS cnt
      FROM store_sales, date_dim, store, household_demographics
     WHERE store_sales.ss_sold_date_sk = date_dim.d_date_sk
       AND store_sales.ss_store_sk = store.s_store_sk  
       AND store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk
       AND ((date_dim.d_dom BETWEEN 1 AND 3) OR 
            (date_dim.d_dom BETWEEN 25 AND 28))
       AND (household_demographics.hd_buy_potential = '>10000' or
            household_demographics.hd_buy_potential = '5001-10000')
       AND household_demographics.hd_vehicle_count > 0
       AND (CASE WHEN household_demographics.hd_vehicle_count > 0 
            THEN household_demographics.hd_dep_count / household_demographics.hd_vehicle_count 
            ELSE 0 END)  > 1.2
       AND date_dim.d_year IN LIST (1999,2000,2001)
       AND store.s_county IN LIST ('Williamson County','Franklin Parish','Barrow County','Luce County', 'Fairfield County','Richland County','Ziebach County','Walker County')
    GROUP BY ss_ticket_number,ss_customer_sk) dn, customer
WHERE ss_customer_sk = c_customer_sk AND cnt BETWEEN 15 AND 20
