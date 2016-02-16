(** Provide hard-coded partitioning information for TPC-H queries *)
type ('key_t) gen_part_info_t = 
  | Local                         (* local result *)
  | DistributedRandom             (* randomly partitioned result *)
  | DistributedByKey of 'key_t list  (* key-partitioned result *)

type ('key_t) gen_part_table_t = (string, ('key_t) gen_part_info_t) Hashtbl.t

let create_hash_table (pairs: ('k * 'v) list) = 
   let hash_table = Hashtbl.create (List.length pairs) in
   List.iter (fun (k, v) -> Hashtbl.add hash_table k v) pairs;
   hash_table

(* let local_part_table_by_file () = 

   let tpch1_part_table = 
      create_hash_table [
         ("SUM_QTY",                          Local);
         ("SUM_QTYLINEITEM1_DELTA",           Local);
         ("SUM_BASE_PRICE",                   Local);
         ("SUM_BASE_PRICELINEITEM1_DELTA",    Local);
         ("SUM_DISC_PRICE",                   Local);
         ("SUM_DISC_PRICELINEITEM1_DELTA",    Local);
         ("SUM_CHARGE",                       Local);
         ("SUM_CHARGELINEITEM1_DELTA",        Local);
         ("AVG_QTY",                          Local);
         ("AVG_QTYLINEITEM1_DOMAIN1",         Local);
         ("AVG_QTYLINEITEM1_L1_1",            Local);
         ("AVG_QTYLINEITEM2",                 Local);
         ("AVG_QTYLINEITEM2_L1_2_DELTA",      Local);
         ("AVG_PRICE",                        Local);
         ("AVG_PRICELINEITEM2",               Local);
         ("AVG_DISC",                         Local);
         ("AVG_DISCLINEITEM1_DELTA",          Local);
         ("AVG_DISCLINEITEM2",                Local);         
         ("COUNT_ORDER",                      Local);
         ("DELTA_LINEITEM",                   Local);
      ]
   in

   let tpch2_part_table = 
      create_hash_table [ 
         ("COUNT",                            DistributedByKey([3]));  (* PK *)
         ("COUNTPARTSUPP1_DOMAIN1",           Local);  
         ("COUNTPARTSUPP1_P_1",               DistributedByKey([0]));  (* PK *)
         ("COUNTPARTSUPP1_L2_2_DELTA",        Local);
         ("COUNTPARTSUPP1_L2_2",              DistributedByKey([0]));  (* SK *)
         ("COUNTPARTSUPP1_L2_2SUPPLIER1_DELTA", Local);
         ("COUNTPARTSUPP1_L2_2SUPPLIER1",     DistributedByKey([]));   (*    *)
         ("COUNTPARTSUPP4_P_2",               DistributedByKey([0]));  (* SK *)
         ("COUNTPARTSUPP4_P_2SUPPLIER1_DELTA",  Local);
         ("COUNTPARTSUPP4_P_2SUPPLIER1",      DistributedByKey([]));   (*    *)
         ("COUNTSUPPLIER1",                   DistributedByKey([0]));  (* PK *)
         ("COUNTSUPPLIER1SUPPLIER1_P_1",      DistributedByKey([0]));  (* PK *)
         ("COUNTSUPPLIER1SUPPLIER1_P_1PART1", DistributedByKey([0]));  (* PK *)
         ("COUNTPART1_DELTA",                 Local);
         ("COUNTPART1",                       DistributedByKey([5]));  (* PK *)
         ("COUNTPART1_L2_1",                  DistributedByKey([0]));  (* PK *)
         ("DELTA_PARTSUPP",                   Local);
         ("DELTA_SUPPLIER",                   Local);
         ("DELTA_PART",                       Local);
         ("NATION",                           Local);
         ("REGION",                           Local);
      ]
   in 

   let tpch3_part_table =
      create_hash_table [ 
         ("QUERY3",                       DistributedByKey([0]));   (* OK *)
         ("QUERY3LINEITEM1_DELTA",        Local);
         ("QUERY3LINEITEM1",              DistributedByKey([0]));   (* OK *)
         ("QUERY3LINEITEM1CUSTOMER1",     DistributedByKey([0]));   (* OK *)
         ("QUERY3ORDERS1_DELTA",          Local);
         ("QUERY3ORDERS1_P_1",            DistributedByKey([0]));   (* CK *)
         ("QUERY3ORDERS1_P_2",            DistributedByKey([0]));   (* OK *)
         ("QUERY3CUSTOMER1_DELTA",        Local);
         ("QUERY3CUSTOMER1",              DistributedByKey([0]));   (* OK *)
         ("DELTA_LINEITEM",               Local);
         ("DELTA_ORDERS",                 Local);
         ("DELTA_CUSTOMER",               Local);
      ]
   in

   let tpch4_part_table = 
      create_hash_table [
         ("ORDER_COUNT",                      Local);
         ("ORDER_COUNTLINEITEM1_DOMAIN1",     Local);
         ("ORDER_COUNTLINEITEM1",             DistributedByKey([0]));  (* OK *)
         ("ORDER_COUNTLINEITEM1_E1_2_DELTA",  Local);
         ("ORDER_COUNTORDERS1_DELTA",         Local);
         ("ORDER_COUNTORDERS1_E1_1",          DistributedByKey([0]));  (* OK *)
         ("DELTA_LINEITEM",                   Local);
         ("DELTA_ORDERS",                     Local);
      ]
   in

   let tpch5_part_table = 
      if (Debug.active "HEURISTICS-DECOMPOSE-OVER-TABLES") then      
         create_hash_table [
            ("REVENUE",                        Local);
            ("REVENUESUPPLIER1_DELTA",         Local);
            ("REVENUESUPPLIER1_P_2",           DistributedByKey([1])); (* SK *)
            ("REVENUESUPPLIER1_P_2ORDERS1_P_2", DistributedByKey([0])); (* OK *)
            ("REVENUESUPPLIER1_P_2CUSTOMER1",  DistributedByKey([0])); (* CK *)
            ("REVENUESUPPLIER1_P_2CUSTOMER1LINEITEM1", DistributedByKey([0])); (* OK *)
            ("REVENUELINEITEM1_DELTA",         Local);
            ("REVENUELINEITEM1_T_2",           DistributedByKey([1])); (* OK *)
            ("REVENUELINEITEM1_T_3",           DistributedByKey([0])); (* SK *)
            ("REVENUEORDERS1_DELTA",           Local);
            ("REVENUEORDERS1_T_2",             DistributedByKey([0])); (* CK *)
            ("REVENUEORDERS1_T_3",             DistributedByKey([0])); (* OK *)
            ("REVENUECUSTOMER1_DELTA",         Local);
            ("REVENUECUSTOMER1_P_1",           DistributedByKey([]));  (* ** *)
            ("REVENUECUSTOMER1_P_2",           DistributedByKey([0])); (* CK *)
            ("DELTA_LINEITEM",                 Local);
            ("DELTA_ORDERS",                   Local);
            ("DELTA_CUSTOMER",                 Local);
            ("DELTA_SUPPLIER",                 Local);
            ("NATION",                         Local);
            ("REGION",                         Local);
         ]      
      else
         create_hash_table [
            ("REVENUE",                        Local);
            ("REVENUESUPPLIER1_DELTA",         Local);
            ("REVENUESUPPLIER1_P_2",           DistributedByKey([1])); (* SK *)
            ("REVENUESUPPLIER1_P_2LINEITEM1",  DistributedByKey([1])); (* OK *)
            ("REVENUESUPPLIER1_P_2LINEITEM1CUSTOMER1", DistributedByKey([0])); (* OK *)
            ("REVENUESUPPLIER1_P_2ORDERS1_P_1", DistributedByKey([0])); (* CK *)
            ("REVENUESUPPLIER1_P_2ORDERS1_P_2", DistributedByKey([0])); (* OK *)
            ("REVENUESUPPLIER1_P_2CUSTOMER1",  DistributedByKey([0])); (* CK *)
            ("REVENUELINEITEM1_DELTA",         Local);
            ("REVENUELINEITEM1",               DistributedByKey([0])); (* OK *)
            ("REVENUELINEITEM1ORDERS1",        DistributedByKey([0])); (* CK *)
            ("REVENUELINEITEM1CUSTOMER1_P_3",  DistributedByKey([0])); (* SK *)
            ("REVENUEORDERS1_DELTA",           Local);
            ("REVENUEORDERS1",                 DistributedByKey([1])); (* OK *)
            ("REVENUEORDERS1CUSTOMER1_P_2",    DistributedByKey([0])); (* OK *)
            ("REVENUECUSTOMER1_DELTA",         Local);
            ("REVENUECUSTOMER1_P_1",           DistributedByKey([]));  (* ** *)
            ("REVENUECUSTOMER1_P_2",           DistributedByKey([0])); (* CK *)
            ("DELTA_LINEITEM",                 Local);
            ("DELTA_ORDERS",                   Local);
            ("DELTA_CUSTOMER",                 Local);
            ("DELTA_SUPPLIER",                 Local);
            ("NATION",                         Local);
            ("REGION",                         Local);
         ]
   in

   let tpch6_part_table = 
      create_hash_table [ 
         ("REVENUE",                Local); 
         ("REVENUELINEITEM1_DELTA", Local);
         ("DELTA_LINEITEM",         Local);
      ]
   in   

   let tpch7_part_table = 
      if (Debug.active "HEURISTICS-DECOMPOSE-OVER-TABLES") then      
         create_hash_table [ 
            ("REVENUE",                        Local); 
            ("REVENUECUSTOMER1_DELTA",         Local);
            ("REVENUECUSTOMER1",               DistributedByKey([0])); (* CK *)
            ("REVENUECUSTOMER1ORDERS1",        DistributedByKey([0])); (* OK *)
            ("REVENUECUSTOMER1ORDERS1SUPPLIER1_P_1", DistributedByKey([0])); (* OK *)
            ("REVENUECUSTOMER1LINEITEM1_P_1",  DistributedByKey([0])); (* SK *)
            ("REVENUECUSTOMER1LINEITEM1_P_2",  DistributedByKey([0])); (* OK *)
            ("REVENUECUSTOMER1SUPPLIER1_P_1",  DistributedByKey([1])); (* CK *)
            ("REVENUEORDERS1_DELTA",           Local);
            ("REVENUEORDERS1_T_1",             DistributedByKey([1])); (* OK *)
            ("REVENUEORDERS1_T_2",             DistributedByKey([0])); (* CK *)
            ("REVENUELINEITEM1_DOMAIN1",       Local);
            ("REVENUELINEITEM1_DELTA",         Local);
            ("REVENUELINEITEM1_T_1",           DistributedByKey([0])); (* SK *)
            ("REVENUELINEITEM1_T_2",           DistributedByKey([0])); (* OK *)
            ("REVENUELINEITEM1_T_3",           DistributedByKey([]));  (* ** *)
            ("REVENUESUPPLIER1_DELTA",         Local);
            ("REVENUESUPPLIER1",               DistributedByKey([0])); (* SK *)
            ("REVENUESUPPLIER1ORDERS1_P_2",    DistributedByKey([0])); (* CK *)
            ("REVENUESUPPLIER1LINEITEM1",      DistributedByKey([0])); (* OK *)
            ("DELTA_LINEITEM",                 Local);
            ("DELTA_ORDERS",                   Local);
            ("DELTA_CUSTOMER",                 Local);
            ("DELTA_SUPPLIER",                 Local);
            ("NATION",                         Local);            
         ]
      else   
         create_hash_table [ 
            ("REVENUE",                        Local); 
            ("REVENUECUSTOMER1_DELTA",         Local);
            ("REVENUECUSTOMER1",               DistributedByKey([0])); (* CK *)
            ("REVENUECUSTOMER1ORDERS1",        DistributedByKey([0])); (* OK *)
            ("REVENUECUSTOMER1ORDERS1SUPPLIER1_P_1", DistributedByKey([0])); (* OK *)
            ("REVENUECUSTOMER1LINEITEM1_P_1",  DistributedByKey([0])); (* SK *)
            ("REVENUECUSTOMER1LINEITEM1_P_2",  DistributedByKey([0])); (* OK *)
            ("REVENUECUSTOMER1SUPPLIER1_P_1",  DistributedByKey([1])); (* CK *)
            ("REVENUECUSTOMER1SUPPLIER1_P_2",  DistributedByKey([]));  (* ** *)
            ("REVENUEORDERS1_DELTA",           Local);
            ("REVENUEORDERS1",                 DistributedByKey([0])); (* OK *)
            ("REVENUEORDERS1LINEITEM1",        DistributedByKey([1])); (* CK *) (* 0:SK *)
            ("REVENUEORDERS1SUPPLIER1_P_2",    DistributedByKey([0])); (* CK *)
            ("REVENUELINEITEM1_DOMAIN1",       Local);
            ("REVENUELINEITEM1_DELTA",         Local);
            ("REVENUELINEITEM1",               DistributedByKey([1])); (* OK *)
            ("REVENUELINEITEM1SUPPLIER1",      DistributedByKey([0])); (* OK *)
            ("REVENUESUPPLIER1_DELTA",         Local);
            ("REVENUESUPPLIER1",               DistributedByKey([0])); (* SK *)
            ("DELTA_LINEITEM",                 Local);
            ("DELTA_ORDERS",                   Local);
            ("DELTA_CUSTOMER",                 Local);
            ("DELTA_SUPPLIER",                 Local);
            ("NATION",                         Local);
         ]
   in

   let tpch8_part_table = 
      create_hash_table [ 
         ("MKT_SHARE",                         Local); 
         ("MKT_SHAREORDERS1_DOMAIN1",          Local);  
         ("MKT_SHAREORDERS1",                  Local);
         ("MKT_SHAREORDERS1CUSTOMER1_DELTA",   Local);
         ("MKT_SHAREORDERS1CUSTOMER1_P_1",     DistributedByKey([0])); (* CK *)
         ("MKT_SHAREORDERS1CUSTOMER1_P_1LINEITEM1_P_3",    DistributedByKey([0])); (* OK *)
         ("MKT_SHAREORDERS1CUSTOMER1_P_1SUPPLIER1_P_2",    DistributedByKey([1])); (* CK *)
         ("MKT_SHAREORDERS1CUSTOMER1_P_1SUPPLIER1_P_2ORDERS1",      DistributedByKey([0])); (* OK *)
         ("MKT_SHAREORDERS1CUSTOMER1_P_1SUPPLIER1_P_2ORDERS1PART1", DistributedByKey([0])); (* OK *)
         ("MKT_SHAREORDERS1CUSTOMER1_P_1SUPPLIER1_P_2PART1",        DistributedByKey([0])); (* PK *)
         ("MKT_SHAREORDERS1CUSTOMER1_P_1PART1", DistributedByKey([0])); (* PK *)
         ("MKT_SHAREORDERS1CUSTOMER1_P_1PART1ORDERS1", DistributedByKey([0])); (* OK *)
         ("MKT_SHAREORDERS1CUSTOMER1_P_2",     DistributedByKey([]));  (* ** *)          
         ("MKT_SHAREORDERS1LINEITEM1_DELTA",   Local);
         ("MKT_SHAREORDERS1LINEITEM1_P_1",     DistributedByKey([0])); (* PK *)
         ("MKT_SHAREORDERS1LINEITEM1_P_2",     DistributedByKey([0])); (* OK *)
         ("MKT_SHAREORDERS1LINEITEM1_P_3",     DistributedByKey([0])); (* SK *)
         ("MKT_SHAREORDERS1SUPPLIER1_DELTA",   Local);
         ("MKT_SHAREORDERS1SUPPLIER1_P_1",     DistributedByKey([0])); (* SK *)
         ("MKT_SHAREORDERS1SUPPLIER1_P_1PART1", DistributedByKey([0])); (* PK *)
         ("MKT_SHAREORDERS1SUPPLIER1_P_2",     DistributedByKey([]));  (* ** *)
         ("MKT_SHAREORDERS1PART1_DELTA",       Local);
         ("MKT_SHAREORDERS1PART1",             DistributedByKey([0])); (* PK *)
         ("MKT_SHAREORDERS1_L1_1_L1_2_DELTA",  Local);
         ("MKT_SHAREORDERS1_L1_1_L1_2_P_1",    DistributedByKey([0])); (* CK *)
         ("MKT_SHAREORDERS1_L1_1_L1_2_P_2",    DistributedByKey([0])); (* OK *)
         ("MKT_SHAREORDERS1_L1_1_L1_2_P_2LINEITEM1_P_2", DistributedByKey([0])); (* SK *)
         ("MKT_SHAREORDERS1_L1_1_L1_2_P_2SUPPLIER1_P_2", DistributedByKey([])); 
         ("MKT_SHAREORDERS1_L1_1_L1_2_P_2PART1", DistributedByKey([0])); (* OK *)
         ("MKT_SHAREORDERS4_DELTA",            Local);
         ("MKT_SHAREORDERS4_P_1",              DistributedByKey([0])); (* OK *)
         ("MKT_SHAREPART1",                    Local);
         ("MKT_SHAREPART1CUSTOMER1_P_2",       DistributedByKey([0])); (* CK *)
         ("MKT_SHAREPART1CUSTOMER1_P_2LINEITEM1_P_3",      DistributedByKey([0])); (* OK *)
         ("MKT_SHAREPART1CUSTOMER1_P_2SUPPLIER1_P_2",      DistributedByKey([1])); (* CK *)
         ("MKT_SHAREPART1CUSTOMER1_P_2SUPPLIER1_P_2PART1", DistributedByKey([0])); (* PK *)
         ("MKT_SHAREPART1CUSTOMER1_P_2PART1",  DistributedByKey([0])); (* PK *)
         ("MKT_SHAREPART1LINEITEM1_P_1",       DistributedByKey([0])); (* OK *)
         ("MKT_SHAREPART1SUPPLIER1_P_1",       DistributedByKey([0])); (* SK *)
         ("MKT_SHAREPART1SUPPLIER1_P_1PART1",  DistributedByKey([0])); (* PK *)
         ("MKT_SHAREPART1PART1",               DistributedByKey([0])); (* PK *)
         ("MKT_SHAREPART1_L2_1_L1_1",          Local);
         ("MKT_SHAREPART1_L2_1_L1_1CUSTOMER1_P_2",      DistributedByKey([0])); (* CK *)
         ("MKT_SHAREPART1_L2_1_L1_1CUSTOMER1_P_2PART1", DistributedByKey([0])); (* PK *)
         ("MKT_SHAREPART1_L2_1_L1_1PART1",     DistributedByKey([0])); (* PK *)
         ("DELTA_LINEITEM",                    Local);
         ("DELTA_ORDERS",                      Local);
         ("DELTA_CUSTOMER",                    Local);
         ("DELTA_SUPPLIER",                    Local);
         ("DELTA_PART",                        Local);
         ("NATION",                            Local);
         ("REGION",                            Local);
      ]
   in

   let tpch9_part_table = 
      create_hash_table [ 
         ("SUM_PROFIT",                      Local); 
         ("SUM_PROFITORDERS11_DOMAIN1",      Local); 
         ("SUM_PROFITORDERS11_DELTA",        Local);
         ("SUM_PROFITORDERS11",              DistributedByKey([0])); (* OK *)
         ("SUM_PROFITORDERS11PARTSUPP1_P_3", DistributedByKey([0])); (* OK *)
         ("SUM_PROFITORDERS11SUPPLIER1_P_1", DistributedByKey([0])); (* OK *)
         ("SUM_PROFITORDERS11SUPPLIER1_P_1PART1", DistributedByKey([0])); (* OK *)
         ("SUM_PROFITORDERS11PART1",         DistributedByKey([0])); (* OK *)
         ("SUM_PROFITORDERS13",              DistributedByKey([0])); (* OK *)
         ("SUM_PROFITORDERS13PARTSUPP1_P_3", DistributedByKey([0])); (* OK *)
         ("SUM_PROFITORDERS13SUPPLIER1_P_1", DistributedByKey([0])); (* OK *)
         ("SUM_PROFITORDERS13SUPPLIER1_P_1PART1", DistributedByKey([0])); (* OK *)
         ("SUM_PROFITORDERS13PART1",         DistributedByKey([0])); (* OK *) 
         ("SUM_PROFITPARTSUPP11_DELTA",      Local);
         ("SUM_PROFITPARTSUPP11_P_3",        DistributedByKey([0])); (* PK *)
         ("SUM_PROFITPARTSUPP13_DELTA",      Local);
         ("SUM_PROFITPARTSUPP13_P_3",        DistributedByKey([0])); (* PK *)
         ("SUM_PROFITLINEITEM11_DELTA",      Local);
         ("SUM_PROFITLINEITEM11_P_1",        DistributedByKey([0])); (* PK *)
         ("SUM_PROFITLINEITEM11_P_2",        DistributedByKey([0])); (* SK *)
         ("SUM_PROFITLINEITEM11_P_3",        DistributedByKey([0])); (* PK *)
         ("SUM_PROFITLINEITEM11_P_4",        DistributedByKey([0])); (* OK *)
         ("SUM_PROFITLINEITEM13_DELTA",      Local);
         ("SUM_PROFITLINEITEM13_P_3",        DistributedByKey([0])); (* PK *)
         ("SUM_PROFITSUPPLIER11_DELTA",      Local);
         ("SUM_PROFITSUPPLIER11_P_1",        DistributedByKey([0])); (* SK *)
         ("SUM_PROFITSUPPLIER11_P_1PART1",   DistributedByKey([0])); (* PK *)
         ("SUM_PROFITSUPPLIER11_P_2",        DistributedByKey([]));  (* ** *)
         ("SUM_PROFITSUPPLIER13_P_1",        DistributedByKey([0])); (* SK *)
         ("SUM_PROFITSUPPLIER13_P_1PART1",   DistributedByKey([0])); (* PK *)
         ("SUM_PROFITPART11_DELTA",          Local);
         ("SUM_PROFITPART11",                DistributedByKey([0])); (* PK *)
         ("SUM_PROFITPART13",                DistributedByKey([0])); (* PK *)
         ("DELTA_LINEITEM",                  Local);
         ("DELTA_ORDERS",                    Local);
         ("DELTA_PARTSUPP",                  Local);
         ("DELTA_SUPPLIER",                  Local);
         ("DELTA_PART",                      Local);
         ("NATION",                          Local);
      ]
   in

   let tpch10_part_table = 
      create_hash_table [ 
         ("REVENUE",                       DistributedByKey([0])); (* CK *)
         ("REVENUELINEITEM1_DELTA",        Local);
         ("REVENUELINEITEM1",              DistributedByKey([6])); (* OK *)
         ("REVENUELINEITEM1CUSTOMER1_P_1", DistributedByKey([0])); (* OK *)
         ("REVENUEORDERS1_DELTA",          Local);
         ("REVENUEORDERS1_P_1",            DistributedByKey([0])); (* OK *)
         ("REVENUEORDERS1_P_2",            DistributedByKey([0])); (* CK *)
         ("REVENUECUSTOMER1_DELTA",        Local);
         ("REVENUECUSTOMER1_P_1",          DistributedByKey([0])); (* CK *)
         ("REVENUECUSTOMER1_P_2",          DistributedByKey([]));  (* ** *)
         ("DELTA_LINEITEM",                Local);
         ("DELTA_ORDERS",                  Local);
         ("DELTA_CUSTOMER",                Local);
         ("NATION",                        Local);
      ]
   in

   let tpch11_part_table = 
      create_hash_table [ 
         ("QUERY11",                              DistributedByKey([0])); (* PK *)
         ("QUERY11PARTSUPP1_L1_1",                Local);
         ("QUERY11PARTSUPP1_L1_1SUPPLIER1_DELTA", Local);
         ("QUERY11PARTSUPP1_L1_1SUPPLIER1_P_1",   DistributedByKey([]));  (* ** *)
         ("QUERY11PARTSUPP1_L1_1SUPPLIER1_P_2",   DistributedByKey([0])); (* SK *)
         ("QUERY11PARTSUPP1_L1_1PARTSUPP1_DELTA", Local);
         ("QUERY11PARTSUPP1_L1_1PARTSUPP1",       DistributedByKey([0])); (* SK *)
         ("QUERY11PARTSUPP1_E2_1",                DistributedByKey([0])); (* PK *)
         ("QUERY11PARTSUPP1_E2_1SUPPLIER1_P_2",   DistributedByKey([1])); (* PK *)
         ("QUERY11PARTSUPP1_E2_1PARTSUPP1_DELTA", Local);
         ("QUERY11PARTSUPP1_L3_1",                DistributedByKey([0])); (* PK *) 
         ("QUERY11PARTSUPP1_L3_1SUPPLIER1_P_2",   DistributedByKey([1])); (* PK *)
         ("QUERY11PARTSUPP1_L3_1PARTSUPP1_DELTA", Local);
         ("DELTA_PARTSUPP",                       Local);
         ("DELTA_SUPPLIER",                       Local);
         ("NATION",                               Local);
      ]
   in

   let tpch12_part_table = 
      create_hash_table [ 
         ("HIGH_LINE_COUNT",                Local); 
         ("HIGH_LINE_COUNTLINEITEM1_DELTA", Local);
         ("HIGH_LINE_COUNTLINEITEM1",       DistributedByKey([0]));     (* OK *)
         ("HIGH_LINE_COUNTLINEITEM2_DELTA", Local);
         ("HIGH_LINE_COUNTLINEITEM3",       DistributedByKey([0]));     (* OK *)
         ("HIGH_LINE_COUNTORDERS1_DELTA",   Local);
         ("HIGH_LINE_COUNTORDERS1",         DistributedByKey([0]));     (* OK *) 
         ("HIGH_LINE_COUNTORDERS2",         DistributedByKey([0]));     (* OK *) 
         ("HIGH_LINE_COUNTORDERS3_DELTA",   Local);
         ("LOW_LINE_COUNT",                 Local);
         ("LOW_LINE_COUNTLINEITEM1",        DistributedByKey([0]));     (* OK *)
         ("LOW_LINE_COUNTORDERS1_DELTA",    Local);
         ("DELTA_LINEITEM",                 Local);
         ("DELTA_ORDERS",                   Local);
      ]
   in

   let tpch13_part_table =   
      create_hash_table [ 
         ("CUSTDIST",                            Local); 
         ("CUSTDISTORDERS1_DOMAIN1",             Local);
         ("CUSTDISTORDERS3_L1_2_DELTA",          Local);
         ("CUSTDISTORDERS3_L1_2",                DistributedByKey([0])); (* CK *)
         ("CUSTDISTORDERS3_L1_2CUSTOMER1_DELTA", Local);
         ("CUSTDISTCUSTOMER1_DOMAIN1",           Local);
         ("CUSTDISTCUSTOMER1_L1_1",              DistributedByKey([0])); (* CK *)
         ("CUSTDISTCUSTOMER3_L1_2_DELTA",        Local);
         ("CUSTDISTCUSTOMER3_L1_2",              DistributedByKey([0])); (* CK *)
         ("CUSTDISTCUSTOMER3_L1_2ORDERS1_DELTA", Local);
         ("DELTA_ORDERS",                        Local);
         ("DELTA_CUSTOMER",                      Local);
      ]
   in

   let tpch14_part_table = 
      create_hash_table [ 
         ("PROMO_REVENUE",                                   Local); 
         ("PROMO_REVENUELINEITEM1_L1_1_L1_1",                Local);
         ("PROMO_REVENUELINEITEM1_L1_1_L1_1PART1_DELTA",     Local);
         ("PROMO_REVENUELINEITEM1_L1_1_L1_1PART1",           DistributedByKey([0])); (* PK *)
         ("PROMO_REVENUELINEITEM1_L1_1_L1_1LINEITEM1_DELTA", Local);
         ("PROMO_REVENUELINEITEM1_L1_1_L1_1LINEITEM1",       DistributedByKey([0])); (* PK *)
         ("PROMO_REVENUELINEITEM2",                          Local); 
         ("PROMO_REVENUELINEITEM2PART1_DELTA",               Local);
         ("PROMO_REVENUELINEITEM2LINEITEM1",                 DistributedByKey([0])); (* PK *)
         ("DELTA_LINEITEM",                                  Local);
         ("DELTA_PART",                                      Local);
      ]
   in

   let tpch15_part_table = 
      create_hash_table [ 
         ("COUNT",                                   DistributedByKey([0])); (* SK *)
         ("COUNTSUPPLIER1_DELTA",                    Local);
         ("COUNTLINEITEM1",                          DistributedByKey([0])); (* SK *)
         ("COUNTLINEITEM1_E2_1",                     DistributedByKey([0])); (* SK *)
         ("COUNTLINEITEM1_E2_1LINEITEM1_DELTA",      Local);
         ("COUNTLINEITEM1_L3_1",                     DistributedByKey([0])); (* SK *)
         ("COUNTLINEITEM1_L3_1LINEITEM1_DELTA",      Local);
         ("COUNTLINEITEM1_L4_1_E1_1",                DistributedByKey([0])); (* SK *)
         ("COUNTLINEITEM1_L4_1_E1_1LINEITEM1_DELTA", Local);
         ("COUNTLINEITEM1_L4_1_L2_1",                DistributedByKey([0])); (* SK *)
         ("COUNTLINEITEM1_L4_1_L2_1LINEITEM1_DELTA", Local);
         ("DELTA_LINEITEM",                          Local);
         ("DELTA_SUPPLIER",                          Local);
      ]
   in

   let tpch16_part_table = 
      create_hash_table [ 
         ("SUPPLIER_CNT",                           Local); 
         ("SUPPLIER_CNTSUPPLIER1_DOMAIN1",          Local);
         ("SUPPLIER_CNTSUPPLIER1_E1_2_L2_2_DELTA",  Local);
         ("SUPPLIER_CNTPART1_DOMAIN1",              Local); 
         ("SUPPLIER_CNTPART1_E1_1",                 DistributedByKey([0])); (* SK *)
         ("SUPPLIER_CNTPART1_E1_1PARTSUPP1",        DistributedByKey([0])); (* PK *)
         ("SUPPLIER_CNTPART1_E1_33_DELTA",          Local);
         ("SUPPLIER_CNTPART1_E1_33",                DistributedByKey([0])); (* PK *)
         ("SUPPLIER_CNTPARTSUPP1_DOMAIN1",          Local);
         ("SUPPLIER_CNTPARTSUPP1_E1_1_L2_1",        DistributedByKey([0])); (* SK *)
         ("SUPPLIER_CNTPARTSUPP1_E1_2",             DistributedByKey([0])); (* SK *)
         ("SUPPLIER_CNTPARTSUPP1_E1_2PART1_DELTA",  Local);
         ("SUPPLIER_CNTPARTSUPP1_E1_4",             DistributedByKey([0])); (* SK *)
         ("SUPPLIER_CNTPARTSUPP1_E1_4PART1_DELTA",  Local);
         ("SUPPLIER_CNTPARTSUPP1_E1_6",             DistributedByKey([0])); (* SK *)
         ("SUPPLIER_CNTPARTSUPP1_E1_6PART1_DELTA",  Local);
         ("SUPPLIER_CNTPARTSUPP1_E1_8",             DistributedByKey([0])); (* SK *)
         ("SUPPLIER_CNTPARTSUPP1_E1_8PART1_DELTA",  Local);
         ("SUPPLIER_CNTPARTSUPP1_E1_10",            DistributedByKey([0])); (* SK *)
         ("SUPPLIER_CNTPARTSUPP1_E1_10PART1_DELTA", Local);
         ("SUPPLIER_CNTPARTSUPP1_E1_12",            DistributedByKey([0])); (* SK *)
         ("SUPPLIER_CNTPARTSUPP1_E1_12PART1_DELTA", Local);
         ("SUPPLIER_CNTPARTSUPP1_E1_14",            DistributedByKey([0])); (* SK *)
         ("SUPPLIER_CNTPARTSUPP1_E1_14PART1_DELTA", Local);
         ("SUPPLIER_CNTPARTSUPP1_E1_16",            DistributedByKey([0])); (* SK *)
         ("SUPPLIER_CNTPARTSUPP1_E1_16PART1_DELTA", Local);
         ("SUPPLIER_CNTPARTSUPP1_E1_18_DELTA",      Local);
         ("SUPPLIER_CNTPARTSUPP1_E1_18",            DistributedByKey([0])); (* PK *)
         ("SUPPLIER_CNTPARTSUPP1_E1_20",            DistributedByKey([0])); (* PK *)
         ("SUPPLIER_CNTPARTSUPP1_E1_22",            DistributedByKey([0])); (* PK *)
         ("SUPPLIER_CNTPARTSUPP1_E1_24",            DistributedByKey([0])); (* PK *)
         ("SUPPLIER_CNTPARTSUPP1_E1_26",            DistributedByKey([0])); (* PK *)
         ("SUPPLIER_CNTPARTSUPP1_E1_28",            DistributedByKey([0])); (* PK *)
         ("SUPPLIER_CNTPARTSUPP1_E1_30",            DistributedByKey([0])); (* PK *)
         ("SUPPLIER_CNTPARTSUPP1_E1_32",            DistributedByKey([0])); (* PK *)
         ("DELTA_PART",                             Local);
         ("DELTA_SUPPLIER",                         Local);
         ("DELTA_PARTSUPP",                         Local);
      ]
   in

   let tpch17_part_table = 
      create_hash_table [ 
         ("AVG_YEARLY",                          Local); 
         ("AVG_YEARLYPART1_DELTA",               Local); 
         ("AVG_YEARLYLINEITEM1_DOMAIN1",         Local); 
         ("AVG_YEARLYLINEITEM1_P_3",             DistributedByKey([0])); (* PK *)
         ("AVG_YEARLYLINEITEM1_P_3PART1_DELTA",  Local); 
         ("AVG_YEARLYLINEITEM1_P_4",             DistributedByKey([0])); (* PK *)
         ("AVG_YEARLYLINEITEM1_L1_1_L1_1",       DistributedByKey([0])); (* PK *)
         ("AVG_YEARLYLINEITEM1_L1_1_L1_2_DELTA", Local); 
         ("AVG_YEARLYLINEITEM1_L1_2",            DistributedByKey([0])); (* PK *)
         ("AVG_YEARLYLINEITEM1_L1_4_DELTA",      Local); 
         ("AVG_YEARLYLINEITEM5_DELTA",           Local); 
         ("DELTA_LINEITEM",                      Local);
         ("DELTA_PART",                          Local);
      ]
   in

   let tpch17a_part_table = 
      create_hash_table [ 
         ("QUERY17",                     Local); 
         ("QUERY17PART1_DELTA",          Local); 
         ("QUERY17LINEITEM1_DOMAIN1",    Local); 
         ("QUERY17LINEITEM1_P_1",        DistributedByKey([0])); (* PK *)
         ("QUERY17LINEITEM1_P_2",        DistributedByKey([0])); (* PK *)
         ("QUERY17LINEITEM1_L1_1",       DistributedByKey([0])); (* PK *)
         ("QUERY17LINEITEM1_L1_3_DELTA", Local); 
         ("QUERY17LINEITEM2_DELTA",      Local); 
         ("DELTA_LINEITEM",              Local);
         ("DELTA_PART",                  Local);
      ]
   in   

   let tpch18_part_table = 
      create_hash_table [ 
         ("QUERY18",                       DistributedByKey([2])); (* OK *) (* 1:CK *)
         ("QUERY18LINEITEM1_DOMAIN1",      Local); 
         ("QUERY18LINEITEM1_P_1",          DistributedByKey([2])); (* OK *) (* 1:CK *)
         ("QUERY18LINEITEM1_P_1CUSTOMER1", DistributedByKey([0])); (* OK *) (* 1:CK *)
         ("QUERY18LINEITEM1_L1_2_DELTA",   Local); 
         ("QUERY18ORDERS1_DELTA",          Local); 
         ("QUERY18ORDERS1_P_1",            DistributedByKey([0])); (* CK *)
         ("QUERY18CUSTOMER1_DELTA",        Local); 
         ("QUERY18CUSTOMER1",              DistributedByKey([0])); (* OK *) (* 1:CK *)
         ("QUERY18CUSTOMER1_L1_1",         DistributedByKey([0])); (* OK *)
         ("DELTA_LINEITEM",                Local);
         ("DELTA_ORDERS",                  Local);
         ("DELTA_CUSTOMER",                Local);
      ]
   in

   let tpch19_part_table = 
      create_hash_table [ 
         ("REVENUE",                Local); 
         ("REVENUEPART1_DELTA",     Local); 
         ("REVENUEPART1",           DistributedByKey([0])); (* PK *)
         ("REVENUELINEITEM1_DELTA", Local); 
         ("REVENUELINEITEM1",       DistributedByKey([0])); (* PK *)
         ("DELTA_LINEITEM",         Local);
         ("DELTA_PART",             Local);
      ]
   in

   let tpch20_part_table = 
      create_hash_table [ 
         ("COUNT",                               Local); (* Alternative: 0:S_NAME   1:S_ADDRESS *) 
         ("COUNTPART1",                          DistributedByKey([0])); (* SK *)
         ("COUNTLINEITEM1_DOMAIN1",              Local); 
         ("COUNTLINEITEM1_E1_1_L1_3_DELTA",      Local); 
         ("COUNTPARTSUPP1_DOMAIN1",              Local); 
         ("COUNTPARTSUPP1_P_2",                  DistributedByKey([0])); (* SK *)
         ("COUNTPARTSUPP1_P_2SUPPLIER1",         DistributedByKey([]));  (* ** *)
         ("COUNTPARTSUPP1_E1_2_DELTA",           Local); 
         ("COUNTSUPPLIER1_DELTA",                Local); 
         ("COUNTSUPPLIER1",                      DistributedByKey([]));  (* ** *)
         ("COUNTSUPPLIER1_E1_1",                 DistributedByKey([0])); (* PK *)
         ("COUNTSUPPLIER1_E1_1_L1_1",            DistributedByKey([0])); (* PK *)
         ("COUNTSUPPLIER1_E1_1_E2_1",            DistributedByKey([0])); (* PK *)
         ("COUNTSUPPLIER1_E1_1_E2_1PART1_DELTA", Local); 
         ("DELTA_LINEITEM",                      Local);
         ("DELTA_PARTSUPP",                      Local);
         ("DELTA_SUPPLIER",                      Local);
         ("DELTA_PART",                          Local);
         ("NATION",                              Local);
      ]
   in

   let tpch21_part_table = 
      create_hash_table [ 
         ("NUMWAIT",                           Local); (* Alternative: 0:S_NAME *)
         ("NUMWAITORDERS1_DELTA",              Local); 
         ("NUMWAITORDERS1",                    DistributedByKey([2])); (* OK *)
         ("NUMWAITORDERS1LINEITEM1",           DistributedByKey([0])); (* SK *)
         ("NUMWAITLINEITEM1_DOMAIN1",          Local); 
         ("NUMWAITLINEITEM1_P_3",              DistributedByKey([2])); (* OK *)
         ("NUMWAITLINEITEM1_P_3SUPPLIER1_P_2", DistributedByKey([]));  (* ** *)
         ("NUMWAITLINEITEM1_P_4",              DistributedByKey([0])); (* OK *)
         ("NUMWAITLINEITEM1_P_4ORDERS1_DELTA", Local); 
         ("NUMWAITLINEITEM3_L2_2_DELTA",       Local); 
         ("NUMWAITLINEITEM3_E3_2_DELTA",       Local); 
         ("NUMWAITLINEITEM4_P_3",              DistributedByKey([0])); (* SK *) 
         ("NUMWAITSUPPLIER1_DELTA",            Local); 
         ("NUMWAITSUPPLIER1_P_1",              DistributedByKey([]));  (* ** *)
         ("NUMWAITSUPPLIER1_P_2",              DistributedByKey([0])); (* OK *)
         ("NUMWAITSUPPLIER1_P_2LINEITEM1",     DistributedByKey([0])); (* OK *)
         ("NUMWAITSUPPLIER1_L2_1",             DistributedByKey([0])); (* OK *)
         ("NUMWAITSUPPLIER1_E3_1",             DistributedByKey([0])); (* OK *) 
         ("DELTA_LINEITEM",                    Local);
         ("DELTA_ORDERS",                      Local);
         ("DELTA_SUPPLIER",                    Local);
         ("NATION",                            Local);
      ]
   in

   let tpch22_part_table = 
      create_hash_table [ 
         ("NUMCUST",                                   Local); 
         ("NUMCUSTORDERS1_DOMAIN1",                    Local); 
         ("NUMCUSTORDERS1_L2_2_DELTA",                 Local); 
         ("NUMCUSTCUSTOMER1",                          DistributedByKey([0])); (* CK *)
         ("NUMCUSTCUSTOMER1CUSTOMER1_DELTA",           Local); 
         ("NUMCUSTCUSTOMER1_L2_1_L1_1",                Local); 
         ("NUMCUSTCUSTOMER1_L2_1_L1_1CUSTOMER1_DELTA", Local);
         ("NUMCUSTCUSTOMER1_L2_1_L1_2",                Local); 
         ("NUMCUSTCUSTOMER1_L2_1_L1_2CUSTOMER1_DELTA", Local);
         ("NUMCUSTCUSTOMER1_L2_1_L1_3",                Local); 
         ("NUMCUSTCUSTOMER1_L2_1_L1_3CUSTOMER1_DELTA", Local);
         ("NUMCUSTCUSTOMER1_L2_1_L1_4",                Local); 
         ("NUMCUSTCUSTOMER1_L2_1_L1_4CUSTOMER1_DELTA", Local);
         ("NUMCUSTCUSTOMER1_L2_1_L1_5",                Local);
         ("NUMCUSTCUSTOMER1_L2_1_L1_5CUSTOMER1_DELTA", Local);
         ("NUMCUSTCUSTOMER1_L2_1_L1_6",                Local);
         ("NUMCUSTCUSTOMER1_L2_1_L1_6CUSTOMER1_DELTA", Local);
         ("NUMCUSTCUSTOMER1_L2_1_L1_7",                Local);
         ("NUMCUSTCUSTOMER1_L2_1_L1_7CUSTOMER1_DELTA", Local);
         ("NUMCUSTCUSTOMER1_L2_2",                     Local);
         ("NUMCUSTCUSTOMER1_L2_2CUSTOMER1_DELTA",      Local);
         ("NUMCUSTCUSTOMER1_L2_4",                     Local);
         ("NUMCUSTCUSTOMER1_L2_4CUSTOMER1_DELTA",      Local);
         ("NUMCUSTCUSTOMER1_L2_6",                     Local);
         ("NUMCUSTCUSTOMER1_L2_6CUSTOMER1_DELTA",      Local);
         ("NUMCUSTCUSTOMER1_L2_8",                     Local);
         ("NUMCUSTCUSTOMER1_L2_8CUSTOMER1_DELTA",      Local);
         ("NUMCUSTCUSTOMER1_L2_10",                    Local);
         ("NUMCUSTCUSTOMER1_L2_10CUSTOMER1_DELTA",     Local);
         ("NUMCUSTCUSTOMER1_L2_12",                    Local);
         ("NUMCUSTCUSTOMER1_L2_12CUSTOMER1_DELTA",     Local);
         ("NUMCUSTCUSTOMER1_L2_14",                    Local);
         ("NUMCUSTCUSTOMER1_L2_14CUSTOMER1_DELTA",     Local);
         ("NUMCUSTCUSTOMER1_L3_1",                     DistributedByKey([0])); (* CK *)
         ("NUMCUSTCUSTOMER2",                          DistributedByKey([0])); (* CK *)
         ("NUMCUSTCUSTOMER2CUSTOMER1_DELTA",           Local); 
         ("NUMCUSTCUSTOMER3",                          DistributedByKey([0])); (* CK *)
         ("NUMCUSTCUSTOMER3CUSTOMER1_DELTA",           Local); 
         ("NUMCUSTCUSTOMER4",                          DistributedByKey([0])); (* CK *)
         ("NUMCUSTCUSTOMER4CUSTOMER1_DELTA",           Local); 
         ("NUMCUSTCUSTOMER5",                          DistributedByKey([0])); (* CK *)
         ("NUMCUSTCUSTOMER5CUSTOMER1_DELTA",           Local); 
         ("NUMCUSTCUSTOMER6",                          DistributedByKey([0])); (* CK *)
         ("NUMCUSTCUSTOMER6CUSTOMER1_DELTA",           Local); 
         ("NUMCUSTCUSTOMER7",                          DistributedByKey([0])); (* CK *)
         ("NUMCUSTCUSTOMER7CUSTOMER1_DELTA",           Local); 
         ("TOTALACCTBAL",                              Local);
         ("TOTALACCTBALCUSTOMER1",                     DistributedByKey([0])); (* CK *)
         ("TOTALACCTBALCUSTOMER1CUSTOMER1_DELTA",      Local); 
         ("TOTALACCTBALCUSTOMER2",                     DistributedByKey([0])); (* CK *)
         ("TOTALACCTBALCUSTOMER2CUSTOMER1_DELTA",      Local); 
         ("TOTALACCTBALCUSTOMER3",                     DistributedByKey([0])); (* CK *)
         ("TOTALACCTBALCUSTOMER3CUSTOMER1_DELTA",      Local); 
         ("TOTALACCTBALCUSTOMER4",                     DistributedByKey([0])); (* CK *)
         ("TOTALACCTBALCUSTOMER4CUSTOMER1_DELTA",      Local); 
         ("TOTALACCTBALCUSTOMER5",                     DistributedByKey([0])); (* CK *)
         ("TOTALACCTBALCUSTOMER5CUSTOMER1_DELTA",      Local); 
         ("TOTALACCTBALCUSTOMER6",                     DistributedByKey([0])); (* CK *)
         ("TOTALACCTBALCUSTOMER6CUSTOMER1_DELTA",      Local); 
         ("TOTALACCTBALCUSTOMER7",                     DistributedByKey([0])); (* CK *)
         ("TOTALACCTBALCUSTOMER7CUSTOMER1_DELTA",      Local); 
         ("DELTA_ORDERS",                              Local);
         ("DELTA_CUSTOMER",                            Local);
      ]
   in

   let ssb4_part_table = 
      create_hash_table [ 
         ("SSB4",                              Local); 
         ("SSB4SUPPLIER1_DELTA",               Local); 
         ("SSB4SUPPLIER1_P_1",                 DistributedByKey([0])); (* SK *)  
         ("SSB4SUPPLIER1_P_1PART1",            DistributedByKey([0])); (* PK *)
         ("SSB4SUPPLIER1_P_1PART1ORDERS1_P_2", DistributedByKey([0])); (* OK *)  
         ("SSB4SUPPLIER1_P_1PART1CUSTOMER1_P_1", DistributedByKey([1])); (* PK *)
         ("SSB4SUPPLIER1_P_1PART1CUSTOMER1_P_1LINEITEM1", DistributedByKey([0])); (* OK *)  
         ("SSB4SUPPLIER1_P_1ORDERS1_P_2",      DistributedByKey([0])); (* OK *)  
         ("SSB4SUPPLIER1_P_1CUSTOMER1_P_1",    DistributedByKey([0])); (* CK *)
         ("SSB4PART1_DELTA",                   Local); 
         ("SSB4PART1",                         DistributedByKey([0])); (* PK *)  
         ("SSB4PART1ORDERS1_P_2",              DistributedByKey([0])); (* OK *)  
         ("SSB4PART1CUSTOMER1_P_1",            DistributedByKey([1])); (* PK *)
         ("SSB4LINEITEM1_DELTA",               Local); 
         ("SSB4LINEITEM1_P_1",                 DistributedByKey([0])); (* OK *)  
         ("SSB4LINEITEM1_P_2",                 DistributedByKey([0])); (* PK *)  
         ("SSB4LINEITEM1_P_3",                 DistributedByKey([0])); (* SK *)  
         ("SSB4ORDERS1_DELTA",                 Local); 
         ("SSB4ORDERS1_P_1",                   DistributedByKey([0])); (* CK *)  
         ("SSB4ORDERS1_P_2",                   DistributedByKey([0])); (* OK *)  
         ("SSB4CUSTOMER1_DELTA",               Local); 
         ("SSB4CUSTOMER1_P_1",                 DistributedByKey([0])); (* CK *)  
         ("SSB4CUSTOMER1_P_2",                 DistributedByKey([]));  (* ** *)  
         ("DELTA_LINEITEM",                    Local);
         ("DELTA_ORDERS",                      Local);
         ("DELTA_CUSTOMER",                    Local);
         ("DELTA_PART",                        Local);
         ("DELTA_SUPPLIER",                    Local);
         ("NATION",                    Local);
      ]
   in

      create_hash_table [
         ("test/queries/tpch/query1.sql", tpch1_part_table);      
         ("test/queries/tpch/query2.sql", tpch2_part_table);      
         ("test/queries/tpch/query3.sql", tpch3_part_table);
         ("test/queries/tpch/query4.sql", tpch4_part_table);
         ("test/queries/tpch/query5.sql", tpch5_part_table);
         ("test/queries/tpch/query6.sql", tpch6_part_table);
         ("test/queries/tpch/query7.sql", tpch7_part_table);
         ("test/queries/tpch/query8.sql", tpch8_part_table);
         ("test/queries/tpch/query9.sql", tpch9_part_table);
         ("test/queries/tpch/query10.sql", tpch10_part_table);
         ("test/queries/tpch/query11.sql", tpch11_part_table);
         ("test/queries/tpch/query12.sql", tpch12_part_table);
         ("test/queries/tpch/query13.sql", tpch13_part_table);
         ("test/queries/tpch/query14.sql", tpch14_part_table);
         ("test/queries/tpch/query15.sql", tpch15_part_table);
         ("test/queries/tpch/query16.sql", tpch16_part_table);
         ("test/queries/tpch/query17.sql", tpch17_part_table);
         ("test/queries/tpch/query17a.sql", tpch17a_part_table);
         ("test/queries/tpch/query18.sql", tpch18_part_table);
         ("test/queries/tpch/query19.sql", tpch19_part_table);
         ("test/queries/tpch/query20.sql", tpch20_part_table);
         ("test/queries/tpch/query21.sql", tpch21_part_table);
         ("test/queries/tpch/query22.sql", tpch22_part_table);
         ("test/queries/tpch/ssb4.sql", ssb4_part_table);
      ] *)

let dist_part_table_by_file() = 

   let tpch1_part_table = 
      create_hash_table [
         ("SUM_QTY",                          Local);
         ("SUM_QTYLINEITEM1_DELTA",           DistributedRandom);
         ("SUM_BASE_PRICE",                   Local);
         ("SUM_BASE_PRICELINEITEM1_DELTA",    DistributedRandom);
         ("SUM_DISC_PRICE",                   Local);
         ("SUM_DISC_PRICELINEITEM1_DELTA",    DistributedRandom);
         ("SUM_CHARGE",                       Local);
         ("SUM_CHARGELINEITEM1_DELTA",        DistributedRandom);
         ("AVG_QTY",                          Local);
         ("AVG_QTYLINEITEM1_DOMAIN1",         DistributedRandom);
         ("AVG_QTYLINEITEM1_L1_1",            Local);
         ("AVG_QTYLINEITEM2",                 Local);
         ("AVG_QTYLINEITEM2_L1_2_DELTA",      DistributedRandom);
         ("AVG_PRICE",                        Local);
         ("AVG_PRICELINEITEM2",               Local);
         ("AVG_DISC",                         Local);
         ("AVG_DISCLINEITEM1_DELTA",          DistributedRandom);
         ("AVG_DISCLINEITEM2",                Local);
         ("COUNT_ORDER",                      Local);
         ("DELTA_LINEITEM",                   DistributedRandom);
      ]
   in

   let tpch2_part_table = 
      if Debug.active "HEURISTICS-WEAK-GRAPH-DECOMPOSITION" 
      then
         create_hash_table [ 
            ("COUNT",                            DistributedByKey([3]));  (* PK *)
            ("COUNTPARTSUPP1_DOMAIN1",           DistributedRandom);
            ("COUNTPARTSUPP1_L2_2_DELTA",        DistributedRandom);
            ("COUNTPARTSUPP1_L2_2",              DistributedByKey([0]));  (* SK *)
            ("COUNTPARTSUPP1_L2_2SUPPLIER1_DELTA", DistributedRandom);
            ("COUNTPARTSUPP1_L2_2SUPPLIER1",     DistributedByKey([]));   (*    *)
            ("COUNTPARTSUPP4_P_1",               DistributedByKey([0]));  (* PK *)
            ("COUNTPARTSUPP4_P_2",               DistributedByKey([0]));  (* SK *)
            ("COUNTPARTSUPP4_P_2SUPPLIER1_DELTA",  DistributedRandom);
            ("COUNTPARTSUPP4_P_2SUPPLIER1",      DistributedByKey([]));   (*    *)
            ("COUNTSUPPLIER1",                   DistributedByKey([0]));  (* PK *)
            ("COUNTSUPPLIER1SUPPLIER1_P_1",      DistributedByKey([0]));  (* PK *)   (* Alternative: SK *)
            ("COUNTSUPPLIER1SUPPLIER1_P_1PART1", DistributedByKey([0]));  (* PK *)   (* Alternative: SK *)
            ("COUNTPART1_DELTA",                 DistributedRandom);
            ("COUNTPART1",                       DistributedByKey([5]));  (* PK *)
            ("COUNTPART1_L2_1",                  DistributedByKey([0]));  (* PK *)
            ("DELTA_PARTSUPP",                   DistributedRandom);
            ("DELTA_SUPPLIER",                   DistributedRandom);
            ("DELTA_PART",                       DistributedRandom);
            ("NATION",                           Local);
            ("REGION",                           Local);
         ]
      else
         create_hash_table [ 
            ("COUNT",                            DistributedByKey([3]));  (* PK *)
            ("COUNTPARTSUPP1_DOMAIN1",           DistributedRandom);
            ("COUNTPARTSUPP1_P_1",               DistributedByKey([0]));  (* PK *)
            ("COUNTPARTSUPP1_L2_2_DELTA",        DistributedRandom);
            ("COUNTPARTSUPP1_L2_2",              DistributedByKey([0]));  (* SK *)
            ("COUNTPARTSUPP1_L2_2SUPPLIER1_DELTA", DistributedRandom);
            ("COUNTPARTSUPP1_L2_2SUPPLIER1",     DistributedByKey([]));   (*    *)
            ("COUNTPARTSUPP4_P_2",               DistributedByKey([0]));  (* SK *)
            ("COUNTPARTSUPP4_P_2SUPPLIER1_DELTA",  DistributedRandom);
            ("COUNTPARTSUPP4_P_2SUPPLIER1",      DistributedByKey([]));   (*    *)
            ("COUNTSUPPLIER1",                   DistributedByKey([0]));  (* PK *)
            ("COUNTSUPPLIER1SUPPLIER1_P_1",      DistributedByKey([0]));  (* PK *)   (* Alternative: SK *)
            ("COUNTSUPPLIER1SUPPLIER1_P_1PART1", DistributedByKey([0]));  (* PK *)   (* Alternative: SK *)
            ("COUNTPART1_DELTA",                 DistributedRandom);
            ("COUNTPART1",                       DistributedByKey([5]));  (* PK *)
            ("COUNTPART1_L2_1",                  DistributedByKey([0]));  (* PK *)
            ("DELTA_PARTSUPP",                   DistributedRandom);
            ("DELTA_SUPPLIER",                   DistributedRandom);
            ("DELTA_PART",                       DistributedRandom);
            ("NATION",                           Local);
            ("REGION",                           Local);
         ]
   in 

   let tpch3_part_table =
      create_hash_table [ 
         ("QUERY3",                      DistributedByKey([0]));   (* OK *)
         ("QUERY3LINEITEM1_DELTA",       DistributedRandom);
         ("QUERY3LINEITEM1",             DistributedByKey([0]));   (* OK *)
         ("QUERY3LINEITEM1CUSTOMER1",    DistributedByKey([0]));   (* OK *)
         ("QUERY3ORDERS1_DELTA",         DistributedRandom);
         ("QUERY3ORDERS1_P_1",           DistributedByKey([0]));   (* CK *)
         ("QUERY3ORDERS1_P_2",           DistributedByKey([0]));   (* OK *)
         ("QUERY3CUSTOMER1_DELTA",       DistributedRandom);
         ("QUERY3CUSTOMER1",             DistributedByKey([0]));   (* OK *)
         ("DELTA_LINEITEM",              DistributedRandom);
         ("DELTA_ORDERS",                DistributedRandom);
         ("DELTA_CUSTOMER",              DistributedRandom);
      ]      
   in

   let tpch4_part_table = 
      create_hash_table [
         ("ORDER_COUNT",                      Local);
         ("ORDER_COUNTLINEITEM1_DOMAIN1",     DistributedRandom);
         ("ORDER_COUNTLINEITEM1",             DistributedByKey([0]));  (* OK *)
         ("ORDER_COUNTLINEITEM1_E1_2_DELTA",  DistributedRandom);
         ("ORDER_COUNTORDERS1_DELTA",         DistributedRandom);
         ("ORDER_COUNTORDERS1_E1_1",          DistributedByKey([0]));  (* OK *)
         ("DELTA_LINEITEM",                   DistributedRandom);
         ("DELTA_ORDERS",                     DistributedRandom);
      ]
   in

   let tpch5_part_table = 
      if (Debug.active "HEURISTICS-DECOMPOSE-OVER-TABLES") then      
         create_hash_table [
            ("REVENUE",                        Local);
            ("REVENUESUPPLIER1_DELTA",         DistributedRandom);
            ("REVENUESUPPLIER1_P_2",           DistributedByKey([0])); (* SK *)
            ("REVENUESUPPLIER1_P_2ORDERS1_P_1", DistributedByKey([0])); (* OK *)   (* Alternative: SK *)
            ("REVENUESUPPLIER1_P_2CUSTOMER1",  DistributedByKey([1])); (* CK *)    (* Alternative: SK *)
            ("REVENUESUPPLIER1_P_2CUSTOMER1LINEITEM1", DistributedByKey([0])); (* OK *)  (* Alternative: CK *)
            ("REVENUELINEITEM1_DELTA",         DistributedRandom);
            ("REVENUELINEITEM1_T_2",           DistributedByKey([0])); (* SK *)
            ("REVENUELINEITEM1_T_3",           DistributedByKey([0])); (* OK *)
            ("REVENUEORDERS1_DELTA",           DistributedRandom);
            ("REVENUEORDERS1_T_2",             DistributedByKey([1])); (* OK *)
            ("REVENUEORDERS1_T_3",             DistributedByKey([0])); (* CK *)
            ("REVENUECUSTOMER1_DELTA",         DistributedRandom);
            ("REVENUECUSTOMER1_P_1",           DistributedByKey([]));  (* ** *)
            ("REVENUECUSTOMER1_P_2",           DistributedByKey([1])); (* CK *)
            ("DELTA_LINEITEM",                 DistributedRandom);
            ("DELTA_ORDERS",                   DistributedRandom);
            ("DELTA_CUSTOMER",                 DistributedRandom);
            ("DELTA_SUPPLIER",                 DistributedRandom);
            ("NATION",                         Local);
            ("REGION",                         Local);
         ]         
      else
         failwith "Running Q5 without HEURISTICS-DECOMPOSE-OVER-TABLES is not supported."
(*          
         create_hash_table [
            ("REVENUE",                        Local);
            ("REVENUESUPPLIER1_DELTA",         DistributedRandom);
            ("REVENUESUPPLIER1_P_2",           DistributedByKey([1])); (* SK *)
            ("REVENUESUPPLIER1_P_2LINEITEM1",  DistributedByKey([1])); (* OK *)
            ("REVENUESUPPLIER1_P_2LINEITEM1CUSTOMER1", DistributedByKey([0])); (* OK *)
            ("REVENUESUPPLIER1_P_2ORDERS1_P_1", DistributedByKey([0])); (* CK *)
            ("REVENUESUPPLIER1_P_2ORDERS1_P_2", DistributedByKey([0])); (* OK *)
            ("REVENUESUPPLIER1_P_2CUSTOMER1",  DistributedByKey([0])); (* CK *)
            ("REVENUELINEITEM1_DELTA",         DistributedRandom);
            ("REVENUELINEITEM1",               DistributedByKey([0])); (* OK *)
            ("REVENUELINEITEM1ORDERS1",        DistributedByKey([0])); (* CK *)
            ("REVENUELINEITEM1CUSTOMER1_P_3",  DistributedByKey([0])); (* SK *)
            ("REVENUEORDERS1_DELTA",           DistributedRandom);
            ("REVENUEORDERS1",                 DistributedByKey([1])); (* OK *)
            ("REVENUEORDERS1CUSTOMER1_P_2",    DistributedByKey([0])); (* OK *)
            ("REVENUECUSTOMER1_DELTA",         DistributedRandom);
            ("REVENUECUSTOMER1_P_1",           DistributedByKey([]));  (* ** *)
            ("REVENUECUSTOMER1_P_2",           DistributedByKey([0])); (* CK *)
            ("DELTA_LINEITEM",                 DistributedRandom);
            ("DELTA_ORDERS",                   DistributedRandom);
            ("DELTA_CUSTOMER",                 DistributedRandom);
            ("DELTA_SUPPLIER",                 DistributedRandom);
            ("NATION",                         Local);
            ("REGION",                         Local);
         ] *)
   in

   let tpch6_part_table = 
      create_hash_table [ 
         ("REVENUE",                Local); 
         ("REVENUELINEITEM1_DELTA", DistributedRandom);
         ("DELTA_LINEITEM",         DistributedRandom);
      ]
   in   

   let tpch7_part_table = 
      if (Debug.active "HEURISTICS-DECOMPOSE-OVER-TABLES") then      
         create_hash_table [ 
            ("REVENUE",                        Local); 
            ("REVENUECUSTOMER1_DELTA",         DistributedRandom);
            ("REVENUECUSTOMER1",               DistributedByKey([0])); (* CK *)
            ("REVENUECUSTOMER1ORDERS1",        DistributedByKey([0])); (* OK *)
            ("REVENUECUSTOMER1ORDERS1SUPPLIER1_P_2", DistributedByKey([0])); (* OK *)  (* Alternative: SK *)
            ("REVENUECUSTOMER1LINEITEM1_P_1",  DistributedByKey([0])); (* OK *)  (* Alternative: CK *)
            ("REVENUECUSTOMER1LINEITEM1_P_2",  DistributedByKey([0])); (* SK *)
            ("REVENUECUSTOMER1SUPPLIER1_P_2",  DistributedByKey([1])); (* CK *)  (* Alternative: SK *)
            ("REVENUEORDERS1_DELTA",           DistributedRandom);
            ("REVENUEORDERS1_T_2",             DistributedByKey([0])); (* CK *)
            ("REVENUEORDERS1_T_3",             DistributedByKey([0])); (* OK *)
            ("REVENUELINEITEM1_DOMAIN1",       DistributedRandom);
            ("REVENUELINEITEM1_DELTA",         DistributedRandom);
            ("REVENUELINEITEM1_T_1",           DistributedByKey([]));  (* ** *)            
            ("REVENUELINEITEM1_T_2",           DistributedByKey([1])); (* OK *)
            ("REVENUELINEITEM1_T_3",           DistributedByKey([0])); (* SK *)
            ("REVENUESUPPLIER1_DELTA",         DistributedRandom);
            ("REVENUESUPPLIER1",               DistributedByKey([0])); (* SK *)
            ("REVENUESUPPLIER1ORDERS1_P_1",    DistributedByKey([0])); (* CK *)
            ("REVENUESUPPLIER1LINEITEM1",      DistributedByKey([0])); (* OK *)
            ("DELTA_LINEITEM",                 DistributedRandom);
            ("DELTA_ORDERS",                   DistributedRandom);
            ("DELTA_CUSTOMER",                 DistributedRandom);
            ("DELTA_SUPPLIER",                 DistributedRandom);
            ("NATION",                         Local);            
         ]
      else   
         failwith "Running Q7 without HEURISTICS-DECOMPOSE-OVER-TABLES is not supported."   
      (* 
         create_hash_table [ 
            ("REVENUE",                        Local); 
            ("REVENUECUSTOMER1_DELTA",         DistributedRandom);
            ("REVENUECUSTOMER1",               DistributedByKey([0])); (* CK *)
            ("REVENUECUSTOMER1ORDERS1",        DistributedByKey([0])); (* OK *)
            ("REVENUECUSTOMER1ORDERS1SUPPLIER1_P_1", DistributedByKey([0])); (* OK *)
            ("REVENUECUSTOMER1LINEITEM1_P_1",  DistributedByKey([0])); (* SK *)
            ("REVENUECUSTOMER1LINEITEM1_P_2",  DistributedByKey([0])); (* OK *)
            ("REVENUECUSTOMER1SUPPLIER1_P_1",  DistributedByKey([1])); (* CK *)
            ("REVENUECUSTOMER1SUPPLIER1_P_2",  DistributedByKey([]));  (* ** *)
            ("REVENUEORDERS1_DELTA",           DistributedRandom);
            ("REVENUEORDERS1",                 DistributedByKey([0])); (* OK *)
            ("REVENUEORDERS1LINEITEM1",        DistributedByKey([1])); (* CK *) (* 0:SK *)
            ("REVENUEORDERS1SUPPLIER1_P_2",    DistributedByKey([0])); (* CK *)
            ("REVENUELINEITEM1_DOMAIN1",       DistributedRandom);
            ("REVENUELINEITEM1_DELTA",         DistributedRandom);
            ("REVENUELINEITEM1",               DistributedByKey([1])); (* OK *)
            ("REVENUELINEITEM1SUPPLIER1",      DistributedByKey([0])); (* OK *)
            ("REVENUESUPPLIER1_DELTA",         DistributedRandom);
            ("REVENUESUPPLIER1",               DistributedByKey([0])); (* SK *)
            ("DELTA_LINEITEM",                 DistributedRandom);
            ("DELTA_ORDERS",                   DistributedRandom);
            ("DELTA_CUSTOMER",                 DistributedRandom);
            ("DELTA_SUPPLIER",                 DistributedRandom);
            ("NATION",                         Local);
         ] *)
   in

   let tpch8_part_table = 
      create_hash_table [ 
         ("MKT_SHARE",                         Local); 
         ("MKT_SHAREORDERS1_DOMAIN1",          DistributedRandom);  
         ("MKT_SHAREORDERS1",                  Local);
         ("MKT_SHAREORDERS1CUSTOMER1_DELTA",   DistributedRandom);
         ("MKT_SHAREORDERS1CUSTOMER1_P_1",     DistributedByKey([0])); (* CK *)
         ("MKT_SHAREORDERS1CUSTOMER1_P_1LINEITEM1_P_3",    DistributedByKey([0])); (* OK *)    (* Alternative: CK *)
         ("MKT_SHAREORDERS1CUSTOMER1_P_1SUPPLIER1_P_2",      DistributedByKey([1])); (* CK *)    (* Alternative: SK *) 
         ("MKT_SHAREORDERS1CUSTOMER1_P_1SUPPLIER1_P_2PART1", DistributedByKey([0])); (* PK *)    (* Alternative: SK, CK *) 
         ("MKT_SHAREORDERS1CUSTOMER1_P_1SUPPLIER1_P_2PART1ORDERS1", DistributedByKey([0])); (* OK *)    (* Alternative: PK, SK *) 
         ("MKT_SHAREORDERS1CUSTOMER1_P_1SUPPLIER1_P_2ORDERS1",      DistributedByKey([0])); (* OK *)    (* Alternative: SK *) 
         ("MKT_SHAREORDERS1CUSTOMER1_P_1PART1", DistributedByKey([1])); (* PK *)     (* Alternative: CK *)
         ("MKT_SHAREORDERS1CUSTOMER1_P_1PART1ORDERS1", DistributedByKey([0])); (* OK *)    (* Alternative: PK *)
         ("MKT_SHAREORDERS1CUSTOMER1_P_2",     DistributedByKey([]));  (* ** *)          
         ("MKT_SHAREORDERS1LINEITEM1_DELTA",   DistributedRandom);
         ("MKT_SHAREORDERS1LINEITEM1_P_1",     DistributedByKey([0])); (* PK *)
         ("MKT_SHAREORDERS1LINEITEM1_P_2",     DistributedByKey([0])); (* OK *)
         ("MKT_SHAREORDERS1LINEITEM1_P_3",     DistributedByKey([0])); (* SK *)
         ("MKT_SHAREORDERS1SUPPLIER1_DELTA",   DistributedRandom);
         ("MKT_SHAREORDERS1SUPPLIER1_P_1",     DistributedByKey([0])); (* SK *)
         ("MKT_SHAREORDERS1SUPPLIER1_P_1PART1", DistributedByKey([0])); (* PK *)   (* Alternative: SK *) 
         ("MKT_SHAREORDERS1SUPPLIER1_P_2",     DistributedByKey([]));  (* ** *)
         ("MKT_SHAREORDERS1PART1_DELTA",       DistributedRandom);
         ("MKT_SHAREORDERS1PART1",             DistributedByKey([0])); (* PK *)
         ("MKT_SHAREORDERS1_L1_1_L1_2_DELTA",  DistributedRandom);
         ("MKT_SHAREORDERS1_L1_1_L1_2_P_1",    DistributedByKey([0])); (* CK *)
         ("MKT_SHAREORDERS1_L1_1_L1_2_P_2",    DistributedByKey([0])); (* OK *)
         ("MKT_SHAREORDERS1_L1_1_L1_2_P_2SUPPLIER1_P_2", DistributedByKey([])); 
         ("MKT_SHAREORDERS1_L1_1_L1_2_P_2PART1", DistributedByKey([0])); (* OK *)   (* Alternative: PK *)
         ("MKT_SHAREORDERS1_L1_1_L1_2_P_2LINEITEM1_P_2", DistributedByKey([0])); (* SK *)
         ("MKT_SHAREORDERS4_P_1",              DistributedByKey([0])); (* OK *)
         ("MKT_SHAREPART1",                    Local);
         ("MKT_SHAREPART1CUSTOMER1_P_2",       DistributedByKey([0])); (* CK *)
         ("MKT_SHAREPART1CUSTOMER1_P_2LINEITEM1_P_3",  DistributedByKey([0])); (* PK *)  (* Alternative: CK *)
         ("MKT_SHAREPART1CUSTOMER1_P_2SUPPLIER1_P_2",  DistributedByKey([1])); (* CK *)  (* Alternative: SK *)         
         ("MKT_SHAREPART1CUSTOMER1_P_2SUPPLIER1_P_2PART1",  DistributedByKey([0])); (* PK *)  (* Alternative: SK, CK *)
         ("MKT_SHAREPART1CUSTOMER1_P_2PART1", DistributedByKey([2])); (* PK *)   (* Alternative: CK *)
         ("MKT_SHAREPART1ORDERS1_DELTA",      DistributedRandom);
         ("MKT_SHAREPART1LINEITEM1_P_1",       DistributedByKey([0])); (* OK *)
         ("MKT_SHAREPART1SUPPLIER1_P_1",       DistributedByKey([0])); (* SK *)
         ("MKT_SHAREPART1SUPPLIER1_P_1PART1",  DistributedByKey([0])); (* PK *)  (* Alternative: SK *)
         ("MKT_SHAREPART1PART1",               DistributedByKey([0])); (* PK *)
         ("MKT_SHAREPART1_L2_1_L1_1",          Local);
         ("MKT_SHAREPART1_L2_1_L1_1CUSTOMER1_P_2",      DistributedByKey([0])); (* CK *)
         ("MKT_SHAREPART1_L2_1_L1_1CUSTOMER1_P_2PART1", DistributedByKey([0])); (* PK *)  (* Alternative: CK *)
         ("MKT_SHAREPART1_L2_1_L1_1PART1",     DistributedByKey([0])); (* PK *)
         ("DELTA_LINEITEM",                    DistributedRandom);
         ("DELTA_ORDERS",                      DistributedRandom);
         ("DELTA_CUSTOMER",                    DistributedRandom);
         ("DELTA_SUPPLIER",                    DistributedRandom);
         ("DELTA_PART",                        DistributedRandom);
         ("NATION",                            Local);
         ("REGION",                            Local);
      ]
   in

   let tpch9_part_table = 
      create_hash_table [ 
         ("SUM_PROFIT",                      Local); 
         ("SUM_PROFITORDERS11_DOMAIN1",      DistributedRandom); 
         ("SUM_PROFITORDERS11_DELTA",        DistributedRandom);
         ("SUM_PROFITORDERS11",              DistributedByKey([0])); (* OK *)
         ("SUM_PROFITORDERS11PARTSUPP1_P_1", DistributedByKey([0])); (* OK *)      (* Alternatives: PK, SK *)
         ("SUM_PROFITORDERS11SUPPLIER1_P_2", DistributedByKey([1])); (* OK *)      (* Alternative: SK *)
         ("SUM_PROFITORDERS11SUPPLIER1_P_2PART1", DistributedByKey([2])); (* OK *) (* Alternatives: PK, SK *)
         ("SUM_PROFITORDERS11PART1",         DistributedByKey([0])); (* OK *)      (* Alternative: PK *)
         ("SUM_PROFITORDERS13",              DistributedByKey([0])); (* OK *)
         ("SUM_PROFITORDERS13PARTSUPP1_P_1", DistributedByKey([0])); (* OK *)      (* Alternatives: PK, SK *)
         ("SUM_PROFITORDERS13SUPPLIER1_P_2", DistributedByKey([1])); (* OK *)      (* Alternative: SK *)
         ("SUM_PROFITORDERS13SUPPLIER1_P_2PART1", DistributedByKey([2])); (* OK *) (* Alternatives: PK, SK *)
         ("SUM_PROFITORDERS13PART1",         DistributedByKey([0])); (* OK *)      (* Alternative: PK *) 
         ("SUM_PROFITPARTSUPP11_DELTA",      DistributedRandom);
         ("SUM_PROFITPARTSUPP11_P_1",        DistributedByKey([0])); (* PK *)      (* Alternative: SK *)
         ("SUM_PROFITPARTSUPP13_DELTA",      DistributedRandom);
         ("SUM_PROFITPARTSUPP13_P_1",        DistributedByKey([0])); (* PK *)      (* Alternative: SK *)
         ("SUM_PROFITLINEITEM11_DELTA",      DistributedRandom);
         ("SUM_PROFITLINEITEM11_P_1",        DistributedByKey([0])); (* OK *)
         ("SUM_PROFITLINEITEM11_P_2",        DistributedByKey([0])); (* PK *)      (* Alternative: SK *)
         ("SUM_PROFITLINEITEM11_P_3",        DistributedByKey([0])); (* SK *)
         ("SUM_PROFITLINEITEM11_P_4",        DistributedByKey([0])); (* PK *)
         ("SUM_PROFITLINEITEM13_DELTA",      DistributedRandom);
         ("SUM_PROFITLINEITEM13_P_2",        DistributedByKey([0])); (* PK *)      (* Alternative: SK *)
         ("SUM_PROFITSUPPLIER11_DELTA",      DistributedRandom);
         ("SUM_PROFITSUPPLIER11_P_1",        DistributedByKey([]));  (* ** *)         
         ("SUM_PROFITSUPPLIER11_P_2",        DistributedByKey([0])); (* SK *)
         ("SUM_PROFITSUPPLIER11_P_2PART1",   DistributedByKey([0])); (* PK *)      (* Alternative: SK *)
         ("SUM_PROFITSUPPLIER13_P_2",        DistributedByKey([0])); (* SK *)
         ("SUM_PROFITSUPPLIER13_P_2PART1",   DistributedByKey([0])); (* PK *)      (* Alternative: SK *)
         ("SUM_PROFITPART11_DELTA",          DistributedRandom);
         ("SUM_PROFITPART11",                DistributedByKey([0])); (* PK *)
         ("SUM_PROFITPART13",                DistributedByKey([0])); (* PK *)
         ("DELTA_LINEITEM",                  DistributedRandom);
         ("DELTA_ORDERS",                    DistributedRandom);
         ("DELTA_PARTSUPP",                  DistributedRandom);
         ("DELTA_SUPPLIER",                  DistributedRandom);
         ("DELTA_PART",                      DistributedRandom);
         ("NATION",                          Local);
      ]
   in

   let tpch10_part_table = 
      create_hash_table [ 
         ("REVENUE",                       DistributedByKey([0])); (* CK *)
         ("REVENUELINEITEM1_DELTA",        DistributedRandom);
         ("REVENUELINEITEM1",              DistributedByKey([6])); (* OK *)      (* Alternative: CK *) 
         ("REVENUELINEITEM1CUSTOMER1_P_2", DistributedByKey([0])); (* OK *)      (* Alternative: CK *)
         ("REVENUEORDERS1_DELTA",          DistributedRandom);
         ("REVENUEORDERS1_P_1",            DistributedByKey([0])); (* OK *)
         ("REVENUEORDERS1_P_2",            DistributedByKey([0])); (* CK *)
         ("REVENUECUSTOMER1_DELTA",        DistributedRandom);
         ("REVENUECUSTOMER1_P_1",          DistributedByKey([0])); (* CK *)
         ("REVENUECUSTOMER1_P_2",          DistributedByKey([]));  (* ** *)
         ("DELTA_LINEITEM",                DistributedRandom);
         ("DELTA_ORDERS",                  DistributedRandom);
         ("DELTA_CUSTOMER",                DistributedRandom);
         ("NATION",                        Local);
      ]
   in

   let tpch11_part_table = 
      create_hash_table [ 
         ("QUERY11",                              DistributedByKey([0])); (* PK *)
         ("QUERY11PARTSUPP1_L1_1",                Local);
         ("QUERY11PARTSUPP1_L1_1SUPPLIER1_DELTA", DistributedRandom);
         ("QUERY11PARTSUPP1_L1_1SUPPLIER1_P_1",   DistributedByKey([]));  (* ** *)
         ("QUERY11PARTSUPP1_L1_1SUPPLIER1_P_2",   DistributedByKey([0])); (* SK *)
         ("QUERY11PARTSUPP1_L1_1PARTSUPP1_DELTA", DistributedRandom);
         ("QUERY11PARTSUPP1_L1_1PARTSUPP1",       DistributedByKey([0])); (* SK *)
         ("QUERY11PARTSUPP1_E2_1",                DistributedByKey([0])); (* PK *)
         ("QUERY11PARTSUPP1_E2_1SUPPLIER1_P_2",   DistributedByKey([1])); (* PK *)      (* Alternative: SK *)
         ("QUERY11PARTSUPP1_E2_1PARTSUPP1_DELTA", DistributedRandom);
         ("QUERY11PARTSUPP1_L3_1",                DistributedByKey([0])); (* PK *) 
         ("QUERY11PARTSUPP1_L3_1SUPPLIER1_P_2",   DistributedByKey([1])); (* PK *)      (* Alternative: SK *)
         ("QUERY11PARTSUPP1_L3_1PARTSUPP1_DELTA", DistributedRandom);
         ("DELTA_PARTSUPP",                       DistributedRandom);
         ("DELTA_SUPPLIER",                       DistributedRandom);
         ("NATION",                               Local);
      ]
   in

   let tpch12_part_table = 
      create_hash_table [ 
         ("HIGH_LINE_COUNT",                Local); 
         ("HIGH_LINE_COUNTLINEITEM1_DELTA", DistributedRandom);
         ("HIGH_LINE_COUNTLINEITEM1",       DistributedByKey([0]));     (* OK *)
         ("HIGH_LINE_COUNTLINEITEM2_DELTA", DistributedRandom);
         ("HIGH_LINE_COUNTLINEITEM3",       DistributedByKey([0]));     (* OK *)
         ("HIGH_LINE_COUNTORDERS1_DELTA",   DistributedRandom);
         ("HIGH_LINE_COUNTORDERS1",         DistributedByKey([0]));     (* OK *) 
         ("HIGH_LINE_COUNTORDERS2",         DistributedByKey([0]));     (* OK *) 
         ("HIGH_LINE_COUNTORDERS3_DELTA",   DistributedRandom);
         ("LOW_LINE_COUNT",                 Local);
         ("LOW_LINE_COUNTLINEITEM1",        DistributedByKey([0]));     (* OK *)
         ("LOW_LINE_COUNTORDERS1_DELTA",    DistributedRandom);
         ("DELTA_LINEITEM",                 DistributedRandom);
         ("DELTA_ORDERS",                   DistributedRandom);
      ]
   in

   let tpch13_part_table =   
      create_hash_table [ 
         ("CUSTDIST",                            Local); 
         ("CUSTDISTORDERS1_DOMAIN1",             DistributedRandom);
         ("CUSTDISTORDERS3_L1_2_DELTA",          DistributedRandom);
         ("CUSTDISTORDERS3_L1_2",                DistributedByKey([0])); (* CK *)
         ("CUSTDISTORDERS3_L1_2CUSTOMER1_DELTA", DistributedRandom);
         ("CUSTDISTCUSTOMER1_DOMAIN1",           DistributedRandom);
         ("CUSTDISTCUSTOMER1_L1_1",              DistributedByKey([0])); (* CK *)
         ("CUSTDISTCUSTOMER3_L1_2_DELTA",        DistributedRandom);
         ("CUSTDISTCUSTOMER3_L1_2",              DistributedByKey([0])); (* CK *)
         ("CUSTDISTCUSTOMER3_L1_2ORDERS1_DELTA", DistributedRandom);
         ("DELTA_ORDERS",                        DistributedRandom);
         ("DELTA_CUSTOMER",                      DistributedRandom);
      ]
   in

   let tpch14_part_table = 
      create_hash_table [ 
         ("PROMO_REVENUE",                                   Local); 
         ("PROMO_REVENUELINEITEM1_L1_1_L1_1",                Local);
         ("PROMO_REVENUELINEITEM1_L1_1_L1_1PART1_DELTA",     DistributedRandom);
         ("PROMO_REVENUELINEITEM1_L1_1_L1_1PART1",           DistributedByKey([0])); (* PK *)
         ("PROMO_REVENUELINEITEM1_L1_1_L1_1LINEITEM1_DELTA", DistributedRandom);
         ("PROMO_REVENUELINEITEM1_L1_1_L1_1LINEITEM1",       DistributedByKey([0])); (* PK *)
         ("PROMO_REVENUELINEITEM2",                          Local); 
         ("PROMO_REVENUELINEITEM2PART1_DELTA",               DistributedRandom);
         ("PROMO_REVENUELINEITEM2LINEITEM1",                 DistributedByKey([0])); (* PK *)
         ("DELTA_LINEITEM",                                  DistributedRandom);
         ("DELTA_PART",                                      DistributedRandom);
      ]
   in

   let tpch15_part_table = 
      create_hash_table [ 
         ("COUNT",                                   DistributedByKey([0])); (* SK *)
         ("COUNTSUPPLIER1_DELTA",                    DistributedRandom);
         ("COUNTLINEITEM1",                          DistributedByKey([0])); (* SK *)
         ("COUNTLINEITEM1_E2_1",                     DistributedByKey([0])); (* SK *)
         ("COUNTLINEITEM1_E2_1LINEITEM1_DELTA",      DistributedRandom);
         ("COUNTLINEITEM1_L3_1",                     DistributedByKey([0])); (* SK *)
         ("COUNTLINEITEM1_L3_1LINEITEM1_DELTA",      DistributedRandom);
         ("COUNTLINEITEM1_L4_1_E1_1",                DistributedByKey([0])); (* SK *)
         ("COUNTLINEITEM1_L4_1_E1_1LINEITEM1_DELTA", DistributedRandom);
         ("COUNTLINEITEM1_L4_1_L2_1",                DistributedByKey([0])); (* SK *)
         ("COUNTLINEITEM1_L4_1_L2_1LINEITEM1_DELTA", DistributedRandom);
         ("DELTA_LINEITEM",                          DistributedRandom);
         ("DELTA_SUPPLIER",                          DistributedRandom);
      ]
   in

   let tpch16_part_table = 
      if Debug.active "HEURISTICS-WEAK-GRAPH-DECOMPOSITION" 
      then
         create_hash_table [ 
            ("SUPPLIER_CNT",                           Local); 
            ("SUPPLIER_CNTSUPPLIER1_DOMAIN1",          DistributedRandom);
            ("SUPPLIER_CNTSUPPLIER1_E1_1_L1_2_DELTA",  DistributedRandom);
            ("SUPPLIER_CNTPART1_DOMAIN1",              DistributedRandom); 
            ("SUPPLIER_CNTPART1_E1_2_DELTA",           DistributedRandom);
            ("SUPPLIER_CNTPART1_E1_2",                 DistributedByKey([0])); (* PK *)  (* Alternative: SK *)
            ("SUPPLIER_CNTPARTSUPP1_DOMAIN1",          DistributedRandom);
            ("SUPPLIER_CNTPARTSUPP1_E1_1",             DistributedByKey([0])); (* SK *)
            ("SUPPLIER_CNTPARTSUPP1_E1_1_L1_1",        DistributedByKey([0])); (* SK *)
            ("SUPPLIER_CNTPARTSUPP1_E1_2_DELTA",       DistributedRandom);
            ("SUPPLIER_CNTPARTSUPP1_E1_2",             DistributedByKey([0])); (* PK *)
            ("DELTA_PART",                             DistributedRandom);
            ("DELTA_SUPPLIER",                         DistributedRandom);
            ("DELTA_PARTSUPP",                         DistributedRandom);
         ]      
      else
         create_hash_table [ 
            ("SUPPLIER_CNT",                           Local); 
            ("SUPPLIER_CNTSUPPLIER1_DOMAIN1",          DistributedRandom);
            ("SUPPLIER_CNTSUPPLIER1_E1_1_L1_2_DELTA",  DistributedRandom);
            ("SUPPLIER_CNTPART1_DOMAIN1",              DistributedRandom); 
            ("SUPPLIER_CNTPART1_E1_4",                 DistributedByKey([3])); (* SK *)
            ("SUPPLIER_CNTPART1_E1_4PARTSUPP1",        DistributedByKey([0])); (* PK *)
            ("SUPPLIER_CNTPART1_E1_8_DELTA",           DistributedRandom);
            ("SUPPLIER_CNTPART1_E1_8",                 DistributedByKey([0])); (* PK *)  (* Alternative: SK *)
            ("SUPPLIER_CNTPARTSUPP1_DOMAIN1",          DistributedRandom);
            ("SUPPLIER_CNTPARTSUPP1_E1_1_L1_1",        DistributedByKey([0])); (* SK *)
            ("SUPPLIER_CNTPARTSUPP1_E1_2",             DistributedByKey([0])); (* SK *)
            ("SUPPLIER_CNTPARTSUPP1_E1_2PART1_DELTA",  DistributedRandom);
            ("SUPPLIER_CNTPARTSUPP1_E1_4_DELTA",       DistributedRandom); 
            ("SUPPLIER_CNTPARTSUPP1_E1_4",             DistributedByKey([0])); (* SK *)
            ("DELTA_PART",                             DistributedRandom);
            ("DELTA_SUPPLIER",                         DistributedRandom);
            ("DELTA_PARTSUPP",                         DistributedRandom);
         ]
   in

   let tpch17_part_table = 
      if Debug.active "HEURISTICS-WEAK-GRAPH-DECOMPOSITION" 
      then
         create_hash_table [ 
            ("AVG_YEARLY",                          Local); 
            ("AVG_YEARLYPART1_DELTA",               DistributedRandom); 
            ("AVG_YEARLYPART1",                     DistributedByKey([0])); (* PK *)
            ("AVG_YEARLYLINEITEM1_DOMAIN1_P_3",     DistributedRandom); 
            ("AVG_YEARLYLINEITEM1",                 DistributedByKey([0])); (* PK *)
            ("AVG_YEARLYLINEITEM1PART1_DELTA",      DistributedRandom); 
            ("AVG_YEARLYLINEITEM1_L1_1",            DistributedByKey([0])); (* PK *)
            ("AVG_YEARLYLINEITEM1_L1_1_L1_1",       DistributedByKey([0])); (* PK *)
            ("AVG_YEARLYLINEITEM1_L1_1_L1_2_DELTA", DistributedRandom); 
            ("AVG_YEARLYLINEITEM1_L1_3_DELTA",      DistributedRandom);             
            ("AVG_YEARLYLINEITEM5_DELTA",           DistributedRandom); 
            ("AVG_YEARLYLINEITEM5",                 DistributedByKey([0])); (* PK *)
            ("DELTA_LINEITEM",                      DistributedRandom);
            ("DELTA_PART",                          DistributedRandom);
         ]      
      else   
         create_hash_table [ 
            ("AVG_YEARLY",                          Local); 
            ("AVG_YEARLYPART1_DELTA",               DistributedRandom); 
            ("AVG_YEARLYLINEITEM1_DOMAIN1_P_3",     DistributedRandom); 
            ("AVG_YEARLYLINEITEM1_P_3",             DistributedByKey([0])); (* PK *)
            ("AVG_YEARLYLINEITEM1_P_3PART1_DELTA",  DistributedRandom); 
            ("AVG_YEARLYLINEITEM1_P_4",             DistributedByKey([0])); (* PK *)
            ("AVG_YEARLYLINEITEM1_L1_1_L1_1",       DistributedByKey([0])); (* PK *)
            ("AVG_YEARLYLINEITEM1_L1_1_L1_2_DELTA", DistributedRandom); 
            ("AVG_YEARLYLINEITEM1_L1_2",            DistributedByKey([0])); (* PK *)
            ("AVG_YEARLYLINEITEM1_L1_4_DELTA",      DistributedRandom); 
            ("AVG_YEARLYLINEITEM5_DELTA",           DistributedRandom); 
            ("DELTA_LINEITEM",                      DistributedRandom);
            ("DELTA_PART",                          DistributedRandom);
         ]
   in

   let tpch17a_part_table = 
      create_hash_table [ 
         ("QUERY17",                     Local); 
         ("QUERY17PART1_DELTA",          DistributedRandom); 
         ("QUERY17LINEITEM1_DOMAIN1",    DistributedRandom); 
         ("QUERY17LINEITEM1_P_1",        DistributedByKey([0])); (* PK *)
         ("QUERY17LINEITEM1_P_2",        DistributedByKey([0])); (* PK *)
         ("QUERY17LINEITEM1_L1_1",       DistributedByKey([0])); (* PK *)
         ("QUERY17LINEITEM1_L1_3_DELTA", DistributedRandom); 
         ("QUERY17LINEITEM2_DELTA",      DistributedRandom); 
         ("DELTA_LINEITEM",              DistributedRandom);
         ("DELTA_PART",                  DistributedRandom);
      ]
   in   

   let tpch18_part_table = 
      if Debug.active "HEURISTICS-WEAK-GRAPH-DECOMPOSITION" 
      then
         create_hash_table [ 
            ("QUERY18",                       DistributedByKey([2])); (* OK *) (* Alternative: CK *)
            ("QUERY18LINEITEM1_DELTA",        DistributedRandom); 
            ("QUERY18LINEITEM1",              DistributedByKey([0])); (* OK *) (* Alternative: CK *)
            ("QUERY18LINEITEM1CUSTOMER1",     DistributedByKey([0])); (* OK *) (* Alternative: CK *)
            ("QUERY18LINEITEM2_DOMAIN1",      DistributedRandom); 
            ("QUERY18LINEITEM2",              DistributedByKey([0])); (* OK *) (* Alternative: CK *)
            ("QUERY18ORDERS1_DELTA",          DistributedRandom); 
            ("QUERY18ORDERS1_P_1",            DistributedByKey([0])); (* CK *)
            ("QUERY18CUSTOMER1_DELTA",        DistributedRandom); 
            ("QUERY18CUSTOMER1",              DistributedByKey([0])); (* OK *) (* Alternative: CK *)
            ("QUERY18CUSTOMER1_L1_1",         DistributedByKey([0])); (* OK *)
            ("DELTA_LINEITEM",                DistributedRandom);
            ("DELTA_ORDERS",                  DistributedRandom);
            ("DELTA_CUSTOMER",                DistributedRandom);
         ]
      else
         create_hash_table [ 
            ("QUERY18",                       DistributedByKey([2])); (* OK *) (* Alternative: CK *)
            ("QUERY18LINEITEM1_DELTA",        DistributedRandom); 
            ("QUERY18LINEITEM1",              DistributedByKey([0])); (* OK *) (* Alternative: CK *)
            ("QUERY18LINEITEM1CUSTOMER1",     DistributedByKey([0])); (* OK *) (* Alternative: CK *)
            ("QUERY18LINEITEM2_DOMAIN1",      DistributedRandom); 
            ("QUERY18ORDERS1_DELTA",          DistributedRandom); 
            ("QUERY18ORDERS1_P_1",            DistributedByKey([0])); (* CK *)
            ("QUERY18CUSTOMER1_DELTA",        DistributedRandom); 
            ("QUERY18CUSTOMER1",              DistributedByKey([0])); (* OK *) (* Alternative: CK *)
            ("QUERY18CUSTOMER1_L1_1",         DistributedByKey([0])); (* OK *)
            ("DELTA_LINEITEM",                DistributedRandom);
            ("DELTA_ORDERS",                  DistributedRandom);
            ("DELTA_CUSTOMER",                DistributedRandom);
         ]
   in

   let tpch19_part_table = 
      create_hash_table [ 
         ("REVENUE",                Local); 
         ("REVENUEPART1_DELTA",     DistributedRandom); 
         ("REVENUEPART1",           DistributedByKey([0])); (* PK *)
         ("REVENUELINEITEM1_DELTA", DistributedRandom); 
         ("REVENUELINEITEM1",       DistributedByKey([0])); (* PK *)
         ("DELTA_LINEITEM",         DistributedRandom);
         ("DELTA_PART",             DistributedRandom);
      ]
   in

   let tpch20_part_table = 
      if Debug.active "HEURISTICS-WEAK-GRAPH-DECOMPOSITION" 
      then
         create_hash_table [ 
            ("COUNT",                               Local); (* Alternative: 0:S_NAME   1:S_ADDRESS *) 
            ("COUNTPART1",                          DistributedByKey([0])); (* SK *)
            ("COUNTLINEITEM1_DOMAIN1_P_2",          DistributedRandom); 
            ("COUNTLINEITEM1_E1_1_L1_3_DELTA",      DistributedRandom); 
            ("COUNTPARTSUPP1_DOMAIN1_P_2",          DistributedRandom); 
            ("COUNTPARTSUPP1",                      DistributedByKey([0])); (* SK *)
            ("COUNTPARTSUPP1SUPPLIER1",             DistributedByKey([]));  (* ** *)
            ("COUNTPARTSUPP1_E1_2_DELTA",           DistributedRandom); 
            ("COUNTSUPPLIER1_DELTA",                DistributedRandom); 
            ("COUNTSUPPLIER1",                      DistributedByKey([]));  (* ** *)
            ("COUNTSUPPLIER1_E1_1",                 DistributedByKey([0])); (* PK *)  (* Alternative: SK *)
            ("COUNTSUPPLIER1_E1_1_L1_1",            DistributedByKey([0])); (* PK *)  (* Alternative: SK *)
            ("COUNTSUPPLIER1_E1_1_E2_1",            DistributedByKey([0])); (* PK *)
            ("COUNTSUPPLIER1_E1_1_E2_1PART1_DELTA", DistributedRandom); 
            ("DELTA_LINEITEM",                      DistributedRandom);
            ("DELTA_PARTSUPP",                      DistributedRandom);
            ("DELTA_SUPPLIER",                      DistributedRandom);
            ("DELTA_PART",                          DistributedRandom);
            ("NATION",                              DistributedRandom);
         ]      
      else
         create_hash_table [ 
            ("COUNT",                               Local); (* Alternative: 0:S_NAME   1:S_ADDRESS *) 
            ("COUNTPART1",                          DistributedByKey([0])); (* SK *)
            ("COUNTLINEITEM1_DOMAIN1_P_2",          DistributedRandom); 
            ("COUNTLINEITEM1_E1_1_L1_3_DELTA",      DistributedRandom); 
            ("COUNTPARTSUPP1_DOMAIN1_P_2",          DistributedRandom); 
            ("COUNTPARTSUPP1_P_2",                  DistributedByKey([1])); (* SK *)
            ("COUNTPARTSUPP1_P_2SUPPLIER1",         DistributedByKey([]));  (* ** *)
            ("COUNTPARTSUPP1_E1_2_DELTA",           DistributedRandom); 
            ("COUNTSUPPLIER1_DELTA",                DistributedRandom); 
            ("COUNTSUPPLIER1",                      DistributedByKey([]));  (* ** *)
            ("COUNTSUPPLIER1_E1_1",                 DistributedByKey([0])); (* PK *)  (* Alternative: SK *)
            ("COUNTSUPPLIER1_E1_1_L1_1",            DistributedByKey([0])); (* PK *)  (* Alternative: SK *)
            ("COUNTSUPPLIER1_E1_1_E2_1",            DistributedByKey([0])); (* PK *)
            ("COUNTSUPPLIER1_E1_1_E2_1PART1_DELTA", DistributedRandom); 
            ("DELTA_LINEITEM",                      DistributedRandom);
            ("DELTA_PARTSUPP",                      DistributedRandom);
            ("DELTA_SUPPLIER",                      DistributedRandom);
            ("DELTA_PART",                          DistributedRandom);
            ("NATION",                              DistributedRandom);
         ]
   in

   let tpch21_part_table = 
      if Debug.active "HEURISTICS-WEAK-GRAPH-DECOMPOSITION" 
      then
         create_hash_table [ 
            ("NUMWAIT",                           Local); (* Alternative: 0: S_NAME *)
            ("NUMWAITORDERS1_DELTA",              DistributedRandom); 
            ("NUMWAITORDERS1",                    DistributedByKey([2])); (* OK *)  (* Alternative: SK *)
            ("NUMWAITORDERS1LINEITEM1",           DistributedByKey([0])); (* SK *)
            ("NUMWAITLINEITEM1_DOMAIN1_P_3",      DistributedRandom); 
            ("NUMWAITLINEITEM1",                  DistributedByKey([0])); (* SK *)
            ("NUMWAITLINEITEM1SUPPLIER1_P_1",     DistributedByKey([]));  (* ** *)
            ("NUMWAITLINEITEM1SUPPLIER1_P_2",     DistributedByKey([0])); (* OK *)  (* Alternative: SK *)
            ("NUMWAITLINEITEM1ORDERS1_DELTA",     DistributedRandom); 
            ("NUMWAITLINEITEM1ORDERS1",           DistributedByKey([3])); (* OK *)
            ("NUMWAITLINEITEM3_L2_2_DELTA",       DistributedRandom); 
            ("NUMWAITLINEITEM3_E3_2_DELTA",       DistributedRandom); 
            ("NUMWAITLINEITEM4_P_1",              DistributedByKey([0])); (* OK *) 
            ("NUMWAITLINEITEM4_P_2",              DistributedByKey([1])); (* SK *) 
            ("NUMWAITSUPPLIER1_DELTA",            DistributedRandom); 
            ("NUMWAITSUPPLIER1_P_1",              DistributedByKey([]));  (* ** *)
            ("NUMWAITSUPPLIER1_P_2",              DistributedByKey([0])); (* OK *) (* Alternative: SK *)
            ("NUMWAITSUPPLIER1_P_2LINEITEM1",     DistributedByKey([0])); (* OK *)
            ("NUMWAITSUPPLIER1_L2_1",             DistributedByKey([0])); (* OK *) (* Alternative: SK *)
            ("NUMWAITSUPPLIER1_E3_1",             DistributedByKey([0])); (* OK *) (* Alternative: SK *) 
            ("DELTA_LINEITEM",                    DistributedRandom);
            ("DELTA_ORDERS",                      DistributedRandom);
            ("DELTA_SUPPLIER",                    DistributedRandom);
            ("NATION",                            Local);
         ]      
      else   
         create_hash_table [ 
            ("NUMWAIT",                           Local); (* Alternative: 0: S_NAME *)
            ("NUMWAITORDERS1_DELTA",              DistributedRandom); 
            ("NUMWAITORDERS1",                    DistributedByKey([2])); (* OK *)  (* Alternative: SK *)
            ("NUMWAITORDERS1LINEITEM1",           DistributedByKey([0])); (* SK *)
            ("NUMWAITLINEITEM1_DOMAIN1_P_3",      DistributedRandom); 
            ("NUMWAITLINEITEM1_P_3",              DistributedByKey([1])); (* OK *)  (* Alternative: SK *)
            ("NUMWAITLINEITEM1_P_3SUPPLIER1_P_1", DistributedByKey([]));  (* ** *)
            ("NUMWAITLINEITEM1_P_4",              DistributedByKey([0])); (* OK *)
            ("NUMWAITLINEITEM1_P_4ORDERS1_DELTA", DistributedRandom); 
            ("NUMWAITLINEITEM3_L2_2_DELTA",       DistributedRandom); 
            ("NUMWAITLINEITEM3_E3_2_DELTA",       DistributedRandom); 
            ("NUMWAITLINEITEM4_P_3",              DistributedByKey([1])); (* SK *) 
            ("NUMWAITSUPPLIER1_DELTA",            DistributedRandom); 
            ("NUMWAITSUPPLIER1_P_1",              DistributedByKey([]));  (* ** *)
            ("NUMWAITSUPPLIER1_P_2",              DistributedByKey([0])); (* OK *) (* Alternative: SK *)
            ("NUMWAITSUPPLIER1_P_2LINEITEM1",     DistributedByKey([0])); (* OK *)
            ("NUMWAITSUPPLIER1_L2_1",             DistributedByKey([0])); (* OK *) (* Alternative: SK *)
            ("NUMWAITSUPPLIER1_E3_1",             DistributedByKey([0])); (* OK *) (* Alternative: SK *) 
            ("DELTA_LINEITEM",                    DistributedRandom);
            ("DELTA_ORDERS",                      DistributedRandom);
            ("DELTA_SUPPLIER",                    DistributedRandom);
            ("NATION",                            Local);
         ]
   in

   let tpch22_part_table = 
      create_hash_table [ 
         ("NUMCUST",                                   Local); 
         ("NUMCUSTORDERS1_DOMAIN1",                    DistributedRandom); 
         ("NUMCUSTORDERS1_L2_2_DELTA",                 DistributedRandom); 
         ("NUMCUSTCUSTOMER1",                          DistributedByKey([0])); (* CK *)
         ("NUMCUSTCUSTOMER1CUSTOMER1_DELTA",           DistributedRandom); 
         ("NUMCUSTCUSTOMER1_L2_1_L1_1",                Local); 
         ("NUMCUSTCUSTOMER1_L2_1_L1_1CUSTOMER1_DELTA", DistributedRandom);
         ("NUMCUSTCUSTOMER1_L2_1_L1_2",                Local); 
         ("NUMCUSTCUSTOMER1_L2_1_L1_2CUSTOMER1_DELTA", DistributedRandom);
         ("NUMCUSTCUSTOMER1_L2_1_L1_3",                Local); 
         ("NUMCUSTCUSTOMER1_L2_1_L1_3CUSTOMER1_DELTA", DistributedRandom);
         ("NUMCUSTCUSTOMER1_L2_1_L1_4",                Local); 
         ("NUMCUSTCUSTOMER1_L2_1_L1_4CUSTOMER1_DELTA", DistributedRandom);
         ("NUMCUSTCUSTOMER1_L2_1_L1_5",                Local);
         ("NUMCUSTCUSTOMER1_L2_1_L1_5CUSTOMER1_DELTA", DistributedRandom);
         ("NUMCUSTCUSTOMER1_L2_1_L1_6",                Local);
         ("NUMCUSTCUSTOMER1_L2_1_L1_6CUSTOMER1_DELTA", DistributedRandom);
         ("NUMCUSTCUSTOMER1_L2_1_L1_7",                Local);
         ("NUMCUSTCUSTOMER1_L2_1_L1_7CUSTOMER1_DELTA", DistributedRandom);
         ("NUMCUSTCUSTOMER1_L2_2",                     Local);
         ("NUMCUSTCUSTOMER1_L2_2CUSTOMER1_DELTA",      DistributedRandom);
         ("NUMCUSTCUSTOMER1_L2_4",                     Local);
         ("NUMCUSTCUSTOMER1_L2_4CUSTOMER1_DELTA",      DistributedRandom);
         ("NUMCUSTCUSTOMER1_L2_6",                     Local);
         ("NUMCUSTCUSTOMER1_L2_6CUSTOMER1_DELTA",      DistributedRandom);
         ("NUMCUSTCUSTOMER1_L2_8",                     Local);
         ("NUMCUSTCUSTOMER1_L2_8CUSTOMER1_DELTA",      DistributedRandom);
         ("NUMCUSTCUSTOMER1_L2_10",                    Local);
         ("NUMCUSTCUSTOMER1_L2_10CUSTOMER1_DELTA",     DistributedRandom);
         ("NUMCUSTCUSTOMER1_L2_12",                    Local);
         ("NUMCUSTCUSTOMER1_L2_12CUSTOMER1_DELTA",     DistributedRandom);
         ("NUMCUSTCUSTOMER1_L2_14",                    Local);
         ("NUMCUSTCUSTOMER1_L2_14CUSTOMER1_DELTA",     DistributedRandom);
         ("NUMCUSTCUSTOMER1_L3_1",                     DistributedByKey([0])); (* CK *)
         ("NUMCUSTCUSTOMER2",                          DistributedByKey([0])); (* CK *)
         ("NUMCUSTCUSTOMER2CUSTOMER1_DELTA",           DistributedRandom); 
         ("NUMCUSTCUSTOMER3",                          DistributedByKey([0])); (* CK *)
         ("NUMCUSTCUSTOMER3CUSTOMER1_DELTA",           DistributedRandom); 
         ("NUMCUSTCUSTOMER4",                          DistributedByKey([0])); (* CK *)
         ("NUMCUSTCUSTOMER4CUSTOMER1_DELTA",           DistributedRandom); 
         ("NUMCUSTCUSTOMER5",                          DistributedByKey([0])); (* CK *)
         ("NUMCUSTCUSTOMER5CUSTOMER1_DELTA",           DistributedRandom); 
         ("NUMCUSTCUSTOMER6",                          DistributedByKey([0])); (* CK *)
         ("NUMCUSTCUSTOMER6CUSTOMER1_DELTA",           DistributedRandom); 
         ("NUMCUSTCUSTOMER7",                          DistributedByKey([0])); (* CK *)
         ("NUMCUSTCUSTOMER7CUSTOMER1_DELTA",           DistributedRandom); 
         ("TOTALACCTBAL",                              Local);
         ("TOTALACCTBALCUSTOMER1",                     DistributedByKey([0])); (* CK *)
         ("TOTALACCTBALCUSTOMER1CUSTOMER1_DELTA",      DistributedRandom); 
         ("TOTALACCTBALCUSTOMER2",                     DistributedByKey([0])); (* CK *)
         ("TOTALACCTBALCUSTOMER2CUSTOMER1_DELTA",      DistributedRandom); 
         ("TOTALACCTBALCUSTOMER3",                     DistributedByKey([0])); (* CK *)
         ("TOTALACCTBALCUSTOMER3CUSTOMER1_DELTA",      DistributedRandom); 
         ("TOTALACCTBALCUSTOMER4",                     DistributedByKey([0])); (* CK *)
         ("TOTALACCTBALCUSTOMER4CUSTOMER1_DELTA",      DistributedRandom); 
         ("TOTALACCTBALCUSTOMER5",                     DistributedByKey([0])); (* CK *)
         ("TOTALACCTBALCUSTOMER5CUSTOMER1_DELTA",      DistributedRandom); 
         ("TOTALACCTBALCUSTOMER6",                     DistributedByKey([0])); (* CK *)
         ("TOTALACCTBALCUSTOMER6CUSTOMER1_DELTA",      DistributedRandom); 
         ("TOTALACCTBALCUSTOMER7",                     DistributedByKey([0])); (* CK *)
         ("TOTALACCTBALCUSTOMER7CUSTOMER1_DELTA",      DistributedRandom); 
         ("DELTA_ORDERS",                              DistributedRandom);
         ("DELTA_CUSTOMER",                            DistributedRandom);
      ]
   in

   let ssb4_part_table = 
      create_hash_table [ 
         ("SSB4",                              Local); 
         ("SSB4SUPPLIER1_DELTA",               DistributedRandom); 
         ("SSB4SUPPLIER1_P_2",                 DistributedByKey([0])); (* SK *)  
         ("SSB4SUPPLIER1_P_2PART1",            DistributedByKey([0])); (* PK *)
         ("SSB4SUPPLIER1_P_2PART1ORDERS1_P_1", DistributedByKey([0])); (* OK *)  
         ("SSB4SUPPLIER1_P_2PART1CUSTOMER1_P_2", DistributedByKey([1])); (* PK *)
         ("SSB4SUPPLIER1_P_2PART1CUSTOMER1_P_2LINEITEM1", DistributedByKey([0])); (* OK *)  
         ("SSB4SUPPLIER1_P_2ORDERS1_P_1",      DistributedByKey([0])); (* OK *)  
         ("SSB4SUPPLIER1_P_2CUSTOMER1_P_2",    DistributedByKey([0])); (* CK *)
         ("SSB4PART1_DELTA",                   DistributedRandom); 
         ("SSB4PART1",                         DistributedByKey([0])); (* PK *)  
         ("SSB4PART1ORDERS1_P_2",              DistributedByKey([0])); (* OK *)  
         ("SSB4PART1CUSTOMER1_P_2",            DistributedByKey([1])); (* PK *)
         ("SSB4LINEITEM1_DELTA",               DistributedRandom); 
         ("SSB4LINEITEM1_P_1",                 DistributedByKey([0])); (* OK *)  
         ("SSB4LINEITEM1_P_2",                 DistributedByKey([0])); (* PK *)  
         ("SSB4LINEITEM1_P_3",                 DistributedByKey([0])); (* SK *)  
         ("SSB4ORDERS1_DELTA",                 DistributedRandom); 
         ("SSB4ORDERS1_P_1",                   DistributedByKey([0])); (* CK *)  
         ("SSB4ORDERS1_P_2",                   DistributedByKey([0])); (* OK *)  
         ("SSB4CUSTOMER1_DELTA",               DistributedRandom); 
         ("SSB4CUSTOMER1_P_1",                 DistributedByKey([]));  (* ** *)  
         ("SSB4CUSTOMER1_P_2",                 DistributedByKey([0])); (* CK *)  
         ("DELTA_LINEITEM",                    DistributedRandom);
         ("DELTA_ORDERS",                      DistributedRandom);
         ("DELTA_CUSTOMER",                    DistributedRandom);
         ("DELTA_PART",                        DistributedRandom);
         ("DELTA_SUPPLIER",                    DistributedRandom);
         ("NATION",                            Local);
      ]
   in

      create_hash_table [
         ("test/queries/tpch/query1.sql", tpch1_part_table);
         ("test/queries/tpch/query2.sql", tpch2_part_table);
         ("test/queries/tpch/query3.sql", tpch3_part_table);
         ("test/queries/tpch/query4.sql", tpch4_part_table);
         ("test/queries/tpch/query5.sql", tpch5_part_table);
         ("test/queries/tpch/query6.sql", tpch6_part_table);
         ("test/queries/tpch/query7.sql", tpch7_part_table);
         ("test/queries/tpch/query8.sql", tpch8_part_table);
         ("test/queries/tpch/query9.sql", tpch9_part_table);
         ("test/queries/tpch/query10.sql", tpch10_part_table);
         ("test/queries/tpch/query11.sql", tpch11_part_table);
         ("test/queries/tpch/query12.sql", tpch12_part_table);
         ("test/queries/tpch/query13.sql", tpch13_part_table);
         ("test/queries/tpch/query14.sql", tpch14_part_table);
         ("test/queries/tpch/query15.sql", tpch15_part_table);
         ("test/queries/tpch/query16.sql", tpch16_part_table);
         ("test/queries/tpch/query17.sql", tpch17_part_table);
         ("test/queries/tpch/query17a.sql", tpch17a_part_table);
         ("test/queries/tpch/query18.sql", tpch18_part_table);
         ("test/queries/tpch/query19.sql", tpch19_part_table);
         ("test/queries/tpch/query20.sql", tpch20_part_table);
         ("test/queries/tpch/query21.sql", tpch21_part_table);
         ("test/queries/tpch/query22.sql", tpch22_part_table);
         ("test/queries/tpch/ssb4.sql", ssb4_part_table);
      ]

let get_part_table (file: string) = 
   try 
      (* if (Debug.active "PARTITION-USING-LOCAL-BATCH") 
      then Hashtbl.find (local_part_table_by_file ()) file
      else  *)
      Hashtbl.find (dist_part_table_by_file ()) file
      
   with Not_found -> create_hash_table []

let string_of_part_info (part_info: ('key_t) gen_part_info_t)
                        (string_fn: 'key_t -> string): string = 
   if not (Debug.active "PRINT-PARTITION-INFO") then "" 
   else match part_info with
      | Local -> "<Local>"
      | DistributedRandom -> "<DistRandom>"
      | DistributedByKey(pkeys) -> 
         "<DistByKey(" ^ 
            (ListExtras.string_of_list ~sep:", " string_fn pkeys) ^ ")>"