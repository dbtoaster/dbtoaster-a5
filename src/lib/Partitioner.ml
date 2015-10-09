(** Provide hard-coded partitioning information for TPC-H queries *)



let create_hashtbl pairs =
   let hashtbl = Hashtbl.create (List.length pairs) in
   List.iter (fun (k, v) -> Hashtbl.add hashtbl k v) pairs;
   hashtbl

let part_table_context =
   let tpch1_part_table = 
      create_hashtbl [
         ("SUM_QTY",                          None);
         ("SUM_QTYLINEITEM1_DELTA",           None);
         ("SUM_BASE_PRICE",                   None);
         ("SUM_BASE_PRICELINEITEM1_DELTA",    None);
         ("SUM_DISC_PRICE",                   None);
         ("SUM_DISC_PRICELINEITEM1_DELTA",    None);
         ("SUM_CHARGE",                       None);
         ("SUM_CHARGELINEITEM1_DELTA",        None);
         ("AVG_QTY",                          None);
         ("AVG_QTYLINEITEM1_DOMAIN1",         None);
         ("AVG_QTYLINEITEM1",                 None);
         ("AVG_QTYLINEITEM1_L1_1",            None);
         ("AVG_QTYLINEITEM1_L1_2_DELTA",      None);
         ("AVG_PRICE",                        None);
         ("AVG_PRICELINEITEM1",               None);
         ("AVG_DISC",                         None);
         ("AVG_DISCLINEITEM1",                None);
         ("AVG_DISCLINEITEM4_DELTA",          None);
         ("COUNT_ORDER",                      None)
      ]
   in
   let tpch2_part_table = 
      create_hashtbl [ 
         ("COUNT",                              Some([3]));  (* PK *)
         ("COUNTPARTSUPP1_DOMAIN1",             None);  
         ("COUNTPARTSUPP1_P_1",                 Some([0]));  (* PK *)
         ("COUNTPARTSUPP1_L2_2_DELTA",          None);
         ("COUNTPARTSUPP1_L2_2",                Some([0]));  (* SK *)
         ("COUNTPARTSUPP1_L2_2SUPPLIER1_DELTA", None);
         ("COUNTPARTSUPP1_L2_2SUPPLIER1",       Some([]));   (*    *)
         ("COUNTPARTSUPP4_P_2",                 Some([0]));  (* SK *) 
         ("COUNTPARTSUPP4_P_2SUPPLIER1_DELTA",  None);
         ("COUNTPARTSUPP4_P_2SUPPLIER1",        Some([]));   (*    *)
         ("COUNTSUPPLIER1",                     Some([0]));  (* PK *)
         ("COUNTSUPPLIER1SUPPLIER1_P_1",        Some([0]));  (* PK *)
         ("COUNTSUPPLIER1SUPPLIER1_P_1PART1",   Some([0]));  (* PK *)
         ("COUNTPART1_DELTA",                   None);
         ("COUNTPART1",                         Some([5]));  (* PK *)          
         ("COUNTPART1_L2_1",                    Some([0]));  (* PK *)
      ]
   in 
   let tpch3_part_table = 
      create_hashtbl [ 
         ("QUERY3",                          Some([0]));   (* OK *)
         ("QUERY3LINEITEM1_DELTA",           None);
         ("QUERY3LINEITEM1",                 Some([0]));   (* OK *)
         ("QUERY3LINEITEM1CUSTOMER1",        Some([0]));   (* OK *)
         ("QUERY3ORDERS1_DELTA",             None);
         ("QUERY3ORDERS1_P_1",               Some([0]));   (* CK *)
         ("QUERY3ORDERS1_P_2",               Some([0]));   (* OK *)
         ("QUERY3CUSTOMER1_DELTA",           None);
         ("QUERY3CUSTOMER1",                 Some([0]))    (* OK *) 
      ]
   in
   let tpch4_part_table = 
      create_hashtbl [
         ("ORDER_COUNT",                          None);
         ("ORDER_COUNTLINEITEM1_DOMAIN1",         None);
         ("ORDER_COUNTLINEITEM1",                 Some([0]));  (* OK *)
         ("ORDER_COUNTLINEITEM1_E1_2_DELTA",      None);
         ("ORDER_COUNTORDERS1_DELTA",             None);
         ("ORDER_COUNTORDERS1_E1_1",              Some([0]));  (* OK *)
      ]
   in
   let tpch5_part_table = 
      if (Debug.active "HEURISTICS-DECOMPOSE-OVER-TABLES") then      
         create_hashtbl [
            ("REVENUE",                                None);
            ("REVENUESUPPLIER1_DELTA",                 None);
            ("REVENUESUPPLIER1_P_2",                   Some([1])); (* SK *)
            ("REVENUESUPPLIER1_P_2ORDERS1_P_2",        Some([0])); (* OK *)
            ("REVENUESUPPLIER1_P_2CUSTOMER1",          Some([0])); (* CK *)
            ("REVENUESUPPLIER1_P_2CUSTOMER1LINEITEM1", Some([0])); (* OK *)
            ("REVENUELINEITEM1_DELTA",                 None);
            ("REVENUELINEITEM1_T_2",                   Some([1])); (* OK *)
            ("REVENUELINEITEM1_T_3",                   Some([0])); (* SK *)
            ("REVENUEORDERS1_DELTA",                   None);
            ("REVENUEORDERS1_T_2",                     Some([0])); (* CK *)
            ("REVENUEORDERS1_T_3",                     Some([0])); (* OK *)
            ("REVENUECUSTOMER1_DELTA",                 None);
            ("REVENUECUSTOMER1_P_1",                   Some([]));  (* ** *)
            ("REVENUECUSTOMER1_P_2",                   Some([0])); (* CK *)
         ]      
      else
         create_hashtbl [
            ("REVENUE",                                None);
            ("REVENUESUPPLIER1_DELTA",                 None);
            ("REVENUESUPPLIER1_P_2",                   Some([1])); (* SK *)
            ("REVENUESUPPLIER1_P_2LINEITEM1",          Some([1])); (* OK *)
            ("REVENUESUPPLIER1_P_2LINEITEM1CUSTOMER1", Some([0])); (* OK *)
            ("REVENUESUPPLIER1_P_2ORDERS1_P_1",        Some([0])); (* CK *)
            ("REVENUESUPPLIER1_P_2ORDERS1_P_2",        Some([0])); (* OK *)
            ("REVENUESUPPLIER1_P_2CUSTOMER1",          Some([0])); (* CK *)
            ("REVENUELINEITEM1_DELTA",                 None);
            ("REVENUELINEITEM1",                       Some([0])); (* OK *)
            ("REVENUELINEITEM1ORDERS1",                Some([0])); (* CK *)
            ("REVENUELINEITEM1CUSTOMER1_P_3",          Some([0])); (* SK *)
            ("REVENUEORDERS1_DELTA",                   None);
            ("REVENUEORDERS1",                         Some([1])); (* OK *)
            ("REVENUEORDERS1CUSTOMER1_P_2",            Some([0])); (* OK *)
            ("REVENUECUSTOMER1_DELTA",                 None);
            ("REVENUECUSTOMER1_P_1",                   Some([]));  (* ** *)
            ("REVENUECUSTOMER1_P_2",                   Some([0])); (* CK *)

         ]
   in
   let tpch6_part_table = 
      create_hashtbl [ 
         ("REVENUE",                None); 
         ("REVENUELINEITEM1_DELTA", None)
      ]
   in
   let tpch7_part_table = 
      if (Debug.active "HEURISTICS-DECOMPOSE-OVER-TABLES") then      
         create_hashtbl [ 
            ("REVENUE",                              None); 
            ("REVENUECUSTOMER1_DELTA",               None);
            ("REVENUECUSTOMER1",                     Some([0])); (* CK *) 
            ("REVENUECUSTOMER1ORDERS1",              Some([0])); (* OK *)
            ("REVENUECUSTOMER1ORDERS1SUPPLIER1_P_1", Some([0])); (* OK *)
            ("REVENUECUSTOMER1LINEITEM1_P_1",        Some([0])); (* SK *) 
            ("REVENUECUSTOMER1LINEITEM1_P_2",        Some([0])); (* OK *)
            ("REVENUECUSTOMER1SUPPLIER1_P_1",        Some([1])); (* CK *)
            ("REVENUEORDERS1_DELTA",                 None);
            ("REVENUEORDERS1_T_1",                   Some([1])); (* OK *)
            ("REVENUEORDERS1_T_2",                   Some([0])); (* CK *)
            ("REVENUELINEITEM1_DOMAIN1",             None);
            ("REVENUELINEITEM1_DELTA",               None);
            ("REVENUELINEITEM1_T_1",                 Some([0])); (* SK *)
            ("REVENUELINEITEM1_T_2",                 Some([0])); (* OK *)
            ("REVENUELINEITEM1_T_3",                 Some([]));  (* ** *)
            ("REVENUESUPPLIER1_DELTA",               None);
            ("REVENUESUPPLIER1",                     Some([0])); (* SK *)
            ("REVENUESUPPLIER1ORDERS1_P_2",          Some([0])); (* CK *)
            ("REVENUESUPPLIER1LINEITEM1",            Some([0])); (* OK *)
         ]
   else   
         create_hashtbl [ 
            ("REVENUE",                              None); 
            ("REVENUECUSTOMER1_DELTA",               None);
            ("REVENUECUSTOMER1",                     Some([0])); (* CK *) 
            ("REVENUECUSTOMER1ORDERS1",              Some([0])); (* OK *)
            ("REVENUECUSTOMER1ORDERS1SUPPLIER1_P_1", Some([0])); (* OK *)
            ("REVENUECUSTOMER1LINEITEM1_P_1",        Some([0])); (* SK *) 
            ("REVENUECUSTOMER1LINEITEM1_P_2",        Some([0])); (* OK *)
            ("REVENUECUSTOMER1SUPPLIER1_P_1",        Some([1])); (* CK *)
            ("REVENUECUSTOMER1SUPPLIER1_P_2",        Some([]));  (* ** *)
            ("REVENUEORDERS1_DELTA",                 None);
            ("REVENUEORDERS1",                       Some([0])); (* OK *)
            ("REVENUEORDERS1LINEITEM1",              Some([1])); (* CK *) (* 0:SK *)
            ("REVENUEORDERS1SUPPLIER1_P_2",          Some([0])); (* CK *)
            ("REVENUELINEITEM1_DOMAIN1",             None);
            ("REVENUELINEITEM1_DELTA",               None);
            ("REVENUELINEITEM1",                     Some([1])); (* OK *)
            ("REVENUELINEITEM1SUPPLIER1",            Some([0])); (* OK *)
            ("REVENUESUPPLIER1_DELTA",               None);
            ("REVENUESUPPLIER1",                     Some([0])); (* SK *)
         ]
   in
   let tpch8_part_table = 
      create_hashtbl [ 
         ("MKT_SHARE",                                     None); 
         ("MKT_SHAREORDERS1_DOMAIN1",                      None);  
         ("MKT_SHAREORDERS1",                              None);
         ("MKT_SHAREORDERS1CUSTOMER1_DELTA",               None);
         ("MKT_SHAREORDERS1CUSTOMER1_P_1",                 Some([0])); (* CK *)
         ("MKT_SHAREORDERS1CUSTOMER1_P_1LINEITEM1_P_3",    Some([0])); (* OK *)
         ("MKT_SHAREORDERS1CUSTOMER1_P_1SUPPLIER1_P_2",    Some([1])); (* CK *)
         ("MKT_SHAREORDERS1CUSTOMER1_P_1SUPPLIER1_P_2ORDERS1",      Some([0])); (* OK *)
         ("MKT_SHAREORDERS1CUSTOMER1_P_1SUPPLIER1_P_2ORDERS1PART1", Some([0])); (* OK *)
         ("MKT_SHAREORDERS1CUSTOMER1_P_1SUPPLIER1_P_2PART1",        Some([0])); (* PK *)
         ("MKT_SHAREORDERS1CUSTOMER1_P_1PART1",            Some([0])); (* PK *)
         ("MKT_SHAREORDERS1CUSTOMER1_P_1PART1ORDERS1",     Some([0])); (* OK *)
         ("MKT_SHAREORDERS1CUSTOMER1_P_2",                 Some([]));  (* ** *)          
         ("MKT_SHAREORDERS1LINEITEM1_DELTA",               None);
         ("MKT_SHAREORDERS1LINEITEM1_P_1",                 Some([0])); (* PK *)
         ("MKT_SHAREORDERS1LINEITEM1_P_2",                 Some([0])); (* OK *)
         ("MKT_SHAREORDERS1LINEITEM1_P_3",                 Some([0])); (* SK *)
         ("MKT_SHAREORDERS1SUPPLIER1_DELTA",               None);
         ("MKT_SHAREORDERS1SUPPLIER1_P_1",                 Some([0])); (* SK *)
         ("MKT_SHAREORDERS1SUPPLIER1_P_1PART1",            Some([0])); (* PK *)
         ("MKT_SHAREORDERS1SUPPLIER1_P_2",                 Some([]));  (* ** *)
         ("MKT_SHAREORDERS1PART1_DELTA",                   None);
         ("MKT_SHAREORDERS1PART1",                         Some([0])); (* PK *)
         ("MKT_SHAREORDERS1_L1_1_L1_2_DELTA",              None);
         ("MKT_SHAREORDERS1_L1_1_L1_2_P_1",                Some([0])); (* CK *)
         ("MKT_SHAREORDERS1_L1_1_L1_2_P_2",                Some([0])); (* OK *)
         ("MKT_SHAREORDERS1_L1_1_L1_2_P_2LINEITEM1_P_2",   Some([0])); (* SK *)
         ("MKT_SHAREORDERS1_L1_1_L1_2_P_2SUPPLIER1_P_2",   Some([])); 
         ("MKT_SHAREORDERS1_L1_1_L1_2_P_2PART1",           Some([0])); (* OK *)
         ("MKT_SHAREORDERS4_DELTA",                        None);
         ("MKT_SHAREORDERS4_P_1",                          Some([0])); (* OK *)
         ("MKT_SHAREPART1",                                None);
         ("MKT_SHAREPART1CUSTOMER1_P_2",                   Some([0])); (* CK *)
         ("MKT_SHAREPART1CUSTOMER1_P_2LINEITEM1_P_3",      Some([0])); (* OK *)
         ("MKT_SHAREPART1CUSTOMER1_P_2SUPPLIER1_P_2",      Some([1])); (* CK *)
         ("MKT_SHAREPART1CUSTOMER1_P_2SUPPLIER1_P_2PART1", Some([0])); (* PK *)
         ("MKT_SHAREPART1CUSTOMER1_P_2PART1",              Some([0])); (* PK *)
         ("MKT_SHAREPART1LINEITEM1_P_1",                   Some([0])); (* OK *)
         ("MKT_SHAREPART1SUPPLIER1_P_1",                   Some([0])); (* SK *)
         ("MKT_SHAREPART1SUPPLIER1_P_1PART1",              Some([0])); (* PK *)
         ("MKT_SHAREPART1PART1",                           Some([0])); (* PK *)
         ("MKT_SHAREPART1_L2_1_L1_1",                      None);
         ("MKT_SHAREPART1_L2_1_L1_1CUSTOMER1_P_2",         Some([0])); (* CK *)
         ("MKT_SHAREPART1_L2_1_L1_1CUSTOMER1_P_2PART1",    Some([0])); (* PK *)
         ("MKT_SHAREPART1_L2_1_L1_1PART1",                 Some([0])); (* PK *)
      ]
   in
   let tpch9_part_table = 
      create_hashtbl [ 
         ("SUM_PROFIT",                           None); 
         ("SUM_PROFITORDERS11_DOMAIN1",           None); 
         ("SUM_PROFITORDERS11_DELTA",             None);
         ("SUM_PROFITORDERS11",                   Some([0])); (* OK *)
         ("SUM_PROFITORDERS11PARTSUPP1_P_3",      Some([0])); (* OK *)
         ("SUM_PROFITORDERS11SUPPLIER1_P_1",      Some([0])); (* OK *)
         ("SUM_PROFITORDERS11SUPPLIER1_P_1PART1", Some([0])); (* OK *)
         ("SUM_PROFITORDERS11PART1",              Some([0])); (* OK *)
         ("SUM_PROFITORDERS13",                   Some([0])); (* OK *)
         ("SUM_PROFITORDERS13PARTSUPP1_P_3",      Some([0])); (* OK *)
         ("SUM_PROFITORDERS13SUPPLIER1_P_1",      Some([0])); (* OK *)
         ("SUM_PROFITORDERS13SUPPLIER1_P_1PART1", Some([0])); (* OK *)
         ("SUM_PROFITORDERS13PART1",              Some([0])); (* OK *) 
         ("SUM_PROFITPARTSUPP11_DELTA",           None);
         ("SUM_PROFITPARTSUPP11_P_3",             Some([0])); (* PK *)
         ("SUM_PROFITPARTSUPP13_DELTA",           None);
         ("SUM_PROFITPARTSUPP13_P_3",             Some([0])); (* PK *)
         ("SUM_PROFITLINEITEM11_DELTA",           None);
         ("SUM_PROFITLINEITEM11_P_1",             Some([0])); (* PK *)
         ("SUM_PROFITLINEITEM11_P_2",             Some([0])); (* SK *)
         ("SUM_PROFITLINEITEM11_P_3",             Some([0])); (* PK *)
         ("SUM_PROFITLINEITEM11_P_4",             Some([0])); (* OK *)
         ("SUM_PROFITLINEITEM13_DELTA",           None);
         ("SUM_PROFITLINEITEM13_P_3",             Some([0])); (* PK *)
         ("SUM_PROFITSUPPLIER11_DELTA",           None);
         ("SUM_PROFITSUPPLIER11_P_1",             Some([0])); (* SK *)
         ("SUM_PROFITSUPPLIER11_P_1PART1",        Some([0])); (* PK *)
         ("SUM_PROFITSUPPLIER11_P_2",             Some([]));  (* ** *)
         ("SUM_PROFITSUPPLIER13_P_1",             Some([0])); (* SK *)
         ("SUM_PROFITSUPPLIER13_P_1PART1",        Some([0])); (* PK *)
         ("SUM_PROFITPART11_DELTA",               None);
         ("SUM_PROFITPART11",                     Some([0])); (* PK *)
         ("SUM_PROFITPART13",                     Some([0])); (* PK *)
      ]
   in
   let tpch10_part_table = 
      create_hashtbl [ 
         ("REVENUE",                       Some([0])); (* CK *)
         ("REVENUELINEITEM1_DELTA",        None);
         ("REVENUELINEITEM1",              Some([6])); (* OK *)
         ("REVENUELINEITEM1CUSTOMER1_P_1", Some([0])); (* OK *)
         ("REVENUEORDERS1_DELTA",          None);
         ("REVENUEORDERS1_P_1",            Some([0])); (* OK *)
         ("REVENUEORDERS1_P_2",            Some([0])); (* CK *)
         ("REVENUECUSTOMER1_DELTA",        None);
         ("REVENUECUSTOMER1_P_1",          Some([0])); (* CK *)
         ("REVENUECUSTOMER1_P_2",          Some([]));  (* ** *)
      ]
   in
   let tpch11_part_table = 
      create_hashtbl [ 
         ("QUERY11",                              Some([0])); (* PK *)
         ("QUERY11PARTSUPP1_L1_1",                None);
         ("QUERY11PARTSUPP1_L1_1SUPPLIER1_DELTA", None);
         ("QUERY11PARTSUPP1_L1_1SUPPLIER1_P_1",   Some([]));  (* ** *)
         ("QUERY11PARTSUPP1_L1_1SUPPLIER1_P_2",   Some([0])); (* SK *)
         ("QUERY11PARTSUPP1_L1_1PARTSUPP1_DELTA", None);
         ("QUERY11PARTSUPP1_L1_1PARTSUPP1",       Some([0])); (* SK *)
         ("QUERY11PARTSUPP1_E2_1",                Some([0])); (* PK *)
         ("QUERY11PARTSUPP1_E2_1SUPPLIER1_P_2",   Some([1])); (* PK *)
         ("QUERY11PARTSUPP1_E2_1PARTSUPP1_DELTA", None);
         ("QUERY11PARTSUPP1_L3_1",                Some([0])); (* PK *) 
         ("QUERY11PARTSUPP1_L3_1SUPPLIER1_P_2",   Some([1])); (* PK *)
         ("QUERY11PARTSUPP1_L3_1PARTSUPP1_DELTA", None);
      ]
   in
   let tpch12_part_table = 
      create_hashtbl [ 
         ("HIGH_LINE_COUNT",                None); 
         ("HIGH_LINE_COUNTLINEITEM1_DELTA", None);
         ("HIGH_LINE_COUNTLINEITEM1",       Some([0]));     (* OK *)
         ("HIGH_LINE_COUNTLINEITEM2_DELTA", None);
         ("HIGH_LINE_COUNTLINEITEM3",       Some([0]));     (* OK *)
         ("HIGH_LINE_COUNTORDERS1_DELTA",   None);
         ("HIGH_LINE_COUNTORDERS1",         Some([0]));     (* OK *) 
         ("HIGH_LINE_COUNTORDERS2",         Some([0]));     (* OK *) 
         ("HIGH_LINE_COUNTORDERS3_DELTA",   None);
         ("LOW_LINE_COUNT",                 None);
         ("LOW_LINE_COUNTLINEITEM1",        Some([0]));     (* OK *)
         ("LOW_LINE_COUNTORDERS1_DELTA",    None);
      ]
   in
   let tpch13_part_table =   
      create_hashtbl [ 
         ("CUSTDIST",                            None); 
         ("CUSTDISTORDERS1_DOMAIN1",             None);
         ("CUSTDISTORDERS3_L1_2_DELTA",          None);
         ("CUSTDISTORDERS3_L1_2",                Some([0])); (* CK *)
         ("CUSTDISTORDERS3_L1_2CUSTOMER1_DELTA", None);
         ("CUSTDISTCUSTOMER1_DOMAIN1",           None);
         ("CUSTDISTCUSTOMER1_L1_1",              Some([0])); (* CK *)
         ("CUSTDISTCUSTOMER3_L1_2_DELTA",        None);
         ("CUSTDISTCUSTOMER3_L1_2",              Some([0])); (* CK *)
         ("CUSTDISTCUSTOMER3_L1_2ORDERS1_DELTA", None);
      ]
   in
   let tpch14_part_table = 
      create_hashtbl [ 
         ("PROMO_REVENUE",                                   None); 
         ("PROMO_REVENUELINEITEM1_L1_1_L1_1",                None);
         ("PROMO_REVENUELINEITEM1_L1_1_L1_1PART1_DELTA",     None);
         ("PROMO_REVENUELINEITEM1_L1_1_L1_1PART1",           Some([0])); (* PK *)
         ("PROMO_REVENUELINEITEM1_L1_1_L1_1LINEITEM1_DELTA", None);
         ("PROMO_REVENUELINEITEM1_L1_1_L1_1LINEITEM1",       Some([0])); (* PK *)
         ("PROMO_REVENUELINEITEM2",                          None); 
         ("PROMO_REVENUELINEITEM2PART1_DELTA",               None);
         ("PROMO_REVENUELINEITEM2LINEITEM1",                 Some([0])); (* PK *)
      ]
   in
   let tpch15_part_table = 
      create_hashtbl [ 
         ("COUNT",                                   Some([0])); (* SK *)
         ("COUNTSUPPLIER1_DELTA",                    None);
         ("COUNTLINEITEM1",                          Some([0])); (* SK *)
         ("COUNTLINEITEM1_E2_1",                     Some([0])); (* SK *)
         ("COUNTLINEITEM1_E2_1LINEITEM1_DELTA",      None);
         ("COUNTLINEITEM1_L3_1",                     Some([0])); (* SK *)
         ("COUNTLINEITEM1_L3_1LINEITEM1_DELTA",      None);
         ("COUNTLINEITEM1_L4_1_E1_1",                Some([0])); (* SK *)
         ("COUNTLINEITEM1_L4_1_E1_1LINEITEM1_DELTA", None);
         ("COUNTLINEITEM1_L4_1_L2_1",                Some([0])); (* SK *)
         ("COUNTLINEITEM1_L4_1_L2_1LINEITEM1_DELTA", None);
      ]
   in
   let tpch16_part_table = 
      create_hashtbl [ 
         ("SUPPLIER_CNT",                           None); 
         ("SUPPLIER_CNTSUPPLIER1_DOMAIN1",          None);
         ("SUPPLIER_CNTSUPPLIER1_E1_2_L2_2_DELTA",  None);
         ("SUPPLIER_CNTPART1_DOMAIN1",              None); 
         ("SUPPLIER_CNTPART1_E1_1",                 Some([0])); (* SK *)
         ("SUPPLIER_CNTPART1_E1_1PARTSUPP1",        Some([0])); (* PK *)
         ("SUPPLIER_CNTPART1_E1_33_DELTA",          None);
         ("SUPPLIER_CNTPART1_E1_33",                Some([0])); (* PK *)
         ("SUPPLIER_CNTPARTSUPP1_DOMAIN1",          None);
         ("SUPPLIER_CNTPARTSUPP1_E1_1_L2_1",        Some([0])); (* SK *)
         ("SUPPLIER_CNTPARTSUPP1_E1_2",             Some([0])); (* SK *)
         ("SUPPLIER_CNTPARTSUPP1_E1_2PART1_DELTA",  None);
         ("SUPPLIER_CNTPARTSUPP1_E1_4",             Some([0])); (* SK *)
         ("SUPPLIER_CNTPARTSUPP1_E1_4PART1_DELTA",  None);
         ("SUPPLIER_CNTPARTSUPP1_E1_6",             Some([0])); (* SK *)
         ("SUPPLIER_CNTPARTSUPP1_E1_6PART1_DELTA",  None);
         ("SUPPLIER_CNTPARTSUPP1_E1_8",             Some([0])); (* SK *)
         ("SUPPLIER_CNTPARTSUPP1_E1_8PART1_DELTA",  None);
         ("SUPPLIER_CNTPARTSUPP1_E1_10",            Some([0])); (* SK *)
         ("SUPPLIER_CNTPARTSUPP1_E1_10PART1_DELTA", None);
         ("SUPPLIER_CNTPARTSUPP1_E1_12",            Some([0])); (* SK *)
         ("SUPPLIER_CNTPARTSUPP1_E1_12PART1_DELTA", None);
         ("SUPPLIER_CNTPARTSUPP1_E1_14",            Some([0])); (* SK *)
         ("SUPPLIER_CNTPARTSUPP1_E1_14PART1_DELTA", None);
         ("SUPPLIER_CNTPARTSUPP1_E1_16",            Some([0])); (* SK *)
         ("SUPPLIER_CNTPARTSUPP1_E1_16PART1_DELTA", None);
         ("SUPPLIER_CNTPARTSUPP1_E1_18_DELTA",      None);
         ("SUPPLIER_CNTPARTSUPP1_E1_18",            Some([0])); (* PK *)
         ("SUPPLIER_CNTPARTSUPP1_E1_20",            Some([0])); (* PK *)
         ("SUPPLIER_CNTPARTSUPP1_E1_22",            Some([0])); (* PK *)
         ("SUPPLIER_CNTPARTSUPP1_E1_24",            Some([0])); (* PK *)
         ("SUPPLIER_CNTPARTSUPP1_E1_26",            Some([0])); (* PK *)
         ("SUPPLIER_CNTPARTSUPP1_E1_28",            Some([0])); (* PK *)
         ("SUPPLIER_CNTPARTSUPP1_E1_30",            Some([0])); (* PK *)
         ("SUPPLIER_CNTPARTSUPP1_E1_32",            Some([0])); (* PK *)
      ]
   in
   let tpch17_part_table = 
      create_hashtbl [ 
         ("AVG_YEARLY",                          None); 
         ("AVG_YEARLYPART1_DELTA",               None); 
         ("AVG_YEARLYLINEITEM1_DOMAIN1",         None); 
         ("AVG_YEARLYLINEITEM1_P_3",             Some([0])); (* PK *)
         ("AVG_YEARLYLINEITEM1_P_3PART1_DELTA",  None); 
         ("AVG_YEARLYLINEITEM1_P_4",             Some([0])); (* PK *)
         ("AVG_YEARLYLINEITEM1_L1_1_L1_1",       Some([0])); (* PK *)
         ("AVG_YEARLYLINEITEM1_L1_1_L1_2_DELTA", None); 
         ("AVG_YEARLYLINEITEM1_L1_2",            Some([0])); (* PK *)
         ("AVG_YEARLYLINEITEM1_L1_4_DELTA",      None); 
         ("AVG_YEARLYLINEITEM5_DELTA",           None); 
      ]
   in
   let tpch17a_part_table = 
      create_hashtbl [ 
         ("QUERY17",                          None); 
         ("QUERY17PART1_DELTA",               None); 
         ("QUERY17LINEITEM1_DOMAIN1",         None); 
         ("QUERY17LINEITEM1_P_1",             Some([0])); (* PK *)
         ("QUERY17LINEITEM1_P_2",             Some([0])); (* PK *)
         ("QUERY17LINEITEM1_L1_1",            Some([0])); (* PK *)
         ("QUERY17LINEITEM1_L1_3_DELTA",      None); 
         ("QUERY17LINEITEM2_DELTA",           None); 
      ]
   in   
   let tpch18_part_table = 
      create_hashtbl [ 
         ("QUERY18",                          Some([2])); (* OK *) (* 1:CK *)
         ("QUERY18LINEITEM1_DOMAIN1",         None); 
         ("QUERY18LINEITEM1_P_1",             Some([2])); (* OK *) (* 1:CK *)
         ("QUERY18LINEITEM1_P_1CUSTOMER1",    Some([0])); (* OK *) (* 1:CK *)
         ("QUERY18LINEITEM1_L1_2_DELTA",      None); 
         ("QUERY18ORDERS1_DELTA",             None); 
         ("QUERY18ORDERS1_P_1",               Some([0])); (* CK *)
         ("QUERY18CUSTOMER1_DELTA",           None); 
         ("QUERY18CUSTOMER1",                 Some([0])); (* OK *) (* 1:CK *)
         ("QUERY18CUSTOMER1_L1_1",            Some([0])); (* OK *)
      ]
   in
   let tpch19_part_table = 
      create_hashtbl [ 
         ("REVENUE",                None); 
         ("REVENUEPART1_DELTA",     None); 
         ("REVENUEPART1",           Some([0])); (* PK *)
         ("REVENUELINEITEM1_DELTA", None); 
         ("REVENUELINEITEM1",       Some([0])); (* PK *)
      ]
   in
   let tpch20_part_table = 
      create_hashtbl [ 
         ("COUNT",                               None); (* Alternative: 0:S_NAME   1:S_ADDRESS *) 
         ("COUNTPART1",                          Some([0])); (* SK *)
         ("COUNTLINEITEM1_DOMAIN1",              None); 
         ("COUNTLINEITEM1_E1_1_L1_3_DELTA",      None); 
         ("COUNTPARTSUPP1_DOMAIN1",              None); 
         ("COUNTPARTSUPP1_P_2",                  Some([0])); (* SK *)
         ("COUNTPARTSUPP1_P_2SUPPLIER1",         Some([]));  (* ** *)
         ("COUNTPARTSUPP1_E1_2_DELTA",           None); 
         ("COUNTSUPPLIER1_DELTA",                None); 
         ("COUNTSUPPLIER1",                      Some([]));  (* ** *)
         ("COUNTSUPPLIER1_E1_1",                 Some([0])); (* PK *)
         ("COUNTSUPPLIER1_E1_1_L1_1",            Some([0])); (* PK *)
         ("COUNTSUPPLIER1_E1_1_E2_1",            Some([0])); (* PK *)
         ("COUNTSUPPLIER1_E1_1_E2_1PART1_DELTA", None); 
      ]
   in
   let tpch21_part_table = 
      create_hashtbl [ 
         ("NUMWAIT",                           None); (* Alternative: 0:S_NAME *)
         ("NUMWAITORDERS1_DELTA",              None); 
         ("NUMWAITORDERS1",                    Some([2])); (* OK *)
         ("NUMWAITORDERS1LINEITEM1",           Some([0])); (* SK *)
         ("NUMWAITLINEITEM1_DOMAIN1",          None); 
         ("NUMWAITLINEITEM1_P_3",              Some([2])); (* OK *)
         ("NUMWAITLINEITEM1_P_3SUPPLIER1_P_2", Some([]));  (* ** *)
         ("NUMWAITLINEITEM1_P_4",              Some([0])); (* OK *)
         ("NUMWAITLINEITEM1_P_4ORDERS1_DELTA", None); 
         ("NUMWAITLINEITEM3_L2_2_DELTA",       None); 
         ("NUMWAITLINEITEM3_E3_2_DELTA",       None); 
         ("NUMWAITLINEITEM4_P_3",              Some([0])); (* SK *) 
         ("NUMWAITSUPPLIER1_DELTA",            None); 
         ("NUMWAITSUPPLIER1_P_1",              Some([]));  (* ** *)
         ("NUMWAITSUPPLIER1_P_2",              Some([0])); (* OK *)
         ("NUMWAITSUPPLIER1_P_2LINEITEM1",     Some([0])); (* OK *)
         ("NUMWAITSUPPLIER1_L2_1",             Some([0])); (* OK *)
         ("NUMWAITSUPPLIER1_E3_1",             Some([0])); (* OK *) 
      ]
   in
   let tpch22_part_table = 
      create_hashtbl [ 
         ("NUMCUST",                                   None); 
         ("NUMCUSTORDERS1_DOMAIN1",                    None); 
         ("NUMCUSTORDERS1_L2_2_DELTA",                 None); 
         ("NUMCUSTCUSTOMER1",                          Some([0])); (* CK *)
         ("NUMCUSTCUSTOMER1CUSTOMER1_DELTA",           None); 
         ("NUMCUSTCUSTOMER1_L2_1_L1_1",                None); 
         ("NUMCUSTCUSTOMER1_L2_1_L1_1CUSTOMER1_DELTA", None);
         ("NUMCUSTCUSTOMER1_L2_1_L1_2",                None); 
         ("NUMCUSTCUSTOMER1_L2_1_L1_2CUSTOMER1_DELTA", None);
         ("NUMCUSTCUSTOMER1_L2_1_L1_3",                None); 
         ("NUMCUSTCUSTOMER1_L2_1_L1_3CUSTOMER1_DELTA", None);
         ("NUMCUSTCUSTOMER1_L2_1_L1_4",                None); 
         ("NUMCUSTCUSTOMER1_L2_1_L1_4CUSTOMER1_DELTA", None);
         ("NUMCUSTCUSTOMER1_L2_1_L1_5",                None);
         ("NUMCUSTCUSTOMER1_L2_1_L1_5CUSTOMER1_DELTA", None);
         ("NUMCUSTCUSTOMER1_L2_1_L1_6",                None);
         ("NUMCUSTCUSTOMER1_L2_1_L1_6CUSTOMER1_DELTA", None);
         ("NUMCUSTCUSTOMER1_L2_1_L1_7",                None);
         ("NUMCUSTCUSTOMER1_L2_1_L1_7CUSTOMER1_DELTA", None);
         ("NUMCUSTCUSTOMER1_L2_2",                     None);
         ("NUMCUSTCUSTOMER1_L2_2CUSTOMER1_DELTA",      None);
         ("NUMCUSTCUSTOMER1_L2_4",                     None);
         ("NUMCUSTCUSTOMER1_L2_4CUSTOMER1_DELTA",      None);
         ("NUMCUSTCUSTOMER1_L2_6",                     None);
         ("NUMCUSTCUSTOMER1_L2_6CUSTOMER1_DELTA",      None);
         ("NUMCUSTCUSTOMER1_L2_8",                     None);
         ("NUMCUSTCUSTOMER1_L2_8CUSTOMER1_DELTA",      None);
         ("NUMCUSTCUSTOMER1_L2_10",                    None);
         ("NUMCUSTCUSTOMER1_L2_10CUSTOMER1_DELTA",     None);
         ("NUMCUSTCUSTOMER1_L2_12",                    None);
         ("NUMCUSTCUSTOMER1_L2_12CUSTOMER1_DELTA",     None);
         ("NUMCUSTCUSTOMER1_L2_14",                    None);
         ("NUMCUSTCUSTOMER1_L2_14CUSTOMER1_DELTA",     None);
         ("NUMCUSTCUSTOMER1_L3_1",                     Some([0])); (* CK *)
         ("NUMCUSTCUSTOMER2",                          Some([0])); (* CK *)
         ("NUMCUSTCUSTOMER2CUSTOMER1_DELTA",           None); 
         ("NUMCUSTCUSTOMER3",                          Some([0])); (* CK *)
         ("NUMCUSTCUSTOMER3CUSTOMER1_DELTA",           None); 
         ("NUMCUSTCUSTOMER4",                          Some([0])); (* CK *)
         ("NUMCUSTCUSTOMER4CUSTOMER1_DELTA",           None); 
         ("NUMCUSTCUSTOMER5",                          Some([0])); (* CK *)
         ("NUMCUSTCUSTOMER5CUSTOMER1_DELTA",           None); 
         ("NUMCUSTCUSTOMER6",                          Some([0])); (* CK *)
         ("NUMCUSTCUSTOMER6CUSTOMER1_DELTA",           None); 
         ("NUMCUSTCUSTOMER7",                          Some([0])); (* CK *)
         ("NUMCUSTCUSTOMER7CUSTOMER1_DELTA",           None); 
         ("TOTALACCTBAL",                              None);
         ("TOTALACCTBALCUSTOMER1",                     Some([0])); (* CK *)
         ("TOTALACCTBALCUSTOMER1CUSTOMER1_DELTA",      None); 
         ("TOTALACCTBALCUSTOMER2",                     Some([0])); (* CK *)
         ("TOTALACCTBALCUSTOMER2CUSTOMER1_DELTA",      None); 
         ("TOTALACCTBALCUSTOMER3",                     Some([0])); (* CK *)
         ("TOTALACCTBALCUSTOMER3CUSTOMER1_DELTA",      None); 
         ("TOTALACCTBALCUSTOMER4",                     Some([0])); (* CK *)
         ("TOTALACCTBALCUSTOMER4CUSTOMER1_DELTA",      None); 
         ("TOTALACCTBALCUSTOMER5",                     Some([0])); (* CK *)
         ("TOTALACCTBALCUSTOMER5CUSTOMER1_DELTA",      None); 
         ("TOTALACCTBALCUSTOMER6",                     Some([0])); (* CK *)
         ("TOTALACCTBALCUSTOMER6CUSTOMER1_DELTA",      None); 
         ("TOTALACCTBALCUSTOMER7",                     Some([0])); (* CK *)
         ("TOTALACCTBALCUSTOMER7CUSTOMER1_DELTA",      None); 
      ]
   in
   let ssb4_part_table = 
      create_hashtbl [ 
         ("SSB4",                                         None); 
         ("SSB4SUPPLIER1_DELTA",                          None); 
         ("SSB4SUPPLIER1_P_1",                            Some([0])); (* SK *)  
         ("SSB4SUPPLIER1_P_1PART1",                       Some([0])); (* PK *)
         ("SSB4SUPPLIER1_P_1PART1ORDERS1_P_2",            Some([0])); (* OK *)  
         ("SSB4SUPPLIER1_P_1PART1CUSTOMER1_P_1",          Some([1])); (* PK *)
         ("SSB4SUPPLIER1_P_1PART1CUSTOMER1_P_1LINEITEM1", Some([0])); (* OK *)  
         ("SSB4SUPPLIER1_P_1ORDERS1_P_2",                 Some([0])); (* OK *)  
         ("SSB4SUPPLIER1_P_1CUSTOMER1_P_1",               Some([0])); (* CK *)
         ("SSB4PART1_DELTA",                              None); 
         ("SSB4PART1",                                    Some([0])); (* PK *)  
         ("SSB4PART1ORDERS1_P_2",                         Some([0])); (* OK *)  
         ("SSB4PART1CUSTOMER1_P_1",                       Some([1])); (* PK *)
         ("SSB4LINEITEM1_DELTA",                          None); 
         ("SSB4LINEITEM1_P_1",                            Some([0])); (* OK *)  
         ("SSB4LINEITEM1_P_2",                            Some([0])); (* PK *)  
         ("SSB4LINEITEM1_P_3",                            Some([0])); (* SK *)  
         ("SSB4ORDERS1_DELTA",                            None); 
         ("SSB4ORDERS1_P_1",                              Some([0])); (* CK *)  
         ("SSB4ORDERS1_P_2",                              Some([0])); (* OK *)  
         ("SSB4CUSTOMER1_DELTA",                          None); 
         ("SSB4CUSTOMER1_P_1",                            Some([0])); (* CK *)  
         ("SSB4CUSTOMER1_P_2",                            Some([]));  (* ** *)  
      ]
   in
      create_hashtbl [
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

let get_partitioning_info (query: string) = 
  try Hashtbl.find part_table_context query
  with Not_found -> create_hashtbl []
