open Algebra
open Codegen
open Compile
open Gui

let date = "D", [ ("DATEKEY", "string"); ("YEAR", "int")]
let part = "P", [ ("PARTKEY1", "long"); ("NAME1", "string"); ("MFGR", "string"); ("CATEGORY", "string"); ("BRAND1", "string"); ("COLOR", "string"); ("TYPE", "string"); ("SIZE", "int"); ("CONTAINER", "string")]
let supplier = "S", [("SUPPKEY1", "long"); ("NAME2", "string"); ("ADDRESS1", "string"); ("CITY1", "string"); ("NATION1", "string"); ("REGION1", "string"); ("PHONE1", "string");]
let customer = "C", [("CUSTKEY1", "long"); ("NAME3", "string"); ("ADDRESS2", "string"); ("CITY2", "string"); ("NATION2", "string"); ("REGION2", "string"); ("PHONE2", "string"); ("MKTSEGMENT", "string")]
let lineorder = "LO", [("ORDERKEY", "long"); ("LINENUMBER", "int"); ("CUSTKEY2", "long"); ("PARTKEY2", "long"); ("SUPPKEY2", "long"); ("ORDERDATE", "string"); ("SHIPPRIORITY", "int"); ("QUANTITY", "int"); ("EXTENDEDPRICE", "int"); ("ORDTOTALPRICE", "int"); ("DISCOUNT", "int"); ("REVENUE", "int"); ("SUPPLYCOST", "int"); ("TAX", "int"); ("COMMITDATE", "string"); ("SHIPMODE", "string")]

let rel_d = `Relation(date)
let rel_p = `Relation(part)
let rel_s = `Relation(supplier)
let rel_c = `Relation(customer)
let rel_l = `Relation(lineorder)

let predicate = 
		`And ( `BTerm (`EQ(`ETerm (`Attribute(`Qualified("D", "YEAR"))), `ETerm (`Variable("y")))),
		`And ( `BTerm (`EQ(`ETerm (`Attribute(`Qualified("C", "NATION2"))), `ETerm (`Variable("n")))),
		`And ( `BTerm (`EQ(`ETerm (`Attribute(`Qualified("LO", "CUSTKEY2"))), `ETerm (`Attribute(`Qualified("C", "CUSTKEY1"))))), 
		`And ( `BTerm (`EQ(`ETerm (`Attribute(`Qualified("LO", "SUPPKEY2"))), `ETerm (`Attribute(`Qualified("S", "SUPPKEY1"))))), 
		`And ( `BTerm (`EQ(`ETerm (`Attribute(`Qualified("LO", "PARTKEY2"))), `ETerm (`Attribute(`Qualified("P", "PARTKEY1"))))), 
		`And ( `BTerm (`EQ(`ETerm (`Attribute(`Qualified("LO", "ORDERDATE"))), `ETerm (`Attribute(`Qualified("D", "DATEKEY"))))), 
		`And ( `BTerm (`EQ(`ETerm (`Attribute(`Qualified("C", "REGION2"))), `ETerm (`String("AMERICA")))), 
		`And ( `BTerm (`EQ(`ETerm (`Attribute(`Qualified("S", "REGION1"))), `ETerm (`String("AMERICA")))), 
		       `Or ( `BTerm (`EQ(`ETerm (`Attribute(`Qualified("P", "MFGR"))), `ETerm (`String("MFGR#1")))), 
			     `BTerm (`EQ(`ETerm (`Attribute(`Qualified("P", "MFGR"))), `ETerm (`String("MFGR#2")))))
		))))))))

let sum = `Minus(`METerm(`Attribute(`Qualified("LO", "REVENUE"))), `METerm(`Attribute(`Qualified("LO", "SUPPLYCOST"))))

let relations = `Cross( rel_d, `Cross( rel_c, `Cross( rel_s, `Cross(rel_p, rel_l))))

let q4_1 = `MapAggregate(`Sum, sum, `Select(predicate, relations));;



(*let (new_m,_) = rename_attributes q4_1 in
(*    print_endline (string_of_map_expression q4_1); ()*)
    print_endline (string_of_map_expression new_m); ()

(*Icompile_target q4_1 event*) *)

compile_query q4_1 "ssb.cc"
