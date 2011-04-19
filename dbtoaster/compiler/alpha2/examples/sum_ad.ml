open Algebra
open Compile
open Gui

let r = `Relation("R", [("A", "int"); ("B1", "int")])
let s = `Relation("S", [("B2", "int"); ("C1", "int")])
let t = `Relation("T", [("C2", "int"); ("D", "int")])

let p = `Cross(`Cross(r, s), t)

let b_expr = 
    `And(
	`BTerm(
	    `EQ(
		(`ETerm(`Attribute(`Qualified("R", "B1")))), 
		(`ETerm(`Attribute(`Qualified("S", "B2")))))),
	`BTerm(
	    `EQ(
		(`ETerm(`Attribute(`Qualified("S", "C1")))),
		(`ETerm(`Attribute(`Qualified("T", "C2")))))))

let sel = `Select(b_expr, p)

let sum_ad = 
    `MapAggregate(
	`Sum, 
	`Product(
	    `METerm(`Attribute(`Qualified("R", "A"))),
	    `METerm(`Attribute(`Qualified("T", "D")))),
	sel)
;;

let print_test_type tt = 
    print_endline ((String.make 50 '-')^"\n\n"^tt^" test\n\n"^(String.make 50 '-'));;

print_test_type "string_of";
print_endline (string_of_map_expression sum_ad);

print_test_type "compile_code_rec";
compile_query [[sum_ad]] "sum_ad.cc";;

(*
print_test_type "treeml_of_map_expression";
print_endline (treeml_string_of_map_expression sum_ad);;
*)

(*
print_test_type "parse_treeml";
let sum_ad_str = treeml_string_of_map_expression sum_ad in
print_endline sum_ad_str;
let new_sum_ad = (List.hd (parse_treeml sum_ad_str)) in
    print_endline (indented_string_of_map_expression new_sum_ad 0);
*)
