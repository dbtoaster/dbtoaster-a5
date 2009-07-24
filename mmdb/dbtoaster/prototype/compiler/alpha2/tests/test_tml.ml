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


let tv = 
    `MapAggregate(`Sum,
    `METerm(`Attribute(`Qualified("B", "V"))),
    `Relation("B",[("P", "int"); ("V", "int")]))

let sv1_pred =
    `BTerm(`GT(
	`ETerm(`Attribute(`Qualified("B", "P1"))),
	`ETerm(`Attribute(`Qualified("B", "P")))))

let select_sv1 =
    `Select(sv1_pred, `Relation("B",[("P1", "int"); ("V1", "int")]))

let k_sv0 = `Product(`METerm(`Float(0.25)), tv)
let sv1 = `MapAggregate(`Sum,
    `METerm(`Attribute(`Qualified("B","V1"))), select_sv1)

let m_p2 = `Sum(k_sv0, sv1)

let vwap =
    `MapAggregate(`Sum,
        `Product(`METerm(`Attribute(`Qualified("B", "P"))),
            `METerm(`Attribute(`Qualified("B", "V")))),
        `Select(`BTerm(`MLT(m_p2)),
            `Relation("B", [("P", "int"); ("V","int")])))
;;


let print_test_type tt = 
    print_endline ((String.make 50 '-')^"\n\n"^
        tt^" test\n\n"^(String.make 50 '-'));;

print_test_type "parse_treeml sum_{a*d}";
let sum_ad_str = treeml_string_of_map_expression sum_ad in
let new_sum_ad = (List.hd (parse_treeml sum_ad_str)) in
    if new_sum_ad <> sum_ad then
        begin
            print_endline sum_ad_str;
            print_endline (indented_string_of_map_expression new_sum_ad 0);
            print_test_type "Failed!"
        end;;


print_test_type "parse_treeml vwap";
let vwap_str = treeml_string_of_map_expression vwap in
let new_vwap = (List.hd (parse_treeml vwap_str)) in
    if new_vwap <> vwap then
        begin
            print_endline vwap_str;
            print_endline (indented_string_of_map_expression new_vwap 0);
            print_test_type "Failed!"
        end;;
