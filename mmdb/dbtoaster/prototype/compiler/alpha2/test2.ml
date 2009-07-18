open Algebra
open Compile

let r = `Relation("R", [("A", "int"); ("B1", "int")])
let s = `Relation("S", [("B2", "int"); ("C1", "int")])
let t = `Relation("T", [("C2", "int"); ("D", "int")])

let p = `Cross(`Cross(r, s), t)
let p2 = `NaturalJoin(r, `NaturalJoin (s, t))

let b_expr = 
	`And(
	    `BTerm(
		`EQ(
		    (`ETerm(`Attribute(`Qualified("R", "B1")))), 
		    (`ETerm(`Attribute(`Qualified("S", "B2"))))
	        )	
	    ),
	    `BTerm(
	        `EQ(
		    (`ETerm(`Attribute(`Qualified("S", "C1")))),
		    (`ETerm(`Attribute(`Qualified("T", "C2"))))
	        )
 	    )
	)
let sel = `Select(b_expr, p)

let sum_ad = 
	    `MapAggregate(
		`Sum, 
		`Product(
		    `METerm(`Attribute(`Qualified("R", "A"))),
		    `METerm(`Attribute(`Qualified("T", "D")))
		),
		sel
	    )
;;

let print_test_type tt = 
	print_endline ((String.make 50 '-')^"\n\n"^tt^" test\n\n"^(String.make 50 '-'));;
print_test_type "string_of";
print_endline (string_of_map_expression sum_ad);


print_test_type "compile_code";
let ((h,u),b)= compile_target sum_ad (`Insert ("R",[("A", "int"); ("B1", "int")])) in
    print_handler_bindings (h,b)
;;

print_test_type "compile_target_all";
let (maps, map_vars, vars_parents, term_parents, result) = compile_target_all sum_ad in
	List.iter 
	    (fun (event, l) -> 
		print_endline ("on event :"^(string_of_delta event));
		List.iter 
		    (fun (h, b) -> print_handler_bindings (h,b)) l;
		print_endline ("\n")
	    ) result;;

print_test_type "compile_code_rec";
compile_code_rec sum_ad "sum_ad.cc";;

