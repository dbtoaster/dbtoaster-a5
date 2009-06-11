open Algebra
open Compile

let r = `Relation("R", [("A", "int"); ("B", "int")])
let s = `Relation("S", [("B", "int"); ("C", "int")])
let t = `Relation("T", [("C", "int"); ("D", "int")])

let p = `Cross(`Cross(r, s), t)
let p2 = `NaturalJoin(r, `NaturalJoin (s, t))

let b_expr = 
	`And(
	    `BTerm(
		`EQ(
		    (`ETerm(`Attribute(`Qualified("R", "B")))), 
		    (`ETerm(`Attribute(`Qualified("S", "B"))))
	        )	
	    ),
	    `BTerm(
	        `EQ(
		    (`ETerm(`Attribute(`Qualified("S", "C")))),
		    (`ETerm(`Attribute(`Qualified("T", "C"))))
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
	print_endline ((String.make 50 '-')^"\n\n"^tt^" tets\n\n"^(String.make 50 '-'));;
print_test_type "string_of";
print_endline (string_of_map_expression sum_ad);

print_test_type "compile_code";
let (h,b)= compile_target sum_ad(`Insert "R") in
    print_handler_bindings (h,b)

let result = compile_target_all sum_ad in
	List.iter 
	    (fun (event, l) -> 
		print_endline ("on event :"^(string_of_delta event));
		List.iter 
		    (fun (h, b) -> print_handler_bindings (h,b)) l;
		print_endline ("\n")
	    ) result
