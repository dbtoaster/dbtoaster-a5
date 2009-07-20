open Algebra
open Codegen
open Compile
open Gui

(*
let tv = 
    `MapAggregate(`Sum,
    `METerm(`Attribute(`Qualified("B", "V"))),
    `Relation("B",[("P", "int"); ("V", "int")]))

let sv1_pred =
    `BTerm(`GT(
	`ETerm(`Attribute(`Qualified("B", "P"))),
	`ETerm(`Attribute(`Qualified("B", "P")))))

let select_sv1 =
    `Select(sv1_pred, `Relation("B",[("P", "int"); ("V", "int")]))

let k_sv0 = `Product(`METerm(`Float(0.25)), tv)
let sv1 = `MapAggregate(`Sum,
`METerm(`Attribute(`Qualified("B","V"))), select_sv1)

let m_p2 = `Sum(k_sv0, sv1)

let vwap =
    `MapAggregate(`Sum,
        `Product(`METerm(`Attribute(`Qualified("B", "P"))),
            `METerm(`Attribute(`Qualified("B", "V")))),
        `Select(`BTerm(`MLT(m_p2)),
            `Relation("B", [("P", "int"); ("V","int")])))
;;
*)

let tv = 
    `MapAggregate(`Sum,
      `METerm(`Attribute(`Qualified("B", "V0"))),
        `Relation("B",[("P0", "int"); ("V0", "int")]))

let sv1_pred =
    `BTerm(`GT(
	`ETerm(`Attribute(`Qualified("B", "P1"))),
	`ETerm(`Attribute(`Qualified("B", "P2")))))

let select_sv1 =
    `Select(sv1_pred, `Relation("B",[("P1", "int"); ("V1", "int")]))

let k_sv0 = `Product(`METerm(`Float(0.25)), tv)
let sv1 = `MapAggregate(`Sum,
`METerm(`Attribute(`Qualified("B","V1"))), select_sv1)

let m_p2 = `Sum(k_sv0, sv1)

let vwap =
    `MapAggregate(`Sum,
      `Product(`METerm(`Attribute(`Qualified("B", "P2"))),
      `METerm(`Attribute(`Qualified("B", "V2")))),
        `Select(`BTerm(`MLT(m_p2)),
          `Relation("B", [("P2", "int"); ("V2","int")])))

;;

(* string_of tests *)
let print_test_type tt =
    print_endline ((String.make 50 '-')^"\n\n"^tt^" tests\n\n"^(String.make 50 '-'))
;;

(*
print_test_type "compile_code";
compile_code vwap (`Insert ("B", [("p", "int"); ("v", "int")])) "vwap.cc";;
*)

print_test_type "compile_code_rec";
compile_code_rec vwap "vwap.cc"
    [(`Insert ("B", [("p", "int"); ("v", "int")]))];;
