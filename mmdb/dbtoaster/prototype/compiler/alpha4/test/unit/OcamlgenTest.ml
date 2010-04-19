(*
  ./dbtoaster_top -I codegen/ocaml test/unit/OcamlgenTest.ml
*)

open Util

module Gen = M3Compiler.Make(M3OCamlgen.CG);;

let trig:M3.trig_t = (
  M3.Insert,
  "BIDS",
  ["QUERY_1_1__1BIDS_B1__PRICE"; "QUERY_1_1__1BIDS_B1__VOLUME"],
  [( "QUERY_3", ["B1_PRICE"], [], 
       M3.IfThenElse0(
         M3.Lt(M3.Var("B1_PRICE"),M3.Var("B2_PRICE")),
         M3.Mult(
           M3.MapAccess("INPUT_MAP_BIDS", [], ["B2_PRICE";"B2_VOLUME"], 
                        M3.Const(M3.CFloat(0.0))),
           M3.Var("B2_VOLUME")
         )
       )
     ),(
       M3.Mult(
         M3.Var("QUERY_1_1__1BIDS_B1__VOLUME"),
         M3.IfThenElse0(
           M3.Lt(M3.Var("B1_PRICE"), M3.Var("QUERY_1_1__1BIDS_B1__PRICE")),
           M3.Const(M3.CFloat(0.0))
         )
       )
     )
  ]
)

let (trigs,_) = 
  M3Compiler.prepare_triggers [trig] (fun x -> "var_");;

let stmts = List.flatten (List.map (fun (_,_,_,stmts) -> stmts) trigs);;

print_endline
  (string_of_list0 "\n\n\n" (fun (mapacc, aggecalc, _) ->
    
  
  
  M3Common.pcalc_to_string stmts);;