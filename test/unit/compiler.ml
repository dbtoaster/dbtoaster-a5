
open Types
open Statement

let var vn = (vn, TInt)
let rel rn rv = (rn, List.map var rv, Schema.StreamRel, TInt)

let test_db = Schema.empty_db ()
;;

Schema.add_rel test_db (rel "R" ["A"; "B"]);;
Schema.add_rel test_db (rel "S" ["B"; "C"]);;
Schema.add_rel test_db (rel "T" ["C"; "D"]);;

let parse_calc ?(opt=false) (expr:string):Calculus.expr_t = 
   let ret = 
      Calculusparser.calculusExpr Calculuslexer.tokenize 
                                  (Lexing.from_string expr)
   in if opt then CalculusOptimizer.optimize_expr 
                     (Calculus.schema_of_expr ret) ret
             else ret
;;
let compile (name:string) (expr:string) =
   let q_expr = parse_calc expr in
   let q_schema = Calculus.schema_of_expr q_expr in
   let q_type = Calculus.type_of_expr q_expr in
      Compiler.compile test_db [{
         Statement.ds_name = Statement.mk_ds_name name q_schema q_type;
         Statement.ds_definition = q_expr
      }]
;;
let test_compile (name:string) (expr:string) 
        (datastructures: (string * string list * string *
                           (bool * string * string list * string) list) list) =
   Debug.log_unit_test ("Compiling "^name)
      Compiler.string_of_plan
      (compile name expr)
      (List.map (fun (ds_name, ds_ovars, ds_defn, ds_triggers) -> 
         let ds = {
            Statement.ds_name = 
               Statement.mk_ds_name ds_name ([],List.map var ds_ovars) TInt;
            Statement.ds_definition = parse_calc ~opt:true ds_defn
         } in {  
            Compiler.definition = ds;
            Compiler.metadata = {
               Compiler.init_at_start = false;
               Compiler.init_on_access = false
            };
            Compiler.triggers = List.map (fun (ins, reln, relv, delta) -> 
               (  (  (if ins then Schema.InsertEvent else Schema.DeleteEvent), 
                     rel reln relv), 
                  {  Statement.target_map = ds.ds_name;
                     Statement.update_type = Statement.UpdateStmt;
                     Statement.update_expr = parse_calc ~opt:true delta
                  })
            ) ds_triggers
         }
      ) datastructures)
;;

Debug.activate "LOG-COMPILE-DETAIL";;
(*
Debug.activate "LOG-CALCOPT-DETAIL";;
Debug.activate "LOG-FACTORIZE";;
*)

test_compile "RTest" "AggSum([], R(A,B))" [
   "RTest", [], "AggSum([], R(A,B))", [
      true,  "R", ["RTest_p_RA"; "RTest_p_RB"], "1"; 
      false, "R", ["RTest_m_RA"; "RTest_m_RB"], "-1"; 
   ]
]
;;
test_compile "RSTest" "AggSum([], R(A,B)*S(B,C))" [
   "RSTest", [], "AggSum([], R(A,B))", [
      true,  "R", ["RTest_p_RA"; "RTest_p_RB"], "1"; 
      false, "R", ["RTest_m_RA"; "RTest_m_RB"], "-1"; 
   ]
]