
open Types
open Statement

let var vn = (vn, TFloat)
let rel rn rv = (rn, List.map var rv, Schema.StreamRel, TFloat)
;;
let test_db = Schema.empty_db ()
;;

Schema.add_rel test_db (rel "R" ["A"; "B"]);;
Schema.add_rel test_db (rel "S" ["B"; "C"]);;
Schema.add_rel test_db (rel "T" ["C"; "D"]);;

let parse_calc ?(opt=false) (expr:string):Calculus.expr_t =
   try  
      let ret = 
         Calculusparser.calculusExpr Calculuslexer.tokenize 
                                     (Lexing.from_string expr)
      in if opt then CalculusOptimizer.optimize_expr 
                        (Calculus.schema_of_expr ret) ret
                else ret
   with Parsing.Parse_error -> (
      print_endline ("Error parsing :'"^expr^"'");
      let _ = Parsing.set_trace true in
      Calculusparser.calculusExpr Calculuslexer.tokenize 
                                     (Lexing.from_string expr)
   )
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
        (datastructures: (string * string list * string * type_t *
                           (bool * string * string list * string) list) list) =
   Debug.log_unit_test_list ("Compiling "^name)
      Compiler.string_of_ds
      (compile name expr)
      (List.map (fun (ds_name, ds_ovars, ds_defn, ds_type, ds_triggers) -> 
         let ds = {
            Statement.ds_name = 
               Statement.mk_ds_name ds_name ([],List.map var ds_ovars) ds_type;
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


test_compile "RTest" "AggSum([], R(A,B))" [
   "RTest", [], "AggSum([], R(A,B))", TInt, [
      true,  "R", ["RTest_p_RA"; "RTest_p_RB"], "1"; 
      false, "R", ["RTest_m_RA"; "RTest_m_RB"], "-1"; 
   ]
]
;;
Debug.activate "VISUAL-DIFF";;
(*
Debug.activate "LOG-COMPILE-DETAIL";;
Debug.activate "LOG-CALCOPT-DETAIL";;
Debug.activate "PRINT-VERBOSE";;
Debug.activate "LOG-UNIFY-LIFTS";;
Debug.activate "LOG-FACTORIZE";;
*)
test_compile "RSTest" "AggSum([], R(A,B)*S(B,C))" [
   "RSTest_m_R_2", ["RSTest_m_RB"], "AggSum([RSTest_m_RB], S(RSTest_m_RB,C))", 
                   TInt, [
      true,  "S", ["RSTest_m_R_2_p_SB"; "RSTest_m_R_2_p_SC"], 
         "(RSTest_m_RB ^= RSTest_m_R_2_p_SB)"; 
      false, "S", ["RSTest_m_R_2_m_SB"; "RSTest_m_R_2_m_SC"], 
         "-1*(RSTest_m_RB ^= RSTest_m_R_2_m_SB)"; 
   ];
   "RSTest_m_S_2", ["RSTest_m_SB"], "AggSum([RSTest_m_SB], R(A,RSTest_m_SB))", 
                   TInt, [
      true,  "R", ["RSTest_m_S_2_p_RA"; "RSTest_m_S_2_p_RB"], 
         "(RSTest_m_SB ^= RSTest_m_S_2_p_RB)"; 
      false, "R", ["RSTest_m_S_2_m_RA"; "RSTest_m_S_2_m_RB"], 
         "-1*(RSTest_m_SB ^= RSTest_m_S_2_m_RB)"; 
   ];
   "RSTest", [], "AggSum([], R(A,B)*S(B,C))", TInt, [
      true,  "S", ["RSTest_p_SB"; "RSTest_p_SC"], 
         "RSTest_m_S_2(int)[][RSTest_p_SB]"; 
      false, "S", ["RSTest_m_SB"; "RSTest_m_SC"], 
         "-1*RSTest_m_S_2(int)[][RSTest_m_SB]"; 
      true,  "R", ["RSTest_p_RA"; "RSTest_p_RB"], 
         "RSTest_m_R_2(int)[][RSTest_p_RB]"; 
      false, "R", ["RSTest_m_RA"; "RSTest_m_RB"], 
         "-1*RSTest_m_R_2(int)[][RSTest_m_RB]"; 
   ];
]
