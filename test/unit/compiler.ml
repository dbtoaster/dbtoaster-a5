
open Types
open Plan
open UnitTest

let test_db = mk_db [
   ("R", ["A"; "B"]);
   ("S", ["B"; "C"]);
   ("T", ["C"; "D"]);
];;

let test_compile (name:string) (expr:string) 
        (datastructures: (string * string list * string * type_t *
                           (bool * string * string list * string list * string) 
                           list) list) =
   log_list_test ("Compiling "^name)
      Compiler.string_of_ds
      (compile test_db name expr)
      (List.map (fun (ds_name, ds_ovars, ds_defn, ds_type, ds_triggers) -> 
         let ds = {
            Plan.ds_name = 
               Plan.mk_ds_name ds_name ([],List.map var ds_ovars) ds_type;
            Plan.ds_definition = parse_calc ~opt:true ds_defn
         } in {  
            Plan.description = ds;
            Plan.ds_triggers = 
               List.map (fun (ins, reln, relv, update_ov, delta) -> 
               (  event ins reln relv, 
                  {  Plan.target_map = 
                        Plan.mk_ds_name ds_name ([],List.map var update_ov)
                                             ds_type;
                     Plan.update_type = Plan.UpdateStmt;
                     Plan.update_expr = parse_calc ~opt:true delta
                  })
            ) ds_triggers
         }
      ) datastructures)
;;

test_compile "RTest" "AggSum([], R(A,B))" [
   "RTest", [], "AggSum([], R(A,B))", TInt, [
      true,  "R", ["RTest_pRA"; "RTest_pRB"], [], "1"; 
      false, "R", ["RTest_mRA"; "RTest_mRB"], [], "-1"; 
   ]
]
;;

test_compile "RSTest" "AggSum([], R(A,B) * S(B,C))" [
   "RSTest", [], "AggSum([], R(A,B) * S(B,C))", TInt, [
      true,  "S", ["RSTest_pSB"; "RSTest_pSC"], [], 
         "RSTest_mS1(int)[][RSTest_pSB]"; 
      false, "S", ["RSTest_mSB"; "RSTest_mSC"], [], 
         "-1 * RSTest_mS1(int)[][RSTest_mSB]"; 
      true,  "R", ["RSTest_pRA"; "RSTest_pRB"], [], 
         "RSTest_mR1(int)[][RSTest_pRB]"; 
      false, "R", ["RSTest_mRA"; "RSTest_mRB"], [], 
         "-1 * RSTest_mR1(int)[][RSTest_mRB]"; 
   ];
   "RSTest_mS1", ["RSTest_mSB"], "AggSum([RSTest_mSB], R(A,RSTest_mSB))", 
                   TInt, [
      true,  "R", ["RSTest_mS1_pRA"; "RSTest_mS1_pRB"], 
         ["RSTest_mS1_pRB"], 
         "1";
      false, "R", ["RSTest_mS1_mRA"; "RSTest_mS1_mRB"], 
         ["RSTest_mS1_mRB"], 
         "-1"; 
   ];
   "RSTest_mR1", ["RSTest_mRB"], "AggSum([RSTest_mRB], S(RSTest_mRB,C))", 
                   TInt, [
      true,  "S", ["RSTest_mR1_pSB"; "RSTest_mR1_pSC"], 
         ["RSTest_mR1_pSB"], 
         "1";
      false, "S", ["RSTest_mR1_mSB"; "RSTest_mR1_mSC"], 
         ["RSTest_mR1_mSB"], 
         "-1";
   ];
]
;;

(*
Debug.activate "VISUAL-DIFF";;
Debug.activate "LOG-COMPILE-DETAIL";;
Debug.activate "LOG-CALCOPT-DETAIL";;
Debug.activate "PRINT-VERBOSE";;
Debug.activate "LOG-UNIFY-LIFTS";;
Debug.activate "LOG-FACTORIZE";;
*)

test_compile "RSACTest" "AggSum([], R(A,B) * S(B,C) * A*C)" [
   "RSACTest", [], "AggSum([], R(A,B) * S(B,C) * A * C)", TFloat, [
      true,  "S", ["RSACTest_pSB"; "RSACTest_pSC"], [], 
         "RSACTest_pSC * RSACTest_mS1(float)[][RSACTest_pSB]"; 
      false, "S", ["RSACTest_mSB"; "RSACTest_mSC"], [], 
         "-1 * RSACTest_mSC * RSACTest_mS1(float)[][RSACTest_mSB]"; 
      true,  "R", ["RSACTest_pRA"; "RSACTest_pRB"], [], 
         "RSACTest_pRA * RSACTest_mR1(float)[][RSACTest_pRB]"; 
      false, "R", ["RSACTest_mRA"; "RSACTest_mRB"], [], 
         "-1 * RSACTest_mRA * RSACTest_mR1(float)[][RSACTest_mRB]"; 
   ];
   "RSACTest_mS1", ["RSACTest_mSB"], "AggSum([RSACTest_mSB], 
                    R(A,RSACTest_mSB) * A)", TFloat, [
      true,  "R", ["RSACTest_mS1_pRA"; "RSACTest_mS1_pRB"], 
         ["RSACTest_mS1_pRB"],
         "RSACTest_mS1_pRA"; 
      false, "R", ["RSACTest_mS1_mRA"; "RSACTest_mS1_mRB"], 
         ["RSACTest_mS1_mRB"],
         "-1 * RSACTest_mS1_mRA"; 
   ];
   "RSACTest_mR1", ["RSACTest_mRB"], "AggSum([RSACTest_mRB], 
                     S(RSACTest_mRB,C) * C)", TFloat, [
      true,  "S", ["RSACTest_mR1_pSB"; "RSACTest_mR1_pSC"], 
         ["RSACTest_mR1_pSB"],
         "RSACTest_mR1_pSC"; 
      false, "S", ["RSACTest_mR1_mSB"; "RSACTest_mR1_mSC"], 
         ["RSACTest_mR1_mSB"],
         "-1 * RSACTest_mR1_mSC"; 
   ];
]

