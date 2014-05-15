
open Type
open Plan
open UnitTest

let test_db = mk_db [
   ("R", ["A"; "B"]);
   ("S", ["B"; "C"]);
   ("T", ["C"; "D"]);
];;

type eventType =
 | InsertEvent
 | DeleteEvent
 | SystemInitEvent

 let test_compile (name:string) (expr:string) 
        (datastructures: (string * string list * string * type_t *
                           (eventType * stmt_type_t * string * string list * string list * string) 
                           list) list) =
   log_list_test ("Compiling "^name)
      Compiler.string_of_ds
      (fst (compile test_db name expr))
      (List.map (fun (ds_name, ds_ovars, ds_defn, ds_type, ds_triggers) -> 
         let ds = {
            Plan.ds_name = 
               Plan.mk_ds_name ds_name ([],List.map var ds_ovars) ds_type;
            Plan.ds_definition = parse_calc ~opt:true ds_defn
         } in {  
            Plan.description = ds;
            Plan.ds_triggers = 
               List.map (fun (evt_type, stmt_type, reln, relv, update_ov, delta) -> 
               (  (match evt_type with
                    | InsertEvent -> event true reln relv
                    | DeleteEvent -> event false reln relv
                    | SystemInitEvent -> Schema.SystemInitializedEvent),
                  {  Plan.target_map = 
                        Plan.mk_ds_name ds_name ([],List.map var update_ov)
                                             ds_type;
                     Plan.update_type = stmt_type;
                     Plan.update_expr = parse_calc ~opt:true delta
                  })
            ) ds_triggers
         }
      ) datastructures)
;;

test_compile "RTest" "AggSum([], R(A,B))" [
   "RTest", [], "AggSum([], R(A,B))", TInt, [
      InsertEvent, UpdateStmt, "R", ["RTest_pRA"; "RTest_pRB"], [], "1"; 
      DeleteEvent, UpdateStmt, "R", ["RTest_mRA"; "RTest_mRB"], [], "-1"; 
   ]
]
;;

test_compile "RSTest" "AggSum([], R(A,B) * S(B,C))" [
   "RSTest", [], "AggSum([], R(A,B) * S(B,C))", TInt, [
      InsertEvent, UpdateStmt, "S", ["RSTest_pSB"; "RSTest_pSC"], [], 
         "RSTest_mS1(int)[][RSTest_pSB]"; 
      DeleteEvent, UpdateStmt, "S", ["RSTest_mSB"; "RSTest_mSC"], [], 
         "-1 * RSTest_mS1(int)[][RSTest_mSB]"; 
      InsertEvent, UpdateStmt, "R", ["RSTest_pRA"; "RSTest_pRB"], [], 
         "RSTest_mR1(int)[][RSTest_pRB]"; 
      DeleteEvent, UpdateStmt, "R", ["RSTest_mRA"; "RSTest_mRB"], [], 
         "-1 * RSTest_mR1(int)[][RSTest_mRB]"; 
   ];
   "RSTest_mS1", ["RSTest_mSB"], "AggSum([RSTest_mSB], R(A,RSTest_mSB))", 
                   TInt, [
      InsertEvent, UpdateStmt, "R", ["RSTest_mS1_pRA"; "RSTest_mS1_pRB"], 
         ["RSTest_mS1_pRB"], 
         "1";
      DeleteEvent, UpdateStmt, "R", ["RSTest_mS1_mRA"; "RSTest_mS1_mRB"], 
         ["RSTest_mS1_mRB"], 
         "-1"; 
   ];
   "RSTest_mR1", ["RSTest_mRB"], "AggSum([RSTest_mRB], S(RSTest_mRB,C))", 
                   TInt, [
      InsertEvent, UpdateStmt, "S", ["RSTest_mR1_pSB"; "RSTest_mR1_pSC"], 
         ["RSTest_mR1_pSB"], 
         "1";
      DeleteEvent, UpdateStmt, "S", ["RSTest_mR1_mSB"; "RSTest_mR1_mSC"], 
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
      SystemInitEvent, ReplaceStmt, "", [], [], "0.";
      InsertEvent, UpdateStmt, "S", ["RSACTest_pSB"; "RSACTest_pSC"], [], 
         "RSACTest_pSC * RSACTest_mS1(float)[][RSACTest_pSB]"; 
      DeleteEvent, UpdateStmt, "S", ["RSACTest_mSB"; "RSACTest_mSC"], [], 
         "-1 * RSACTest_mSC * RSACTest_mS1(float)[][RSACTest_mSB]"; 
      InsertEvent, UpdateStmt, "R", ["RSACTest_pRA"; "RSACTest_pRB"], [], 
         "RSACTest_pRA * RSACTest_mR1(float)[][RSACTest_pRB]"; 
      DeleteEvent, UpdateStmt, "R", ["RSACTest_mRA"; "RSACTest_mRB"], [], 
         "-1 * RSACTest_mRA * RSACTest_mR1(float)[][RSACTest_mRB]"; 
   ];
   "RSACTest_mS1", ["RSACTest_mSB"], "AggSum([RSACTest_mSB], 
                    R(A,RSACTest_mSB) * A)", TFloat, [
      InsertEvent, UpdateStmt, "R", ["RSACTest_mS1_pRA"; "RSACTest_mS1_pRB"], 
         ["RSACTest_mS1_pRB"],
         "RSACTest_mS1_pRA"; 
      DeleteEvent, UpdateStmt, "R", ["RSACTest_mS1_mRA"; "RSACTest_mS1_mRB"], 
         ["RSACTest_mS1_mRB"],
         "-1 * RSACTest_mS1_mRA"; 
   ];
   "RSACTest_mR1", ["RSACTest_mRB"], "AggSum([RSACTest_mRB], 
                     S(RSACTest_mRB,C) * C)", TFloat, [
      InsertEvent, UpdateStmt, "S", ["RSACTest_mR1_pSB"; "RSACTest_mR1_pSC"], 
         ["RSACTest_mR1_pSB"],
         "RSACTest_mR1_pSC"; 
      DeleteEvent, UpdateStmt, "S", ["RSACTest_mR1_mSB"; "RSACTest_mR1_mSC"], 
         ["RSACTest_mR1_mSB"],
         "-1 * RSACTest_mR1_mSC"; 
   ];
]

