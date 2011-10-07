open Calculus
open Util;;

(* Helpers *)
let cg = Compiler.generate_unit_test_code;;

let test_query query_str =
   Debug.log_unit_test ("Compile "^query_str) (String.concat "\n");;

let trigger_definitions_as_string defs =
   let strvars vl = String.concat "," (List.map fst vl) in 
   List.fold_left 
   (fun acc (delete, reln, relvars, (params, bigsum_vars), new_term) ->
      acc^(String.concat ";"
         [(string_of_bool delete); reln;
          (strvars relvars);
          ("(["^(strvars params)^"],["^(strvars bigsum_vars)^"])");
          (term_as_string new_term)]))
   "" defs;;

let term_mapping_as_string tl = String.concat ";"
   (List.map (fun (l,r) -> (term_as_string l)^"->"^(term_as_string r)) tl);;

let cdfr_as_string (trigger_def_l, term_mapping_l) =
   (trigger_definitions_as_string trigger_def_l)^
   (term_mapping_as_string term_mapping_l);;


(* the schema *)
let sch = [("R", [("A", TInt); ("B", TInt)]); ("S", [("B", TInt); ("C", TInt)]);
           ("T", [("C", TInt); ("D", TInt)]); ("U", [("A", TInt); ("D", TInt)]);
           ("V", [("D", TInt); ("E", TInt)])]

let relR = RA_Leaf(Rel("R", [("A", TInt); ("B", TInt)]));;
let relS = RA_Leaf(Rel("S", [("B", TInt); ("C", TInt)]));;
let relT = RA_Leaf(Rel("T", [("C", TInt); ("D", TInt)]));;
let relU = RA_Leaf(Rel("U", [("A", TInt); ("C", TInt)]));;
let relV = RA_Leaf(Rel("V", [("D", TInt); ("E", TInt)]));;

let m = make_term(RVal(
   AggSum(RProd[RVal (Var("A", TInt)); RVal (Var("C", TInt))],
          RA_MultiNatJoin([relR; relS]))));;

let mt = map_term "m" [];;

Debug.log_unit_test
"Compile compile_delta_for_rel 1" cdfr_as_string
(Compiler.compile_delta_for_rel true "R" [("A", TInt); ("B", TInt)] 
                                false mt [] [] sch m)
([(false, "R", [("mR_A", TInt); ("mR_B", TInt)], ([],[]),
   make_term(RProd [RVal (Var ("mR_A", TInt));
                    RVal (External("m_pR1", [("mR_B", TInt)]))]))],
 [(make_term(RVal
       (AggSum (RVal (Var ("C", TInt)),
                RA_Leaf (Rel ("S", [("mR_B", TInt); ("C", TInt)]))))),
  (map_term "m_pR1" [("mR_B", TInt)]))])
;;

Debug.log_unit_test
"Compile compile_delta_for_rel 2" trigger_definitions_as_string
(fst (
Compiler.compile_delta_for_rel true "S" [("B", TInt); ("C", TInt)] false
   (map_term "mR1" [("mR_B", TInt)]) [] [] sch
   (make_term(RVal
      (AggSum (RVal (Var ("C", TInt)),
               RA_Leaf (Rel ("S", [("mR_B", TInt); ("C", TInt)]))))))))
([(false, "S", [("mR1S_B", TInt); ("mR1S_C", TInt)],
   ([("mR1S_B", TInt)], []), make_term(RVal (Var ("mR1S_C", TInt))))]);;


(* select sum(A*C) from R, S where R.B=S.B *)
(*
test_query "select sum(A*C) from R, S where R.B=S.B"
(Compiler.compile Calculus.ModeExtractFromCond sch (m,mt) cg [])
(["+R(x_mR_A, x_mR_B): m[] += (x_mR_A*mR1[x_mR_B])";
 "+S(x_mS_B, x_mS_C): m[] += (mS1[x_mS_B]*x_mS_C)";
 "+S(x_mR1S_B, x_mR1S_C): mR1[x_mR1S_B] += x_mR1S_C";
 "+R(x_mS1R_A, x_mS1R_B): mS1[x_mS1R_B] += x_mS1R_A"])
;;
*)

(* select sum(A*C) from R, S where R.B<S.B *)
(*
let m = 
(make_term(RVal(AggSum(
   RProd[RVal (Var(("A", TInt))); RVal (Var(("C", TInt)))],
   RA_MultiNatJoin([
      RA_Leaf(Rel("R", [("A", TInt); ("B1", TInt)]));
      RA_Leaf(Rel("S", [("B2", TInt); ("C", TInt)]));
      RA_Leaf(AtomicConstraint(Lt,
         RVal(Var(("B1", TInt))), RVal(Var(("B2", TInt)))))
   ])))))
in
test_query "select sum(A*C) from R, S where R.B<S.B"
(Compiler.compile Calculus.ModeExtractFromCond sch (m,mt) cg [])
(["+R(x_mR_A, x_mR_B): m[] += (x_mR_A*mR1[x_mR_B])";
  "+S(x_mS_B, x_mS_C): m[] += (mS1[x_mS_B]*x_mS_C)";
  "+S(x_mR1S_B, x_mR1S_C): foreach x_mR_B do mR1[x_mR_B] += "^
     "(x_mR1S_C*(if x_mR_B<x_mR1S_B then 1 else 0))";
  "+R(x_mS1R_A, x_mS1R_B): foreach x_mS_B do mS1[x_mS_B] += "^
     "(x_mS1R_A*(if x_mS1R_B<x_mS_B then 1 else 0))"])
;;
*)

(* select sum(A*C) from R, S where R.B=S.B group by A *)
(*
test_query "select sum(A*C) from R, S where R.B=S.B group by A"
(Compiler.compile Calculus.ModeExtractFromCond
   sch (m, (map_term "m" [("A", TInt)])) cg [])
(["+R(x_mR_A, x_mR_B): m[x_mR_A] += (x_mR_A*mR1[x_mR_B])";
  "+S(x_mS_B, x_mS_C): foreach A do m[A] += (mS1[A, x_mS_B]*x_mS_C)";
  "+S(x_mR1S_B, x_mR1S_C): mR1[x_mR1S_B] += x_mR1S_C";
  "+R(x_mS1R_A, x_mS1R_B): mS1[x_mS1R_A, x_mS1R_B] += x_mS1R_A"])
;;
*)


(* select sum(A*D) from R, S, T where R.B=S.B and S.C=T.C *)
(*
let m =
(make_term(
   RVal(AggSum(RProd[RVal (Var(("A", TInt))); RVal (Var(("D", TInt)))],
                RA_MultiNatJoin([relR; relS; relT]))))) 
in test_query "select sum(A*D) from R, S, T where R.B=S.B and S.C=T.C"
(Compiler.compile Calculus.ModeExtractFromCond sch (m, mt) cg [])
(["+R(x_mR_A, x_mR_B): m[] += (x_mR_A*mR1[x_mR_B])";
  "+S(x_mS_B, x_mS_C): m[] += (mS1[x_mS_B]*mS2[x_mS_C])";
  "+T(x_mT_C, x_mT_D): m[] += (mT1[x_mT_C]*x_mT_D)";
  "+S(x_mR1S_B, x_mR1S_C): mR1[x_mR1S_B] += mR1S1[x_mR1S_C]";
  "+T(x_mR1T_C, x_mR1T_D): foreach x_mR_B do mR1[x_mR_B] += "^
     "(x_mR1T_D*mR1T1[x_mR_B, x_mR1T_C])";
  "+T(x_mR1S1T_C, x_mR1S1T_D): mR1S1[x_mR1S1T_C] += x_mR1S1T_D";
  "+S(x_mR1T1S_B, x_mR1T1S_C): mR1T1[x_mR1T1S_B, x_mR1T1S_C] += 1";
  "+R(x_mS1R_A, x_mS1R_B): mS1[x_mS1R_B] += x_mS1R_A";
  "+T(x_mS2T_C, x_mS2T_D): mS2[x_mS2T_C] += x_mS2T_D";
  "+R(x_mT1R_A, x_mT1R_B): foreach x_mT_C do mT1[x_mT_C] += "^
     "(x_mT1R_A*mT1R1[x_mT1R_B, x_mT_C])";
  "+S(x_mT1S_B, x_mT1S_C): mT1[x_mT1S_C] += mT1S1[x_mT1S_B]";
  "+S(x_mT1R1S_B, x_mT1R1S_C): mT1R1[x_mT1R1S_B, x_mT1R1S_C] += 1";
  "+R(x_mT1S1R_A, x_mT1S1R_B): mT1S1[x_mT1S1R_B] += x_mT1S1R_A"])
;;
*)

(* select sum(A) from R, S, T where R.B=S.B and S.C=T.C group by D *)
let m =  (make_term(RVal(
   AggSum(RVal(Var(("A", TInt))), RA_MultiNatJoin([relR; relS; relT])))))
in
test_query "select sum(A) from R, S, T where R.B=S.B and S.C=T.C group by D"

(Compiler.compile Calculus.ModeExtractFromCond
   sch (m, (map_term "m" [("D", TInt)]), false) cg [])

(* There are a lot of terms here -- we do some simple duplicate elimination
   in the compiler, but that only catches maps with the same name.  The insert
   and delete branches of the compilation process produce different names
   by necessity, so those don't get caught until M3 generation.  It's ugly,
   but not something that can really be fixed cleanly until alpha5. *)
(["+R(mR_A, mR_B): foreach D do m[D] += (mR_A*m_pR1[mR_B, D])";
 "-R(mR_A, mR_B): foreach D do m[D] += (mR_A*m_mR1[mR_B, D]*-1)";
 "+S(mS_B, mS_C): foreach D do m[D] += (m_pS1[mS_B]*m_pS2[mS_C, D])";
 "-S(mS_B, mS_C): foreach D do m[D] += (m_mS1[mS_B]*m_mS2[mS_C, D]*-1)";
 "+T(mT_C, mT_D): m[mT_D] += m_pT1[mT_C]";
 "-T(mT_C, mT_D): m[mT_D] += (m_mT1[mT_C]*-1)";
 "+R(m_mT1R_A, m_mT1R_B): foreach mT_C do m_mT1[mT_C] += "^
    "(m_mT1R_A*m_mT1_pR1[m_mT1R_B, mT_C])";
 "-R(m_mT1R_A, m_mT1R_B): foreach mT_C do m_mT1[mT_C] += "^
    "(m_mT1R_A*m_mT1_mR1[m_mT1R_B, mT_C]*-1)";
 "+S(m_mT1S_B, m_mT1S_C): m_mT1[m_mT1S_C] += m_mT1_pS1[m_mT1S_B]";
 "-S(m_mT1S_B, m_mT1S_C): m_mT1[m_mT1S_C] += (m_mT1_mS1[m_mT1S_B]*-1)";
 "+R(m_mT1_mS1R_A, m_mT1_mS1R_B): m_mT1_mS1[m_mT1_mS1R_B] += m_mT1_mS1R_A";
 "-R(m_mT1_mS1R_A, m_mT1_mS1R_B): m_mT1_mS1[m_mT1_mS1R_B] += (m_mT1_mS1R_A*-1)";
 "+R(m_mT1_pS1R_A, m_mT1_pS1R_B): m_mT1_pS1[m_mT1_pS1R_B] += m_mT1_pS1R_A";
 "-R(m_mT1_pS1R_A, m_mT1_pS1R_B): m_mT1_pS1[m_mT1_pS1R_B] += (m_mT1_pS1R_A*-1)";
 "+S(m_mT1_mR1S_B, m_mT1_mR1S_C): m_mT1_mR1[m_mT1_mR1S_B, m_mT1_mR1S_C] += 1";
 "-S(m_mT1_mR1S_B, m_mT1_mR1S_C): m_mT1_mR1[m_mT1_mR1S_B, m_mT1_mR1S_C] += -1";
 "+S(m_mT1_pR1S_B, m_mT1_pR1S_C): m_mT1_pR1[m_mT1_pR1S_B, m_mT1_pR1S_C] += 1";
 "-S(m_mT1_pR1S_B, m_mT1_pR1S_C): m_mT1_pR1[m_mT1_pR1S_B, m_mT1_pR1S_C] += -1";
 "+R(m_pT1R_A, m_pT1R_B): foreach mT_C do m_pT1[mT_C] += "^
    "(m_pT1R_A*m_pT1_pR1[m_pT1R_B, mT_C])";
 "-R(m_pT1R_A, m_pT1R_B): foreach mT_C do m_pT1[mT_C] += "^
    "(m_pT1R_A*m_pT1_mR1[m_pT1R_B, mT_C]*-1)";
 "+S(m_pT1S_B, m_pT1S_C): m_pT1[m_pT1S_C] += m_pT1_pS1[m_pT1S_B]";
 "-S(m_pT1S_B, m_pT1S_C): m_pT1[m_pT1S_C] += (m_pT1_mS1[m_pT1S_B]*-1)";
 "+R(m_pT1_mS1R_A, m_pT1_mS1R_B): m_pT1_mS1[m_pT1_mS1R_B] += m_pT1_mS1R_A";
 "-R(m_pT1_mS1R_A, m_pT1_mS1R_B): m_pT1_mS1[m_pT1_mS1R_B] += (m_pT1_mS1R_A*-1)";
 "+R(m_pT1_pS1R_A, m_pT1_pS1R_B): m_pT1_pS1[m_pT1_pS1R_B] += m_pT1_pS1R_A";
 "-R(m_pT1_pS1R_A, m_pT1_pS1R_B): m_pT1_pS1[m_pT1_pS1R_B] += (m_pT1_pS1R_A*-1)";
 "+S(m_pT1_mR1S_B, m_pT1_mR1S_C): m_pT1_mR1[m_pT1_mR1S_B, m_pT1_mR1S_C] += 1";
 "-S(m_pT1_mR1S_B, m_pT1_mR1S_C): m_pT1_mR1[m_pT1_mR1S_B, m_pT1_mR1S_C] += -1";
 "+S(m_pT1_pR1S_B, m_pT1_pR1S_C): m_pT1_pR1[m_pT1_pR1S_B, m_pT1_pR1S_C] += 1";
 "-S(m_pT1_pR1S_B, m_pT1_pR1S_C): m_pT1_pR1[m_pT1_pR1S_B, m_pT1_pR1S_C] += -1";
 "+T(m_mS2T_C, m_mS2T_D): m_mS2[m_mS2T_C, m_mS2T_D] += 1";
 "-T(m_mS2T_C, m_mS2T_D): m_mS2[m_mS2T_C, m_mS2T_D] += -1";
 "+R(m_mS1R_A, m_mS1R_B): m_mS1[m_mS1R_B] += m_mS1R_A";
 "-R(m_mS1R_A, m_mS1R_B): m_mS1[m_mS1R_B] += (m_mS1R_A*-1)";
 "+T(m_pS2T_C, m_pS2T_D): m_pS2[m_pS2T_C, m_pS2T_D] += 1";
 "-T(m_pS2T_C, m_pS2T_D): m_pS2[m_pS2T_C, m_pS2T_D] += -1";
 "+R(m_pS1R_A, m_pS1R_B): m_pS1[m_pS1R_B] += m_pS1R_A";
 "-R(m_pS1R_A, m_pS1R_B): m_pS1[m_pS1R_B] += (m_pS1R_A*-1)";
 "+S(m_mR1S_B, m_mR1S_C): foreach D do m_mR1[m_mR1S_B, D] += m_mR1_pS1[m_mR1S_C, D]";
 "-S(m_mR1S_B, m_mR1S_C): foreach D do m_mR1[m_mR1S_B, D] += (m_mR1_mS1[m_mR1S_C, D]*-1)";
 "+T(m_mR1T_C, m_mR1T_D): foreach mR_B do m_mR1[mR_B, m_mR1T_D] += m_mR1_pT1[mR_B, m_mR1T_C]";
 "-T(m_mR1T_C, m_mR1T_D): foreach mR_B do m_mR1[mR_B, m_mR1T_D] += (m_mR1_mT1[mR_B, m_mR1T_C]*-1)";
 "+S(m_mR1_mT1S_B, m_mR1_mT1S_C): m_mR1_mT1[m_mR1_mT1S_B, m_mR1_mT1S_C] += 1";
 "-S(m_mR1_mT1S_B, m_mR1_mT1S_C): m_mR1_mT1[m_mR1_mT1S_B, m_mR1_mT1S_C] += -1";
 "+S(m_mR1_pT1S_B, m_mR1_pT1S_C): m_mR1_pT1[m_mR1_pT1S_B, m_mR1_pT1S_C] += 1";
 "-S(m_mR1_pT1S_B, m_mR1_pT1S_C): m_mR1_pT1[m_mR1_pT1S_B, m_mR1_pT1S_C] += -1";
 "+T(m_mR1_mS1T_C, m_mR1_mS1T_D): m_mR1_mS1[m_mR1_mS1T_C, m_mR1_mS1T_D] += 1";
 "-T(m_mR1_mS1T_C, m_mR1_mS1T_D): m_mR1_mS1[m_mR1_mS1T_C, m_mR1_mS1T_D] += -1";
 "+T(m_mR1_pS1T_C, m_mR1_pS1T_D): m_mR1_pS1[m_mR1_pS1T_C, m_mR1_pS1T_D] += 1";
 "-T(m_mR1_pS1T_C, m_mR1_pS1T_D): m_mR1_pS1[m_mR1_pS1T_C, m_mR1_pS1T_D] += -1";
 "+S(m_pR1S_B, m_pR1S_C): foreach D do m_pR1[m_pR1S_B, D] += m_pR1_pS1[m_pR1S_C, D]";
 "-S(m_pR1S_B, m_pR1S_C): foreach D do m_pR1[m_pR1S_B, D] += (m_pR1_mS1[m_pR1S_C, D]*-1)";
 "+T(m_pR1T_C, m_pR1T_D): foreach mR_B do m_pR1[mR_B, m_pR1T_D] += m_pR1_pT1[mR_B, m_pR1T_C]";
 "-T(m_pR1T_C, m_pR1T_D): foreach mR_B do m_pR1[mR_B, m_pR1T_D] += (m_pR1_mT1[mR_B, m_pR1T_C]*-1)";
 "+S(m_pR1_mT1S_B, m_pR1_mT1S_C): m_pR1_mT1[m_pR1_mT1S_B, m_pR1_mT1S_C] += 1";
 "-S(m_pR1_mT1S_B, m_pR1_mT1S_C): m_pR1_mT1[m_pR1_mT1S_B, m_pR1_mT1S_C] += -1";
 "+S(m_pR1_pT1S_B, m_pR1_pT1S_C): m_pR1_pT1[m_pR1_pT1S_B, m_pR1_pT1S_C] += 1";
 "-S(m_pR1_pT1S_B, m_pR1_pT1S_C): m_pR1_pT1[m_pR1_pT1S_B, m_pR1_pT1S_C] += -1";
 "+T(m_pR1_mS1T_C, m_pR1_mS1T_D): m_pR1_mS1[m_pR1_mS1T_C, m_pR1_mS1T_D] += 1";
 "-T(m_pR1_mS1T_C, m_pR1_mS1T_D): m_pR1_mS1[m_pR1_mS1T_C, m_pR1_mS1T_D] += -1";
 "+T(m_pR1_pS1T_C, m_pR1_pS1T_D): m_pR1_pS1[m_pR1_pS1T_C, m_pR1_pS1T_D] += 1";
 "-T(m_pR1_pS1T_C, m_pR1_pS1T_D): m_pR1_pS1[m_pR1_pS1T_C, m_pR1_pS1T_D] += -1"])
;;


(* select count( * ) from R group by B *)
let m = (make_term( RVal(AggSum(RVal(Const (Int 1)), relR)))) in
test_query "select count( * ) from R group by B"
(Compiler.compile Calculus.ModeExtractFromCond
   sch (m, (map_term "m" [("B", TInt)]), false) cg [])
(["+R(mR_A, mR_B): m[mR_B] += 1";
  "-R(mR_A, mR_B): m[mR_B] += -1"]);;


(* select count( * ) from R, S where R.B=S.B group by C *)
let m = 
   (make_term(
      RVal(AggSum(RVal(Const (Int 1)),
                   RA_MultiNatJoin([relR; relS])))))
in
test_query "select count( * ) from R, S where R.B=S.B group by C"
(Compiler.compile Calculus.ModeExtractFromCond
                 sch (m, (map_term "m" [("C", TInt)]), false) cg [])
(["+R(mR_A, mR_B): foreach C do m[C] += m_pR1[mR_B, C]";
  "-R(mR_A, mR_B): foreach C do m[C] += (m_mR1[mR_B, C]*-1)";
  "+S(mS_B, mS_C): m[mS_C] += m_pS1[mS_B]";
  "-S(mS_B, mS_C): m[mS_C] += (m_mS1[mS_B]*-1)";
  "+R(m_mS1R_A, m_mS1R_B): m_mS1[m_mS1R_B] += 1";
  "-R(m_mS1R_A, m_mS1R_B): m_mS1[m_mS1R_B] += -1";
  "+R(m_pS1R_A, m_pS1R_B): m_pS1[m_pS1R_B] += 1";
  "-R(m_pS1R_A, m_pS1R_B): m_pS1[m_pS1R_B] += -1";
  "+S(m_mR1S_B, m_mR1S_C): m_mR1[m_mR1S_B, m_mR1S_C] += 1";
  "-S(m_mR1S_B, m_mR1S_C): m_mR1[m_mR1S_B, m_mR1S_C] += -1";
  "+S(m_pR1S_B, m_pR1S_C): m_pR1[m_pR1S_B, m_pR1S_C] += 1";
  "-S(m_pR1S_B, m_pR1S_C): m_pR1[m_pR1S_B, m_pR1S_C] += -1"])
;;


(* select count( * ) from R, S where R.B = S.B group by B *)
let m = 
   (make_term(
      RVal(AggSum(RVal(Const (Int 1)),
                   RA_MultiNatJoin([relR; relS])))))
in
test_query "select count( * ) from R, S where R.B = S.B group by B"
(Compiler.compile Calculus.ModeExtractFromCond
                 sch (m, (map_term "m" [("B", TInt)]), false) cg [])
(["+R(mR_A, mR_B): m[mR_B] += m_pR1[mR_B]";
  "-R(mR_A, mR_B): m[mR_B] += (m_mR1[mR_B]*-1)";
  "+S(mS_B, mS_C): m[mS_B] += m_pS1[mS_B]";
  "-S(mS_B, mS_C): m[mS_B] += (m_mS1[mS_B]*-1)";
  "+R(m_mS1R_A, m_mS1R_B): m_mS1[m_mS1R_B] += 1";
  "-R(m_mS1R_A, m_mS1R_B): m_mS1[m_mS1R_B] += -1";
  "+R(m_pS1R_A, m_pS1R_B): m_pS1[m_pS1R_B] += 1";
  "-R(m_pS1R_A, m_pS1R_B): m_pS1[m_pS1R_B] += -1";
  "+S(m_mR1S_B, m_mR1S_C): m_mR1[m_mR1S_B] += 1";
  "-S(m_mR1S_B, m_mR1S_C): m_mR1[m_mR1S_B] += -1";
  "+S(m_pR1S_B, m_pR1S_C): m_pR1[m_pR1S_B] += 1";
  "-S(m_pR1S_B, m_pR1S_C): m_pR1[m_pR1S_B] += -1"])
;;


(* select count( * ) from R, S where R.B = S.B group by B, C *)
let m = 
   (make_term(RVal(AggSum(RVal(Const (Int 1)),
                   RA_MultiNatJoin([relR; relS]))))) 
in
test_query "select count( * ) from R, S where R.B = S.B group by B, C"
(Compiler.compile Calculus.ModeExtractFromCond
   sch (m, (map_term "m" [("B", TInt); ("C", TInt)]), false) cg [])
(["+R(mR_A, mR_B): foreach C do m[mR_B, C] += m_pR1[mR_B, C]";
  "-R(mR_A, mR_B): foreach C do m[mR_B, C] += (m_mR1[mR_B, C]*-1)";
  "+S(mS_B, mS_C): m[mS_B, mS_C] += m_pS1[mS_B]";
  "-S(mS_B, mS_C): m[mS_B, mS_C] += (m_mS1[mS_B]*-1)";
  "+R(m_mS1R_A, m_mS1R_B): m_mS1[m_mS1R_B] += 1";
  "-R(m_mS1R_A, m_mS1R_B): m_mS1[m_mS1R_B] += -1";
  "+R(m_pS1R_A, m_pS1R_B): m_pS1[m_pS1R_B] += 1";
  "-R(m_pS1R_A, m_pS1R_B): m_pS1[m_pS1R_B] += -1";
  "+S(m_mR1S_B, m_mR1S_C): m_mR1[m_mR1S_B, m_mR1S_C] += 1";
  "-S(m_mR1S_B, m_mR1S_C): m_mR1[m_mR1S_B, m_mR1S_C] += -1";
  "+S(m_pR1S_B, m_pR1S_C): m_pR1[m_pR1S_B, m_pR1S_C] += 1";
  "-S(m_pR1S_B, m_pR1S_C): m_pR1[m_pR1S_B, m_pR1S_C] += -1"])
;;


(* self-join *)
(* select count( * ) from R r1, R r2 where r1.B = r2.A *)
let m = 
   (make_term(RVal(AggSum(RVal(Const (Int 1)),
                   RA_MultiNatJoin([
 RA_Leaf(Rel("R", [("x", TInt); ("y", TInt)]));
 RA_Leaf(Rel("R", [("y", TInt); ("z", TInt)]))
])))))
in test_query "select count( * ) from R r1, R r2 where r1.B = r2.A" 
(Compiler.compile Calculus.ModeExtractFromCond sch (m, mt, false) cg [])
(["+R(mR_A, mR_B): m[] += (m_pR1[mR_B]+m_pR2[mR_A]+(if mR_A=mR_B then 1 else 0))";
  "-R(mR_A, mR_B): m[] += ((if mR_A=mR_B then 1 else 0)+(-1*(m_mR1[mR_B]+m_mR2[mR_A])))";
  "+R(m_mR2R_A, m_mR2R_B): m_mR2[m_mR2R_B] += 1";
  "-R(m_mR2R_A, m_mR2R_B): m_mR2[m_mR2R_B] += -1";
  "+R(m_mR1R_A, m_mR1R_B): m_mR1[m_mR1R_A] += 1";
  "-R(m_mR1R_A, m_mR1R_B): m_mR1[m_mR1R_A] += -1";
  "+R(m_pR2R_A, m_pR2R_B): m_pR2[m_pR2R_B] += 1";
  "-R(m_pR2R_A, m_pR2R_B): m_pR2[m_pR2R_B] += -1";
  "+R(m_pR1R_A, m_pR1R_B): m_pR1[m_pR1R_A] += 1";
  "-R(m_pR1R_A, m_pR1R_B): m_pR1[m_pR1R_A] += -1"])
;;


(* too large to look at:
(* select sum(A) from R, S, U
   where R.B=S.B and R.A=U.A and S.C=U.C
   group by C
*)
Compiler.compile Calculus.ModeExtractFromCond
                 sch (map_term "m" [("C", TInt)])
(make_term(
   RVal(AggSum(RVal (Var(("A", TInt))),
                RA_MultiNatJoin([relR; relS; relU])))))
;;
*)


(* select count( * ) from R, S where R.A=S.A and R.B=S.B *)
let m = (make_term( RVal(AggSum(RVal(Const (Int 1)),
   RA_MultiNatJoin([
      RA_Leaf(Rel("R", [("x", TInt); ("y", TInt)]));
      RA_Leaf(Rel("S", [("x", TInt); ("y", TInt)]))
   ])))))
in test_query "select count( * ) from R, S where R.A=S.A and R.B=S.B"
(Compiler.compile Calculus.ModeExtractFromCond sch (m, mt, false) cg [])
(["+R(mR_A, mR_B): m[] += m_pR1[mR_A, mR_B]";
  "-R(mR_A, mR_B): m[] += (m_mR1[mR_A, mR_B]*-1)";
  "+S(mS_B, mS_C): m[] += m_pS1[mS_B, mS_C]";
  "-S(mS_B, mS_C): m[] += (m_mS1[mS_B, mS_C]*-1)";
  "+R(m_mS1R_A, m_mS1R_B): m_mS1[m_mS1R_A, m_mS1R_B] += 1";
  "-R(m_mS1R_A, m_mS1R_B): m_mS1[m_mS1R_A, m_mS1R_B] += -1";
  "+R(m_pS1R_A, m_pS1R_B): m_pS1[m_pS1R_A, m_pS1R_B] += 1";
  "-R(m_pS1R_A, m_pS1R_B): m_pS1[m_pS1R_A, m_pS1R_B] += -1";
  "+S(m_mR1S_B, m_mR1S_C): m_mR1[m_mR1S_B, m_mR1S_C] += 1";
  "-S(m_mR1S_B, m_mR1S_C): m_mR1[m_mR1S_B, m_mR1S_C] += -1";
  "+S(m_pR1S_B, m_pR1S_C): m_pR1[m_pR1S_B, m_pR1S_C] += 1";
  "-S(m_pR1S_B, m_pR1S_C): m_pR1[m_pR1S_B, m_pR1S_C] += -1"])
;;


(* select count( * ) from R where R.A < 5 and R.B = 'Bla' *)
let m = (make_term(RVal(AggSum(RVal(Const (Int 1)),
   RA_MultiNatJoin([
      RA_Leaf(Rel("R", [("A", TInt); ("B", TInt)]));
      RA_Leaf(AtomicConstraint(Lt,
                 RVal(Var(("A", TInt))), RVal(Const(Int 5))));
      RA_Leaf(AtomicConstraint(Eq,
                 RVal(Var(("B", TInt))), RVal(Const(String "Bla"))));
   ])))))
in
test_query "select count( * ) from R where R.A < 5 and R.B = 'Bla'"
(Compiler.compile Calculus.ModeExtractFromCond
   sch (m, (map_term "q" []), false) cg [])
(["+R(qR_A, qR_B): q[] += "^
    "((if qR_A<5 then 1 else 0)*(if qR_B='Bla' then 1 else 0))";
  "-R(qR_A, qR_B): q[] += "^
    "((if qR_A<5 then 1 else 0)*(if qR_B='Bla' then 1 else 0)*-1)"])
;;



(* if(0 < AggSum(1, R(A,B))) then 1 else 0 *)
let m = (make_term(RVal(AggSum(RVal(Const(Int 1)),
   RA_Leaf(AtomicConstraint(Lt, RVal(Const (Int 0)),
      RVal(AggSum(RVal(Const(Int 1)),
                  RA_Leaf(Rel("R", [("A", TInt); ("B", TInt)]))))))))
))
in
test_query "if(0 < AggSum(1, R(A,B))) then 1 else 0"
(Compiler.compile Calculus.ModeExtractFromCond
   [("R", [("A", TInt); ("B", TInt)])] (m, mt, false) cg [])
(["+R(mR_A, mR_B): m[] += ((if 0<(m_pR1[]+1) and m_pR1[]<=0 then 1 else 0)+((if 0<m_pR1[] and (m_pR1[]+1)<=0 then 1 else 0)*-1))";
  "-R(mR_A, mR_B): m[] += ((if 0<(m_mR1[]+-1) and m_mR1[]<=0 then 1 else 0)+((if 0<m_mR1[] and (m_mR1[]+-1)<=0 then 1 else 0)*-1))";
  "+R(m_mR1R_A, m_mR1R_B): m_mR1[] += 1";
  "-R(m_mR1R_A, m_mR1R_B): m_mR1[] += -1";
  "+R(m_pR1R_A, m_pR1R_B): m_pR1[] += 1";
  "-R(m_pR1R_A, m_pR1R_B): m_pR1[] += -1"])
;;

(* old version: SemiRing and old delta -- it's equivalent.

["+R(x_mR_A, x_mR_B): m[] += (if 0<(mR1[]+1) and mR1[]<=0 then 1 else 0)";
 "+R(x_mR_A, x_mR_B): m[] += (-1*(if (mR1[]+1)<=0 and 0<mR1[] then 1 else 0))";
 "+R(x_mR1R_A, x_mR1R_B): mR1[] += 1"]
;;
*)


(*
let m = make_term(RVal(AggSum(
    RSum[RVal (Var("A", TInt)); RVal (Var ("C", TInt));],
    RA_MultiNatJoin([relR; relS])))) 
in
test_query "factorized query compilation test: select sum(a) from R,U"
(Compiler.compile Calculus.ModeExtractFromCond sch (m, mt, false) cg [])
([])
;;
*)


