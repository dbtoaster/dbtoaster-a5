#use "MapAlgebra.ml";;


let rec extract_maps c =
    match c with
       CMultiplication(c1, c2) -> (extract_maps c1) @ (extract_maps c2)
    |  CAddition (c1, c2) -> (extract_maps c1) @ (extract_maps c2)
    |  CSumAgg(_, relalg, _) -> [c]
    |  _ -> []
    ;;


let mod_insert_c relname code =
   match code with
      CSumAgg(a,r,g) ->
         CSumAgg(a, simplify (mod_insert relname r), g)
   | _ -> code
   ;;


let rec decorate_with_bindings c =
    match c with
       CMultiplication(c1, c2) ->
          CMultiplication((decorate_with_bindings c1),
                          (decorate_with_bindings c2))
    |  CAddition (c1, c2) ->
          CAddition((decorate_with_bindings c1),
                    (decorate_with_bindings c2))
    |  CSumAgg(arith_expr, relalg, groupby) ->
          let bindings = (extract_bindings relalg) in
          let relalg2 = (eliminate_tups relalg) in
          let used_bindings = (ListAsSet.inter bindings
                        (schema relalg2)) in
          CSumAgg(arith_expr, relalg2, (groupby @ used_bindings))
    |  _ -> c
    ;;







(****************************************************************************
                      GENERATE C CODE
****************************************************************************)


let print_stringlist l =
   if (l = []) then ()
   else
      print_string (List.hd l);
      if ((List.tl l) != []) then
         List.iter (print_string ", "; print_string) (List.tl l);;


let print_mp (a, relalg, groupby) =
   print_string "mp";
   print_int (Hashtbl.hash_param 1000 1000 (a, relalg, groupby));
   if (groupby != []) then
      (print_string "[";
      print_stringlist groupby;
      print_string "]")
   else ();;


let print_mp2 code =
   match code with
      CSumAgg(x,y,z) -> print_mp(x,y,z)
   |  _ -> print_string "ERROR"
   ;;


let rec ccode c =
   match c with
      CMultiplication(c1, c2) ->
         (ccode c1); print_string " * "; (ccode c2)
   |  CAddition(c1, c2) ->
         print_string "(";
         (ccode c1);
         print_string " + ";
         (ccode c2);
         print_string ")"
   |  MapVar(col) ->
         print_string col
   |  CConstant(co) ->
         print_int co
   |  CSumAgg(a, relalg, groupby) ->
         print_mp (a, relalg, groupby)
   ;;



let print_mapalg mapalg =
   match mapalg with
      CSumAgg(a, r, g) ->
         print_string "CSumAgg(";
         print_arith  a;
         print_string ", ";
         print_relalg r;
         print_string ", [XXX";
         (* print_stringlist g; *)
         print_string "])"
   |   _ -> print_string "ERROR";;



let ccode2 (rname, lval, rval) =
(*
   print_string "/*
";
   print_mp2 lval;
   print_string ": ";
   print_mapalg lval;
   print_string "
*/
";
*)
   print_string "on insert into ";
   print_string rname;
   print_string " do: ";
   print_mp2 lval;
   print_string " := ";
   ccode rval;
   print_newline();;






(****************************************************************************
                        COMPILATION ALGORITHM 
****************************************************************************)


let build_insert_handler relname code =
   (relname, code,
    simplify_code(simplify_code(decorate_with_bindings(
      simplify_code(simplify_code(mod_insert_c relname code))))));;


(* naive quadratic time insertion.
   if l1 has no duplicates, then the result will be the union of
   l1 and l2 without duplicates (even if l2 has duplicates)
*)
let insert_if_new l1 l2 =
   let insert_if_new2 l x =
      if (List.mem x l) then l else (l @ [x]) in
   List.sort Pervasives.compare (List.fold_left insert_if_new2 l1 l2);; 


let mr(ready, todo) schema =
   let handlers =
      let handlers_for_rel relname =
         List.map (build_insert_handler relname) todo in
      List.filter (function (_, x, y) -> x <> y)
                  (List.flatten(List.map handlers_for_rel schema)) in
   let new_ready = (insert_if_new ready handlers) in
   let new_todo = (ListAsSet.diff
      (let f (_, _, x) = (extract_maps x) in
       List.flatten (List.map f handlers))
      (List.map (function (_, x, _) -> x) new_ready)) in
   (new_ready, (List.tl todo) @ new_todo);;














(****************************************************************************
                                TESTING 
****************************************************************************)



let q =  SumAgg((Multiplication (Column "A", Column "D")),
                (NaturalJoin [NamedRelation ("R", ["A"; "B"]);
                              NamedRelation ("S", ["B"; "C"]);
                              NamedRelation ("T", ["C"; "D"])]), []);;

let q_code = make_code(simplify q);;

print_mp2 q_code;;

let my_schema = ["R"; "S"; "T"];;

let phase1 = mr ([], [q_code]) my_schema;;
let phase2 = mr phase1 my_schema;;
let phase3 = mr phase2 my_schema;;
let phase4 = mr phase3 my_schema;;

List.iter (ccode2) ((function (x, _) -> x) phase4);;






