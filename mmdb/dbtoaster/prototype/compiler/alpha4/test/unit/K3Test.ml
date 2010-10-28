open M3
open Util
open M3Compiler
open K3
open K3.SR;;

let test_typechecker s = Debug.log_unit_test s type_as_string;;

(* Tuples *)
let tup = List.map (fun c -> Const(CFloat(c))) [1.0;2.0;3.0] in
test_typechecker "project(T(1.0,2.0,3.0),[1,2]) : T(F,F)"
    (typecheck_expr (Project(Tuple(tup),[1;2]))) (TTuple([Float;Float]));


test_typechecker "project(T(T(10.0,20.0),1.0,2.0,3.0),[0,1]) : T(T(F,F),F)"
    (typecheck_expr
    (Project(
	    Tuple([Tuple([Const(CFloat(10.0));Const(CFloat(20.0))])]@tup),
        [0;1])))
    (TTuple([TTuple([Float;Float]);Float]))


(* Native collections *)

(* External collections *)

(* Operators (arithmetic, comparison) *)

(* Map operation *)
let fn1 = Lambda("a",Float,Lambda("b",Float,Lambda("v",Float,
    Tuple([Var("b",Float);Add(Var("v",Float),Const(CFloat(3.0)))]),false),false),false)
;;

let coll = OutPC("m",["a",Float;"b",Float],Float)
in
test_typechecker "map app: Map((fun a b v -> (b,v+3)), m[][a,b]) : C(T(F,F))"
    (typecheck_expr (Map(fn1,coll)))
    (Collection(TTuple([Float;Float])));;


let coll = OutPC("m",["b",Float],Float)
in
test_typechecker "map currying: Map((fun a b v -> (b,v+3)), m[b]) : (F -> C(T(F,F)))"
    (typecheck_expr (Map(fn1,coll)))
    (Fn(Float,Collection(TTuple([Float;Float]))));;

let fn2 = Lambda("a",Float,Lambda("b",TTuple([Float;Float]),Lambda("v",Float,
    Tuple([Var("b",TTuple([Float;Float]));
           Add(Var("v",Float),Const(CFloat(3.0)))]),false),false),false)
;;

let coll = OutPC("m",["b",TTuple([Float;Float])],Float)
in test_typechecker
    "suffix currying: Map((fun a b v -> (b,v+3)), m[b]) : (F -> C(T(T(F,F),F)))"
    (typecheck_expr (Map(fn2,coll)))
    (Fn(Float,Collection(TTuple([TTuple([Float;Float]);Float]))));;


(* Flatten operation *)
let nested = collection_of_list
    (List.map collection_of_float_list [[1.0; 2.0]; [3.0; 4.0; 5.0]])
in
test_typechecker "flatten(C({1.0,2.0}; {3.0,4.0,5.0})) : C(Float)"
    (typecheck_expr (Flatten(nested))) (Collection(Float));;

let nested = Lambda("a",Float, collection_of_list
    (List.map collection_of_float_list [[1.0; 2.0]; [3.0; 4.0; 5.0]]),false)
in
test_typechecker
    ("flatten(fun a -> C({1.0,2.0},{3.0,4.0,5.0})) : fun a -> C(F)")
    (typecheck_expr (Flatten(nested))) (Fn(Float,Collection(Float)))

(* Aggregate operation *)


(* These examples should fail *)

(* Invalid projection index *)
(*
let tup = List.map (fun c -> Const(CFloat(c))) [1.0;2.0;3.0] in
test_typechecker "project(T(1.0,2.0,3.0),[1,2]) : T(F,F)"
    (typecheck_expr (Project(Tuple(tup),[1;4]))) (TTuple([Float;Float]))
*)

(* Inner functions may be different functions, even if their type is the same.
 * Thus we should not be able to lift out a common function *)
(*
let nested = Lambda("a",Float,
    collection_of_list (List.map
        (fun l -> (Lambda("b",TTuple([Float;Float]),collection_of_float_list l)))
        [[1.0; 2.0]; [3.0; 4.0; 5.0]]))
in
test_typechecker
    ("flatten(fun a -> C(fun b ->{1.0,2.0}, fun b ->{3.0,4.0,5.0})) "^
        ": fun a b -> C(F)")
    (typecheck_expr (Flatten(nested)))
    (Fn(Float,Fn(TTuple([Float;Float]),Collection(Float))))
*) 
    

(**************************
 *  Construction from M3
 **************************)
let uma n i o = (n,i,o, (mk_c 0.0, ()))
let pma n i o = mk_ma (n,i,o, (mk_c 0.0,()))
;;

(* select sum(a) from R *)
let sumr_schema = [ "q",[],[] ];;

let sumr_trigs =
    [(Insert, "R", ["a";"b"],
      [ (uma "q" [] [], (mk_v "a", ()), ()) ]);
     (Delete, "R", ["a"; "b"],
      [ (uma "q" [] [], ((mk_prod (mk_v "a") (mk_c (-1.0))), ()), ()) ])]
in
let sumr_prog = prepare_triggers sumr_trigs
in collection_prog (sumr_schema, fst sumr_prog) []
;;

(* select b,sum(a) from R group by b *)
let sumr_byb_schema = [ "q",[],[VT_Int] ];;
let sumr_byb_patterns = [ "q", [M3Common.Patterns.Out([], [])] ];;

let sumr_byb_trigs =
    [(Insert, "R", ["a";"b"],
      [ (uma "q" [] ["b"], (mk_v "a", ()), ()) ]);
     (Delete, "R", ["a"; "b"],
      [ (uma "q" [] ["b"], ((mk_prod (mk_v "a") (mk_c (-1.0))), ()), ()) ])]
in
let sumr_byb_prog = prepare_triggers sumr_byb_trigs
in collection_prog (sumr_byb_schema, fst sumr_byb_prog) sumr_byb_patterns
;;


(**************************
 * K3 Interpreter tests
 **************************)
open Database
open Values
open K3Interpreter.MK3CG;;

module Env = M3Valuation;;
module DB  = NamedM3Database;;

let th = Env.make [] [], [];;
let db = DB.make_empty_db sumr_schema [];;
let db2 = DB.make_empty_db sumr_byb_schema sumr_byb_patterns;;

let test_interpreter s = Debug.log_unit_test s string_of_value;;

let addf = op add_op (const (CFloat(1.0))) (const (CFloat(1.0))) in
test_interpreter "1.0 + 1.0"
  (eval addf [] [] db) (Float(2.0));;

let addf = op add_op (const (CFloat(1.0))) (var "x") in
test_interpreter "1.0 + x where x = 2.0"
  (eval addf ["x"] [CFloat(2.0)] db) (Float(3.0));;

let ltf = op lt_op (const (CFloat(1.0))) (var "x") in
test_interpreter "1.0 < x where x = 2.0"
  (eval ltf ["x"] [CFloat(2.0)] db) (Int(1));;

let ltf = op lt_op (const (CFloat(1.0))) (var "x") in
test_interpreter "1.0 < x where x = 0.0"
  (eval ltf ["x"] [CFloat(0.0)] db) (Int(0));;

(* Tuples *)
let ftuple l = List.map (fun x -> const(CFloat(x))) l;;

(* tuple 1.0, 2.0, 3.0 *)
let t = tuple (ftuple [1.0; 2.0; 3.0]) in
test_interpreter "tuple 1.0, 2.0, 3.0"
  (eval t [] [] db) (Tuple [1.0;2.0;3.0]);;

let pop_back l =
  let x,y = List.fold_left
    (fun (acc,rem) v -> match rem with 
	 | [] -> (acc, [v]) | _ -> (acc@[v], List.tl rem)) 
    ([], List.tl l) l
  in x, List.hd y;;

let string_of_kv (k,v) = 
  String.concat ";" (List.map string_of_float (k@[v]));;

Debug.log_unit_test "pop_back" string_of_kv
  (pop_back [1.0;2.0;3.0]) ([1.0;2.0], 3.0);;


(* Function tests *)
(* ((lambda x. lambda y. x+y) 1) 2 *)
let fxy = lambda "x" false
  (lambda "y" false (op add_op (var "x") (var "y"))) in
let app_fxy =
  (apply (apply fxy (const (CFloat(1.0)))) (const (CFloat(2.0))))
in test_interpreter "((lambda x. lambda y. x+y) 1) 2"
  (eval app_fxy [] [] db) (Float(3.0));;

(* Schema application tests *)
let build_lambda e vl = List.fold_left (fun acc v -> lambda v true acc) e (List.rev vl);;

let tuple_xyza = tuple [var "x"; var "y"; var "z"; var "a"] in
let fxyz = build_lambda (lambda "a" false tuple_xyza) ["x"; "y"; "z"] in
let schema_app = apply fxyz (tuple (ftuple [1.0; 2.0; 3.0; 4.0]))
in test_interpreter "(lambda x,y,z,a. tuple x,y,z,a) (tuple 1,2,3,4)"
    (eval schema_app [] [] db) (Tuple [1.0; 2.0; 3.0; 4.0]);;

(* TODO: associative lambda tests *)


(*************************
 * Collection tests
 *************************)

let build_val_list l = List.fold_left (fun acc e ->
    combine acc (singleton (const (CFloat(e)))))
  (singleton (const (CFloat(List.hd l)))) (List.tl l);;

let build_tuple_list l = List.fold_left (fun acc t ->
    combine acc (singleton (tuple (ftuple t))))
  (singleton (tuple (ftuple (List.hd l)))) (List.tl l);;

(* singleton 1.0 *)
let s = singleton (const (CFloat(1.0))) in
test_interpreter "singleton 1.0" (eval s [] [] db) (List [1.0]);;

(* singleton (tuple 1.0, 2.0, 3.0) *)
let t = tuple (ftuple [1.0; 2.0; 3.0]) in
test_interpreter "singleton (tuple 1.0, 2.0, 3.0)"
  (eval (singleton t) [] [] db) (TupleList [[1.0;2.0], 3.0]);;

(* (lambda y. map(lambda x. x+y, [1.0; 2.0])) 5.0 *)
let fx = lambda "x" false (op add_op (var "x") (var "y")) in
let cx = build_val_list [1.0; 2.0] in
let mapx = lambda "y" false (map fx cx) in
let app_fmx = apply mapx (const (CFloat(5.0))) in
test_interpreter "(lambda y. map(lambda x. x+y, [1.0; 2.0])) 5.0"
  (eval app_fmx [] [] db) (List [6.0; 7.0]);;

(* aggregate(lambda x.lambda acc. x + acc, 0.0, [1.0; 2.0. 3.0]) *)
let aggf = lambda "x" false (lambda "acc" false (op add_op (var "x") (var "acc"))) in
let aggc = aggregate aggf (const(CFloat 0.0)) (build_val_list [1.0;2.0;3.0]) in
test_interpreter "aggregate(lambda x.lambda acc. x + acc, 0.0, [1.0; 2.0. 3.0])"
    (eval aggc [] [] db) (Float(6.0));;

(* map(lambda x,y,z. tuple (x,y+z), [1.0,2.0,3.0; 4.0,5.0,6.0; 7.0,8.0,9.0;]) *)
let fxyz = build_lambda
  (tuple [var "x"; op add_op (var "y") (var "z")]) ["x"; "y"; "z"] in
let c = build_tuple_list [[1.0;2.0;3.0]; [4.0;5.0;6.0]; [7.0;8.0;9.0]] in
test_interpreter
  "map(lambda x,y,z. tuple (x,y+z), [1.0,2.0,3.0; 4.0,5.0,6.0; 7.0,8.0,9.0;])"
  (eval (map fxyz c) [] [] db) (TupleList[[1.0],5.0; [4.0],11.0; [7.0],17.0]);;

(* TODO: group-by aggregate tests *)

(* TODO: nested map tests -- needs arbitrarily nested collections, not just List/TupleList *)


(*************************
 * Database tests
 *************************)

(* Value reads *)
let get_q_value = get_value "q";;
test_interpreter "get_value q" (eval get_q_value [] [] db) (Float(0.0));;

(* Value writes *)
let wc = update_value "q" (const (CFloat(1.0))) in
test_interpreter "update_value q 1.0" (eval wc [] [] db) (Unit);;

test_interpreter "(get_value q) = 1.0"
  (eval get_q_value [] [] db) (Float(1.0));;

(* Out map writes*)
let wc = update_out_map_value "q" [const(CFloat(4.0))] (const (CFloat(1.0))) in
test_interpreter "update_out_map_value q [4.0] 1.0" (eval wc [] [] db2) (Unit);;

let lookup_q = lookup (get_out_map "q") [const (CFloat(4.0))] in
test_interpreter "(lookup (get_out_map q) [4.0]) = 1.0"
  (eval lookup_q [] [] db2) (Float(1.0));;

(* Slices *)
let kv = [1.0,2.0; 10.0,5.0; 3.0,7.0] in
let updates = List.map
  (fun (k,v) -> update_out_map_value "q" [const(CFloat(k))] (const (CFloat(v)))) kv
in
test_interpreter "update_out_map_value q [1.0;2.0; 10.0,5.0; 3.0,7.0]"
  (List.iter (fun wc -> ignore(eval wc [] [] db2)) updates; Unit) (Unit);; 

let slice_q = slice (get_out_map "q") [] [] in
test_interpreter "slice (get_out_map q) [] []"
  (eval slice_q [] [] db2)
  (TupleList [[4.0], 1.0; [10.0], 5.0; [3.0], 7.0; [1.0], 2.0]);;

(* TODO: double map tests *)

(* Control flow tests
 * -- conditional, based on map membership, perform updates
 * -- sequence
 * -- iterate, updating a database *)


(******************
 * Compiler tests
 ******************)