open M3
open Util
open M3Compiler
open K3.SR
open K3Builder
open K3Typechecker
open K3Optimizer;;

(* K3 Helpers *)
let test_string_of_expr s e =
    Debug.log_unit_test ("string_of_expr: "^s) (fun x -> x) e s;;

test_string_of_expr "Var(\"a\")" (string_of_expr (Var ("a", TFloat)));;

test_string_of_expr "Tuple(Var(\"a\"),Var(\"b\"))"
  (string_of_expr (Tuple[Var ("a", TFloat); Var ("b", TFloat)]));;

test_string_of_expr "Tuple(Tuple(Var(\"a\"),Var(\"b\")),Tuple(Var(\"c\"),Var(\"d\")))"
  (string_of_expr (Tuple [Tuple[Var ("a", TFloat); Var ("b", TFloat)];
                          Tuple[Var ("c", TFloat); Var ("d", TFloat)]]));;


(* K3 Typechecker *)
let test_typechecker s = Debug.log_unit_test s string_of_type;;

(* Tuples *)
let tup = List.map (fun c -> Const(CFloat(c))) [1.0;2.0;3.0] in
test_typechecker "project(T(1.0,2.0,3.0),[1,2]) : T(F,F)"
    (typecheck_expr (Project(Tuple(tup),[1;2]))) (TTuple([TFloat;TFloat]));


test_typechecker "project(T(T(10.0,20.0),1.0,2.0,3.0),[0,1]) : T(T(F,F),F)"
    (typecheck_expr
    (Project(
	    Tuple([Tuple([Const(CFloat(10.0));Const(CFloat(20.0))])]@tup),
        [0;1])))
    (TTuple([TTuple([TFloat;TFloat]);TFloat]));;


(* Native collections *)

(* External collections *)

(* Operators (arithmetic, comparison) *)

(* Map operation *)
let fn1 = Lambda(ATuple(["a",TFloat;"b",TFloat;"v",TFloat]),
    Tuple([Var("b",TFloat);Add(Var("v",TFloat),Const(CFloat(3.0)))]))
in let coll = OutPC("m",["a",TFloat;"b",TFloat],TFloat)
in test_typechecker
  "map app: Map((fun a b v -> (b,v+3)), m[][a,b]) : C(T(F,F))"
  (typecheck_expr (Map(fn1,coll)))
  (Collection(TTuple([TFloat;TFloat])))
;;

let fn1 = Lambda(AVar("a",TFloat),Lambda(ATuple(["b",TFloat;"v",TFloat]),
    Tuple([Var("b",TFloat);Add(Var("v",TFloat),Const(CFloat(3.0)))])))
in let coll = OutPC("m",["b",TFloat],TFloat)
in test_typechecker ("map suffix currying: "^
  "Map((fun a -> (fun b v -> (b,v+3))), m[b]) : (F -> C(T(F,F)))")
  (typecheck_expr (Map(fn1,coll)))
  (Fn([TFloat],Collection(TTuple([TFloat;TFloat]))))
;;

let fn2 = Lambda(AVar("a",TFloat),
            Lambda(ATuple(["b",TTuple([TFloat;TFloat]);"v",TFloat]),
              Tuple([Var("b",TTuple([TFloat;TFloat]));
                     Add(Var("v",TFloat),Const(CFloat(3.0)))])))
;;

let coll = OutPC("m",["b",TTuple([TFloat;TFloat])],TFloat)
in test_typechecker ("map tuple suffix currying: "^
    "Map(fun a -> (fun a b v -> (b,v+3)), m[b]) : (F -> C(T(T(F,F),F)))")
    (typecheck_expr (Map(fn2,coll)))
    (Fn([TFloat],Collection(TTuple([TTuple([TFloat;TFloat]);TFloat]))))
;;


(* Flatten operation *)
let nested = collection_of_list
    (List.map collection_of_float_list [[1.0; 2.0]; [3.0; 4.0; 5.0]])
in
test_typechecker "flatten(C({1.0,2.0}; {3.0,4.0,5.0})) : C(Float)"
    (typecheck_expr (Flatten(nested))) (Collection(TFloat));;

let nested = Lambda(AVar("a",TFloat), collection_of_list
    (List.map collection_of_float_list [[1.0; 2.0]; [3.0; 4.0; 5.0]]))
in
test_typechecker
    ("flatten(fun a -> C({1.0,2.0},{3.0,4.0,5.0})) : fun a -> C(F)")
    (typecheck_expr (Flatten(nested))) (Fn([TFloat],Collection(TFloat)))

(* Aggregate operation *)


(* These examples should fail
 * TODO: revise with new typechecking *)

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
    [ (uma "q" [] [], ((mk_prod (mk_v "a") (mk_c (-1.0))), ()), ()) ])];;
let sumr_prog = prepare_triggers sumr_trigs
in collection_prog (sumr_schema, fst sumr_prog) [];;

(* select b,sum(a) from R group by b *)
let sumr_byb_schema = [ "q",[],[VT_Int] ];;
let sumr_byb_patterns = [ "q", [M3Common.Patterns.Out([], [])] ];;

let sumr_byb_trigs =
    [(Insert, "R", ["a";"b"],
      [ (uma "q" [] ["b"], (mk_v "a", ()), ()) ]);
     (Delete, "R", ["a"; "b"],
      [ (uma "q" [] ["b"], ((mk_prod (mk_v "a") (mk_c (-1.0))), ()), ()) ])];;

let sumr_byb_prog = prepare_triggers sumr_byb_trigs
in collection_prog (sumr_byb_schema, fst sumr_byb_prog) sumr_byb_patterns;;


(**************************
 * Optimizer tests
 **************************)

let test_optimizer s = Debug.log_unit_test s string_of_expr;;

(* Expression construction helpers *)
let pop_back l =
    let r = List.rev l in List.rev (List.tl r), List.hd r

let build_schema_tuple sch = List.map (fun (id,t) -> Var(id,t)) sch;;

let add_n n sch =
    let x,y = pop_back sch in
    let m_tup = build_schema_tuple x in
    Lambda(ATuple(sch),
      Tuple(m_tup@[Add(Var(fst y,snd y), Const(CFloat(n)))]));; 

let add_v vl sch =
    let x,y = pop_back sch in
    let m_tup = build_schema_tuple x in
    let add_expr = List.fold_left (fun acc v -> Add(acc, Var(v,TFloat)))
      (Var(List.hd vl, TFloat)) (List.tl vl)
    in Lambda(ATuple(sch), Tuple(m_tup@[add_expr]));; 

let build_chain le (re,rsch) = Map(Lambda(ATuple(rsch), le), re);;

let build_binary_join e_sch_l =
    List.fold_left build_chain (fst (List.hd e_sch_l)) (List.tl e_sch_l);;

let build_cross le (re,rsch) =
    let rf, rc = get_map_parts re in
    Map(compose (Lambda(ATuple(rsch), le)) rf, rc);;

let build_nway_cross e_sch_l =
    List.fold_left build_cross (fst (List.hd e_sch_l)) (List.tl e_sch_l);;

(***************************
 * Collection var inlining
 ***************************)

let e = 
  Apply
   (Lambda
     (AVar ("slice",
       Collection (TTuple [TFloat; TFloat; TFloat])),
     Slice
      (Var ("slice",
        Collection (TTuple [TFloat; TFloat; TFloat])),
      [("QUERY_1_1BIDS1_initASKS_A__BROKER_ID", TFloat);
       ("A__P", TFloat)],
      [("QUERY_1_1BIDS1_initASKS_A__BROKER_ID",
        Var ("QUERY_1_1BIDS1_initASKS_A__BROKER_ID",
         TFloat))])),
   OutPC ("QUERY_1_1BIDS2_init",
    [("QUERY_1_1BIDS1_initASKS_A__BROKER_ID", TFloat);
     ("A__P", TFloat)], TFloat))
in
let r_e =
  Slice
   (OutPC ("QUERY_1_1BIDS2_init",
     [("QUERY_1_1BIDS1_initASKS_A__BROKER_ID", TFloat);
      ("A__P", TFloat)], TFloat),
   [("QUERY_1_1BIDS1_initASKS_A__BROKER_ID", TFloat);
    ("A__P", TFloat)],
   [("QUERY_1_1BIDS1_initASKS_A__BROKER_ID",
     Var ("QUERY_1_1BIDS1_initASKS_A__BROKER_ID", TFloat))])
in test_optimizer "inline_collection_functions 1"
     (inline_collection_functions [] e) r_e;;

let e =
Apply
  (Lambda (AVar ("slice", Collection (TTuple [TFloat; TFloat])),
    IfThenElse
     (Member
       (Var ("slice", Collection (TTuple [TFloat; TFloat])),
       [Var ("QUERY_1_1BIDS1_initASKS_A__BROKER_ID",
         TFloat)]),
     Lookup
      (Var ("slice", Collection (TTuple [TFloat; TFloat])),
      [Var ("QUERY_1_1BIDS1_initASKS_A__BROKER_ID", TFloat)]),
     Apply
      (Lambda (AVar ("init_val", TFloat),
        Block
         [PCValueUpdate
           (PC ("QUERY_1_1BIDS4",
             [("QUERY_1_1BIDS1_initASKS_A__P", TFloat)],
             [("QUERY_1_1BIDS1_initASKS_A__BROKER_ID", TFloat)],
             TFloat),
           [Var ("QUERY_1_1BIDS1_initASKS_A__P", TFloat)],
           [Var ("QUERY_1_1BIDS1_initASKS_A__BROKER_ID",
             TFloat)],
           Var ("init_val", TFloat));
          Var ("init_val", TFloat)]),
      Aggregate
       (AssocLambda
         (ATuple
           [("QUERY_1_1BIDS1_initASKS_A__BROKER_ID", TFloat);
            ("A__P", TFloat); ("v", TFloat)],
         AVar ("accv", TFloat),
         Add (Var ("v", TFloat),
          Var ("accv", TFloat))),
       Const (M3.CFloat 0.),
       Map
        (Lambda
          (ATuple
            [("QUERY_1_1BIDS1_initASKS_A__BROKER_ID", TFloat);
             ("A__P", TFloat); ("v1", TFloat)],
          Tuple
           [Var ("QUERY_1_1BIDS1_initASKS_A__BROKER_ID",
             TFloat);
            Var ("A__P", TFloat);
            IfThenElse0
             (Lt (Const (M3.CFloat 1000.),
               Add
                (Var ("QUERY_1_1BIDS1_initASKS_A__P",
                  TFloat),
                Mult (Const (M3.CFloat (-1.)),
                 Var ("A__P", TFloat)))),
             Var ("v1", TFloat))]),
        Apply
         (Lambda
           (AVar ("slice",
             Collection (TTuple [TFloat; TFloat; TFloat])),
           Slice
            (Var ("slice",
              Collection (TTuple [TFloat; TFloat; TFloat])),
            [("QUERY_1_1BIDS1_initASKS_A__BROKER_ID", TFloat);
             ("A__P", TFloat)],
            [("QUERY_1_1BIDS1_initASKS_A__BROKER_ID",
              Var ("QUERY_1_1BIDS1_initASKS_A__BROKER_ID",
               TFloat))])),
         OutPC ("QUERY_1_1BIDS2_init",
          [("QUERY_1_1BIDS1_initASKS_A__BROKER_ID", TFloat);
           ("A__P", TFloat)],
          TFloat))))))),
  Lookup
   (Var ("m",
     Collection
      (TTuple [TFloat; Collection (TTuple [TFloat; TFloat])])),
   [Var ("QUERY_1_1BIDS1_initASKS_A__P", TFloat)]))
in
let r_e =
IfThenElse
 (Member
   (Lookup
     (Var ("m",
       Collection(TTuple [TFloat; Collection (TTuple [TFloat; TFloat])])),
     [Var ("QUERY_1_1BIDS1_initASKS_A__P", TFloat)]),
   [Var ("QUERY_1_1BIDS1_initASKS_A__BROKER_ID", TFloat)]),
 Lookup
   (Lookup
     (Var ("m", Collection
       (TTuple [TFloat; Collection (TTuple [TFloat; TFloat])])),
     [Var ("QUERY_1_1BIDS1_initASKS_A__P", TFloat)]),
  [Var ("QUERY_1_1BIDS1_initASKS_A__BROKER_ID", TFloat)]),
 Apply
  (Lambda (AVar ("init_val", TFloat),
    Block
     [PCValueUpdate
       (PC ("QUERY_1_1BIDS4",
         [("QUERY_1_1BIDS1_initASKS_A__P", TFloat)],
         [("QUERY_1_1BIDS1_initASKS_A__BROKER_ID", TFloat)],
         TFloat),
       [Var ("QUERY_1_1BIDS1_initASKS_A__P", TFloat)],
       [Var ("QUERY_1_1BIDS1_initASKS_A__BROKER_ID",
         TFloat)],
       Var ("init_val", TFloat));
      Var ("init_val", TFloat)]),
  Aggregate
   (AssocLambda
     (ATuple
       [("QUERY_1_1BIDS1_initASKS_A__BROKER_ID", TFloat);
        ("A__P", TFloat); ("v", TFloat)],
     AVar ("accv", TFloat),
     Add (Var ("v", TFloat),
      Var ("accv", TFloat))),
   Const (M3.CFloat 0.),
   Map
    (Lambda
      (ATuple
        [("QUERY_1_1BIDS1_initASKS_A__BROKER_ID", TFloat);
         ("A__P", TFloat); ("v1", TFloat)],
      Tuple
       [Var ("QUERY_1_1BIDS1_initASKS_A__BROKER_ID",
         TFloat);
        Var ("A__P", TFloat);
        IfThenElse0
         (Lt (Const (M3.CFloat 1000.),
           Add
            (Var ("QUERY_1_1BIDS1_initASKS_A__P",
              TFloat),
            Mult (Const (M3.CFloat (-1.)),
             Var ("A__P", TFloat)))),
         Var ("v1", TFloat))]),
     Slice
       (OutPC ("QUERY_1_1BIDS2_init",
         [("QUERY_1_1BIDS1_initASKS_A__BROKER_ID", TFloat);
           ("A__P", TFloat)], TFloat),
         [("QUERY_1_1BIDS1_initASKS_A__BROKER_ID", TFloat);
          ("A__P", TFloat)],
         [("QUERY_1_1BIDS1_initASKS_A__BROKER_ID",
           Var ("QUERY_1_1BIDS1_initASKS_A__BROKER_ID", TFloat))])))))
in test_optimizer "inline_collection_functions 2"
     (inline_collection_functions [] e) r_e

(************************
 * Lifting ifs
 ************************)
let zero = Const (CFloat 0.0);;
let one = Const (CFloat 1.0);;
let n f = Const (CFloat f);;
let c_01_if = IfThenElse(Leq(zero, one), zero, one);;
let c_23_if = IfThenElse(Leq(n 2.0, n 3.0), n 2.0, n 3.0);;
 
test_optimizer "lift-ifs 1.0" (lift_ifs [] zero) zero;;

test_optimizer "lift-ifs (if 0 < 1 then 0 else 1)" (lift_ifs [] c_01_if) c_01_if;;

let if_tuple = Tuple([c_01_if]) in
let r = IfThenElse(Leq(zero,one), Tuple([zero]), Tuple([one])) in
test_optimizer "lift-ifs Tuple(01-if)" (lift_ifs [] if_tuple) r;;

let if_tuple = Tuple([c_01_if; c_23_if]) in
let inner v =
    IfThenElse(Leq(n 2.0, n 3.0), Tuple([v;n 2.0]), Tuple([v; n 3.0])) in
let r = IfThenElse(Leq(zero,one), inner zero, inner one) in
test_optimizer "lift-ifs Tuple(01-if, 23-if)" (lift_ifs [] if_tuple) r;;


let lambda_if = Lambda(AVar("x",TFloat), c_01_if) in
let r = IfThenElse(Leq(zero,one),
    Lambda(AVar("x", TFloat), zero), Lambda(AVar("x", TFloat), one))
in
test_optimizer "lift-ifs independent: lambda x.c_01_if"
  (lift_ifs [] lambda_if) r;;

let lambda_if = Lambda(AVar("x",TFloat),
    IfThenElse(Leq(Var("x",TFloat),n 100.0), Var("x",TFloat), one)) in
test_optimizer "lift-ifs dependent: lambda x. x<100 ? x:1"
  (lift_ifs [] lambda_if) lambda_if;;


(***********************
 * SR optimizations
 ***********************)

(* Example expressions *)
let sch1 = ["a", TFloat; "b", TFloat];;
let m1_sch = sch1@["v1",TFloat];;

let sch2 = ["c", TFloat; "d", TFloat];;
let m2_sch = sch2@["v2",TFloat];;

let sch3 = ["e", TFloat; "f", TFloat];;
let m3_sch = sch3@["v3",TFloat];;

let c1 = OutPC("m1", sch1, TFloat);;
let c2 = OutPC("m2", sch2, TFloat);;
let c3 = OutPC("m3", sch3, TFloat);;

(* Test 1
 * map(f=lambda x.x+2,map(g=lambda y.y+1,c)) => map(lambda y.(y+1)+2, c)
 *)
let map_2_chain =
  Map(Lambda(ATuple(["x",TFloat; "y", TFloat; "v1", TFloat]),
             Add(Var("v1",TFloat), Const(CFloat(2.0)))),
    Map(add_n 1.0 m1_sch, OutPC("m1", sch1, TFloat)));;

let composed_map =
  Map(Lambda(ATuple(m1_sch),
             Add(Add(Var("v1", TFloat), Const(CFloat(1.0))),
                 Const(CFloat(2.0)))),
      OutPC("m1", sch1, TFloat))
in
test_optimizer "simplify map chain"
  (simplify_collections map_2_chain) composed_map;; 


(* Test 2
 * map(map(f,c1), map(g,c2)) => map(map(f \circ g, c2), c1)
 *)
let add_to_m1 = add_n 2.0 m1_sch in
let cross_mult =
    Lambda(ATuple(m2_sch), Mult(Var("v1", TFloat), Var("d", TFloat)))
in
let map_cross = build_chain
    (Map(cross_mult, c2)) ((Map(add_to_m1, c1)), m1_sch)
in
let composed_cross_mult = 
    Lambda(ATuple(m2_sch),
      Mult(Add(Var("v1", TFloat), Const(CFloat(2.0))), Var("d",TFloat)))
in
let simple_map_cross =
    build_chain (Map(composed_cross_mult, c2)) (c1, m1_sch)
in
test_optimizer "simplify 2-way cross product"
  (simplify_collections map_cross) simple_map_cross;;


(* Test 3
 * map(map(map(f,c1), map(g,c2)), map(h,c3))
 *  => map(map(map(f \circ g \circ h, c1), c2), c3)
 *)
let scans = [
    (Map(add_v ["v1"; "v2"; "v3"] (sch3@sch2@m1_sch), c1), m1_sch);
    (Map(add_n 2.0 m2_sch, c2), m2_sch);
    (Map(add_n 3.0 m3_sch, c3), m3_sch)];;

let join3 = build_binary_join scans in 
let cross3 = build_nway_cross scans in
test_optimizer "simplify 3-way join = 3-way cross product"
  (simplify_collections join3) cross3;;


(* TODO: aggregation simplification tests *)


(**************************
 * K3 Interpreter tests
 **************************)
open Database
open Values
open Values.K3Value
open K3Interpreter.K3CG;;

module Env = K3Valuation;;
module DB  = NamedK3Database;;

let th = Env.make [] [], [];;
let db = DB.make_empty_db sumr_schema [];;
let db2 = DB.make_empty_db sumr_byb_schema sumr_byb_patterns;;

let test_interpreter s = Debug.log_unit_test s Values.K3Value.string_of_value;;

(* Value constructor helpers *)
let ftuple l = List.map (fun x -> const(CFloat(x))) l;;
let vfloat x = Float x;;
let vtuple l = Tuple(List.map vfloat l);;
let vlist l = FloatList(List.map vfloat l);;
let tlist l = TupleList(List.map vtuple l);;
let cross xyz =
  let zyx = List.rev xyz in
  List.fold_left (fun acc v ->
    ListCollection(List.map (fun x -> Tuple([x;acc])) (List.map vtuple v)))
    (tlist (List.hd zyx)) (List.tl zyx);;

(* Binop tests *)
let addf = op add_op (const (CFloat(1.0))) (const (CFloat(1.0))) in
test_interpreter "1.0 + 1.0"
  (eval addf [] [] db) (Float(2.0));;

let addf = op add_op (const (CFloat(1.0))) (var "x" TFloat) in
test_interpreter "1.0 + x where x = 2.0"
  (eval addf ["x"] [CFloat(2.0)] db) (Float(3.0));;

let ltf = op lt_op (const (CFloat(1.0))) (var "x" TFloat) in
test_interpreter "1.0 < x where x = 2.0"
  (eval ltf ["x"] [CFloat(2.0)] db) (Int(1));;

let ltf = op lt_op (const (CFloat(1.0))) (var "x" TFloat) in
test_interpreter "1.0 < x where x = 0.0"
  (eval ltf ["x"] [CFloat(0.0)] db) (Int(0));;


(* tuple 1.0, 2.0, 3.0 *)
let t = tuple (ftuple [1.0; 2.0; 3.0]) in
test_interpreter "tuple 1.0, 2.0, 3.0"
  (eval t [] [] db) (vtuple [1.0;2.0;3.0]);;


(* Function tests *)
(* ((lambda x. lambda y. x+y) 1) 2 *)
let fxy = lambda (AVar("x",TFloat))
    (lambda (AVar("y",TFloat)) (op add_op (var "x" TFloat) (var "y" TFloat))) in
let app_fxy =
  (apply (apply fxy (const (CFloat(1.0)))) (const (CFloat(2.0))))
in test_interpreter "((lambda x. lambda y. x+y) 1) 2"
  (eval app_fxy [] [] db) (Float(3.0));;

(* Schema application tests *)
let build_lambda e vl =
    lambda (ATuple(List.map (fun v -> v,TFloat) vl)) e;;

let tuple_xyza = tuple (List.map (fun x -> var x TFloat) ["x";"y";"z";"a"]) in
let fxyz = build_lambda tuple_xyza ["x"; "y"; "z"; "a"] in
let schema_app = apply fxyz (tuple (ftuple [1.0; 2.0; 3.0; 4.0]))
in test_interpreter "(lambda x,y,z,a. tuple x,y,z,a) (tuple 1,2,3,4)"
    (eval schema_app [] [] db) (vtuple [1.0; 2.0; 3.0; 4.0]);;

(* TODO: associative lambda tests *)


(*************************
 * Collection tests
 *************************)

let build_val_list l = List.fold_left (fun acc e ->
    combine acc (singleton (const (CFloat(e))) TFloat))
  (singleton (const (CFloat(List.hd l))) TFloat) (List.tl l);;

let build_tuple_list l =
    let t_f _ = TFloat in
    let ht = TTuple(List.map t_f (List.hd l)) in
    let r =
      List.fold_left (fun acc t ->
        let tt = TTuple(List.map t_f t) in
        combine acc (singleton (tuple (ftuple t)) tt))
      (singleton (tuple (ftuple (List.hd l))) ht) (List.tl l)
    in r, Collection(ht);;

let crossc xyz =
  let t_f _ = TFloat in
  let zyx = List.rev xyz in
  List.fold_left (fun (acc,acc_t) v -> 
      let el_t = TTuple([TTuple(List.map t_f (List.hd v)); acc_t]) in
      let vt = List.map (fun l -> singleton (tuple [tuple (ftuple l);acc]) el_t) v in
      let r = List.fold_left combine (List.hd vt) (List.tl vt)
      in r,Collection(el_t))
    (build_tuple_list (List.hd zyx)) (List.tl zyx);;

(* singleton 1.0 *)
let s = singleton (const (CFloat(1.0))) TFloat in
test_interpreter "singleton 1.0" (eval s [] [] db) (vlist [1.0]);;

(* singleton (tuple 1.0, 2.0, 3.0) *)
let t = tuple (ftuple [1.0; 2.0; 3.0]) in
test_interpreter "singleton (tuple 1.0, 2.0, 3.0)"
  (eval (singleton t (TTuple([TFloat; TFloat; TFloat]))) [] [] db)
  (tlist [[1.0;2.0;3.0]]);;

(* (lambda y. map(lambda x. x+y, [1.0; 2.0])) 5.0 *)
let fx = lambda (AVar("x",TFloat)) (op add_op (var "x" TFloat) (var "y" TFloat)) in
let cx = build_val_list [1.0; 2.0] in
let mapx = lambda (AVar("y",TFloat)) (map fx TFloat cx) in
let app_fmx = apply mapx (const (CFloat(5.0))) in
test_interpreter "(lambda y. map(lambda x. x+y, [1.0; 2.0])) 5.0"
  (eval app_fmx [] [] db) (vlist [6.0; 7.0]);;

(* aggregate(lambda x.lambda acc. x + acc, 0.0, [1.0; 2.0. 3.0]) *)
let aggf = lambda (AVar("x",TFloat))
  (lambda (AVar("acc",TFloat)) (op add_op (var "x" TFloat) (var "acc" TFloat)))
in
let aggc = aggregate aggf (const(CFloat 0.0)) (build_val_list [1.0;2.0;3.0]) in
test_interpreter "aggregate(lambda x.lambda acc. x + acc, 0.0, [1.0; 2.0. 3.0])"
    (eval aggc [] [] db) (Float(6.0));;

(* map(lambda x,y,z. tuple (x,y+z), [1.0,2.0,3.0; 4.0,5.0,6.0; 7.0,8.0,9.0;]) *)
let fxyz = build_lambda
  (tuple [var "x" TFloat; op add_op (var "y" TFloat) (var "z" TFloat)])
  ["x"; "y"; "z"]
in
let c = fst (build_tuple_list [[1.0;2.0;3.0]; [4.0;5.0;6.0]; [7.0;8.0;9.0]]) in
test_interpreter
  "map(lambda x,y,z. tuple (x,y+z), [1.0,2.0,3.0; 4.0,5.0,6.0; 7.0,8.0,9.0;])"
  (eval (map fxyz (TTuple([TFloat;TFloat])) c) [] [] db) (tlist [[1.0;5.0]; [4.0;11.0]; [7.0;17.0]]);;

(* TODO: group-by aggregate tests *)

(* flatten(Coll [Coll TL(rx) ; Coll TL(ry) ; Coll TL(rz) ]) *)
let rx = [[1.0;2.0;3.0]; [4.0;5.0;6.0]; [7.0;8.0;9.0]];;
let ry = [[10.0;20.0;30.0]; [40.0;50.0;60.0]; [70.0;80.0;90.0]];;
let rz = [[100.0;200.0;300.0]; [400.0;500.0;600.0]; [700.0;800.0;900.0]];;
let xc,x_t = build_tuple_list rx;;
let yc,y_t = build_tuple_list ry;;
let zc,z_t = build_tuple_list rz;;


let xyzc =
  let ss = List.map (fun (c,t) ->
    singleton (singleton c t) (Collection(t))) [xc,x_t;yc,y_t;zc,z_t]
  in List.fold_left combine (List.hd ss) (List.tl ss);; 

let xyz = ListCollection(
    List.map (fun v -> ListCollection([tlist v])) [rx;ry;rz])
in test_interpreter "build nested collection" (eval xyzc [] [] db) (xyz);;

let flat_xyzc = flatten xyzc in
let flat_xyz = ListCollection(List.map (fun v -> tlist v) [rx;ry;rz])
in test_interpreter "flatten 3-way cross" (eval flat_xyzc [] [] db) (flat_xyz);;

(* TODO: pairwith *)
(*
let rels =     
     [( [[100.0;200.0]; [300.0;400.0]; [500.0;600.0]]); 
      ( [[10.0;20.0];[30.0;40.0];[50.0;60.0]]);
      ( [[1.0;2.0]; [3.0;4.0]; [5.0;6.0]])]
in
let xyz = cross rels in
xyz;;
*)

(*************************
 * Database tests
 *************************)

(* Value reads *)
let get_q_value = get_value TFloat "q";;
test_interpreter "get_value q" (eval get_q_value [] [] db) (Float(0.0));;

(* Value writes *)
let wc = update_value "q" (const (CFloat(1.0))) in
test_interpreter "update_value q 1.0" (eval wc [] [] db) (Unit);;

test_interpreter "(get_value q) = 1.0"
  (eval get_q_value [] [] db) (Float(1.0));;

(* Out map writes*)
let wc = update_out_map_value "q" [const(CFloat(4.0))] (const (CFloat(1.0))) in
test_interpreter "update_out_map_value q [4.0] 1.0" (eval wc [] [] db2) (Unit);;

let lookup_q = lookup
  (get_out_map ["x", TFloat] TFloat "q") [const (CFloat(4.0))] in
test_interpreter "(lookup (get_out_map q) [4.0]) = 1.0"
  (eval lookup_q [] [] db2) (Float(1.0));;

(* Slices *)
let kv = [1.0,2.0; 10.0,5.0; 3.0,7.0] in
let updates = List.map
  (fun (k,v) -> update_out_map_value "q" [const(CFloat(k))] (const (CFloat(v)))) kv
in
test_interpreter "update_out_map_value q [1.0;2.0; 10.0,5.0; 3.0,7.0]"
  (List.iter (fun wc -> ignore(eval wc [] [] db2)) updates; Unit) (Unit);; 

let slice_q = slice (get_out_map ["x", TFloat] TFloat "q") [] [] in
test_interpreter "slice (get_out_map q) [] []"
  (eval slice_q [] [] db2)
  (tlist [[4.0; 1.0]; [10.0; 5.0]; [3.0; 7.0]; [1.0; 2.0]]);;

(* insert trigger for sum (a) from R *)
let fcv = lambda (AVar("current_v",TFloat))
  (op add_op (var "current_v" TFloat) (var "a" TFloat)) in
let stmt = update_value "q" (apply fcv (get_value TFloat "q")) in
test_interpreter "update_value q ((lambda cv. cv + a) get_value q)"
  (eval stmt ["a"; "b"] [CFloat(1.0); CFloat(1.0)] db) (Unit);;

test_interpreter "(get_value q) = 2.0"
  (eval get_q_value [] [] db) (Float(2.0));;

(* delete trigger for sum (a) from R *)
let fcv = lambda (AVar("current_v",TFloat)) (op add_op (var "current_v" TFloat)
  (op mult_op (var "a" TFloat) (const (CFloat(-1.0)))))
in
let stmt = update_value "q" (apply fcv (get_value TFloat "q")) in
test_interpreter "update_value q ((lambda cv. cv + -1 * a) get_value q)"
  (eval stmt ["a"; "b"] [CFloat(1.0); CFloat(1.0)] db) (Unit);;

test_interpreter "(get_value q) = 1.0"
  (eval get_q_value [] [] db) (Float(1.0));;

(* TODO: double map tests *)

(* Control flow tests
 * -- conditional, based on map membership, perform updates
 * -- sequence
 * -- iterate, updating a database *)


(******************
 * Compiler tests
 ******************)
module K3C = K3Compiler.Make(K3Interpreter.K3CG);;

let test_prog (schema,trigs) tuples trigs_to_run tests =
  let prog_db = DB.make_empty_db schema [] in
  let ptrigs,patterns = M3Compiler.prepare_triggers trigs in
  let (_,_,trigs) = collection_prog (schema,ptrigs) patterns in
  let ctrigs = K3C.compile_triggers trigs in
  let inputs = List.map (fun l -> List.map (fun x -> CFloat(x)) l) tuples in
  List.iter (fun ((idx, trig_args),test) ->
      let trig_fn = List.nth ctrigs idx in
      List.iter (fun t -> ignore(eval trig_fn trig_args t prog_db)) inputs;
      test prog_db)
    (List.combine trigs_to_run tests);;

let tuples =
   [[1.0;4.0];
    [2.0;1.0];
    [1.0;2.0];
    [4.0;1.0];
    [3.0;3.0]];;

(* select sum(a) from R *)
test_prog (sumr_schema,sumr_trigs) tuples [0, ["a"; "b"]; 1, ["a"; "b"]]
[(fun db -> test_interpreter "insert; (get_value q) = 11.0"
              (eval (get_value TFloat "q") [] [] db) (Float(11.0)));
 (fun db -> test_interpreter "delete; (get_value q) = 0.0"
              (eval (get_value TFloat "q") [] [] db) (Float(0.0)))];;

(* select b,sum(a) from R group by b *)
test_prog (sumr_byb_schema, sumr_byb_trigs) tuples [0, ["a"; "b"]; 1, ["a"; "b"]]
[(fun db ->
    test_interpreter "insert; (get_out_map q) = [4,1; 3,3; 2,1; 1,6]"
      (eval (slice (get_out_map ["x", TFloat] TFloat "q") [] []) [] [] db)
      (tlist [[4.0; 1.0]; [3.0; 3.0]; [2.0; 1.0]; [1.0; 6.0]]));
 (fun db ->
    test_interpreter "delete; (get_out_map q) = [4,0; 3,0; 2,0; 1,0]"
      (eval (slice (get_out_map ["x", TFloat] TFloat "q") [] []) [] [] db)
      (tlist [[4.0; 0.0]; [3.0; 0.0]; [2.0; 0.0]; [1.0; 0.0]]))];;


(****************************
 * SQL file toplevel helpers
 ****************************)
let compile_sql_file_to_m3 fname =
  let compile_depth = None in
  let input_files = [fname] in
  let sql_file_to_calc f =
    let lexbuff = Lexing.from_channel (open_in f) in
    Sqlparser.dbtoasterSqlList Sqllexer.tokenize lexbuff
  in
  Debug.exec "PARSE" (fun () -> Parsing.set_trace true);
  let (queries, sources) = 
    let (ql, sl) = 
      List.split (List.map sql_file_to_calc input_files)
    in (List.flatten ql, List.flatten sl)
  in
  let toplevel_queries = ref [] in
  let calc_into_m3_inprogress qname (qlist,dbschema,qvars) m3ip = 
    fst (List.fold_left (fun (accum,sub_id) q ->
        let tm =
          Calculus.make_term q,
          Calculus.map_term (qname^"_"^(string_of_int sub_id)) qvars
        in
        let subq_name = (qname^"_"^(string_of_int sub_id)) in
        toplevel_queries := !toplevel_queries @ [subq_name];
        let r = CalcToM3.compile ~top_down_depth:compile_depth dbschema tm accum 
        in (r, sub_id+1))
      (m3ip,1) qlist)
  in
  let m3_prog = 
    let (m3_prog_in_prog,_) = 
      List.fold_left (fun (accum,id) q ->
        let qn = "QUERY_"^(string_of_int id) in
        (calc_into_m3_inprogress qn q accum, id + 1)) 
      (CalcToM3.M3InProgress.init, 1) queries
    in CalcToM3.M3InProgress.finalize m3_prog_in_prog
  in (m3_prog, sources)
;;

let compile_aux aux_f fname =
  let (schema, m3prog) = (fst (compile_sql_file_to_m3 fname)) in
  let m3ptrigs,patterns = M3Compiler.prepare_triggers m3prog in
  let (_,_,trigs) = collection_prog (schema,m3ptrigs) patterns in
  let ctrigs = 
    List.map (fun (_,_,trig_args,stmtl) ->
       List.map (fun (_,e) -> aux_f trig_args e)
         stmtl) trigs
  in (schema, ctrigs)
  (*
  m3ptrigs
  *)
;;

let compile_to_k3 = compile_aux (fun ta e -> ta,e);;

let compile_to_k3_opt = compile_aux
  (fun trig_args e ->
    let optimize e =
      simplify_collections 
        (lift_ifs trig_args (inline_collection_functions [] e)) 
    in
    let rec fixpoint e =
      let new_e = optimize e in
      if e = new_e then e else fixpoint new_e    
    in fixpoint e);;



module K3S = K3Compiler.Make(K3Plsql.CG);;
open K3Plsql.CG;;

let compile_to_k3sql fname =
  let (schema,ctrigs) = compile_to_k3_opt fname in  
  List.map (fun stmtl ->
      List.map (fun e -> (linearize_code (K3S.compile_k3_expr e))) stmtl)
    ctrigs;;

(*
compile_to_k3sql "test/sql/vwap.sql";;

compile_to_k3 "test/sql/finance/axfinder.sql";;
*)

