#use "RelAlg.ml";;


type code = CMultiplication of code * code
          | CAddition of code * code
          | CSumAgg of arith * relalg * string list
          | MapVar of string
          | CConstant of int
          ;;



(****************************************************************************
****************************************************************************)


let make_code relalg =
   match relalg with
      SumAgg(a,b,c) -> CSumAgg(a,b,c)
   | _ -> CConstant(1234)   (* must not happen *)
   ;;



(****************************************************************************
            REWRITE RULES FOR TURNING RELATIONAL ALGEBRA INTO CODE
****************************************************************************)


let cbigprod l =
   let f x y = CMultiplication(x,y) in
   if l = [] then CConstant(1)
   else List.fold_left (f) (List.hd l) (List.tl l);;

let cbigsum l =
   let f x y = CAddition(x,y) in
   if l = [] then CConstant(0)
   else List.fold_left (f) (List.hd l) (List.tl l);;

let abigprod l =
   let f x y = Multiplication(x,y) in
   if l = [] then Constant(1)
   else List.fold_left (f) (List.hd l) (List.tl l);;



(* decomposes a set (conceptually a natural join) of relational algebra
   expressions where additional constraint hyperedges (from arithmetic
   expressions) can be taken into account.
   Before call connected_components, the relational algebra expressions
   are mapped to hyperedges; Afterwards, the components are again matched
   with the relational algebra expressions to return those.
*)
let connected_components2 (l: relalg list)
                          additional_constraints
                          (guards: 'column list) =
   let hypergraph = (List.map schema l) @ additional_constraints in
   let algebra_expressions_for component =
      let belongs_to_component alg_expr =
         let matches hyperedge = (hyperedge = (schema alg_expr)) in
         (List.exists matches component) in
      List.filter belongs_to_component l in
   List.map algebra_expressions_for (connected_components hypergraph guards);;




exception ComponentizationException;;

let componentize_sum a1 a2 alg2 groupby =
   let vars = (extract_bindings alg2) in
   let components a1 a2 =
      let hypergraph =
         match alg2 with
            (NaturalJoin l) -> l
         |   _ -> raise ComponentizationException in
      connected_components2 hypergraph
                            [(aschema a1); (aschema a2)]
                            (vars @ groupby) in  (* is that right? *)
      if (List.length (components a1 a2)) > 1 then
         let f comp =
            let g arith = (ListAsSet.inter (aschema arith)
                             (List.flatten (List.map schema comp))) != [] in
            CSumAgg( (abigprod (List.filter g [a1; a2])),
                     (NaturalJoin comp),
                     (ListAsSet.inter (schema (NaturalJoin comp))
                            vars) ) in (* this is wrong, but does not matter *)
         (cbigprod (List.map f (components a1 a2)))
      else
         CSumAgg(Multiplication(a1, a2), alg2, groupby);;




let simplify_sumagg (a, alg, groupby) =
   let alg2 = simplify (NaturalJoin [alg]) in (* is simplify still needed? *)
   let vars = (extract_bindings alg2) in
   match a with
      Addition(Column(a1), a2)
         when (ListAsSet.subset [a1] vars)
         -> CAddition(MapVar(a1), CSumAgg(a2, alg2, groupby))
   |  Addition(a2, Column(a1))
         when (ListAsSet.subset [a1] vars)
         -> CAddition(MapVar(a1), CSumAgg(a2, alg2, groupby))
   |  Addition(a1, a2)
         -> CAddition(CSumAgg(a1, alg2, groupby), CSumAgg(a2, alg2, groupby))
   |  Multiplication(Column(a1), a2)
         when (ListAsSet.subset [a1] vars)
         -> CMultiplication(MapVar(a1), CSumAgg(a2, alg2, groupby))
   |  Multiplication(a2, Column(a1))
         when (ListAsSet.subset [a1] vars)
         -> CMultiplication(MapVar(a1), CSumAgg(a2, alg2, groupby))
   |  Multiplication(a1, a2) -> (componentize_sum a1 a2 alg2 groupby)
   | _ -> CSumAgg(a, alg, groupby)
   ;;





(* all the vars in arith_expr must be bound, i.e. in the groupby list *)
let rec arith_to_code arith_expr =
   match arith_expr with
      Multiplication(a1, a2) ->
         CMultiplication((arith_to_code a1), (arith_to_code a2))
   |  Addition(a1, a2) ->
         CAddition((arith_to_code a1), (arith_to_code a2))
   |  Constant(c) -> CConstant(c)
   |  Column(col) -> MapVar(col)
   ;;


let rec simplify_code code =
   match code with
      CSumAgg(arith_expr, NaturalJoin l, groupby) when l = [] ->
         (arith_to_code arith_expr)
         (* zero joins return the nullary tuple.
            If this change is made, we have to rerun to be sure to
            minimize.
         *)
   |  CSumAgg(arith_expr, RUnion l, groupby) ->
         let f x = simplify_code(CSumAgg(arith_expr, x, groupby)) in
         cbigsum (List.map f l)
   |  CSumAgg(arith_expr, relalg, groupby) ->
         let aux = (simplify_sumagg (arith_expr, relalg, groupby)) in
         if (aux = code) then code
         else (simplify_code aux)
   |  CMultiplication(CConstant(1), c) -> simplify_code c
   |  CMultiplication(c, CConstant(1)) -> simplify_code c
   |  CMultiplication(c1, c2) ->
         CMultiplication((simplify_code c1), (simplify_code c2))
   |  CAddition(CConstant(0), c) -> simplify_code c
   |  CAddition(c, CConstant(0)) -> simplify_code c
   |  CAddition(c1, c2) ->
         CAddition((simplify_code c1), (simplify_code c2))
   |  CConstant(_) -> code
   |  MapVar(_) -> code
   ;;










(****************************************************************************
                             TESTING
****************************************************************************)


let w = [LTRelation ("C", "D");
         NamedRelation ("S", ["B"; "C"]);
         NamedRelation ("T", ["D"; "E"])];;

connected_components2 w [] ["C"];;
(* [[LTRelation ("C", "D"); NamedRelation ("T", ["D"; "E"])];
    [NamedRelation ("S", ["B"; "C"])]]
*)

connected_components2 w [] ["C"; "D"];;
connected_components2 w [] ["B"; "C"];;




let v = SumAgg(Multiplication(Column("A"), Column("D")),
               NaturalJoin([NamedRelation("R", ["A"; "B"]);
                            Selection(LT("C", "D"),
                              NaturalJoin([NamedRelation("S", ["B"; "C"]);
                                           NamedRelation("T", ["D"; "E"])]))]),
               ["B"]);;
simplify v;;
schema v;;
extract_bindings v;;
simplify_sumagg ((function (SumAgg(x,y,z)) -> (x, y, z)) v);;
simplify_sumagg ((function (SumAgg(x,y,_)) -> (x, y, ["C"])) v);;



let v2 = simplify(mod_insert "R" v);;
extract_bindings v2;;


let q =  SumAgg((Multiplication (Column "A", Column "E")),
                (NaturalJoin [NamedRelation ("R", ["A"; "B"]);
                              NamedRelation ("S", ["B"; "C"]);
                              LTRelation ("C", "D");
                              NamedRelation ("T", ["D"; "E"])]), []);;

let q1 = simplify q;;
let q2 = simplify (mod_insert "R" q1);;
extract_bindings q2;;
let c = simplify_code (make_code q2);;




let q3 =
 NaturalJoin
  [Tup ["A"; "B"]; NamedRelation ("S", ["B"; "C"]); LTRelation ("C", "D");
   NamedRelation ("T", ["D"; "E"])];;




let q4 = CSumAgg (Multiplication (Column "A", Column "E"), q3, ["A"; "B"]);;
simplify_code q4;;










let q =  SumAgg((Multiplication (Column "A", Column "D")),
                (NaturalJoin [NamedRelation ("R", ["A"; "B"]);
                              NamedRelation ("S", ["B"; "C"]);
                              NamedRelation ("T", ["C"; "D"])]), []);;


let monster1 =
  CAddition
   (CSumAgg (Multiplication (Column "A", Column "D"),
     NaturalJoin
      [NamedRelation ("R", ["A"; "B"]); NamedRelation ("S", ["B"; "C"]);
       NamedRelation ("T", ["C"; "D"])],
     []),
   CMultiplication
    (CMultiplication
      (CSumAgg (Column "A", NaturalJoin [NamedRelation ("R", ["A"; "B"])],
        ["B"]),
      CSumAgg (Constant 1, NaturalJoin [Tup ["B"; "C"]],
       ["B"; "C"; "B"; "C"])),
    CSumAgg (Column "D", NaturalJoin [NamedRelation ("T", ["C"; "D"])],
     ["C"])));;



