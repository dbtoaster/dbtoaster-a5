
#use "Hypertree.ml";;




(*************************************************************************
              TYPES FOR RELATIONAL ALGEBRA EXPRESSIONS
*************************************************************************)


type arith = Multiplication of arith * arith
           | Addition       of arith * arith
           | Column         of string
           | Constant       of int
           ;;

type atomic_cond = EQ of string * string
                 | LT of string * string
                 ;;

type relalg = NaturalJoin    of relalg list 
            | Selection      of atomic_cond * relalg
            | EQRelation     of string * string
            | LTRelation     of string * string
            | NamedRelation  of string * string list
                                (* relation name plus list of column names *)
            | SumAgg         of arith * relalg * string list
            | RUnion         of relalg list
            | Tup            of string list  (* variable binding *)
            ;;




(*************************************************************************
                  SCHEMA ACCESS AND INTEGRITY CHECKING
*************************************************************************)


(* TODO: Check consistency. E.g., that conditions and arithmetic expressions
   only use columns in the schema
*)


(* TODO: should Tup really contribute to the schema? *)
let rec schema alg =
   match alg with
      NamedRelation(_, sch)  -> sch
   |  LTRelation(col1, col2) -> [col1; col2]
   |  EQRelation(col1, col2) -> [col1; col2]
   |  Selection(_, alg2)     -> (schema alg2)
   |  NaturalJoin l          -> (List.flatten (List.map (schema) l))
   |  SumAgg(_, alg2, _)     -> (schema alg2)
   |  RUnion(algset)         -> (schema (List.hd algset))
            (* TODO: Check that (schema alg2) is the same
               otherwise raise exception UnionSchemaException;;
             *)
   |  Tup(sch)               -> sch
   ;;


let rec aschema arith_expr =
   match arith_expr with
      Multiplication(a1, a2) -> (aschema a1) @ (aschema a2)
   |  Addition(a1, a2)       -> (aschema a1) @ (aschema a2)
   |  Constant(_)            -> []
   |  Column(col)            -> [col]
   ;;






(*************************************************************************
                  PRINTING DATA STRUCTURES
*************************************************************************)


let rec print_arith arith =
   match arith with
      Multiplication(a1, a2) ->
         print_arith a1;
         print_string " * ";
         print_arith a2
   |  Addition(a1, a2) ->
         print_string "(";
         print_arith a1;
         print_string " + ";
         print_arith a2;
         print_string ")"
   |  Constant(a) -> print_int a
   |  Column(col) -> print_string col
   ;;


let print_cond atomic_cond =
   match atomic_cond with
      EQ(col1, col2) ->
         print_string col1;
         print_string " = ";
         print_string col2
   |  LT(col1, col2) ->
         print_string col1;
         print_string " < ";
         print_string col2
   ;;

let rec print_relalg relalg =
   match relalg with
      NaturalJoin(l) ->
         print_string "NaturalJoin[";
         List.iter (print_relalg) l;
         print_string "]"
    | Selection(atomic_cond, relalg) ->
         print_string "Selection(";
         print_cond atomic_cond;
         print_string ", ";
         print_relalg relalg;
         print_string ")"
    | EQRelation(col1, col2) ->
         print_string "EQRelation(";
         print_string col1;
         print_string col2;
         print_string ")"
    | LTRelation(col1, col2) ->
         print_string "LTRelation(";
         print_string col1;
         print_string col2;
         print_string ")"
    | NamedRelation(name, l) ->
         print_string "NamedRelation(";
         print_string name;
         print_string ", [";
         List.iter print_string l;
         print_string "])"
    | SumAgg(arith_expr, relalg, groupby) ->
         print_string "SumAgg([";
         print_arith arith_expr;
         print_string "], ";
         print_relalg relalg;
         print_string ", [";
         List.iter (print_string) groupby;
         print_string "])"
    | RUnion(relalg_exprs) ->
         print_string "RUnion([";
         List.iter print_relalg relalg_exprs;
         print_string "])"
    | Tup(cols) ->
         print_string "Tup([";
         List.iter print_string cols;
         print_string "])"
    ;;





 
(*************************************************************************
                  SIMPLIFY ALGEBRAIC EXPRESSIONS
*************************************************************************)


let rec flatten_natjoin alg =
   match alg with
   |  NaturalJoin c -> (List.flatten (List.map flatten_natjoin c))
   |  b -> [b]
     ;;

(* simplify the algebra *)
let rec simplify alg =
   match alg with
      NaturalJoin(a) ->
         let split l =
            let f x = match x with RUnion(algset) -> algset | _ -> [x] in
            ListAsSet.distribute (List.map f l) in
         let f x = NaturalJoin (flatten_natjoin (NaturalJoin x)) in
         let aux = List.map f (split (List.map simplify a)) in
         if(List.length aux) = 1 then (List.hd aux)
         else RUnion(aux)
   |  RUnion(algset) -> 
         RUnion (List.map (simplify) algset)
   |  Selection(EQ(col1, col2), a) ->
         simplify(NaturalJoin [EQRelation(col1, col2); a])
   |  Selection(LT(col1, col2), a) ->
         simplify(NaturalJoin [LTRelation(col1, col2); a])
   |  SumAgg(a, b, c) ->
         SumAgg(a, simplify(b), c)
   |  b -> b
   ;;










(*************************************************************************
                       UTILITY FUNCTIONS
*************************************************************************)


let rec extract_bindings relalg =
    match relalg with
       Tup(x) -> x
    |  NaturalJoin l ->
          List.flatten (List.map (extract_bindings) l)
    |  RUnion l ->
          List.flatten (List.map (extract_bindings) l)
    |  SumAgg(_, alg, _) ->
         (extract_bindings alg)
    | _ -> []
    ;;


(* to be called after extract_bindings has been used to move the bindings
   into the groupby list *)
let eliminate_tups relalg =
   (* elim_tups0 should return a _SINGLETON LIST_ of relalg *)
   let rec eliminate_tups0 relalg =
       match relalg with
          Tup(x) -> []
       |  NaturalJoin l ->
            [NaturalJoin(List.flatten (List.map (eliminate_tups0) l))]
       |  RUnion l ->
            [RUnion(List.flatten (List.map (eliminate_tups0) l))]
       |  SumAgg(a, alg, g) ->
            [SumAgg(a, alg, g)]
       | _ -> [relalg] in
    List.hd (eliminate_tups0 relalg);;



(* Modifies the algebra expression to insert a tuple into relation relname

   ASSUMPTION: This must be called AFTER simplify -- the traversal
   does not examine subexpressions of selection and union, for instance.
*)
let rec mod_insert relname relalg =
    match relalg with
       NamedRelation(relname2, x)
          when relname=relname2
          -> RUnion [relalg; Tup(x)]
    |  NaturalJoin l ->
          NaturalJoin (List.map (mod_insert relname) l)
    |  SumAgg(arith_expr, alg, groupby) ->
          SumAgg(arith_expr, (mod_insert relname alg), groupby)
    |  _ -> relalg
    ;;









(*************************************************************************
                            TESTING
*************************************************************************)


let v = SumAgg(Multiplication(Column("A"), Column("D")),
               NaturalJoin([NamedRelation("R", ["A"; "B"]);
                            Selection(LT("C", "D"),
                              NaturalJoin([NamedRelation("S", ["B"; "C"]);
                                           NamedRelation("T", ["D"; "E"])]))]),
               ["B"]);;
simplify v;;
schema v;;
extract_bindings v;;
let v2 = simplify(mod_insert "R" v);;
extract_bindings v2;;

print_relalg v;;


let q =  SumAgg((Multiplication (Column "A", Column "E")),
                (NaturalJoin [NamedRelation ("R", ["A"; "B"]);
                              NamedRelation ("S", ["B"; "C"]);
                              LTRelation ("C", "D");
                              NamedRelation ("T", ["D"; "E"])]), []);;

let q1 = simplify q;;
let q2 = simplify (mod_insert "R" q1);;
   

