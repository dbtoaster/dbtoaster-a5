exception InvalidExpression
exception DuplicateException
exception PlanException of string
exception RewriteException of string
exception ValidationException of string
exception CodegenException of string

let dbt_hash x = Hashtbl.hash_param 50 100 x

type identifier = string

type column_name = [
| `Qualified of identifier * identifier
| `Unqualified of identifier ]

type attribute_identifier = column_name
type variable_identifier = identifier
type function_identifier = identifier
type type_identifier = identifier
type field_identifier = identifier
type relation_identifier = identifier
type state_identifier = identifier

type field = field_identifier * type_identifier 
type domain = attribute_identifier list

type delta = [
| `Insert of relation_identifier * field list
| `Delete of relation_identifier * field list]

type eterm = [
| `Int of int
| `Float of float
| `String of string
| `Long of int64
| `Attribute of attribute_identifier 
| `Variable of variable_identifier ]

type expression = [
| `ETerm of eterm
| `UnaryMinus of expression
| `Sum of expression * expression
| `Product of expression * expression
| `Minus of expression * expression
| `Divide of expression * expression
| `Function of function_identifier * (expression list) ]

type meterm = [
| `Int of int
| `Float of float
| `String of string
| `Long of int64
| `Variable of variable_identifier
| `Attribute of attribute_identifier ]

type aggregate_function = [ `Sum | `Min | `Max ]

type oplus = [`Plus | `Minus | `Min | `Max
| `Decrmin of map_expression * state_identifier
| `Decrmax of map_expression * state_identifier ]

and  poplus = [`Union | `Diff ]

and  map_expression = [
| `METerm of meterm
| `Sum of map_expression * map_expression
| `Minus of map_expression * map_expression
| `Product of map_expression * map_expression
| `Min of map_expression * map_expression
| `Max of map_expression * map_expression
| `MapAggregate of aggregate_function * map_expression * plan
| `Delta of delta * map_expression
| `New of map_expression
| `Init of state_identifier * map_expression
| `Incr of state_identifier * oplus * map_expression * map_expression
| `IncrDiff of state_identifier * oplus * map_expression * map_expression
| `Insert of state_identifier * meterm * map_expression
| `Update of state_identifier * oplus * meterm * map_expression
| `Delete of state_identifier * meterm
| `IfThenElse of bterm * map_expression * map_expression ]

and bterm = [ `True | `False 
| `LT of expression * expression
| `LE of expression * expression
| `GT of expression * expression
| `GE of expression * expression
| `EQ of expression * expression
| `NE of expression * expression
| `MEQ of map_expression
| `MLT of map_expression
| `MLE of map_expression 
| `MGT of map_expression 
| `MGE of map_expression 
| `MNEQ of map_expression ]

and boolean_expression = [
| `BTerm of bterm
| `Not of boolean_expression
| `And of boolean_expression * boolean_expression
| `Or of boolean_expression * boolean_expression ]

and plan = [
| `TrueRelation (* aka nullary singleton *)
| `FalseRelation (* aka emptyset *)
| `Relation of relation_identifier * field list
| `Domain of state_identifier * domain
| `Select of boolean_expression * plan
| `Project of (attribute_identifier * expression) list * plan
| `Union of plan list
| `Cross of plan * plan
| `DeltaPlan of delta * plan
| `NewPlan of plan
| `IncrPlan of state_identifier * poplus * plan * domain
| `IncrDiffPlan of state_identifier * poplus * plan * domain ]

type unifications = attribute_identifier * expression list

type binding = [
| `BindExpr of variable_identifier * expression
| `BindBoolExpr of variable_identifier * boolean_expression
| `BindMapExpr of variable_identifier * map_expression * unifications ]

type recompute_state = New | Incr

(* symbol generation *)
let id_counter = ref 0

let gen_rel_id () =
    let new_sym = !id_counter in
	incr id_counter;
	"rel"^(string_of_int new_sym)

let var_counter = ref 0

let gen_var_sym() =
    let new_sym = !var_counter in
	incr var_counter;
	"var"^(string_of_int new_sym)

let state_counter = ref 0

let gen_state_sym() =
    let new_sym = !state_counter in
	incr state_counter;
	"state"^(string_of_int new_sym)

type state_symbols = (string, string) Hashtbl.t
let indexed_syms : (string, int ref * state_symbols) Hashtbl.t = Hashtbl.create 10

let gen_indexed_sym category sid =
    let (cat_counter, cat_syms) =
        if not (Hashtbl.mem indexed_syms category) then
            Hashtbl.replace indexed_syms category (ref 0, Hashtbl.create 10);
        Hashtbl.find indexed_syms category
    in
        if Hashtbl.mem cat_syms sid then
            Hashtbl.find cat_syms sid
        else
            let new_sym = category^(string_of_int !cat_counter) in
                incr cat_counter;
                Hashtbl.add cat_syms sid new_sym;
                new_sym

let gen_map_sym = gen_indexed_sym "map"
let gen_dom_sym = gen_indexed_sym "dom"

(* Basic helpers *)
let field_of_attribute_identifier id =
    match id with
	| `Qualified(r,a) -> a
	| `Unqualified a -> a

let attribute_identifiers_of_field_list relation_id field_l =
    List.map (fun (id, ty) -> `Qualified(relation_id, id)) field_l

(* Plan transformation helpers *)
let rec fold_map_expr fn acc expr =
    match expr with 
	| `UnaryMinus e ->
	      let (acc2, e2) = fold_map_expr fn acc e in (fn acc2 (`UnaryMinus e2))

	| `Sum (l,r) ->
	      let (acc2, l2) = fold_map_expr fn acc l in
	      let (acc3, r2) = fold_map_expr fn acc2 r in
		  (fn acc3 (`Sum (l2, r2)))

	| `Product (l,r) ->
	      let (acc2, l2) = fold_map_expr fn acc l in
	      let (acc3, r2) = fold_map_expr fn acc2 r in
		  (fn acc3 (`Product (l2, r2)))

	| `Minus (l,r) -> 
	      let (acc2, l2) = fold_map_expr fn acc l in
	      let (acc3, r2) = fold_map_expr fn acc2 r in
		  (fn acc3 (`Minus (l2, r2)))

	| `Divide (l,r) -> 
	      let (acc2, l2) = fold_map_expr fn acc l in
	      let (acc3, r2) = fold_map_expr fn acc2 r in
		  (fn acc3 (`Divide (l2, r2)))

	| `Function (id, args) ->
	      let (acc2, args2) =
		  List.fold_left
		      (fun (acc, eacc) e ->
			   let (acc2, e2) = fold_map_expr fn acc e in (acc2, eacc@[e2]))
		      (acc, []) args
	      in
		  (fn acc2 (`Function (id, args2)))

	| (`ETerm e) as t -> (fn acc t)


let rec fold_map_bool_expr fn acc bool_expr =
    match bool_expr with
	| `Not e ->
	      let (acc2, e2) = fold_map_bool_expr fn acc e in
		  (fn acc2 (`Not e2))

	| `And (l,r) ->
	      let (acc2,l2) = fold_map_bool_expr fn acc l in
	      let (acc3,r2) = fold_map_bool_expr fn acc2 r in
		  (fn acc3 (`And (l2,r2)))
		      
	| `Or (l,r) ->
	      let (acc2,l2) = fold_map_bool_expr fn acc l in
	      let (acc3,r2) = fold_map_bool_expr fn acc2 r in
		  (fn acc3 (`Or (l2,r2)))

	| (`BTerm b) as t -> (fn acc t)

let map_expr fn expr =
    let rec map_aux e = 
        match expr with 
	    | (`ETerm e) as t -> fn t
	    | `UnaryMinus e -> fn (`UnaryMinus (map_aux e))
	    | `Sum (l,r) -> fn (`Sum (map_aux l, map_aux r))
	    | `Product (l,r) -> fn (`Product (map_aux l, map_aux r))
	    | `Minus (l,r) -> fn (`Minus (map_aux l, map_aux r))
	    | `Divide (l,r) -> fn (`Divide (map_aux l, map_aux r))
	    | `Function (id, args) -> fn (`Function (id, List.map map_aux args))
    in
        map_aux expr
                  
let rec map_bool_expr e_fn b_fn me_fn mp_fn bool_expr =
    let map_aux = map_bool_expr e_fn b_fn me_fn mp_fn in
    let map_e expr = map_expr e_fn expr in
    let map_me_aux m_expr = map_map_expr e_fn b_fn me_fn mp_fn m_expr in
        match bool_expr with
	    | `BTerm b -> b_fn (`BTerm(
                  begin
                      match b with
                          | `True | `False -> b
                          | `LT (l,r) -> `LT (map_e l, map_e r)
                          | `LE (l,r) -> `LE (map_e l, map_e r)
                          | `GT (l,r) -> `GT (map_e l, map_e r)
                          | `GE (l,r) -> `GE (map_e l, map_e r)
                          | `EQ (l,r) -> `EQ (map_e l, map_e r)
                          | `NE (l,r) -> `NE (map_e l, map_e r)
                          | `MEQ m_expr -> `MEQ (map_me_aux m_expr)
                          | `MNEQ m_expr -> `MNEQ (map_me_aux m_expr)
                          | `MLT m_expr -> `MLT (map_me_aux m_expr)
                          | `MLE m_expr -> `MLE (map_me_aux m_expr)
                          | `MGT m_expr -> `MGT (map_me_aux m_expr)
                          | `MGE m_expr -> `MGE (map_me_aux m_expr)
                  end))
	    | `And (l,r) -> b_fn (`And (map_aux l, map_aux r))
	    | `Or (l,r) -> b_fn (`Or (map_aux l, map_aux r))
	    | `Not e -> b_fn (`Not (map_aux e))

and map_map_expr e_fn b_fn me_fn mp_fn (m_expr : map_expression) =
    let map_aux = map_map_expr e_fn b_fn me_fn mp_fn in
        match m_expr with
            | `METerm x -> me_fn m_expr
            | `Sum (l,r) -> me_fn (`Sum(map_aux l, map_aux r))
            | `Minus (l,r) -> me_fn (`Minus(map_aux l, map_aux r))
            | `Product (l,r) -> me_fn (`Product(map_aux l, map_aux r))
            | `Min (l,r) -> me_fn (`Min(map_aux l, map_aux r))
            | `Max (l,r) -> me_fn (`Max(map_aux l, map_aux r))
            | `MapAggregate (fn, f, q) ->
                  me_fn (`MapAggregate(fn, map_aux f, map_plan e_fn b_fn me_fn mp_fn q))
            | `IfThenElse (bt, l, r) ->
                  let r_bt = map_bool_expr e_fn b_fn me_fn mp_fn (`BTerm bt) in
                      begin
                          match r_bt with
                              | `BTerm(x) -> me_fn (`IfThenElse(x, map_aux l, map_aux r))
                              | _ -> raise InvalidExpression
                      end

            | `Delta (b, e) -> me_fn (`Delta(b, map_aux e))
            | `New (e) -> me_fn (`New(map_aux e))
            | `Init (sid, e) -> me_fn (`Init (sid, map_aux e))
            | `Incr (sid, op, re, e) -> me_fn (`Incr(sid, op, re, map_aux e))
            | `IncrDiff (sid, op, re, e) -> me_fn (`IncrDiff(sid, op, re, map_aux e))
            
            | `Insert (sid, t, e) ->
                  let r_mt = map_aux (`METerm t) in
                      begin
                          match r_mt with
                              | `METerm(x) -> me_fn (`Insert(sid, x, map_aux e))
                              | _ -> raise InvalidExpression
                      end

            | `Update (sid, op, t, e) ->
                  let r_mt = map_aux (`METerm t) in
                      begin
                          match r_mt with
                              | `METerm(x) -> me_fn(`Update (sid, op, x, map_aux e))
                              | _ -> raise InvalidExpression
                      end

            | `Delete (sid, t) -> 
                  let r_mt = map_aux (`METerm t) in
                      begin
                          match r_mt with
                              | `METerm(x) -> me_fn(`Delete (sid, x))
                              | _ -> raise InvalidExpression
                      end

and map_plan e_fn b_fn me_fn mp_fn (plan : plan) =
    let map_aux = map_plan e_fn b_fn me_fn mp_fn in
        match plan with
            | `TrueRelation | `FalseRelation
            | `Relation (_,_) | `Domain _ -> mp_fn plan
            | `Select (p,ch) -> mp_fn (`Select(map_bool_expr e_fn b_fn me_fn mp_fn p, map_aux ch))
            | `Project (projs, ch) ->
                  mp_fn (
                      `Project(List.map (fun (a,e) -> (a, map_expr e_fn e)) projs,
                      map_aux ch))
            | `Union ch -> mp_fn (`Union (List.map map_aux ch))
            | `Cross (l,r) -> mp_fn (`Cross (map_aux l, map_aux r))
            | `DeltaPlan (b, p) -> mp_fn (`DeltaPlan(b, map_aux p))
            | `NewPlan (p) -> mp_fn (`NewPlan (map_aux p))
            | `IncrPlan (sid, pop, p, d) -> mp_fn (`IncrPlan (sid, pop, map_aux p, d))
            | `IncrDiffPlan (sid, pop, p, d) -> mp_fn (`IncrDiffPlan (sid, pop, map_aux p, d))


(* Pretty printing *)	
let string_of_attribute_identifier id =
    match id with 
	| `Qualified(r,a) -> r^"."^a
	| `Unqualified a -> a

let string_of_attribute_identifier_list idl=
    List.fold_left
        (fun acc aid ->
            (if (String.length acc) = 0 then "" else (acc^", "))^
                (string_of_attribute_identifier aid))
        "" idl

let string_of_schema schema =
    "{"^
	(List.fold_left
	     (fun acc (id, ty) ->
		  (if (String.length acc) = 0 then "" else (acc^","))^
		      id^":"^ty) "" schema) ^"}"

let string_of_schema_fields schema =
    List.fold_left
	(fun acc (id, ty) ->
	    (if (String.length acc) = 0 then "" else (acc^", "))^id)
        "" schema

let string_of_delta d =
    match d with
	| `Insert (r, f) -> r^","^(string_of_schema f)
	| `Delete (r, f) -> r^","^(string_of_schema f)

let string_of_expr_terminal eterm =
    match eterm with 
	| `Int i -> string_of_int i
	| `Float f -> string_of_float f
	| `Long l -> Int64.to_string l
	| `String s -> "'"^s^"'"
	| `Attribute (`Qualified(r,a)) -> r^"."^a
	| `Attribute (`Unqualified a) -> a
	| `Variable v -> "Var("^v^")"

let string_of_map_expr_terminal meterm =
    match meterm with 
	| `Int i -> string_of_int i
	| `Float f -> string_of_float f
	| `Long l -> Int64.to_string l
	| `String s -> "'"^s^"'"
	| `Variable v -> "Var("^v^")"
	| `Attribute (`Qualified(r,a)) -> r^"."^a
	| `Attribute (`Unqualified a) -> a

let rec string_of_expression expr =
    match expr with
	| `ETerm e -> string_of_expr_terminal e
	| `UnaryMinus e -> "-"^(string_of_expression e)
	| `Sum (l,r) -> (string_of_expression l)^" + "^(string_of_expression r)
	| `Product(l,r) -> (string_of_expression l)^" * "^(string_of_expression r) 
	| `Minus (l,r) -> (string_of_expression l)^" - "^(string_of_expression r)
	| `Divide (l,r) -> (string_of_expression l)^" / "^(string_of_expression r)
	| `Function (id, args) -> id^"("^
	      (List.fold_left
		   (fun acc e ->
			(if (String.length acc) = 0 then "" else (acc^","))^
			    (string_of_expression e))
		   "" args)^")"

let string_of_projections projections =
    List.fold_left
	(fun acc (id, expr) ->
	     (if (String.length acc) = 0 then "" else (acc^","))^
		 (string_of_attribute_identifier id)^"<-"^(string_of_expression expr))
	"" projections

let rec string_of_bool_expression b_expr =
    let dispatch_expr l r op =
	(string_of_expression l)^op^(string_of_expression r)
    in
	match b_expr with
	    | `BTerm b ->
		  begin
		      match b with
			  | `LT (l,r) -> dispatch_expr l r "<"
			  | `LE (l,r) -> dispatch_expr l r "<="
			  | `GT (l,r) -> dispatch_expr l r ">"
			  | `GE (l,r) -> dispatch_expr l r ">="
			  | `EQ (l,r) -> dispatch_expr l r "="
			  | `NE (l,r) -> dispatch_expr l r "!="
			  | `MEQ m_expr -> (string_of_map_expression m_expr)^" = 0"
			  | `MNEQ m_expr -> (string_of_map_expression m_expr)^" <> 0" 
			  | `MLT m_expr -> (string_of_map_expression m_expr)^" < 0" 
			  | `MLE m_expr -> (string_of_map_expression m_expr)^" <= 0"
			  | `MGT m_expr -> (string_of_map_expression m_expr)^" > 0" 
			  | `MGE m_expr -> (string_of_map_expression m_expr)^" => 0"
			  | `True -> "true"
			  | `False -> "false"
		  end
	    | `Not (e) -> ("not("^(string_of_bool_expression e)^")")
	    | `And (l,r) -> 
		  ("("^(string_of_bool_expression l)^") and ("^
		       (string_of_bool_expression r)^")")
	    | `Or (l,r) ->
		  ("("^(string_of_bool_expression l)^") or ("^
		       (string_of_bool_expression r)^")")

and string_of_oplus op =
    match op with
	| `Plus -> "+"
	| `Minus -> "-"
	| `Min -> "min"
	| `Max -> "max"
	| `Decrmin (m, sid) -> "decrmin("^(string_of_map_expression m)^", "^sid^")"
	| `Decrmax (m, sid) -> "decrmax("^(string_of_map_expression m)^", "^sid^")"

and string_of_poplus op =
    match op with
	| `Union -> "U"
	| `Diff -> "-"

and string_of_map_expression m_expr =
    match m_expr with
	| `METerm t -> string_of_map_expr_terminal t

	| `Sum (l,r) ->
	      (string_of_map_expression l)^" + "^(string_of_map_expression r)

	| `Minus (l,r) ->
	      (string_of_map_expression l)^" - "^(string_of_map_expression r)

	| `Product (l, r) -> 
	      (string_of_map_expression l)^" * "^(string_of_map_expression r)
		  
	| `Min  (l, r) ->
	      "min("^(string_of_map_expression l)^", "^(string_of_map_expression r)^")"

	| `Max  (l, r) ->
	      "max("^(string_of_map_expression l)^", "^(string_of_map_expression r)^")"

	| `MapAggregate (fn, f, q) ->
	      (match fn with | `Sum -> "sum" | `Min -> "min" | `Max -> "max")^
		  "{"^(string_of_map_expression f)^"}["^(string_of_plan q)^"]"

	| `Delta (binding, dm_expr) ->
	      "Delta{"^(string_of_delta binding)^"}("^
		  (string_of_map_expression dm_expr)^")"

	| `New (e) -> "New("^(string_of_map_expression e)^"}"

	| `Init (sid, e) -> "Init["^(sid)^"]{"^(string_of_map_expression e)^"}"

	| `Incr (sid, op, re, e) ->
              "Incr["^(string_of_oplus op)^","^(sid)^"]{"^
                  (string_of_map_expression e)^"}"

	| `IncrDiff (sid, op, re, e) ->
              "IncrDiff["^(string_of_oplus op)^","^(sid)^"]{"^
                  (string_of_map_expression e)^"}"

	| `Insert (sid, m, e) ->
              "Insert{"^(sid)^"["^(string_of_map_expr_terminal m)^"]}("^
                  (string_of_map_expression e)^")"

	| `Update (sid, op, m, e) ->
              "Update{"^(string_of_oplus op)^","^(sid)^
                  "["^(string_of_map_expr_terminal m)^"]}("^
                  (string_of_map_expression e)^")"

	| `Delete (sid, m) -> "Delete{"^(sid)^"["^(string_of_map_expr_terminal m)^"]}"

	| `IfThenElse (b, m1, m2) ->
              "If("^(string_of_bool_expression (`BTerm b))^
                  " ) then "^(string_of_map_expression m1)^
                  " else "^(string_of_map_expression m2)
	
and string_of_plan p =
    match p with
	| `TrueRelation -> "TrueRelation"

	| `FalseRelation -> "FalseRelation"

	| `Relation (name, schema) -> "Relation ["^name^"]"^(string_of_schema schema)

        | `Domain (sid, attrs) -> "Domain ["^sid^"]("^(string_of_attribute_identifier_list attrs)^")"

	| `Select (pred, child) ->
	      "select {"^(string_of_bool_expression pred)^"}("^(string_of_plan child)^")"

	| `Project (projs, child) ->
	      "project {"^(string_of_projections projs)^"}("^(string_of_plan child)^")"

	| `Union (children) ->
	      "union {"^
		  (List.fold_left
		       (fun acc c ->
			    (if String.length acc = 0 then "" else acc^",")^(string_of_plan c))
		       "" children)^"}"

	| `Cross (l, r) ->
	      "cross("^(string_of_plan l)^","^(string_of_plan r)^")"

	| `DeltaPlan (binding, ch) ->
	      "Delta{"^(string_of_delta binding)^"}("^(string_of_plan ch)^")"

	| `NewPlan (ch) -> 
	      "New("^(string_of_plan ch)^")"

	| `IncrPlan (sid, po, ch, d) ->
              "IncrP["^(string_of_poplus po)^","^(sid)^","^
                  (string_of_attribute_identifier_list d)^"]{"^(string_of_plan ch)^"}"

	| `IncrDiffPlan (sid, po, ch, d) ->
              "IncrDiffP["^(string_of_poplus po)^","^(sid)^","^
                  (string_of_attribute_identifier_list d)^"]{"^(string_of_plan ch)^"}"


let rec indented_string_of_bool_expression b_expr level =
    let indent s = (String.make (4*level) '-')^s in
    let dispatch_expr l r op =
	(string_of_expression l)^op^(string_of_expression r)
    in
	match b_expr with
	    | `BTerm b ->
		  begin
		      match b with
			  | `LT (l,r) -> dispatch_expr l r "<"
			  | `LE (l,r) -> dispatch_expr l r "<="
			  | `GT (l,r) -> dispatch_expr l r ">"
			  | `GE (l,r) -> dispatch_expr l r ">="
			  | `EQ (l,r) -> dispatch_expr l r "="
			  | `NE (l,r) -> dispatch_expr l r "!="
			  | `MEQ m_expr -> "\n"^(indented_string_of_map_expression m_expr (level+1))^(indent " = 0")
			  | `MNEQ m_expr -> "\n"^(indented_string_of_map_expression m_expr (level+1))^(indent " <> 0")
			  | `MLT m_expr -> "\n"^(indented_string_of_map_expression m_expr (level+1))^(indent " < 0")
			  | `MLE m_expr -> "\n"^(indented_string_of_map_expression m_expr (level+1))^(indent " <= 0")
			  | `MGT m_expr -> "\n"^(indented_string_of_map_expression m_expr (level+1))^(indent " > 0")
			  | `MGE m_expr -> "\n"^(indented_string_of_map_expression m_expr (level+1))^(indent " >= 0")
			  | `True -> "true"
			  | `False -> "false"
		  end
	    | `Not (e) -> ("not("^(indented_string_of_bool_expression e (level+1))^")")
	    | `And (l,r) -> 
		  ("("^(indented_string_of_bool_expression l (level+1))^") and ("^
		       (indented_string_of_bool_expression r (level+1))^")")
	    | `Or (l,r) ->
		  ("("^(indented_string_of_bool_expression l (level+1))^") or ("^
		       (indented_string_of_bool_expression r (level+1))^")")

and indented_string_of_map_expression m_expr level =
    let indent s = (String.make (4*level) '-')^"> "^s^"\n" in
    let indent_level s l = (String.make (4*l) '-')^"> "^s^"\n" in
    let indent_binary l op r = l^(indent op)^r in
    let indent_binary_prefix l op r lv = l^(indent_level op lv)^r
    in
    match m_expr with
	| `METerm t -> (indent (string_of_map_expr_terminal t))

	| `Sum (l,r) ->
	      (indent_binary
		   (indented_string_of_map_expression l (level+1))
		   " + "
		   (indented_string_of_map_expression r (level+1)))

	| `Minus (l,r) ->
	      (indent_binary
		   (indented_string_of_map_expression l (level+1))
		   " - "
		   (indented_string_of_map_expression r (level+1)))

	| `Product (l, r) -> 
	      (indent_binary
		   (indented_string_of_map_expression l (level+1))
		   " * "
		   (indented_string_of_map_expression r (level+1)))
		  
	| `Min  (l, r) ->
	      (indent "min(")^
		  (indent_binary_prefix
		       (indented_string_of_map_expression l (level+2))
		       ", "
		       (indented_string_of_map_expression r (level+2))
		       (level+1))^
		  (indent ")")

	| `Max  (l, r) ->
	      (indent "max(")^
		  (indent_binary_prefix
		       (indented_string_of_map_expression l (level+2))
		       ", "
		       (indented_string_of_map_expression r (level+2))
		       (level+1))^
		  (indent ")")

	| `MapAggregate (fn, f, q) ->
	      (indent ((match fn with | `Sum -> "sum" | `Min -> "min" | `Max -> "max" )^"{"))^
		  (indented_string_of_map_expression f (level+1))^
		  (indent "}[")^
		  (indented_string_of_plan q (level+1))^
		  (indent "]")

	| `Delta (binding, dm_expr) ->
	      (indent ("Delta{"^(string_of_delta binding)^"}("))^
		   (indented_string_of_map_expression dm_expr (level+1))^
	       	   (indent ")")

	| `New (e) -> 
	      (indent "New{")^
		   (indented_string_of_map_expression e (level+1))^
		   (indent "}")

	| `Init (sid, e) ->
	      (indent ("Init["^(sid)^"]{"))^
		  (indented_string_of_map_expression e (level+1))^
		  (indent "}")

	| `Incr (sid, op, re, e) ->
	      (indent ("Incr["^(string_of_oplus op)^","^(sid)^"]{"))^
		  (indented_string_of_map_expression e (level+1))^
		  (indent "}")

	| `IncrDiff (sid, op, re, e) ->
	      (indent ("IncrDiff["^(string_of_oplus op)^","^(sid)^"]{"))^
		  (indented_string_of_map_expression e (level+1))^
		  (indent "}")

	| `Insert (sid, m, e) ->
	      (indent ("Insert{"^(sid)^"["^(string_of_map_expr_terminal m)^"]}("))^
		  (indented_string_of_map_expression e (level+1))^
		  (indent ")")

	| `Update (sid, op, m, e) ->
	      (indent ("Update{"^(string_of_oplus op)^","^(sid)^"["^(string_of_map_expr_terminal m)^"]}("))^
		  (indented_string_of_map_expression e (level+1))^
		  (indent ")")

	| `Delete (sid, m) ->
	      (indent ("Delete{"^(sid)^"["^(string_of_map_expr_terminal m)^"]}"))

	| `IfThenElse (b, m1, m2) ->
	      (indent ("If ("))^
		  (indented_string_of_bool_expression (`BTerm b) (level+1))^
		  (indent (")"))^
		  (indent ("then"))^
		  (indented_string_of_map_expression m1 (level+1))^
		  (indent ("else"))^
		  (indented_string_of_map_expression m2 (level+1))

and indented_string_of_plan p level =
    let indent s = (String.make (4*level) '-')^"> "^s^"\n" in
    let indent_binary l op r = l^(indent op)^r in
    match p with
	| `TrueRelation -> (indent "TrueRelation")

	| `FalseRelation -> (indent "FalseRelation")

	| `Relation (name, schema) ->
              (indent ("Relation ["^name^"]"^(string_of_schema schema)))

	| `Domain (sid, attrs) ->
              (indent ("Domain ["^sid^"]("^(string_of_attribute_identifier_list attrs)^")"))

	| `Select (pred, child) ->
	      (indent ("select {"^
		  (indented_string_of_bool_expression pred (level+1))^"}("))^
		  (indented_string_of_plan child (level+1))^
                                  (indent ")")

	| `Project (projs, child) ->
	      (indent ("project {"^(string_of_projections projs)^"}("))^
		  (indented_string_of_plan child (level+1))^
		  (indent ")")

	| `Union (children) ->
	      (indent "union {")^
		  (List.fold_left
		       (fun acc c -> acc^(indented_string_of_plan c (level+1)))
		       "" children)^
		  (indent "}")

	| `Cross (l, r) ->
	      (indent "cross(")^
		  (indent_binary
		       (indented_string_of_plan l (level+1))
		       ","
		       (indented_string_of_plan r (level+1)))^
		  (indent ")")

	| `DeltaPlan (binding, ch) ->
	      (indent ("Delta{"^(string_of_delta binding)^"}("))^
		  (indented_string_of_plan ch (level+1))^
		  (indent ")")

	| `NewPlan (ch) ->
	      (indent ("NewPlan("))^
		  (indented_string_of_plan ch (level+1))^
		  (indent ")")

	| `IncrPlan (sid, po, ch, d) ->
	      (indent ("IncrPlan["^(string_of_poplus po)^","^(sid)^","^
                  (string_of_attribute_identifier_list d)^"]{"))^
		  (indented_string_of_plan ch (level+1))^
		  (indent "}")

	| `IncrDiffPlan (sid, po, ch, d) ->
	      (indent ("IncrDiffPlan["^(string_of_poplus po)^","^(sid)^","^
                  (string_of_attribute_identifier_list d)^"]{"))^
		  (indented_string_of_plan ch (level+1))^
		  (indent "}")



(*
 * Accessors
 *
 *)

type accessor_element = [ `MapExpression of map_expression | `Plan of plan ]

let string_of_accessor_element ae =
    match ae with
	| `MapExpression m_expr -> string_of_map_expression m_expr
	| `Plan pl -> string_of_plan pl

let string_of_accessor_element_opt ae_opt =
    match ae_opt with
	| None -> "<null>"
	| Some(`MapExpression m_expr) -> string_of_map_expression m_expr
	| Some(`Plan pl) -> string_of_plan pl

(* map_expression -> accessor_element -> accessor_element option *)
let parent m_expr ch_e_or_p =
    let rec parent_aux e_or_p prev_e_or_p =
	match e_or_p with
	    | `MapExpression (`METerm _) 
	    | `MapExpression (`Delete _) -> None

	    | `MapExpression (`Delta (_, e))
	    | `MapExpression (`New (e))
	    | `MapExpression (`Init (_, e)) 
	    | `MapExpression (`Incr (_, _, _, e))
	    | `MapExpression (`IncrDiff (_, _, _, e))
	    | `MapExpression (`Insert (_, _, e))
	    | `MapExpression (`Update (_, _, _, e)) ->
		  let match_e = `MapExpression e in
		      if (ch_e_or_p = match_e) then Some prev_e_or_p
		      else
			  parent_aux match_e prev_e_or_p

	    | `MapExpression(`Sum (l, r))
	    | `MapExpression(`Minus (l, r))
	    | `MapExpression(`Product (l, r))
	    | `MapExpression(`Min (l,r))
	    | `MapExpression(`Max (l, r))
	    | `MapExpression(`IfThenElse(_, l, r)) ->
		  let match_l = `MapExpression l in
		  let match_r = `MapExpression r in
		      if (match_l = ch_e_or_p) || (match_r = ch_e_or_p) then
			  Some e_or_p
		      else
			  let lp = parent_aux match_l e_or_p in
			      if lp = None then parent_aux match_r e_or_p else lp 

	    | `MapExpression(`MapAggregate (fn, f, q)) ->
		  let match_f = `MapExpression f in
		  let match_q = `Plan q in
		      if (match_f = ch_e_or_p) || (match_q = ch_e_or_p) then
			  Some e_or_p
		      else
			  let fp = parent_aux match_f e_or_p in
			      if fp = None then parent_aux match_q e_or_p else fp

	    | `Plan (`TrueRelation) -> None
	    | `Plan (`FalseRelation) -> None
	    | `Plan (`Relation _) -> None 
	    | `Plan (`Domain _) -> None

	    | `Plan (`Select (p, cq)) ->
		  begin
		      let match_cq = `Plan cq in
			  match p with
			      | `BTerm(`MEQ(m_expr)) | `BTerm(`MNEQ(m_expr))
                              | `BTerm(`MLT(m_expr)) | `BTerm(`MLE(m_expr))
                              | `BTerm(`MGT(m_expr)) | `BTerm(`MGE(m_expr)) ->
				    let match_p = `MapExpression m_expr in
					if (match_p = ch_e_or_p) || (match_cq = ch_e_or_p) then Some e_or_p
					else
					    let pp = parent_aux match_p e_or_p in
						if pp = None then parent_aux match_cq e_or_p else pp

			      | _ ->
				  print_endline ("Matching "^(string_of_plan cq));
				    if match_cq = ch_e_or_p then Some e_or_p
				    else
					parent_aux match_cq e_or_p
		  end

	    | `Plan (`Project (a, cq)) ->
		  let match_cq = `Plan cq in
		      if match_cq = ch_e_or_p then Some e_or_p else parent_aux match_cq e_or_p

	    | `Plan (`Union ch) ->
		  List.fold_left
		      (fun acc c ->
			   match acc with
			       | None ->
				     let match_c = `Plan c in
					 if match_c = ch_e_or_p then Some e_or_p
					 else parent_aux (`Plan c) e_or_p
			       | Some r as x -> x)
		      None ch

	    | `Plan (`Cross (l,r)) ->
		  let match_l = `Plan l in
		  let match_r = `Plan r in
		      if (match_l = ch_e_or_p) || (match_r = ch_e_or_p) then Some e_or_p
		      else
			  let lp = parent_aux match_l e_or_p in
			      if lp = None then parent_aux match_r e_or_p else lp
		      
	    | `Plan (`DeltaPlan (_, p)) | `Plan (`NewPlan(p))
            | `Plan (`IncrPlan(_, _, p,_)) | `Plan (`IncrDiffPlan(_, _, p,_)) ->
		  let match_p = `Plan p in
		      if match_p = ch_e_or_p then Some e_or_p
		      else parent_aux match_p e_or_p

	    | _ -> raise InvalidExpression
    in
    let match_root = `MapExpression m_expr in
	if ch_e_or_p = match_root then None
	else
	    parent_aux match_root match_root

(* accessor_element -> accessor_element list *)
let children e_or_p =
    let get_map_expressions b_expr =
	match b_expr with
	    | `BTerm(`MEQ(m_expr)) | `BTerm(`MNEQ(m_expr)) -> [m_expr]
	    | `BTerm(`MLT(m_expr)) | `BTerm(`MLE(m_expr)) -> [m_expr]
	    | `BTerm(`MGT(m_expr)) | `BTerm(`MGE(m_expr)) -> [m_expr]
	    | _ -> []
    in
	match e_or_p with 
	    | `MapExression (`METerm _) | `MapExpression (`Delete _) -> []
	    | `MapExpression (`Delta (_,e))
	    | `MapExpression (`New(e))
	    | `MapExpression (`Init(_,e))
	    | `MapExpression (`Incr(_,_,_,e))
	    | `MapExpression (`IncrDiff(_,_,_,e))
	    | `MapExpression (`Insert(_,_, e))
	    | `MapExpression (`Update(_,_, _, e)) -> [`MapExpression(e)]

	    | `MapExpression (`Sum(l,r)) | `MapExpression (`Product(l,r))
	    | `MapExpression (`Minus(l,r))
	    | `MapExpression (`IfThenElse(_, l, r))
	    | `MapExpression (`Min(l,r)) | `MapExpression (`Max(l,r)) ->
		  [`MapExpression(l); `MapExpression(r)]
		      
	    | `MapExpression (`MapAggregate(fn,f,q)) -> [`MapExpression(f); `Plan(q)]

	    | `Plan (`TrueRelation) -> []
	    | `Plan (`FalseRelation) -> []
	    | `Plan (`Relation _) -> []
            | `Plan (`Domain _) -> []
	    | `Plan (`Select (p, cq)) ->
		  (List.map (fun x -> `MapExpression(x)) (get_map_expressions p))@([`Plan(cq)])
	    | `Plan (`Project (a,cq)) -> [`Plan(cq)]
	    | `Plan (`Union ch) -> List.map (fun x -> `Plan(x)) ch
	    | `Plan (`Cross(l,r)) -> [`Plan(l); `Plan(r)]
	    | `Plan (`DeltaPlan (_,e)) | `Plan (`NewPlan(e))
            | `Plan (`IncrPlan(_,_,e,_)) | `Plan (`IncrDiffPlan (_,_,e,_)) ->
		  [`MapExpression(e)]
	    | _ -> raise InvalidExpression

(* accessor_element -> accessor_element list *)
let rec descendants_l e_or_p =
    if e_or_p = [] then []
    else
	(List.hd e_or_p)::(
	    (descendants_l (children (List.hd e_or_p)))@
		(descendants_l (List.tl e_or_p)))

(* accessor_element -> accessor_element -> accessor_element list *)
let rec ancestors m_expr expr_or_plan =
    match (parent m_expr expr_or_plan) with
	| None -> []
	| Some x -> x::(ancestors m_expr x)


(* map_expression -> accessor_element -> accessor_element -> map_expression *)
let splice m_expr orig rewrite =
    let rec splice_map_expr_aux e =
	if (`MapExpression e) = orig then
	    begin
		match rewrite with
		    | `MapExpression r -> r
		    | `Plan _ ->
			  raise (PlanException "Cannot change from MapExpression to Plan during splice.")
	    end
	else
	    begin
		match e with
		    | `METerm(_) as x -> x

		    | `Sum(l,r) ->
			  `Sum(splice_map_expr_aux l, splice_map_expr_aux r)

		    | `Minus(l,r) ->
			  `Minus(splice_map_expr_aux l, splice_map_expr_aux r)

		    | `Product(l,r) ->
			  `Product(splice_map_expr_aux l, splice_map_expr_aux r)

		    | `Min(l,r) ->
			  `Min(splice_map_expr_aux l, splice_map_expr_aux r)
			      
		    | `Max(l,r) ->
			  `Max(splice_map_expr_aux l, splice_map_expr_aux r)
			      
		    | `MapAggregate(fn,f,q) ->
			  `MapAggregate(fn, splice_map_expr_aux f, splice_plan_aux q)
			      
		    | `Delta(b,e) -> `Delta(b, splice_map_expr_aux e)
		    | `New(e) -> `New(splice_map_expr_aux e) 
		    | `Init(sid,e) -> `Init(sid, splice_map_expr_aux e)
		    | `Incr(sid,op,re,e) -> `Incr(sid, op, re, splice_map_expr_aux e)
		    | `IncrDiff(sid,op,re,e) -> `IncrDiff(sid, op, re, splice_map_expr_aux e)

		    | `Insert(sid, m, e) -> `Insert(sid, m, splice_map_expr_aux e)
		    | `Update(sid, op, m, e) -> `Update(sid, op, m, splice_map_expr_aux e)
		    | `Delete(_) as x -> x
		    | `IfThenElse(b, l, r) -> `IfThenElse(b, splice_map_expr_aux l, splice_map_expr_aux r)
	    end
    and splice_plan_aux q =
        let splice_nested_m_expr mbterm cq = 
            let spliced_bterm =
                match mbterm with
		    | `BTerm(`MEQ(m_expr)) -> `BTerm(`MEQ(splice_map_expr_aux m_expr))
		    | `BTerm(`MNEQ(m_expr)) -> `BTerm(`MNEQ(splice_map_expr_aux m_expr))
		    | `BTerm(`MLT(m_expr)) -> `BTerm(`MLT(splice_map_expr_aux m_expr))
		    | `BTerm(`MLE(m_expr)) -> `BTerm(`MLE(splice_map_expr_aux m_expr))
		    | `BTerm(`MGT(m_expr)) -> `BTerm(`MGT(splice_map_expr_aux m_expr))
		    | `BTerm(`MGE(m_expr)) -> `BTerm(`MGE(splice_map_expr_aux m_expr))
                    | _ -> raise InvalidExpression
            in
		`Select(spliced_bterm, splice_plan_aux cq)
        in
	if (`Plan q) = orig then
	    begin
		match rewrite with
		    | `Plan r -> r
		    | `MapExpression _ ->
			  raise (PlanException "Cannot change from Plan to MapExpression during splice.")
	    end
	else
	    begin
		match q with
		    | `TrueRelation -> `TrueRelation
		    | `FalseRelation -> `FalseRelation
		    | `Relation _ as x -> x
                    | `Domain _ as x -> x
		    | `Select (p,cq) ->
			  begin
			      match p with
				  | `BTerm(`MEQ(m_expr)) | `BTerm(`MNEQ(m_expr))
				  | `BTerm(`MLT(m_expr)) | `BTerm(`MLE(m_expr))
				  | `BTerm(`MGT(m_expr)) | `BTerm(`MGE(m_expr))
                                      -> splice_nested_m_expr p cq
				  | _ -> `Select(p, splice_plan_aux cq)
			  end
			      
		    | `Project (a, cq) -> `Project(a, splice_plan_aux cq)
		    | `Union ch -> `Union (List.map (fun c -> splice_plan_aux c) ch)
		    | `Cross (l,r) -> `Cross (splice_plan_aux l, splice_plan_aux r)
			      
		    | `DeltaPlan (b, e) -> `DeltaPlan (b, splice_plan_aux e)
		    | `NewPlan (e) -> `NewPlan(splice_plan_aux e)
		    | `IncrPlan (sid, p, e, d) -> `IncrPlan (sid, p, splice_plan_aux e, d)
		    | `IncrDiffPlan (sid, p, e, d) -> `IncrDiffPlan (sid, p, splice_plan_aux e, d)
		    | _ -> raise InvalidExpression
	    end
    in
	splice_map_expr_aux m_expr


(* map_expression -> plan list *)
let get_base_relations m_expr =
    let rec gbr_aux ae acc =
	match ae with
	    | `MapExpression (`METerm _) | `MapExpression (`Delete _) -> acc
		  
	    | `MapExpression (`Delta(_,e))
	    | `MapExpression (`New(e)) 
	    | `MapExpression (`Init(_, e))
	    | `MapExpression (`Incr(_,_,_,e))
	    | `MapExpression (`IncrDiff(_,_,_,e))
	    | `MapExpression (`Insert(_,_, e))
	    | `MapExpression (`Update(_,_,_, e)) -> (gbr_aux (`MapExpression e) acc)

	    | `MapExpression (`Sum(l,r))
	    | `MapExpression (`Minus(l,r))
	    | `MapExpression (`Product(l,r))
	    | `MapExpression (`IfThenElse(_, l,r))
	    | `MapExpression (`Min(l,r))
	    | `MapExpression (`Max(l,r))
                -> (gbr_aux (`MapExpression r) (gbr_aux (`MapExpression l) acc))

	    | `MapExpression (`MapAggregate(fn,f,q)) ->
		  (gbr_aux (`Plan q) (gbr_aux (`MapExpression f) acc))

	    | `Plan (`TrueRelation) | `Plan (`FalseRelation) -> acc

	    | `Plan (`Relation _ as x) -> x::acc

            | `Plan (`Domain _) -> acc
		  
	    | `Plan (`Select (p,cq)) ->
		  begin
		      match p with
			  | `BTerm(`MEQ(m_expr)) | `BTerm(`MNEQ(m_expr))
			  | `BTerm(`MLT(m_expr)) | `BTerm(`MLE(m_expr))
			  | `BTerm(`MGT(m_expr)) | `BTerm(`MGE(m_expr))
                                ->
				(gbr_aux (`Plan cq) (gbr_aux (`MapExpression m_expr) acc))

			  | _ -> (gbr_aux (`Plan cq) acc)
		  end
		      
	    | `Plan (`Project (a, cq)) -> (gbr_aux (`Plan cq) acc)

	    | `Plan (`Union ch) -> List.fold_left (fun acc c -> gbr_aux (`Plan c) acc) acc ch

	    | `Plan (`Cross (l,r)) ->
		  (gbr_aux (`Plan r) (gbr_aux (`Plan l) acc))

	    | `Plan (`DeltaPlan (_, p)) | `Plan (`NewPlan (p))
            | `Plan (`IncrPlan (_,_, p,_)) | `Plan (`IncrDiffPlan (_, _,p,_)) ->
		  (gbr_aux (`Plan p) acc)

	    | _ -> raise InvalidExpression
    in
	gbr_aux (`MapExpression m_expr) []


(*
 * Binding helpers
 * Note: DBToaster compilation assumes unique column names
 *)

let get_bound_relations m_expr =
    let rec gbor_aux ae acc =
	match ae with
	    | `MapExpression (`METerm _) | `MapExpression (`Delete _) -> acc
		  
	    | `MapExpression (`Delta(_,e))
	    | `MapExpression (`New(e)) 
	    | `MapExpression (`Init(_,e))
	    | `MapExpression (`Incr(_,_,_,e))
	    | `MapExpression (`IncrDiff(_,_,_,e))
	    | `MapExpression (`Insert(_,_, e))
	    | `MapExpression (`Update(_,_,_, e)) -> (gbor_aux (`MapExpression e) acc)

	    | `MapExpression (`Sum(l,r))
	    | `MapExpression (`Minus(l,r))
	    | `MapExpression (`Product(l,r))
	    | `MapExpression (`IfThenElse(_, l,r))
	    | `MapExpression (`Min(l,r))
	    | `MapExpression (`Max(l,r))
                -> (gbor_aux (`MapExpression r) (gbor_aux (`MapExpression l) acc))

	    | `MapExpression (`MapAggregate(fn,f,q)) ->
		  (gbor_aux (`Plan q) (gbor_aux (`MapExpression f) acc))

	    | `Plan (`Relation _) -> acc

            | `Plan (`Domain _) -> acc

	    | `Plan (`Project(projs, `TrueRelation) as q) -> q::acc

	    | `Plan (`Select (p,cq)) ->
		  begin
		      match p with
			  | `BTerm(`MEQ(m_expr)) | `BTerm(`MNEQ(m_expr))
			  | `BTerm(`MLT(m_expr)) | `BTerm(`MLE(m_expr))
			  | `BTerm(`MGT(m_expr)) | `BTerm(`MGE(m_expr))
                                -> (gbor_aux (`Plan cq) (gbor_aux (`MapExpression m_expr) acc))

			  | _ -> (gbor_aux (`Plan cq) acc)
		  end

	    | `Plan (`Project (a, cq)) -> (gbor_aux (`Plan cq) acc)

	    | `Plan (`Union ch) -> List.fold_left (fun acc c -> gbor_aux (`Plan c) acc) acc ch

	    | `Plan (`Cross (l,r)) ->
		  (gbor_aux (`Plan r) (gbor_aux (`Plan l) acc))

	    | `Plan (`DeltaPlan (_, p)) | `Plan (`NewPlan (p))
            | `Plan (`IncrPlan (_,_,p,_)) | `Plan (`IncrDiffPlan(_,_,p,_)) ->
		  (gbor_aux (`Plan p) acc)

	    | `Plan (`TrueRelation) -> acc
	    | `Plan (`FalseRelation) -> acc
	    | _ -> raise InvalidExpression
    in
	gbor_aux (`MapExpression m_expr) []  


let get_bound_relation =
    function
	| `Insert (r, _) -> r
	| `Delete (r, _) -> r

let rec get_bound_attributes q =
    match q with
	| `TrueRelation -> []		  
	| `FalseRelation -> []

	| `Relation (n,f) -> List.map (fun (id,typ) -> `Qualified(n,id)) f
        | `Domain (_, attrs) -> attrs

	| `Select(pred, cq) -> get_bound_attributes cq
	| `Project(attrs, cq) -> List.map (fun (a,e) -> a) attrs

	| `Union ch -> get_bound_attributes (List.hd ch)
	| `Cross (l,r) ->
              (get_bound_attributes l)@(get_bound_attributes r)

	| `DeltaPlan (_, cq) | `NewPlan(cq)
        | `IncrPlan(_,_,cq,_) | `IncrDiffPlan(_,_,cq,_) -> (get_bound_attributes cq)


(* Note: performs qualified comparison if possible *)
(* TODO: think if we ever want to force unqualified comparison *)
let compare_attributes a1 a2 =
    match (a1, a2) with
	| (`Qualified (n1, f1), `Qualified (n2, f2)) -> n1 = n2 && f1 = f2
	| (`Qualified (n1, f1), `Unqualified f2) -> f1 = f2
	| (`Unqualified f1, `Qualified (n2, f2)) -> f1 = f2
	| (`Unqualified f1, `Unqualified f2) -> f1 = f2


let resolve_unbound_attributes attrs bindings =
    List.filter
	(fun x ->
            let matches = List.filter (compare_attributes x) bindings in
                (List.length matches) = 0)
	attrs

let rec get_unbound_attributes_from_expression expr include_vars =
    match expr with
	| `ETerm (`Attribute x) -> [x]
	| `ETerm (`Int _) | `ETerm (`Float _)
	| `ETerm (`String _) | `ETerm (`Long _) -> []
	| `ETerm (`Variable x) -> if include_vars then [`Unqualified x]  else []

	| `UnaryMinus e ->
	      get_unbound_attributes_from_expression e include_vars
		  
	| `Sum(l,r) | `Product(l,r) | `Minus(l,r) | `Divide(l,r) ->
	      (get_unbound_attributes_from_expression l include_vars)@
		  (get_unbound_attributes_from_expression r include_vars)

	| `Function(fid, args) ->
	      List.flatten
		  (List.map
		      (fun a ->
			  get_unbound_attributes_from_expression a include_vars)
		      args)

let rec get_unbound_attributes_from_predicate b_expr include_vars =
    match b_expr with
	| `BTerm(`True) | `BTerm(`False) -> []

	| `BTerm(`LT(l,r)) | `BTerm(`LE(l,r)) | `BTerm(`GT(l,r))
	| `BTerm(`GE(l,r)) | `BTerm(`EQ(l,r)) | `BTerm(`NE(l,r)) ->
	      (get_unbound_attributes_from_expression l include_vars)@
		  (get_unbound_attributes_from_expression r include_vars)

	| `BTerm(`MEQ(m_expr)) | `BTerm(`MNEQ(m_expr))
	| `BTerm(`MLT(m_expr)) | `BTerm(`MLE(m_expr))
	| `BTerm(`MGT(m_expr)) | `BTerm(`MGE(m_expr))
              -> (get_unbound_attributes_from_map_expression m_expr include_vars)

	| `And(l,r) | `Or(l,r) ->
	      (get_unbound_attributes_from_predicate l include_vars)@
		  (get_unbound_attributes_from_predicate r include_vars)

	| `Not(e) -> (get_unbound_attributes_from_predicate e include_vars)

and get_unbound_attributes_from_map_expression m_expr include_vars =
    match m_expr with
	| `METerm (`Attribute x) -> [x]
	| `METerm (`Int _) | `METerm (`Float _) | `METerm (`String _)
	| `METerm (`Long _) -> []
	| `METerm (`Variable x) -> if include_vars then [`Unqualified x] else []

	| `Delta (b,e) -> get_unbound_attributes_from_map_expression e include_vars
	      
	| `Sum (l,r) | `Minus (l,r) | `Product (l,r) | `Min(l,r) | `Max(l,r) ->
	      (get_unbound_attributes_from_map_expression l include_vars)@
		  (get_unbound_attributes_from_map_expression r include_vars)

	| `MapAggregate (fn,f,q) ->
	      let f_uba = get_unbound_attributes_from_map_expression f include_vars in
	      let q_ba = get_bound_attributes q in
		  (resolve_unbound_attributes f_uba q_ba)@
		      (get_unbound_attributes_from_plan q include_vars)

	| `New(e) | `Init(_,e)
        | `Incr(_,_,_,e) | `IncrDiff(_,_,_,e) ->
	      get_unbound_attributes_from_map_expression e include_vars

	| `Insert(_,m,e) | `Update(_,_,m,e) ->
	      (get_unbound_attributes_from_map_expression (`METerm (m)) include_vars)@
	          (get_unbound_attributes_from_map_expression e include_vars)

	| `Delete(_,m) ->
	      get_unbound_attributes_from_map_expression (`METerm (m)) include_vars

	| `IfThenElse(b,l,r) ->
	      (get_unbound_attributes_from_predicate (`BTerm (b)) include_vars)@ 
	          (get_unbound_attributes_from_map_expression l include_vars)@
		  (get_unbound_attributes_from_map_expression r include_vars)

and get_unbound_attributes_from_plan q include_vars =
    match q with
	| `TrueRelation | `FalseRelation | `Relation _ | `Domain _ -> []

	| `Select(pred, cq) ->
	      let pred_uba = get_unbound_attributes_from_predicate pred include_vars in
	      let cq_ba = get_bound_attributes cq in
		  (resolve_unbound_attributes pred_uba cq_ba)@
		      (get_unbound_attributes_from_plan cq include_vars)

	| `Project(attrs, cq) ->
	      let attrs_uba = 
		  List.flatten
		      (List.map
			  (fun (a,e) -> get_unbound_attributes_from_expression e include_vars)
			  attrs)
	      in
	      let cq_ba = get_bound_attributes cq in
		  (resolve_unbound_attributes attrs_uba cq_ba)@
		      (get_unbound_attributes_from_plan cq include_vars)

	| `Union ch ->
	      List.flatten
		  (List.map
		      (fun c -> get_unbound_attributes_from_plan c include_vars) ch)

	| `Cross (l, r) ->
	      (get_unbound_attributes_from_plan l include_vars)@
		  (get_unbound_attributes_from_plan r include_vars)

	| `DeltaPlan(_,e) | `NewPlan(e) | `IncrPlan(_,_,e,_) | `IncrDiffPlan(_,_,e,_) ->
	      get_unbound_attributes_from_plan e include_vars

let get_unbound_map_attributes_from_plan f q include_vars =
    let f_uba = get_unbound_attributes_from_map_expression f include_vars in
    let q_ba = get_bound_attributes q in
    let unresolved_fa = resolve_unbound_attributes f_uba q_ba in
    let unresolved_qa = get_unbound_attributes_from_plan q include_vars
    in
        print_endline "get_unbound_map_attributes_from_plan:";
        print_endline ("\tf_uba: "^(string_of_attribute_identifier_list f_uba));
        print_endline ("\tq_ba: "^(string_of_attribute_identifier_list q_ba));
        print_endline ("\tu_fa: "^(string_of_attribute_identifier_list unresolved_fa));
        print_endline ("\tu_qa: "^(string_of_attribute_identifier_list unresolved_qa));
        unresolved_fa@unresolved_qa

(* Returns the first schema encountered top-down in a plan, i.e. the nearest
   renaming or base relation *)
let rec get_flat_schema plan =
    match plan with 
        | `TrueRelation | `FalseRelation -> []
        | `Relation (n,f) -> attribute_identifiers_of_field_list n f
        | `Domain (_, attrs) -> attrs
        | `Select (pred, cq) -> get_flat_schema cq
        | `Project (projs, cq) -> List.map (fun (aid, _) -> aid) projs
        | `Union ch -> List.flatten (List.map get_flat_schema ch)
        | `Cross (l, r) -> (get_flat_schema l)@(get_flat_schema r)
        | `DeltaPlan (_, cq) | `NewPlan cq -> get_flat_schema cq
        | `IncrPlan (_, _, _,d) | `IncrDiffPlan (_, _, _,d) -> d

(* Returns all attributes used in the map expression, except unbound attributes.
 * Note this includes intermediate attributes such as those from projections. *)
let rec get_attributes_used_in_map_expression m_expr =
    let get_query_attributes attrs query_attrs =
        List.filter (fun a -> List.exists (compare_attributes a) query_attrs) attrs
    in
    match m_expr with
	| `METerm _ -> []

	| `Delta (b,e) -> get_attributes_used_in_map_expression e
	      
	| `Sum (l,r) | `Minus (l,r) | `Product (l,r) | `Min(l,r) | `Max(l,r) ->
	      (get_attributes_used_in_map_expression l)@
		  (get_attributes_used_in_map_expression r)

	| `MapAggregate (fn,f,q) ->
              let f_uba = get_unbound_attributes_from_map_expression f false in
	      let q_ba = get_bound_attributes q in
              let f_used =
                  (get_attributes_used_in_map_expression f)@
                      (get_query_attributes f_uba q_ba)
              in
                  get_attributes_used_in_plan q f_used

	| `New(e) | `Init (_,e)
        | `Incr(_,_,_,e) | `IncrDiff(_,_,_,e) ->
	      get_attributes_used_in_map_expression e

	| `Insert(_,m,e) | `Update(_,_,m,e) ->
	      (get_attributes_used_in_map_expression (`METerm (m)))@
	          (get_attributes_used_in_map_expression e)

	| `Delete(_,m) ->
	      get_attributes_used_in_map_expression (`METerm (m))

	| `IfThenElse(b,l,r) ->
              (get_attributes_used_in_map_expression l)@
		  (get_attributes_used_in_map_expression r)

and get_attributes_used_in_plan plan attrs_used_above =
    let intersect_attributes attrs existing_attrs =
        List.filter (fun a -> List.exists (compare_attributes a) existing_attrs) attrs
    in
    let disjoint_attributes attrs existing_attrs =
        List.filter (fun a -> not(List.exists (compare_attributes a) existing_attrs)) attrs
    in
    match plan with
        | `TrueRelation | `FalseRelation | `Relation _ | `Domain _ -> attrs_used_above

	| `Select(pred, cq) ->
	      let pred_uba = get_unbound_attributes_from_predicate pred false in
	      let cq_ba = get_bound_attributes cq in
              let pred_used =
                  (match pred with
                      | `BTerm(`MEQ(me)) | `BTerm(`MNEQ(me)) 
                      | `BTerm(`MLT(me)) | `BTerm(`MLE(me)) 
                      | `BTerm(`MGT(me)) | `BTerm(`MGE(me)) 
                            -> (get_attributes_used_in_map_expression me)
                      | _ -> [])
                  @(intersect_attributes pred_uba cq_ba)
              in
              let used_above_cq =
                  attrs_used_above@(disjoint_attributes pred_used attrs_used_above)
              in
                  get_attributes_used_in_plan cq used_above_cq

	| `Project(attrs, cq) ->
              (* Assumes attrs_used_above is a subset of attrs *)
              let attrs_used =
                  intersect_attributes
                      (List.map (fun (a,_) -> a) attrs) attrs_used_above
              in
	      let attrs_uba = 
		  List.flatten
		      (List.map
			  (fun a -> let e = List.assoc a attrs in
                              get_unbound_attributes_from_expression e false)
			  attrs_used)
	      in
	      let cq_ba = get_bound_attributes cq in
              let projs_used = intersect_attributes attrs_uba cq_ba in
                  get_attributes_used_in_plan cq (attrs_used_above@projs_used)

	| `Union ch ->
              (* helper function to get index of element in list *)
              let idx el l =
                  let (f, pos) = 
                      List.fold_left
                          (fun (found, cnt) el2 ->
                              if found || (el = el2) then (true, cnt) else (false, cnt+1) )
                          (false, 0) l
                  in
                      if f then pos else -1
              in
              let ch_schema = List.map get_flat_schema ch in
              let all_idx_used =
                  List.fold_left2
                      (fun acc c c_schema ->
                          (* get schema attributes used above *)
                          let c_used = intersect_attributes c_schema attrs_used_above in

                          (* get attribute position in the schema *)
                          let new_idx =
                              List.filter
                                  (fun i -> not (List.mem i acc))
                                  (List.map
                                      (fun attr ->
                                          let attr_pos = idx attr c_schema in
                                              if attr_pos = -1 then raise InvalidExpression
                                              else attr_pos)
                                      c_used)
                          in
                              acc@new_idx)
                      [] ch ch_schema
              in
              (* get all attributes across each children corresponding to positions used *)
              let ch_attrs_used =
                  List.map
                      (fun c_schema -> List.map (List.nth c_schema) all_idx_used)
                      ch_schema
              in
              (* recur over each child *)
                  List.fold_left2
                      (fun acc c c_attrs_used ->
                          let new_c_attrs_used = get_attributes_used_in_plan c c_attrs_used in
                          let dedup_attrs_used = disjoint_attributes new_c_attrs_used acc in
                              acc@dedup_attrs_used)
                      attrs_used_above ch ch_attrs_used

	| `Cross (l, r) ->
              let l_ba = get_bound_attributes l in
              let r_ba = get_bound_attributes r in
                  (get_attributes_used_in_plan l
                      (intersect_attributes attrs_used_above l_ba))@
                  (get_attributes_used_in_plan r
                      (intersect_attributes attrs_used_above r_ba))

	| `DeltaPlan(_,e) | `NewPlan(e) ->
              get_attributes_used_in_plan e attrs_used_above

        | `IncrPlan(_,_,e,d) | `IncrDiffPlan(_,_,e,d) ->
              (* check each domain attribute is used *)
              let attrs_used = intersect_attributes d attrs_used_above in
                  if ( (List.length attrs_used) != (List.length d) ) then
                      begin
                          print_endline ("Attrs used above: "^
                              (string_of_attribute_identifier_list attrs_used_above));
                          print_endline ("Subplan:\n"^(indented_string_of_plan plan 0));
                          print_endline ("Domain: "^(string_of_attribute_identifier_list d));
                          raise (ValidationException "Non-minimal domain found.");
                      end;
                  get_attributes_used_in_plan e attrs_used_above


let rec find_cmp_aggregate m_expr =
    match m_expr with
        | `METerm _ -> false
        | `Sum (l,r) | `Minus (l,r) | `Product (l,r)
        | `Min (l,r) | `Max (l,r) ->
              (find_cmp_aggregate l) || (find_cmp_aggregate r)

        | `MapAggregate (`Sum, f, q) ->
              (find_cmp_aggregate f) || (find_cmp_aggregate_plan q)

        | `MapAggregate (`Min, f, q) | `MapAggregate(`Max, f, q) -> true

        | `Delta (_, e) | `New e | `Init(_,e)
        | `Incr (_,_,_,e) | `IncrDiff (_,_,_,e)
        | `Insert (_,_,e) | `Update (_,_,_,e) -> find_cmp_aggregate e
        | `Delete _ -> false
        | `IfThenElse(`MEQ(p), l, r) | `IfThenElse(`MNEQ(p), l, r) 
        | `IfThenElse(`MLT(p), l, r) | `IfThenElse(`MLE(p), l, r) 
        | `IfThenElse(`MGT(p), l, r) | `IfThenElse(`MGE(p), l, r) 
            -> List.exists (fun x -> x) (List.map find_cmp_aggregate [p;l;r])

        | `IfThenElse(_, l, r) ->  (find_cmp_aggregate l) || (find_cmp_aggregate r)

and find_cmp_aggregate_plan q =
    match q with
        | `TrueRelation | `FalseRelation | `Relation _ | `Domain _ -> false
        | `Select(pred, cq) -> 
              (match pred with
                  | `BTerm(`MEQ(m_expr)) | `BTerm(`MNEQ(m_expr))
                  | `BTerm(`MLT(m_expr)) | `BTerm(`MLE(m_expr))
                  | `BTerm(`MGT(m_expr)) | `BTerm(`MGE(m_expr))
                        -> find_cmp_aggregate m_expr
                  | _ -> false)
              || (find_cmp_aggregate_plan cq)

        | `Project (_, cq) -> find_cmp_aggregate_plan cq
        | `Union ch -> List.exists find_cmp_aggregate_plan ch
        | `Cross (l,r) ->
              (find_cmp_aggregate_plan l) || (find_cmp_aggregate_plan r)
        | `DeltaPlan (_, cq) | `NewPlan cq
        | `IncrPlan (_,_,cq,_) | `IncrDiffPlan (_,_,cq,_) -> find_cmp_aggregate_plan cq


(* returns whether any bindings are used by m_expr and the bound variables *)
let is_binding_used m_expr bindings =
    (* returns vars as `Unqualified(var_name) *)
    let m_ubav = get_unbound_attributes_from_map_expression m_expr true in
    let bindings_used =
        List.filter
            (fun x -> match x with
                | `BindMapExpr(var_id, _, _) ->
                      List.mem (`Unqualified(var_id)) m_ubav
                | _ -> false)
            bindings
    in
    let binding_used_rv =
        List.map
            (fun x -> match x with
                | `BindMapExpr(v, bc, _) -> v
                | _ -> raise InvalidExpression)
            bindings_used
    in
        match binding_used_rv with
            | [] -> (false, [])
            | x -> (true, x)



(* Monotonicity analysis
 *
 * TODO: detecting insert/update/delete monotonicity
 *
 * Database monotonicity.
 * -- assume expression f[A]
 * -- compute Delta f | Delta A
 * -- TODO: multivariate monotonicity analysis
 *)

exception MonotonicityException

type monotonicity = Inc | Dec | Same | Undef

(* Note: captures set cardinalities not values *)
type set_monotonicity = IncSet | DecSet | SameSet | UndefSet

let string_of_monotonicity m =
    match m with
	| Inc -> "Inc" | Dec -> "Dec" | Same -> "Same" | Undef -> "Undef"

let string_of_plan_monotonicity mp =
    match mp with
	| IncSet -> "IncS" | DecSet -> "DecS" | SameSet -> "SameS" | UndefSet -> "UndefS"

let bin_op_monotonicity lm rm =
    match (lm, rm) with
	| (Inc, Inc) | (Inc, Same) | (Same, Inc) -> Inc
	| (Dec, Dec) | (Dec, Same) | (Same, Dec) -> Dec
	| (Same, Same) -> Same
	| (Inc, Dec) | (Dec, Inc)	| (_, Undef) | (Undef, _) -> Undef 

let agg_monotonicity fm qm =
    match (fm, qm) with
	| (Inc, IncSet) | (Inc, SameSet) | (Same, IncSet) -> Inc
	| (Dec, DecSet) | (Dec, SameSet) | (Same, DecSet) -> Dec
	| (Same, SameSet) -> Same
	| (_,_) -> Undef

(* Note: asymmetry between (Same, IncSet) and (Same, DecSet)
 * -- these rules are conservative towards IncSet *)
let select_monotonicity pm cm =
    match (pm, cm) with
	| (Inc, IncSet) | (Inc, SameSet) -> IncSet
	| (Dec, DecSet) | (Dec, SameSet) | (Same, DecSet) -> DecSet
	| (Same, IncSet) | (Same, SameSet) -> SameSet
	| (Dec, IncSet) | (Inc, DecSet) | (Undef, _) | (_, UndefSet) -> UndefSet

let union_monotonicity accm im =
    match (accm, im) with
	| (IncSet, IncSet) | (IncSet, SameSet) | (SameSet, IncSet) -> IncSet
	| (DecSet, DecSet) | (DecSet, SameSet) | (SameSet, DecSet) -> DecSet
	| (SameSet, SameSet) -> SameSet
	| (IncSet, DecSet) | (DecSet, IncSet) | (_, UndefSet) | (UndefSet,_) -> UndefSet

let cross_monotonicity lm rm =
    match (lm, rm) with
	| (IncSet, IncSet) | (IncSet, SameSet) | (SameSet, IncSet) -> IncSet
	| (DecSet, DecSet) | (DecSet, SameSet) | (SameSet, DecSet) -> DecSet
	| (SameSet, SameSet) -> SameSet
	| (IncSet, DecSet) | (DecSet, IncSet) | (_, UndefSet) | (UndefSet,_) -> UndefSet

let join_monotonicity pm lm rm =
    match pm with
	| Inc ->
	      begin
		  match (lm, rm) with
		      | (IncSet, IncSet) | (IncSet, SameSet) | (SameSet, IncSet) | (SameSet, SameSet) -> IncSet
		      | _ -> UndefSet
	      end
	| Dec ->
	      begin
		  match (lm, rm) with
		      | (DecSet, DecSet) | (DecSet, SameSet) | (SameSet, DecSet) | (SameSet, SameSet) -> DecSet
		      | _ -> UndefSet
	      end
	| Same ->
	      begin
		  match (lm, rm) with
		      | (IncSet, IncSet) | (IncSet, SameSet) | (SameSet, IncSet) -> IncSet
		      | (DecSet, DecSet) | (DecSet, SameSet) | (SameSet, DecSet) -> DecSet
		      | (SameSet, SameSet) -> SameSet
		      | _ -> UndefSet
	      end
	| Undef -> UndefSet


(* Note these are value oriented monotonicities *)
let rec compute_expr_monotonicity attr attr_m expr =
    match expr with
	| `ETerm (`Attribute aid) ->
	      let matched_attr =
		  match (aid, attr) with
		      | (`Qualified (n1, f1), `Qualified (n2, f2)) -> n1 = n2 && f1 = f2
		      | (`Qualified (n1, f1), `Unqualified f2) -> f1 = f2
		      | (`Unqualified f1, `Qualified (n2, f2)) -> f1 = f2
		      | (`Unqualified f1, `Unqualified f2) -> f1 = f2
	      in 
		  if matched_attr then attr_m else Same

	| `ETerm (_) -> Same
	| `UnaryMinus e ->
	      begin
		  let em = compute_expr_monotonicity attr attr_m e in
		      match em with
			  | Inc -> Dec
			  | Dec -> Inc 
			  | Same -> Same
			  | Undef -> Undef
	      end
	| `Sum (l,r) | `Product (l,r) ->
	      let lm = compute_expr_monotonicity attr attr_m l in
	      let rm = compute_expr_monotonicity attr attr_m r in
		  begin
		      match (lm, rm) with
			  | (Inc, Inc) | (Inc, Same) | (Same, Inc) -> Inc
			  | (Dec, Dec) | (Dec, Same) | (Same, Dec) -> Dec
			  | (Same, Same) -> Same
			  | (Inc, Dec) | (Dec, Inc) | (Undef, _) | (_, Undef) -> Undef
		  end

	| `Minus (l,r) | `Divide (l,r) ->
	      let lm = compute_expr_monotonicity attr attr_m l in
	      let rm = compute_expr_monotonicity attr attr_m r in
		  begin
		      match (lm, rm) with
			  | (Inc, Dec) | (Inc, Same) | (Same, Dec) -> Inc
			  | (Dec, Inc) | (Dec, Same) | (Same, Inc) -> Dec
			  | (Same, Same) -> Same
			  | (Inc, Inc) | (Dec, Dec) | (Undef, _) | (_, Undef) -> Undef
		  end
		      
	(* TODO: allow users to specify monotonicity properties as in SQL-Server C# interface *) 
	| `Function (fid, args) -> Undef

let rec compute_bool_expr_monotonicity attr attr_m b_expr =
    match b_expr with
	| `BTerm (`True) | `BTerm(`False) -> Undef

	| `BTerm (`LT(l,r)) | `BTerm (`LE(l,r)) ->
	      let lm = compute_expr_monotonicity attr attr_m l in
	      let rm = compute_expr_monotonicity attr attr_m r in
		  begin
		      match (lm, rm) with
			  | (Dec, Same) | (Dec, Inc) | (Same, Inc) -> Inc
			  | (Inc, Same) | (Inc, Dec) | (Same, Dec) -> Dec
			  | (Same, Same) -> Same
			  | (Inc, Inc) | (Dec, Dec) | (Undef, _) | (_, Undef) -> Undef
		  end

	| `BTerm (`GT(l,r)) | `BTerm (`GE(l,r)) ->
	      let lm = compute_expr_monotonicity attr attr_m l in
	      let rm = compute_expr_monotonicity attr attr_m r in
		  begin
		      match (lm, rm) with
			  | (Inc, Same) | (Inc, Dec) | (Same, Dec) -> Inc
			  | (Dec, Same) | (Same, Inc) | (Dec, Inc) -> Dec
			  | (Same, Same) -> Same
			  | (Inc, Inc) | (Dec, Dec) | (Undef, _) | (_, Undef) -> Undef
		  end

	| `BTerm (`EQ(l,r)) | `BTerm (`NE(l,r)) ->
	      let lm = compute_expr_monotonicity attr attr_m l in
	      let rm = compute_expr_monotonicity attr attr_m r in
		  begin
		      match (lm, rm) with
			  | (Same, Same) -> Same
			  | _ -> Undef
		  end

	| `BTerm (`MEQ(m_expr)) | `BTerm (`MNEQ(m_expr))
	| `BTerm (`MLT(m_expr)) | `BTerm (`MLE(m_expr))
	| `BTerm (`MGT(m_expr)) | `BTerm (`MGE(m_expr))
              -> compute_monotonicity attr attr_m m_expr

	| `And (l,r) ->
	      let lm = compute_bool_expr_monotonicity attr attr_m l in
	      let rm = compute_bool_expr_monotonicity attr attr_m r in
		  begin
		      match (lm, rm) with
			  | (Inc, Inc) -> Inc
			  | (Dec, Dec) | (Dec, Inc) | (Dec, Same) | (Inc, Dec) | (Same, Dec) -> Dec
			  | (Inc, Same) | (Same, Inc) | (Same, Same) -> Same
			  | (Undef, _) | (_, Undef) -> Undef
		  end

	| `Or (l,r) ->
	      let lm = compute_bool_expr_monotonicity attr attr_m l in
	      let rm = compute_bool_expr_monotonicity attr attr_m r in
		  begin
		      match (lm, rm) with
			  | (Inc, Inc) | (Inc, Dec) | (Inc, Same) | (Dec, Inc) | (Same, Inc) -> Inc
			  | (Dec, Dec) -> Dec
			  | (Same, Same) | (Dec, Same) | (Same, Dec) -> Same
			  | (Undef, _) | (_, Undef) -> Undef
		  end

	| `Not (e) ->
	      begin
		  let em = compute_bool_expr_monotonicity attr attr_m e in 
		      match em with
			  | Inc -> Dec
			  | Dec -> Inc
			  | Same -> Same
			  | Undef -> Undef
	      end

and compute_monotonicity attr attr_m m_expr =
    match m_expr with
	| `METerm (`Attribute aid) -> 
	      let matched_attr =
		  match (aid, attr) with
		      | (`Qualified (n1, f1), `Qualified (n2, f2)) -> n1 = n2 && f1 = f2
		      | (`Qualified (n1, f1), `Unqualified f2) -> f1 = f2
		      | (`Unqualified f1, `Qualified (n2, f2)) -> f1 = f2
		      | (`Unqualified f1, `Unqualified f2) -> f1 = f2
	      in 
		  if matched_attr then attr_m else Same

	| `METerm (_) -> Same

	| `Sum(l,r) | `Minus(l,r) | `Product(l,r) | `Min(l,r) | `Max(l,r) | `IfThenElse(_, l, r) ->
	      let lm = compute_monotonicity attr attr_m l in
	      let rm = compute_monotonicity attr attr_m r in
		  bin_op_monotonicity lm rm

	| `MapAggregate(fn,f,q) ->
	      let fm = compute_monotonicity attr attr_m f in
	      let qm = compute_plan_monotonicity attr attr_m q in
		  agg_monotonicity fm qm
		      
	| `Delta _ | `New _ | `Init _
        | `Incr _ | `IncrDiff _
        | `Insert _ | `Update _ | `Delete _ -> raise MonotonicityException

and compute_plan_monotonicity attr attr_m q =
    match q with
	| `Relation (name, fields) ->
	      let attr_rel =
		  match attr with
		      | `Qualified (n,f) -> n = name &&
		  (List.exists (fun (id, ty) -> id = f) fields)
		      | `Unqualified f -> (List.exists (fun (id, ty) -> id = f) fields)
	      in
		  if attr_rel then
		      (match attr_m with
			   | Inc -> IncSet | Dec -> DecSet | Same -> SameSet | Undef -> UndefSet)
		  else SameSet

	| `Select (b_expr, cq) ->
	      let bem = compute_bool_expr_monotonicity attr attr_m b_expr in
	      let cqm = compute_plan_monotonicity attr attr_m cq in
		  select_monotonicity bem cqm

	| `Project (attrs, cq) ->
	      (* TODO: think about duplicate elimination for set semantics *)
	      compute_plan_monotonicity attr attr_m cq

	| `Union children ->
	      let chm = List.map (compute_plan_monotonicity attr attr_m) children in
		  if (List.length chm) = 1 then
		      (List.hd chm)
		  else
		      List.fold_left union_monotonicity (List.hd chm) (List.tl chm)
			  
	| `Cross (l,r) ->
	      cross_monotonicity
		  (compute_plan_monotonicity attr attr_m l)
		  (compute_plan_monotonicity attr attr_m r)

        (* Singleton relations, domains, incr terms should not exist yet,
           * i.e. this should be done before compilation *)
	| `TrueRelation | `FalseRelation | `Domain _
        | `DeltaPlan(_) | `NewPlan(_)
        | `IncrPlan(_) | `IncrDiffPlan(_) -> raise MonotonicityException


let rec is_monotonic b_expr attr =
    match b_expr with
	| `BTerm x ->
	      begin
		  match x with
		      | `MEQ(m_expr) | `MNEQ(m_expr)
		      | `MLT(m_expr) | `MLE(m_expr) 
		      | `MGT(m_expr) | `MGE(m_expr)
                            ->
			    let me_mon = compute_monotonicity attr Inc m_expr in
				(me_mon = Inc) || (me_mon = Dec)
				    
		      | _ ->
			    let be_mon = compute_bool_expr_monotonicity attr Inc b_expr in
				(be_mon = Inc) || (be_mon = Dec) 
	      end

	| _ ->
	      let be_mon = compute_bool_expr_monotonicity attr Inc b_expr in
		  (be_mon = Inc) || (be_mon = Dec)

let rec get_monotonicity b_expr attr =
    match b_expr with
	| `BTerm x ->
	      begin
		  match x with
		      | `MEQ(m_expr) | `MNEQ(m_expr)
		      | `MLT(m_expr) | `MLE(m_expr) 
		      | `MGT(m_expr) | `MGE(m_expr)
                            -> compute_monotonicity attr Inc m_expr
		      | _ -> compute_bool_expr_monotonicity attr Inc b_expr
	      end
	| _ -> compute_bool_expr_monotonicity attr Inc b_expr

(*
 * Code generation
 *)

type map_identifier = identifier

(* Datastructure notes:
 * underlying map must support: begin(), end()
 *     iterator = begin(), iterator = end()
 *     operator[](val)
 *     insert(val)
 *     erase(val)
 * underlying multiset must support:
 *     iterator = begin(), iterator = end()
 *     iterator = find(val)
 *     insert(val)
 *     delete(iterator)
 * other operations: operator*(iterator), get<idx>(val)
 *)
type datastructure = [
| `Map of map_identifier * (field list) * type_identifier 
| `Set of relation_identifier * (field list)
| `Multiset of relation_identifier * (field list) ]

type code_variable = variable_identifier

type map_key = map_identifier * (code_variable list)

type map_iterator = [ `Begin of map_identifier | `End of map_identifier ]

type relation_variable = relation_identifier * (field list)

type declaration = [
| `Variable of code_variable * type_identifier
| `Relation of relation_identifier * (field list) 
| `Map of map_identifier * (field list) * type_identifier
| `Domain of relation_identifier * (field list) ]

type code_terminal = [
| `Int of int
| `Float of float
| `String of string
| `Long of int64
| `Variable of code_variable
| `MapAccess of map_key
| `MapContains of map_key
| `MapIterator of map_iterator ]

type arith_code_expression = [
| `CTerm of code_terminal
| `Sum of arith_code_expression * arith_code_expression
| `Minus of arith_code_expression * arith_code_expression
| `Product of arith_code_expression * arith_code_expression
| `Min of arith_code_expression * arith_code_expression	
| `Max of arith_code_expression * arith_code_expression	]

and bool_code_term = [ `True | `False 
| `LT of arith_code_expression * arith_code_expression
| `LE of arith_code_expression * arith_code_expression
| `GT of arith_code_expression * arith_code_expression
| `GE of arith_code_expression * arith_code_expression
| `EQ of arith_code_expression * arith_code_expression
| `NE of arith_code_expression * arith_code_expression ]

type bool_code_expression = [
| `BCTerm of bool_code_term
| `Not of bool_code_expression
| `And of bool_code_expression * bool_code_expression
| `Or of bool_code_expression * bool_code_expression ]

type code_expression = [
| `Declare of declaration
| `Assign of code_variable * arith_code_expression
| `AssignMap of map_key * arith_code_expression 
| `EraseMap of map_key * arith_code_expression
| `InsertTuple of datastructure * (code_variable list)
| `DeleteTuple of datastructure * (code_variable list)
| `Eval of arith_code_expression 
| `IfNoElse of bool_code_expression * code_expression
| `IfElse of bool_code_expression * code_expression * code_expression
| `ForEach of datastructure * code_expression
| `Block of code_expression list
| `Return of arith_code_expression
| `Handler of function_identifier * (field list) * type_identifier * code_expression list ]


(* Basic code type helpers *)
let is_block c_expr =
    match c_expr with | `Block _ -> true | _ -> false

let identifier_of_datastructure =
    function | `Map (n,_,_) | `Set (n,_) | `Multiset (n,_) -> n

let datastructure_of_declaration decl =
    match decl with
        | `Variable (n,ty) -> raise (CodegenException ("Invalid datastructure: "^n))
        | `Relation(n,f) -> `Multiset(n,f)
        | `Map (id, f, rt) -> `Map(id, f, rt)
        | `Domain(n,f) -> `Set(n,f)

let identifier_of_declaration decl =
    match decl with
        | `Variable (v, ty) -> v
        | `Relation(n, f) -> n
        | `Map (n, f, rt) -> n
        | `Domain(n, f) -> n

(*
 * C-code generation helpers
 *)


let ctype_of_type_identifier t = t

let ctype_of_datastructure_fields f =
    List.fold_left
	(fun acc (_,t) ->
	     (if (String.length acc) = 0 then "" else acc^",")^
		 (ctype_of_type_identifier t))
	"" f

let ctype_of_datastructure =
    function 
	| `Map (n,f,r) ->
	      let key_type = 
		  let ftype = ctype_of_datastructure_fields f in
                      match f with
                          | [] -> raise (CodegenException "Invalid datastructure type, no fields found.")
                          | [x] -> ftype
                          | _ -> "tuple<"^ftype^">" 
	      in
		  "map<"^key_type^","^(ctype_of_type_identifier r)^">"

        | (`Set(n, f) as ds) 
	| (`Multiset (n,f) as ds) ->
	      let (el_type, nested_type) = 
		  let ftype = ctype_of_datastructure_fields f in
		      if (List.length f) = 1 then (ftype, false) else ("tuple<"^ftype^">", true)
	      in
              let ds_type = match ds with | `Set _ -> "set" | `Multiset _ -> "multiset" in
		  ds_type^"<"^el_type^(if nested_type then " >" else ">")

let ctype_of_code_var_list =
    function
        | [] -> raise (CodegenException "Invalid code var list")
        | [x] -> x
        | x -> 
              let svl =
                  List.fold_left
	              (fun acc v -> (if (String.length acc) = 0 then "" else acc^", ")^v)
	              "" x
              in
                  "make_tuple("^svl^")"

(* TODO *)
let ctype_of_arith_code_expression ac_expr = "int"

let iterator_ref = ref 0

let point_iterator_declaration_of_datastructure ds =
    match ds with
	| `Map(id, f, _) | `Set(id, f) | `Multiset (id, f) ->
	      let it_id = incr iterator_ref; (string_of_int !iterator_ref) in
	      let it_typ = (ctype_of_datastructure ds)^"::iterator" in
	      let point_it = id^"_it"^it_id in
		  (point_it, it_typ^" "^point_it)
            
let range_iterator_declarations_of_datastructure ds =
    match ds with
	| `Map(id, f, _) | `Set(id, f) | `Multiset (id, f) ->
	      let it_id = incr iterator_ref; (string_of_int !iterator_ref) in
	      let it_typ = (ctype_of_datastructure ds)^"::iterator" in
	      let begin_it = id^"_it"^it_id in
	      let end_it = id^"_end"^it_id in
		  (begin_it, end_it, it_typ^" "^begin_it, it_typ^" "^end_it)

let field_declarations_of_datastructure ds iterator tab =
    let deref = match ds with
	| `Map _ -> iterator^"->first"
        | `Set _ | `Multiset _ -> "*"^iterator
    in 
	match ds with
	    | `Map(id, f, _) | `Set(id, f) | `Multiset (id, f) ->
		  if (List.length f) = 1 then
		      let (id, typ) = List.hd f in
			  tab^typ^" "^id^" = "^deref^";\n"
		  else 
		      let (_, r) =
			  List.fold_left
			      (fun (cnt, acc) (id, typ) ->
				   (cnt+1,
				    acc^tab^
					(typ^" "^id^" = get<"^(string_of_int cnt)^">("^deref^");\n")))
			      (0, "") f
		      in
			  r

let handler_name_of_event ev =
    match ev with
	| `Insert (n, _) -> "on_insert_"^n
	| `Delete (n, _) -> "on_delete_"^n

(* 
 * Code generation main
 *)

let string_of_datastructure =
    function | `Map (n,f,_) | `Set (n,f) -> n | `Multiset (n,f) -> n

let string_of_datastructure_fields =
    function
	| `Map (n,f,_) | `Set(n,f) | `Multiset (n,f) ->
	      List.fold_left
		  (fun acc (id, typ) ->
		       (if (String.length acc) = 0 then "" else acc^", ")^
			   typ^" "^id)
		  "" f

let string_of_code_var_list vl =
    List.fold_left
	(fun acc v -> (if (String.length acc) = 0 then "" else acc^", ")^v)
	"" vl

let string_of_map_key (mid, keys) = mid^"["^(ctype_of_code_var_list keys)^"]"

let string_of_code_expr_terminal cterm =
    match cterm with 
	| `Int i -> string_of_int i
	| `Float f -> string_of_float f
	| `Long l -> Int64.to_string l
	| `String s -> "'"^s^"'"
	| `Variable v -> v
	| `MapAccess mk -> string_of_map_key mk
	| `MapContains (mid, keys) -> mid^".find("^(ctype_of_code_var_list keys)^")"
	| `MapIterator (`Begin(mid)) -> mid^".begin()"
	| `MapIterator (`End(mid)) -> mid^".end()"

let rec string_of_arith_code_expression ac_expr =
    match ac_expr with
	| `CTerm e -> string_of_code_expr_terminal e
	| `Sum (l,r) ->
	      (string_of_arith_code_expression l)^" + "^
		  (string_of_arith_code_expression r)

	| `Minus (l,r) ->
	      (string_of_arith_code_expression l)^" - "^
		  (string_of_arith_code_expression r)

	| `Product(l,r) ->
	      (string_of_arith_code_expression l)^" * "^
		  (string_of_arith_code_expression r)

	| `Min(l,r) ->
	      "min("^(string_of_arith_code_expression l)^", "^
		  (string_of_arith_code_expression r)^")"

	| `Max(l,r) ->
	      "max("^(string_of_arith_code_expression l)^", "^
		  (string_of_arith_code_expression r)^")"

let rec string_of_bool_code_expression bc_expr =
    let dispatch_expr l r op =
	(string_of_arith_code_expression l)^op^(string_of_arith_code_expression r)
    in
	match bc_expr with
	    | `BCTerm b ->
		  begin
		      match b with
			  | `LT (l,r) -> dispatch_expr l r "<"
			  | `LE (l,r) -> dispatch_expr l r "<="
			  | `GT (l,r) -> dispatch_expr l r ">"
			  | `GE (l,r) -> dispatch_expr l r ">="
			  | `EQ (l,r) -> dispatch_expr l r "=="
			  | `NE (l,r) -> dispatch_expr l r "!="
			  | `True -> "true"
			  | `False -> "false"
		  end
	    | `Not e -> "not("^(string_of_bool_code_expression e)^")"
	    | `And (l,r) ->
		  ("("^(string_of_bool_code_expression l)^") and ("^
		       (string_of_bool_code_expression r)^")")
	    | `Or (l,r) ->
		  ("("^(string_of_bool_code_expression l)^") or ("^
		       (string_of_bool_code_expression r)^")")

let rec string_of_code_expression c_expr =
    let string_of_code_block c_expr_l =
	List.fold_left
	    (fun acc c_expr ->
		 (if (String.length acc) = 0 then "" else acc^"\n")^
		     (string_of_code_expression c_expr))
	    "" c_expr_l
    in
	match c_expr with
	    | `Declare x ->
                  begin
                      match x with
		          | `Variable(n, typ) -> typ^" "^n^";"
                          | (`Relation _ as y) | (`Map _ as y) | (`Domain _ as y) ->
                                let ctype = ctype_of_datastructure (datastructure_of_declaration y) in
                                    ctype^" "^(identifier_of_declaration y)^";"
                  end

	    | `Assign(v,ac) ->
		  v^" = "^(string_of_arith_code_expression ac)^";"

	    | `AssignMap(mk, vc) ->
		  (string_of_map_key mk)^" = "^(string_of_arith_code_expression vc)^";"

	    | `EraseMap(mk, c) ->
		  (string_of_map_key mk)^".erase("^(string_of_arith_code_expression c)^");"

            | `InsertTuple (ds, cv_list) ->
                  (identifier_of_datastructure ds)^
                      ".insert("^(ctype_of_code_var_list cv_list)^");";

            | `DeleteTuple (ds, cv_list) -> 
                  let id = identifier_of_datastructure ds in
                  let (find_it, find_decl) = point_iterator_declaration_of_datastructure ds in
                      find_decl^" = "^id^".find("^(ctype_of_code_var_list cv_list)^");"^
                      id^".delete("^find_it^");";

	    | `Eval(ac) -> string_of_arith_code_expression ac

	    | `IfNoElse(p,c) ->
		  "if ( "^(string_of_bool_code_expression p)^" ) {"^
		      (string_of_code_expression c)^" }"

	    | `IfElse(p,l,r) ->
		  "if ( "^(string_of_bool_code_expression p)^" ) {"^
		      (string_of_code_expression l)^" }"^
		  "else {"^
		      (string_of_code_expression r)^" }"

	    | `ForEach(m,c) ->
		  let (begin_it, end_it, begin_decl, end_decl) =
		      range_iterator_declarations_of_datastructure m
		  in
		      "\n"^begin_decl^" = "^(string_of_datastructure m)^".begin();\n"^
			  end_decl^" = "^(string_of_datastructure m)^".end();\n"^
			  "for(; "^begin_it^" != "^end_it^"; ++"^begin_it^"){"^
			  (field_declarations_of_datastructure m begin_it "")^"\n"^
			  (string_of_code_expression c)^
			  "}"

	    | `Block (c_expr_l) -> "{"^(string_of_code_block c_expr_l)^"}"

	    | `Return (ac) -> "return "^(string_of_arith_code_expression ac)^";"

	    | `Handler (name, args, rt, c_expr_l) ->
		  let h_fields =
		      List.fold_left
			  (fun acc (id, typ) ->
			       (if (String.length acc) = 0 then "" else acc^",")^(typ^" "^id))
			  "" args
		  in
		      rt^" "^name^"("^h_fields^") {\n"^(string_of_code_block c_expr_l)^"\n}"

let indented_string_of_code_expression c_expr =
    let rec sce_aux e level =
	let tab = String.make (4*level) ' ' in
	let ch_tab = String.make (4*(level+1)) ' ' in
	let string_of_code_block c_expr_l =
	    List.fold_left
		(fun acc c ->
		     (if (String.length acc) = 0 then "" else acc^"\n")^
			 (sce_aux c (level+1)))
		"" c_expr_l
	in
	let out =
	    match e with
		| `Declare x ->
		      begin
			  match x with 
		              | `Variable(n, typ) -> typ^" "^n^";"
                              | (`Relation _ as y) | (`Map _ as y) | (`Domain _ as y) ->
                                    let ctype = ctype_of_datastructure (datastructure_of_declaration y) in
                                        ctype^" "^(identifier_of_declaration y)^";"
		      end
			  
		| `Assign(v,ac) ->
		      v^" = "^(string_of_arith_code_expression ac)^";"

		| `AssignMap(mk, vc) ->
		      (string_of_map_key mk)^" = "^(string_of_arith_code_expression vc)^";"
			  
	        | `EraseMap(mk, c) ->
		      (string_of_map_key mk)^".erase("^(string_of_arith_code_expression c)^");"

                | `InsertTuple (ds, cv_list) ->
                      (identifier_of_datastructure ds)^
                          ".insert("^(ctype_of_code_var_list cv_list)^");";

                | `DeleteTuple (ds, cv_list) -> 
                      let id = identifier_of_datastructure ds in
                      let (find_it, find_decl) = point_iterator_declaration_of_datastructure ds in
                          find_decl^" = "^id^".find("^(ctype_of_code_var_list cv_list)^");\n"^
                              tab^id^".delete("^find_it^");";

		| `Eval(ac) -> string_of_arith_code_expression ac

		| `IfNoElse(p,c) ->
		      "if ( "^(string_of_bool_code_expression p)^" ) {\n"^
			  (sce_aux c (level+1))^"\n"^
			  tab^"}\n"
			  
		| `IfElse(p,l,r) ->
		      "if ( "^(string_of_bool_code_expression p)^" ) {\n"^
			  (sce_aux l (level+1))^"\n"^
			  tab^"}\n"^
		      "else {\n"^
			  (sce_aux r (level+1))^"\n"^
			  tab^"}\n"
			  
		| `ForEach(m,c) ->
		      let (begin_it, end_it, begin_decl, end_decl) =
			  range_iterator_declarations_of_datastructure m
		      in
			  "\n"^tab^
			      begin_decl^" = "^(string_of_datastructure m)^".begin();\n"^
			      tab^end_decl^" = "^(string_of_datastructure m)^".end();\n"^
			      tab^"for(; "^begin_it^" != "^end_it^"; ++"^begin_it^")\n"^
			      tab^"{\n"^
			      (field_declarations_of_datastructure m begin_it ch_tab)^"\n"^
			      (sce_aux c (level+1))^"\n"^
			      tab^"}"

		| `Block (c_expr_l) ->
		      "{\n"^(string_of_code_block c_expr_l)^"\n"^
			  tab^"}\n"

		| `Return (ac) -> "return "^(string_of_arith_code_expression ac)^";"
		      
		| `Handler (name, args, rt, c_expr_l) ->
		      let h_fields =
			  List.fold_left
			      (fun acc (id, typ) ->
				   (if (String.length acc) = 0 then "" else acc^",")^(typ^" "^id))
			      "" args
		      in
			  (* TODO: handler return type should be same as last code expr*)
			  (* Hacked for now... *)
			  rt^" "^name^"("^h_fields^") {\n"^
			      (string_of_code_block c_expr_l)^
			      tab^"\n}"
	in
	    tab^out 
    in
	sce_aux c_expr 0

(* Code helpers *)
let get_block_last c_expr =
    match c_expr with
	| `Block cl -> List.nth cl ((List.length cl)-1)
	| _ -> raise InvalidExpression

let remove_block_last c_expr =
    match c_expr with
	| `Block cl -> `Block (List.rev (List.tl (List.rev cl)))
	| _ -> raise InvalidExpression

(* TODO: inefficient!*)
let replace_block_last c_expr append_expr =
    match c_expr with
	| `Block cl -> `Block(
	      (List.rev (List.tl(List.rev cl)))@[ append_expr ])
	| _ -> raise InvalidExpression

let append_to_block c_expr append_expr =
    match c_expr with
	| `Block cl -> `Block (cl@[append_expr])
	| _ -> raise InvalidExpression

let append_blocks l_block r_block =
    match (l_block, r_block) with
	| (`Block lcl, `Block rcl) -> `Block(lcl@rcl)
	| _ -> raise InvalidExpression

(* `EraseMap should not occur as return val
 * TODO: think about `DeleteDomain, since this may occur on IncrPlan(_,`Minus,_,_)
 *)
let rec get_return_val c_expr =
    match c_expr with
        | `Eval _ -> c_expr
        | `Assign(v, _) -> `Eval(`CTerm(`Variable(v)))
        | `AssignMap(mk,_) -> `Eval(`CTerm(`MapAccess(mk)))
        | `IfNoElse(_, c) -> get_return_val c
        | `Block(y) -> get_return_val (get_block_last c_expr)
        | _ ->
              print_endline ("get_return_val: "^(indented_string_of_code_expression c_expr));
              raise InvalidExpression

(* TODO: see above note on `DeleteDomain *)
let remove_return_val c_expr =
    let rec remove_aux c =
        match c with
        | `Eval _ -> None
        | `Assign _ -> Some(c)
        | `AssignMap _ -> Some(c)
        | `IfNoElse (p, cc) ->
              begin
                  match remove_aux cc with
                      | None -> None
                      | Some x -> Some(`IfNoElse(p,x))
              end
        | `Block x ->
              let last = get_block_last c in
              let block_without_last = remove_block_last c in
                  begin
                      match remove_aux last with
                          | None -> Some(block_without_last)
                          | Some y -> Some(append_to_block block_without_last y)
                  end
        | _ ->
              print_endline ("remove_return_val: "^(indented_string_of_code_expression c));
              raise InvalidExpression                
    in
        match (remove_aux c_expr) with
            | None -> raise (RewriteException "Attempted to remove top level expression")
            | Some x -> x

(* TODO: see above note on `DeleteDomain *)
let rec replace_return_val c_expr new_rv =
    match c_expr with
        | `Eval _ -> new_rv
        | `Assign _ | `AssignMap _ ->
              `Block([c_expr; new_rv])
        | `IfNoElse (p, c) -> `IfNoElse(p, replace_return_val c new_rv)
        | `Block x ->
              let last = get_block_last c_expr in
              let new_last = replace_return_val last new_rv in
                  `Block(List.rev (new_last::(List.tl (List.rev x))))
        | _ ->
              print_endline ("replace_return_val: "^(indented_string_of_code_expression c_expr));
              raise InvalidExpression

(* indicates whether the given code is a return value, and whether it is replaceable *)
(* TODO: see above note on `DeleteDomain *)
let rec is_local_return_val code rv =
    match code with
        | `Eval _ -> (code = rv, false)
        | `Assign (v, _) -> (rv = `Eval(`CTerm(`Variable(v))), false)
        | `AssignMap (mk, _) -> (rv = `Eval(`CTerm(`MapAccess(mk))), false)
        | `Block cl -> 
              let block_last = get_block_last code in
              let (local, _) = is_local_return_val (get_block_last code) rv in
                  (local, match block_last with | `Eval _ -> true | _ -> false)
        | _ -> (false, false)

(* code_expression -> declaration list ->
     code_expression * arith_code_expression * declaration list
 * Transform:
 * local && replaceable => remove last statement
 * local => do nothing
 * non-local => force return val assigment to variable, to ensure scoping *)
let prepare_block_for_merge block decl =
    match block with
        | `Block cl ->
              let rv = get_return_val block in
              let arith_rv = match rv with | `Eval x -> x | _ -> raise InvalidExpression in
              let (local, repl) = is_local_return_val block rv in
                  if local then
                      ((if repl then (remove_block_last block) else block), arith_rv, decl)
                  else
                      let var = gen_var_sym() in
                          (replace_return_val block (`Assign(var, arith_rv)),
                          `CTerm(`Variable(var)), (`Variable(var, "int"))::decl)
        | _ -> 
              print_endline ("prepare_block_for_merge: invalid arg\n"^
                  (indented_string_of_code_expression block));
              raise InvalidExpression

(* code_expression -> (arith_code_expression -> code_expression) -> declaration list ->
     code_expression * declaration list
 * Replace, and merge the return val of a block with the expression
 * generated by a merging function
*)
let merge_with_block block merge_fn decl =
    match block with
        | `Block cl ->
              let (b, new_av, new_decl) = prepare_block_for_merge block decl in
                  (append_to_block b (merge_fn new_av), new_decl)

        | _ ->
              print_endline ("merge_with_block: invalid block\n"^
                  (indented_string_of_code_expression block));
              raise InvalidExpression

(* code_expression -> (arith_code_expression -> code_expression) -> declaration list ->
     code_expression * declaration list
 * Merges the bodies of two blocks, replacing their return vals with the expression
 * generated by a merging function
*)
let merge_blocks l_block r_block merge_fn decl =
    match (l_block, r_block) with
        | (`Block lcl, `Block rcl) ->
              let (lb, new_lav, new_ldecl) = prepare_block_for_merge l_block decl in
              let (rb, new_rav, new_rdecl) = prepare_block_for_merge r_block new_ldecl in
                  (append_to_block (append_blocks lb rb) (merge_fn new_lav new_rav), new_rdecl)

        | _ ->
              print_endline ("merge_blocks: invalid args\n"^
                  "left:\n"^(indented_string_of_code_expression l_block)^"\n"^
                  "right:\n"^(indented_string_of_code_expression r_block));
              raise InvalidExpression

(**/**)
let rec get_last_code_expr c_expr = 
    match c_expr with
	| `Declare _ | `Assign _ 
        | `AssignMap _  | `EraseMap _
        | `InsertTuple _ | `DeleteTuple _
        | `Eval _ | `Return _ -> c_expr 
	| `IfNoElse (b_expr, c_expr) -> get_last_code_expr c_expr
	| `IfElse (b_expr, c_expr_l, c_expr_r) -> get_last_code_expr c_expr_r
	| `ForEach (ds, c_expr) -> get_last_code_expr c_expr
	| `Block cl -> get_last_code_expr (List.nth cl ((List.length cl) - 1))
	| `Handler (_, args, _, cl) -> get_last_code_expr (List.nth cl ((List.length cl) - 1))



