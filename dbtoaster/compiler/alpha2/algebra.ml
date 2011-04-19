exception InvalidExpression
exception DuplicateException
exception PlanException of string
exception RewriteException of string
exception ValidationException of string
exception CodegenException of string

let dbt_hash x = Hashtbl.hash_param 200 250 x

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

type aggregate_function = [ `Sum | `Min | `Max ]

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

type bindings = (attribute_identifier * (expression option)) list

type initop = [`Init of domain | `Final of domain ]

type poplus = [`Union | `Diff ]

type oplus = [`Plus | `Minus | `Min | `Max
| `Decrmin of map_expression * state_identifier
| `Decrmax of map_expression * state_identifier ]

and map_expression = [
| `METerm of meterm
| `Sum of map_expression * map_expression
| `Minus of map_expression * map_expression
| `Product of map_expression * map_expression
| `Min of map_expression * map_expression
| `Max of map_expression * map_expression
| `MapAggregate of aggregate_function * map_expression * plan
| `Delta of delta * map_expression
| `New of map_expression
| `MaintainMap of state_identifier * initop * bindings * map_expression
| `Incr of state_identifier * oplus * map_expression * bindings * map_expression
| `IncrDiff of state_identifier * oplus * map_expression * bindings * map_expression
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
| `IncrPlan of state_identifier * poplus * domain * bindings * plan
| `IncrDiffPlan of state_identifier * poplus * domain * bindings * plan ]

type accessor_element = [ `MapExpression of map_expression | `Plan of plan ]

type unifications = attribute_identifier * expression list

type binding = [
| `BindExpr of variable_identifier * expression
| `BindMapExpr of variable_identifier * map_expression ]

type recompute_state = New | Incr

type stream_type = string
type source_type = string
type source_args = string
type tuple_type = string
type adaptor_type = string
type adaptor_bindings = (string * string) list
type source_instance = string
type source_info =
        string *
            (stream_type * source_type * source_args * tuple_type *
                adaptor_type * adaptor_bindings * string * source_instance)


(* Pretty printing *)	
let string_of_attribute_identifier id =
    match id with 
	| `Qualified(r,a) -> r^"."^a
	| `Unqualified a -> a

let string_of_attribute_identifier_list idl =
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

let string_of_initop iop =
    match iop with
        | `Init d -> "+("^(string_of_attribute_identifier_list d)^")"
        | `Final d -> "-("^(string_of_attribute_identifier_list d)^")"

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
	| `Sum (l,r) -> "("^(string_of_expression l)^" + "^(string_of_expression r)^")"
	| `Product(l,r) -> "("^(string_of_expression l)^" * "^(string_of_expression r)^")"
	| `Minus (l,r) -> "("^(string_of_expression l)^" - "^(string_of_expression r)^")"
	| `Divide (l,r) -> "("^(string_of_expression l)^" / "^(string_of_expression r)^")"
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

let string_of_bindings bindings = 
    List.fold_left
        (fun acc (id, e_opt) ->
            (if (String.length acc) = 0 then "" else (acc^","))^
                (string_of_attribute_identifier id)^
                (match e_opt with
                    | None -> "" | Some(x) -> ("<-"^(string_of_expression x))))
        "" bindings

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
	      "("^(string_of_map_expression l)^" + "^(string_of_map_expression r)^")"

	| `Minus (l,r) ->
	      "("^(string_of_map_expression l)^" - "^(string_of_map_expression r)^")"

	| `Product (l, r) -> 
	      "("^(string_of_map_expression l)^" * "^(string_of_map_expression r)^")"
		  
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

	| `MaintainMap (sid,iop,bd,e) ->
              "MaintainMap["^(sid)^","^(string_of_initop iop)^","^
                  (string_of_bindings bd)^"]{"^(string_of_map_expression e)^"}"

	| `Incr (sid, op, re, bd, e) ->
              "Incr["^(string_of_oplus op)^","^(sid)^","^(string_of_bindings bd)^"]{"^
                  (string_of_map_expression e)^"}"

	| `IncrDiff (sid, op, re, bd, e) ->
              "IncrDiff["^(string_of_oplus op)^","^(sid)^","^(string_of_bindings bd)^"]{"^
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

	| `IncrPlan (sid, po, d, bd, ch) ->
              "IncrP["^(string_of_poplus po)^","^(sid)^",b<"^
                  (string_of_bindings bd)^">,d<"^(string_of_attribute_identifier_list d)^">"^
                  "]{"^(string_of_plan ch)^"}"

	| `IncrDiffPlan (sid, po, d, bd, ch) ->
              "IncrDiffP["^(string_of_poplus po)^","^(sid)^",b<"^
                  (string_of_bindings bd)^">,d<"^(string_of_attribute_identifier_list d)^">"^
                  "]{"^(string_of_plan ch)^"}"


let rec indented_string_of_bool_expression b_expr level =
    let indent s = (String.make (4*level) '-')^s in
    let dispatch_expr l r op =
	(string_of_expression l)^op^(string_of_expression r)
    in
	match b_expr with
	    | `BTerm b ->
		  begin
		      match b with
			  | `True -> "true"
			  | `False -> "false"
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

	| `MaintainMap (sid, iop, bd, e) ->
	      (indent ("MaintainMap["^(sid)^","^
                  (string_of_initop iop)^","^
                  (string_of_bindings bd)^"]{"))^
		  (indented_string_of_map_expression e (level+1))^
		  (indent "}")

	| `Incr (sid, op, re, bd, e) ->
	      (indent ("Incr["^(string_of_oplus op)^","^(sid)^","^(string_of_bindings bd)^"]{"))^
		  (indented_string_of_map_expression e (level+1))^
		  (indent "}")

	| `IncrDiff (sid, op, re, bd, e) ->
	      (indent ("IncrDiff["^(string_of_oplus op)^","^(sid)^","^(string_of_bindings bd)^"]{"))^
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

	| `IncrPlan (sid, po, d, bd, ch) ->
	      (indent ("IncrPlan["^(string_of_poplus po)^","^(sid)^","^
                  (string_of_bindings bd)^">,d<"^(string_of_attribute_identifier_list d)^">]{"))^
		  (indented_string_of_plan ch (level+1))^
		  (indent "}")

	| `IncrDiffPlan (sid, po, d, bd, ch) ->
	      (indent ("IncrDiffPlan["^(string_of_poplus po)^","^(sid)^",b<"^
                  (string_of_bindings bd)^">,d<"^(string_of_attribute_identifier_list d)^">]{"))^
		  (indented_string_of_plan ch (level+1))^
		  (indent "}")


(*******************************
 *
 * Symbol generation
 *
 * For: relations, variables, bindings and datastructures (state ids, maps, domain)
 *******************************)

(* Relations *)
let id_counter = ref 0

let gen_rel_id () =
    let new_sym = !id_counter in
	incr id_counter;
	"rel"^(string_of_int new_sym)

(* Variables *)
let var_counter = ref 0

let gen_var_sym() =
    let new_sym = !var_counter in
	incr var_counter;
	"var"^(string_of_int new_sym)


(* Datastructures *)

(* Categorical symbol generation *)
type datastructure_symbols = (string, string) Hashtbl.t
let indexed_syms : (string, int ref * datastructure_symbols) Hashtbl.t = Hashtbl.create 10

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

(*
 * Symbol generation based on structural equivalence
 *)

(* Binding symbol generation *)
type binding_signature = [
    | `Expression of expression
    | `MapExpression of map_expression ]

let string_of_binding_signature bind_sig =
    match bind_sig with
        | `Expression e -> string_of_expression e
        | `MapExpression e -> string_of_map_expression e

module BindingSigHashtype =
struct
    type t = binding_signature
    let equal = Pervasives.(=)
    let hash = dbt_hash
end

module BindingSigHashtbl = Hashtbl.Make(BindingSigHashtype)

type binding_symbols = string BindingSigHashtbl.t
let binding_syms : binding_symbols = BindingSigHashtbl.create 10
let binding_counter = ref 0

let gen_binding_sym bind_sig =
    if BindingSigHashtbl.mem binding_syms bind_sig then
        begin
            let r = BindingSigHashtbl.find binding_syms bind_sig in
                print_endline ("Found binding sym: "^(string_of_binding_signature bind_sig));
                r
        end
    else
        begin
            let new_id = !binding_counter in
            let new_symbol = 
                incr binding_counter;
                "bind"^(string_of_int new_id) in

                print_endline ("New binding sym: "^(new_symbol)^" "^
                    (string_of_binding_signature bind_sig));

                BindingSigHashtbl.iter
                    (fun k v -> print_endline ("binding sym: "^(string_of_binding_signature k)^"->"^v))
                    binding_syms;

                BindingSigHashtbl.add binding_syms bind_sig new_symbol;
                new_symbol
        end



(* State identifiers *)
type state_signature = [
    | `MapExpression of map_expression * bindings
    | `Plan of plan * domain ]

let string_of_state_signature state_sig =
    match state_sig with
        | `MapExpression (e,b) -> (string_of_map_expression e)^"::"^(string_of_bindings b)
        | `Plan (p,d) -> (string_of_plan p)^"::"^(string_of_attribute_identifier_list d)

module StateSigHashtype =
struct
    type t = state_signature
    let equal = Pervasives.(=)
    let hash = dbt_hash
end

module StateSigHashtbl = Hashtbl.Make(StateSigHashtype)

type state_symbols = string StateSigHashtbl.t
let state_syms : state_symbols = StateSigHashtbl.create 10
let sid_counter = ref 0

let gen_state_sym state_sig =
    if StateSigHashtbl.mem state_syms state_sig then
        begin
            let r = StateSigHashtbl.find state_syms state_sig in
                print_endline ("Found state sym: "^(string_of_state_signature state_sig));
                r
        end
    else
        begin
            let new_id = !sid_counter in
            let new_symbol = 
                incr sid_counter;
                "state"^(string_of_int new_id) in

                print_endline ("New state sym: "^(new_symbol)^" "^
                    (string_of_state_signature state_sig));

                StateSigHashtbl.iter
                    (fun k v -> print_endline ("existing sym: "^(string_of_state_signature k)^"->"^v))
                    state_syms;

                StateSigHashtbl.add state_syms state_sig new_symbol;
                new_symbol
        end

let remove_state_sym state_sig = 
    StateSigHashtbl.remove state_syms state_sig

let update_state_sym state_sig new_state_sig state_sym = 
    remove_state_sym state_sig;  
    if StateSigHashtbl.mem state_syms new_state_sig then
        begin
            let r = StateSigHashtbl.find state_syms new_state_sig in
                print_endline ("Found state sym: "^(string_of_state_signature new_state_sig));
                r
        end
    else
        begin
            StateSigHashtbl.add state_syms new_state_sig state_sym;
            state_sym
        end

(* merging two state ids. remove binding of state_sig1 and state_sig2 
 * then update binding of new_state_sig1 with sym *)
let merge_state_sym state_sig1 state_sig2 new_state_sig sym = 
    remove_state_sym state_sig2;
    update_state_sym state_sig1 new_state_sig sym



(* Basic helpers *)
let field_of_attribute_identifier id =
    match id with
	| `Qualified(r,a) -> a
	| `Unqualified a -> a

let attribute_identifiers_of_field_list relation_id field_l =
    List.map (fun (id, ty) -> `Qualified(relation_id, id)) field_l

let attributes_of_bindings bindings =
    List.map (fun (aid, e_opt) -> aid) bindings


(*
 * Map algebra manipulation
 *)

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
            | `MaintainMap (sid, iop, bd, e) -> me_fn (`MaintainMap (sid, iop, bd, map_aux e))
            | `Incr (sid, op, re, bd, e) -> me_fn (`Incr(sid, op, re, bd, map_aux e))
            | `IncrDiff (sid, op, re, bd, e) -> me_fn (`IncrDiff(sid, op, re, bd, map_aux e))
            
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
            | `IncrPlan (sid, pop, d, bd, p) -> mp_fn (`IncrPlan (sid, pop, d, bd, map_aux p))
            | `IncrDiffPlan (sid, pop, d, bd, p) -> mp_fn (`IncrDiffPlan (sid, pop, d, bd, map_aux p))



(*******************************
 * Map algebra accessors
 *******************************)

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
	    | `MapExpression (`MaintainMap (_,_,_,e)) 
	    | `MapExpression (`Incr (_, _, _, _, e))
	    | `MapExpression (`IncrDiff (_, _, _, _, e))
	    | `MapExpression (`Insert (_, _, e))
	    | `MapExpression (`Update (_, _, _, e)) ->
		  let match_e = `MapExpression e in
		      if (ch_e_or_p = match_e) then Some e_or_p
		      else
			  parent_aux match_e e_or_p

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
            | `Plan (`IncrPlan(_, _, _, _, p)) | `Plan (`IncrDiffPlan(_, _, _, _, p)) ->
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
	    | `MapExpression (`MaintainMap(_,_,_,e))
	    | `MapExpression (`Incr(_,_,_,_,e))
	    | `MapExpression (`IncrDiff(_,_,_,_,e))
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
            | `Plan (`IncrPlan(_,_,_,_,e)) | `Plan (`IncrDiffPlan (_,_,_,_,e)) ->
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
		    | `MaintainMap(sid,iop,bd,e) -> `MaintainMap(sid, iop, bd, splice_map_expr_aux e)
		    | `Incr(sid,op,re,bd,e) -> `Incr(sid, op, re, bd, splice_map_expr_aux e)
		    | `IncrDiff(sid,op,re,bd,e) -> `IncrDiff(sid, op, re, bd, splice_map_expr_aux e)

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
		    | `IncrPlan (sid, p, d, bd, e) -> `IncrPlan (sid, p, d, bd, splice_plan_aux e)
		    | `IncrDiffPlan (sid, p, d, bd, e) -> `IncrDiffPlan (sid, p, d, bd, splice_plan_aux e)
		    | _ -> raise InvalidExpression
	    end
    in
	splice_map_expr_aux m_expr


let rec get_base_relations_accessor_element ae acc =
    let gbr_aux = get_base_relations_accessor_element in
        match ae with
	    | `MapExpression (`METerm _) | `MapExpression (`Delete _) -> acc
	          
	    | `MapExpression (`Delta(_,e))
	    | `MapExpression (`New(e)) 
	    | `MapExpression (`MaintainMap(_,_,_,e))
	    | `MapExpression (`Incr(_,_,_,_,e))
	    | `MapExpression (`IncrDiff(_,_,_,_,e))
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
	          (gbr_aux (`MapExpression f) (gbr_aux (`Plan q) acc))

	    | `Plan (`TrueRelation) | `Plan (`FalseRelation) -> acc

	    | `Plan (`Relation _ as x) -> acc@[x]

            | `Plan (`Domain _) -> acc
	          
	    | `Plan (`Select (p,cq)) ->
	          begin
		      match p with
		          | `BTerm(`MEQ(m_expr)) | `BTerm(`MNEQ(m_expr))
		          | `BTerm(`MLT(m_expr)) | `BTerm(`MLE(m_expr))
		          | `BTerm(`MGT(m_expr)) | `BTerm(`MGE(m_expr))
                                ->
			        (gbr_aux (`MapExpression m_expr) (gbr_aux (`Plan cq) acc))

		          | _ -> (gbr_aux (`Plan cq) acc)
	          end
		      
	    | `Plan (`Project (a, cq)) -> (gbr_aux (`Plan cq) acc)

	    | `Plan (`Union ch) -> List.fold_left (fun acc c -> gbr_aux (`Plan c) acc) acc ch

	    | `Plan (`Cross (l,r)) ->
	          (gbr_aux (`Plan r) (gbr_aux (`Plan l) acc))

	    | `Plan (`DeltaPlan (_, p)) | `Plan (`NewPlan (p))
            | `Plan (`IncrPlan (_,_,_,_,p)) | `Plan (`IncrDiffPlan (_,_,_,_,p)) ->
	          (gbr_aux (`Plan p) acc)

	    | _ -> raise InvalidExpression

(* map_expression -> plan list *)
let get_base_relations m_expr =
    get_base_relations_accessor_element (`MapExpression m_expr) []

let get_base_relations_plan plan =
    get_base_relations_accessor_element (`Plan plan) []


(*******************************
 * Binding helpers
 * Note: DBToaster compilation assumes unique column names
 *******************************)

let get_bound_relations m_expr =
    let rec gbor_aux ae acc =
	match ae with
	    | `MapExpression (`METerm _) | `MapExpression (`Delete _) -> acc
		  
	    | `MapExpression (`Delta(_,e))
	    | `MapExpression (`New(e)) 
	    | `MapExpression (`MaintainMap(_,_,_,e))
	    | `MapExpression (`Incr(_,_,_,_,e))
	    | `MapExpression (`IncrDiff(_,_,_,_,e))
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
            | `Plan (`IncrPlan (_,_,_,_,p)) | `Plan (`IncrDiffPlan(_,_,_,_,p)) ->
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

        (* Assumes the same attributes across all inputs to the union *)
	| `Union ch -> get_bound_attributes (List.hd ch)
	| `Cross (l,r) ->
              (get_bound_attributes l)@(get_bound_attributes r)

	| `DeltaPlan (_, cq) | `NewPlan(cq)
        | `IncrPlan(_,_,_,_,cq) | `IncrDiffPlan(_,_,_,_,cq) -> (get_bound_attributes cq)


(* Note: performs qualified comparison if possible *)
(* TODO: think if we ever want to force unqualified comparison *)
let compare_attributes a1 a2 =
    match (a1, a2) with
	| (`Qualified (n1, f1), `Qualified (n2, f2)) -> n1 = n2 && f1 = f2
	| (`Qualified (n1, f1), `Unqualified f2) -> f1 = f2
	| (`Unqualified f1, `Qualified (n2, f2)) -> f1 = f2
	| (`Unqualified f1, `Unqualified f2) -> f1 = f2


let resolve_unbound_attributes attrs bound_attrs =
    List.filter
	(fun x ->
            let matches = List.filter (compare_attributes x) bound_attrs in
                (List.length matches) = 0)
	attrs


(*******************************
 * Scope helpers
 *******************************)

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

	| `New(e) | `MaintainMap(_,_,_,e)

        (* TODO: we can return from bindings if they are filled in, i.e. we don't
         * always need to go into the child map expression *)
        | `Incr(_,_,_,_,e) | `IncrDiff(_,_,_,_,e) ->
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

	| `DeltaPlan(_,e) | `NewPlan(e)

        (* TODO: we can answer from the bindings if they are filled in *)
        | `IncrPlan(_,_,_,_,e) | `IncrDiffPlan(_,_,_,_,e) ->
	      get_unbound_attributes_from_plan e include_vars


let get_unbound_map_attributes_from_plan f q include_vars =
    let print_debug f_uba q_ba unresolved_fa unresolved_qa =
        print_endline "get_unbound_map_attributes_from_plan:";
        print_endline ("\tf_uba: "^(string_of_attribute_identifier_list f_uba));
        print_endline ("\tq_ba: "^(string_of_attribute_identifier_list q_ba));
        print_endline ("\tu_fa: "^(string_of_attribute_identifier_list unresolved_fa));
        print_endline ("\tu_qa: "^(string_of_attribute_identifier_list unresolved_qa));
    in
    let f_uba = get_unbound_attributes_from_map_expression f include_vars in
    let q_ba = get_bound_attributes q in
    let unresolved_fa = resolve_unbound_attributes f_uba q_ba in
    let unresolved_qa = get_unbound_attributes_from_plan q include_vars in
        print_debug f_uba q_ba unresolved_fa unresolved_qa;
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
        | `IncrPlan (_, _, d, _, cq) | `IncrDiffPlan (_, _, d, _, cq) ->
              if List.length d = 0 then get_flat_schema cq else d

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

	| `New(e) | `MaintainMap (_,_,_,e)
        | `Incr(_,_,_,_,e) | `IncrDiff(_,_,_,_,e) ->
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

	| `DeltaPlan(_,q) | `NewPlan(q) ->
              get_attributes_used_in_plan q attrs_used_above

        | `IncrPlan(_,_,d,_,q) | `IncrDiffPlan(_,_,d,_,q) ->
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
                  get_attributes_used_in_plan q attrs_used_above


(*******************************
 * Nesting helpers
 *******************************)

let rec find_aggregate_type m_expr agg_types =
    let recur me = find_aggregate_type me agg_types in
    let recur_plan p = find_aggregate_type_plan p agg_types in
    match m_expr with
        | `METerm _ -> false
        | `Sum (l,r) | `Minus (l,r) | `Product (l,r)
        | `Min (l,r) | `Max (l,r) ->
              (recur l) || (recur r)

        | `MapAggregate (agg_fn, f, q) ->
              let found = List.mem agg_fn agg_types in
                  if not found then
                      (recur f) || (recur_plan q)
                  else found

        | `Delta (_, e) | `New e | `MaintainMap(_,_,_,e)
        | `Incr (_,_,_,_,e) | `IncrDiff (_,_,_,_,e)
        | `Insert (_,_,e) | `Update (_,_,_,e) -> recur e
        | `Delete _ -> false
        | `IfThenElse(`MEQ(p), l, r) | `IfThenElse(`MNEQ(p), l, r) 
        | `IfThenElse(`MLT(p), l, r) | `IfThenElse(`MLE(p), l, r) 
        | `IfThenElse(`MGT(p), l, r) | `IfThenElse(`MGE(p), l, r) 
            -> List.exists (fun x -> x) (List.map recur [p;l;r])

        | `IfThenElse(_, l, r) ->  (recur l) || (recur r)

and find_aggregate_type_plan q agg_types =
    let recur me = find_aggregate_type me agg_types in
    let recur_plan p = find_aggregate_type_plan p agg_types in
    match q with
        | `TrueRelation | `FalseRelation | `Relation _ | `Domain _ -> false
        | `Select(pred, cq) -> 
              (match pred with
                  | `BTerm(`MEQ(m_expr)) | `BTerm(`MNEQ(m_expr))
                  | `BTerm(`MLT(m_expr)) | `BTerm(`MLE(m_expr))
                  | `BTerm(`MGT(m_expr)) | `BTerm(`MGE(m_expr))
                        -> recur m_expr
                  | _ -> false)
              || (recur_plan cq)

        | `Project (_, cq) -> recur_plan cq
        | `Union ch -> List.exists recur_plan ch
        | `Cross (l,r) -> (recur_plan l) || (recur_plan r)
        | `DeltaPlan (_, cq) | `NewPlan cq
        | `IncrPlan (_,_,_,_,cq) | `IncrDiffPlan (_,_,_,_,cq) -> recur_plan cq

let find_cmp_aggregate m_expr =
    find_aggregate_type m_expr [`Min; `Max]

let find_cmp_aggregate_plan q =
    find_aggregate_type_plan q [`Min; `Max]

let find_aggregate m_expr =
    find_aggregate_type m_expr [`Sum; `Min; `Max]


(* map_expression -> plan -> bool
 * returns whether a map aggregate expression is independent from its query *)
let is_independent m_expr q =
    let q_ba = get_bound_attributes q in
    let me_a = get_unbound_attributes_from_map_expression m_expr false in
    let me_al = List.length me_a in
    let unresolved_l = List.length (resolve_unbound_attributes me_a q_ba) in
	(me_al = 0) || (me_al > 0 && (me_al = unresolved_l))


(* returns whether any bindings are used by m_expr and the bound variables *)
let is_binding_used m_expr bindings =
    (* returns vars as `Unqualified(var_name) *)
    let m_ubav = get_unbound_attributes_from_map_expression m_expr true in
    let bindings_used =
        List.filter
            (fun x -> match x with
                | `BindMapExpr(var_id, _) ->
                      List.mem (`Unqualified(var_id)) m_ubav
                | _ -> false)
            bindings
    in
    let binding_used_rv =
        List.map
            (fun x -> match x with
                | `BindMapExpr(v, bc) -> (v, bc)
                | _ -> raise InvalidExpression)
            bindings_used
    in
        match binding_used_rv with
            | [] -> (false, [])
            | x -> (true, x)

(*
 * Extracted binding helpers
 *)

(* var_id -> extracted bindings -> expression *)
let get_expr_definition var ext_bindings =
    match var with
	| `Variable(sym) ->
	      let matches =
		  List.filter
		      (fun x -> match x with
			  | `BindExpr (n,d) -> n = sym
			  | `BindMapExpr _ -> false)
		      ext_bindings
	      in
		  begin
		      match matches with
			  | [] -> raise Not_found
			  | [`BindExpr(n,d)] -> d
			  | (`BindExpr(n,d))::t -> d (* raise exception for duplicate bindings? *)
			  | _ -> raise Not_found 
		  end
	| _ -> raise Not_found

(* Note removes duplicate bindings *)
let remove_expr_binding var ext_bindings =
    match var with
	| `Variable(sym) ->
	      List.filter
		  (fun x -> match x with
		      | `BindExpr (n,d) -> n <> sym
		      | `BindMapExpr _ -> true)
		  ext_bindings
	| _ -> raise Not_found


(* expression -> bool 
 * returns whether the expression is constant,
 * i.e. uses only constants and variables, not attributes *)
let rec is_constant_expr expr =
    match expr with
	| `ETerm (`Attribute _) -> false
	| `ETerm (`Int _) | `ETerm (`Float _)
	| `ETerm (`String _) | `ETerm (`Long _) | `ETerm (`Variable _) -> true
	| `UnaryMinus e -> is_constant_expr e
	| `Sum(l,r) | `Product(l,r) | `Minus(l,r) | `Divide(l,r) ->
	      is_constant_expr(l) && is_constant_expr(r)

	(* TODO: && is_deterministic(fid) *)
	| `Function(fid, args) -> List.for_all is_constant_expr args

let rec is_static_constant_expr expr =
    match expr with
	| `ETerm (`Attribute _) | `ETerm (`Variable _) -> false
	| `ETerm (`Int _) | `ETerm (`Float _)
	| `ETerm (`String _) | `ETerm (`Long _) -> true
	| `UnaryMinus e -> is_static_constant_expr e
	| `Sum(l,r) | `Product(l,r) | `Minus(l,r) | `Divide(l,r) ->
	      is_static_constant_expr(l) && is_static_constant_expr(r)

	(* TODO: && is_deterministic(fid) *)
	| `Function(fid, args) -> List.for_all is_static_constant_expr args

(* expression -> extracted bindings -> bool *)
let is_bound_expr expr ext_bindings =
    try
	match expr with
	    | `ETerm(`Variable v) ->
		  let _ = get_expr_definition (`Variable v) ext_bindings in true
	    | _ -> false
    with Not_found -> false

let validate_incr_ops expr_op plan_op =
    match (expr_op, plan_op) with
        | (`Plus, `Union) | (`Min, `Union) | (`Max, `Union)
        | (`Minus, `Diff) | (`Decrmin _, `Diff) | (`Decrmax _, `Diff)
              -> ()
        | _ -> raise (ValidationException "Invalid op pair.")

let is_insert_incr expr_op plan_op =
    match (expr_op, plan_op) with
        | (`Plus, `Union) | (`Min, `Union) | (`Max, `Union) -> true
        | (`Minus, `Diff) | (`Decrmin _, `Diff) | (`Decrmax _, `Diff) -> false
        | _ -> raise (ValidationException "Invalid op pair.")


(*******************************
 * Preprocessing helpers
 *******************************) 

let rename_attributes m_expr =
    let base_relation_ht = Hashtbl.create 10 in
    let attributes_ht = Hashtbl.create 100 in
    let rename_fields f =
        List.map ( fun (id, ty) -> 
            if Hashtbl.mem attributes_ht id then
                let new_counter = (Hashtbl.find attributes_ht id) + 1 in
                    Hashtbl.replace attributes_ht id new_counter;
                    (id^(string_of_int new_counter), ty)
            else 
                (Hashtbl.add attributes_ht id 1;
                (id^(string_of_int 1), ty))) f 
    in
    let rec rename_expression e replacements =
        let rename_binary l r fn =
            let (nl, rl) = rename_expression l replacements in
            let (nr, rr) = rename_expression r replacements in
                (fn nl nr, rl@rr)
        in
            match e with
                | `ETerm(`Attribute(x)) ->
                      let f = field_of_attribute_identifier x in
                      begin
                          match x with  
                              | `Qualified(rname, _) -> 
                                    print_string ("Checking "^rname^"/"^f^" ...");
                                    if List.mem_assoc (rname,f) replacements then
                                        begin
                                            print_endline " matched.";
                                            (`ETerm(`Attribute(
                                                `Unqualified(List.assoc (rname,f) replacements))),
                                            replacements)
                                        end
                                    else
                                        begin
                                            print_endline " failed.";
                                            (e, replacements)
                                        end
                              | `Unqualified(y) ->
                                    print_endline ("Checking "^y^" ...");
                                    let l =
                                        List.filter (fun ((rname, f1), f2) -> f1 = f) replacements
                                    in
                                    if List.length l != 0 then
                                        begin
                                            print_endline " matched.";
                                            let f2 = snd (List.hd l) in
                                                (`ETerm(`Attribute(`Unqualified(f2))), replacements)
                                        end
                                    else
                                        begin
                                            print_endline " failed.";
                                            (e, replacements)
                                        end
                      end
                      (*let f = field_of_attribute_identifier x in
                          if List.mem_assoc f replacements then
                              (`ETerm(`Attribute(
                                  `Unqualified(List.assoc f replacements))),
                              replacements)
                          else (e, replacements)*)

                | `ETerm(_) -> (e, replacements)
                | `UnaryMinus e -> let (ne, nr) = rename_expression e replacements in
                      (`UnaryMinus(ne), nr)

                | `Sum (l,r) -> rename_binary l r (fun x y -> `Sum(x,y))
                | `Product (l,r) -> rename_binary l r (fun x y -> `Product(x,y))
                | `Minus (l,r) -> rename_binary l r (fun x y -> `Minus(x,y))
                | `Divide (l,r) -> rename_binary l r (fun x y -> `Divide(x,y))
                | `Function (fid, args) ->
                      let (new_args, ar) = List.split(
                          List.map (fun a -> rename_expression a replacements) args)
                      in
                          (`Function(fid, new_args), List.flatten ar)
                              
    and rename_predicate b replacements =
        let rename_binary l r fn =
            let (nl, rl) = rename_predicate l replacements in
            let (nr, rr) = rename_predicate r replacements in
                (fn nl nr, rl@rr)
        in
        let rbt_bin l r fn =
            let (nl, rl) = rename_expression l replacements in
            let (nr, rr) = rename_expression r replacements in
                (`BTerm(fn nl nr), rl@rr)
        in
        let rbt_me m_expr fn =
            let (nme, r) = rename_map_expression m_expr replacements in
                (`BTerm(fn nme), r)
        in
            match b with
                | `BTerm(x) ->
                      begin
                          match x with
                              | `True | `False -> (b, replacements)
                              | `LT (l,r) -> rbt_bin l r (fun x y -> `LT(x,y))
                              | `LE (l,r) -> rbt_bin l r (fun x y -> `LE(x,y))
                              | `GT (l,r) -> rbt_bin l r (fun x y -> `GT(x,y))
                              | `GE (l,r) -> rbt_bin l r (fun x y -> `GE(x,y))
                              | `EQ (l,r) -> rbt_bin l r (fun x y -> `EQ(x,y))
                              | `NE (l,r) -> rbt_bin l r (fun x y -> `NE(x,y))
                              | `MEQ m_expr -> rbt_me m_expr (fun e -> `MEQ(e))
                              | `MNEQ m_expr -> rbt_me m_expr (fun e -> `MNEQ(e))
                              | `MLT m_expr -> rbt_me m_expr (fun e -> `MLT(e))
                              | `MLE m_expr -> rbt_me m_expr (fun e -> `MLE(e))
                              | `MGT m_expr -> rbt_me m_expr (fun e -> `MGT(e))
                              | `MGE m_expr -> rbt_me m_expr (fun e -> `MGE(e))
                      end
                | `Not cb -> let (n,r) = rename_predicate cb replacements in (`Not(n), r)
                | `And (l,r) -> rename_binary l r (fun x y -> `And(x,y))
                | `Or (l,r) -> rename_binary l r (fun x y -> `Or(x,y))

                      
    and rename_map_expression e replacements =
        let rename_binary l r fn =
            let (new_l, rep_l) = rename_map_expression l replacements in
            let (new_r, rep_r) = rename_map_expression r replacements in
                (fn new_l new_r, rep_l@rep_r)
        in
            match e with
                | `METerm(`Attribute(x)) ->
                      let f = field_of_attribute_identifier x in
                      begin
                          match x with  
                              | `Qualified(rname, _) -> 
                                    print_string ("Checking "^rname^"/"^f^" ...");
                                    if List.mem_assoc (rname,f) replacements then
                                        begin
                                            print_endline " matched.";
                                            (`METerm(`Attribute(
                                                `Unqualified(List.assoc (rname,f) replacements))),
                                            replacements)
                                        end
                                    else
                                        begin
                                            print_endline " failed.";
                                            (e, replacements)
                                        end
                              | `Unqualified y ->
                                    print_string ("Checking "^y^" ...");
                                    let l = List.filter 
                                        (fun ((rname, f1), f2) -> f1 = f) replacements
                                    in
                                        if List.length l != 0 then
                                            begin
                                                print_endline " matched.";
                                                let (rname, f1), f2 = List.hd l in
                                                    (`METerm(`Attribute(`Unqualified(f2))), replacements)
                                            end
                                        else
                                            begin
                                                print_endline " failed.";
                                                (e, replacements)
                                            end
                      end
                | `METerm _ -> (e, replacements)
                | `Sum (l,r) -> rename_binary l r (fun x y -> `Sum(x,y))
                | `Minus (l,r) -> rename_binary l r (fun x y -> `Minus(x,y))
                | `Product (l,r) -> rename_binary l r (fun x y -> `Product(x,y))
                | `Min (l,r) -> rename_binary l r (fun x y -> `Min(x,y))
                | `Max (l,r) -> rename_binary l r (fun x y -> `Max(x,y))
                | `MapAggregate (fn, f, q) ->
                      let (new_q, q_replaced) = rename_plan q replacements in
                      let (new_f, f_replaced) = rename_map_expression f q_replaced in
                          (`MapAggregate(fn, new_f, new_q), [])

                | `IfThenElse _
                | `Delta _ | `New _ | `MaintainMap _
                | `Incr _ | `IncrDiff _ 
                | `Insert _ | `Update _ | `Delete _ -> raise InvalidExpression

    and rename_plan p replacements =
        let override_replacements rname old_fields new_fields =
            let local_replacements = 
                List.combine
                    (List.map 
                        (fun x -> (rname, x))
                            (let (fnames,_) = List.split old_fields in fnames))
                    (let (nfnames,_) = List.split new_fields in nfnames)
            in
            let filtered_replacements =
                List.filter
                    (fun (f,_) -> not(List.mem_assoc f local_replacements))
                    replacements
            in
                filtered_replacements@local_replacements
        in
            match p with
                | `TrueRelation | `FalseRelation -> (p, replacements)
                | `Relation (n,f) ->
                      if Hashtbl.mem base_relation_ht n then
                          let new_counter = (Hashtbl.find base_relation_ht n) + 1 in
                              Hashtbl.replace base_relation_ht n new_counter;
                              let new_f = rename_fields f in
                              let new_replacements = override_replacements n f new_f in
                                  (`Relation (n, new_f), new_replacements)
                      else
                          begin
                              Hashtbl.add base_relation_ht n 0;
                              let new_f = rename_fields f in
                              let new_replacements = override_replacements n f new_f in
                                  (`Relation (n, new_f), new_replacements)
                          end

                | `Select (pred, cq) ->
                      let (new_cq, cq_replaced) = rename_plan cq replacements in
                      let (new_pred, _) = rename_predicate pred cq_replaced in
                          (`Select(new_pred, new_cq), cq_replaced)

                | `Project (projs, cq) ->
                      let (new_cq, cq_replaced) = rename_plan cq replacements in
                      let new_projs =
                          List.map (fun (a, e) ->
                              let (ne, _) = rename_expression e cq_replaced in (a,ne))
                              projs
                      in
                          (`Project(new_projs, new_cq), cq_replaced)

                | `Union ch ->
                      let (new_ch, replaced) = List.split
                          (List.map (fun c -> rename_plan c replacements) ch)
                      in
                          (`Union ch, List.flatten replaced)

                | `Cross (l,r) ->
                      let (new_l, rep_l) = rename_plan l replacements in
                      let (new_r, rep_r) = rename_plan r replacements in
                          (`Cross(new_l,new_r), rep_l@rep_r)

                | `Domain _
                | `DeltaPlan _ | `NewPlan _
                | `IncrPlan _ | `IncrDiffPlan _
                      -> raise InvalidExpression
    in
        rename_map_expression m_expr []


(*******************************
 * Monotonicity analysis
 *
 * TODO: detecting insert/update/delete monotonicity
 *
 * Database monotonicity.
 * -- assume expression f[A]
 * -- compute Delta f | Delta A
 * -- TODO: multivariate monotonicity analysis
 *******************************)

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

let bin_op_monotonicity lm rm flipr =
    let actual_rm =
        if flipr then
            match rm with | Inc -> Dec | Dec -> Inc | x -> x
        else rm
    in
    match (lm, actual_rm) with
	| (Inc, Inc) | (Inc, Same) | (Same, Inc) -> Inc
	| (Dec, Dec) | (Dec, Same) | (Same, Dec) -> Dec
	| (Same, Same) -> Same
	| (Inc, Dec) | (Dec, Inc) | (_, Undef) | (Undef, _) -> Undef 

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

	| `Sum(l,r) | `Product(l,r) | `Min(l,r) | `Max(l,r) | `IfThenElse(_, l, r) ->
	      let lm = compute_monotonicity attr attr_m l in
	      let rm = compute_monotonicity attr attr_m r in
		  bin_op_monotonicity lm rm false

        | `Minus(l,r) ->
	      let lm = compute_monotonicity attr attr_m l in
	      let rm = compute_monotonicity attr attr_m r in
		  bin_op_monotonicity lm rm true

	| `MapAggregate(fn,f,q) ->
	      let fm = compute_monotonicity attr attr_m f in
	      let qm = compute_plan_monotonicity attr attr_m q in
		  agg_monotonicity fm qm
		      
	| `Delta _ | `New _ | `MaintainMap _
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

