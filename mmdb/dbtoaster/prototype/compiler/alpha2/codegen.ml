open Algebra
open Types
open Compilepass

(*******************************
 *
 * Code generation
 *
 *******************************)

type map_identifier = identifier
type profile_identifier = identifier

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
| `Tuple of variable_identifier * field list 
| `Map of map_identifier * (field list) * type_identifier 
| `Set of relation_identifier * (field list)
| `Multiset of relation_identifier * (field list) ]

type code_variable = variable_identifier

type map_key = map_identifier * (code_variable list)

type map_iterator = [ `Begin of map_identifier | `End of map_identifier ]

type domain_key = relation_identifier * (code_variable list)

type domain_iterator = [ `Begin of relation_identifier | `End of relation_identifier ]

type code_terminal = [
| `Int of int
| `Float of float
| `String of string
| `Long of int64
| `Variable of code_variable
| `MapAccess of map_key
| `MapContains of map_key
| `MapIterator of map_iterator
| `DomainContains of domain_key
| `DomainIterator of domain_iterator ]

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

type return_val = [ `Arith of arith_code_expression | `Map of map_identifier ]

type declaration = [
| `Variable of code_variable * type_identifier
| `Tuple of code_variable * (field * return_val) list
| `Relation of relation_identifier * (field list) 
| `Map of map_identifier * (field list) * type_identifier
| `Domain of relation_identifier * (field list)
| `ProfileLocation of profile_identifier ]

type code_expression = [
| `Declare of declaration
| `Assign of code_variable * arith_code_expression
| `AssignMap of map_key * arith_code_expression
| `EraseMap of map_key
| `InsertTuple of datastructure * (code_variable list)
| `DeleteTuple of datastructure * (code_variable list)
| `Eval of arith_code_expression 
| `IfNoElse of bool_code_expression * code_expression
| `IfElse of bool_code_expression * code_expression * code_expression
| `ForEach of datastructure * code_expression
| `ForEachResume of datastructure * code_expression
| `Resume of string list option
| `Block of code_expression list
| `Return of arith_code_expression
| `ReturnMap of map_identifier
| `ReturnMultiple of return_val list * code_variable option
| `Handler of function_identifier * (field list) * type_identifier * code_expression list
| `Profile of string * profile_identifier * code_expression ]

type stream_source_type = File | Socket

(*
 * Non-nested code expression construction
 *)
let rec create_code_expression expr =
    match expr with
	| `ETerm (x) ->
	      begin
		  `CTerm(
		      match x with
			  | `Int y -> `Int y
			  | `Float y -> `Float y
			  | `String y -> `String y
			  | `Long y -> `Long y
			  | `Variable y -> `Variable y
			  | `Attribute y ->
				begin
				    match y with |`Qualified(_,f) | `Unqualified f -> `Variable(f)
				end) 
	      end
	| `UnaryMinus (e) -> raise InvalidExpression
	| `Sum (l,r) -> `Sum(create_code_expression l, create_code_expression r)
	| `Product (l,r)  -> `Product(create_code_expression l, create_code_expression r)
	| `Minus (l,r) -> `Minus(create_code_expression l, create_code_expression r)
	| `Divide (l,r) -> raise InvalidExpression
	| `Function (fid, args) -> raise InvalidExpression

let rec create_code_predicate b_expr =
    match b_expr with
	| `BTerm(x) ->
              let dummy_pair = (`CTerm(`Variable("dummy")), `CTerm(`Int 0)) in
              let binary l r fn = fn (create_code_expression l) (create_code_expression r) in
	      begin
		  `BCTerm(
		      match x with
			  | `True -> `True
			  | `False -> `False

			  | `EQ(l,r) -> binary l r (fun x y -> `EQ(x,y)) 
			  | `NE(l,r) -> binary l r (fun x y -> `NE(x,y))
			  | `LT(l,r) -> binary l r (fun x y -> `LT(x,y))
			  | `LE(l,r) -> binary l r (fun x y -> `LE(x,y))
			  | `GT(l,r) -> binary l r (fun x y -> `GT(x,y))
			  | `GE(l,r) -> binary l r (fun x y -> `GE(x,y))

			  | `MEQ(_) -> `EQ(dummy_pair)
			  | `MNEQ(_) -> `NE(dummy_pair)
			  | `MLT(_) -> `LT(dummy_pair)
			  | `MLE(_) -> `LE(dummy_pair)
			  | `MGT(_) -> `GT(dummy_pair)
			  | `MGE(_) -> `GE(dummy_pair))
	      end
	| `And(l,r) -> `And(create_code_predicate l, create_code_predicate r)
	| `Or(l,r) -> `Or(create_code_predicate l, create_code_predicate r)
	| `Not(e) -> `Not(create_code_predicate e)


(* Basic code type helpers *)
let is_block c_expr =
    match c_expr with | `Block _ -> true | _ -> false

let identifier_of_datastructure =
    function | `Tuple(n,_) | `Map (n,_,_) | `Set (n,_) | `Multiset (n,_) -> n

let datastructure_of_declaration decl =
    match decl with
        | `Variable (n,_)
        | `ProfileLocation n ->
              raise (CodegenException ("Invalid datastructure: "^n))

        | `Tuple (n,f) -> `Tuple (n, (fst (List.split f)))
        | `Relation(n,f) -> `Multiset(n,f)
        | `Map (id, f, rt) -> `Map(id, f, rt)
        | `Domain(n,f) -> `Map(n,f,"int")

let identifier_of_declaration decl =
    match decl with
        | `Variable (v, ty) -> v
        | `Tuple (v,_) -> v
        | `Relation(n, f) -> n
        | `Map (n, f, rt) -> n
        | `Domain(n, f) -> n
        | `ProfileLocation p -> p

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
        | `Tuple (n,f) -> "tuple<"^(String.concat "," (List.map snd f))^">"
	| `Map (n,f,r) ->
	      let key_type = 
		  let ftype = ctype_of_datastructure_fields f in
                      match f with
                          | [] -> raise (CodegenException ("Invalid datastructure type, no fields found: "^n))
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


let element_ctype_of_datastructure d =
    match d with
        | `Variable(n,_)
        | `ProfileLocation n
        | `Tuple(n,_)
            -> raise (CodegenException
                  ("Invalid datastructure for elements: "^n))

	| `Map (n,f,r) ->
	      let key_type = 
		  let ftype = ctype_of_datastructure_fields f in
                      match f with
                          | [] -> raise (CodegenException "Invalid datastructure type, no fields found.")
                          | [x] -> ("const "^ftype)
                          | _ -> ("const tuple<"^ftype^">")
	      in
		  "pair<"^key_type^","^(ctype_of_type_identifier r)^">"

        | `Set(n, f)
	| `Multiset (n,f) ->
	      let ftype = ctype_of_datastructure_fields f in
		  if (List.length f) = 1 then ("const "^ftype) else ("const tuple<"^ftype^">")


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

let advance_iterator it = "++"^it

let iterator_type_of_datastructure ds =
    match ds with
        | `Tuple _ -> raise (CodegenException "No iterator for tuples!")
	| `Map(id, f, _) | `Set(id, f) | `Multiset (id, f) ->
	      (ctype_of_datastructure ds)^"::iterator"

let point_iterator_declaration_of_datastructure ds =
    match ds with
        | `Tuple _ -> raise (CodegenException "No iterator for tuples!")
	| `Map(id, f, _) | `Set(id, f) | `Multiset (id, f) ->
	      let it_id = incr iterator_ref; (string_of_int !iterator_ref) in
	      let it_typ = (ctype_of_datastructure ds)^"::iterator" in
	      let point_it = id^"_it"^it_id in
		  (point_it, it_typ^" "^point_it)
            
let range_iterator_declarations_of_datastructure ds =
    match ds with
        | `Tuple _ -> raise (CodegenException "No iterator for tuples!")
	| `Map(id, f, _) | `Set(id, f) | `Multiset (id, f) ->
	      let it_id = incr iterator_ref; (string_of_int !iterator_ref) in
	      let it_typ = (ctype_of_datastructure ds)^"::iterator" in
	      let begin_it = id^"_it"^it_id in
	      let end_it = id^"_end"^it_id in
		  (begin_it, end_it, it_typ^" "^begin_it, it_typ^" "^end_it)

let field_declarations_of_datastructure ds iterator tab =
    let deref = match ds with
        | `Tuple _ -> raise (CodegenException "No iterator for tuples!")
	| `Map _ -> iterator^"->first"
        | `Set _ | `Multiset _ -> "*"^iterator
    in 
	match ds with
            | `Tuple _ -> raise (CodegenException "No iterator for tuples!")
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

let resume_declarations_of_datastructure ds =
    match ds with
        | `Tuple _ -> raise (CodegenException "No iterator for tuples!")
        | `Map(id, f, _) | `Set(id, f) | `Multiset (id, f) ->
	      let it_id = incr iterator_ref; (string_of_int !iterator_ref) in
	      let it_typ = (ctype_of_datastructure ds)^"::iterator" in
              let resume_it = "resume_"^id^"_it"^it_id in
              let resume_it_decl = it_typ^" "^resume_it in

	      let val_id = incr iterator_ref; (string_of_int !iterator_ref) in
	      let val_typ = (ctype_of_datastructure ds)^"::key_type" in
              let resume_val = "resume_"^id^"_val"^val_id in
              let resume_val_decl = val_typ^" "^resume_val in
                  Some((resume_it_decl, resume_it), (resume_val_decl, resume_val))


let handler_name_of_event ev =
    match ev with
	| `Insert (n, _) -> "on_insert_"^n
	| `Delete (n, _) -> "on_delete_"^n


(*
 * Profiling helpers
 *)

let event_counters = Hashtbl.create 10

let generate_profile_id (event : delta) =
    let new_count = 
        if Hashtbl.mem event_counters event then
            let c = Hashtbl.find event_counters event in
                Hashtbl.replace event_counters event (c+1);
                c+1
        else
            begin Hashtbl.add event_counters event 0; 0 end
    in
    let event_name = match event with
        | `Insert(r,_) -> "insert_"^r
        | `Delete(r,_) -> "delete_"^r
    in
        (event_name^"_prof_"^(string_of_int new_count))

let generate_handler_profile_id handler_name = handler_name^"_prof_id"


let code_locations = Hashtbl.create 10

let generate_profile_location loc_id = 
    if Hashtbl.mem code_locations loc_id then
        Hashtbl.find code_locations loc_id
    else
        begin
            Hashtbl.add code_locations loc_id
                (Hashtbl.length code_locations);
            Hashtbl.find code_locations loc_id
        end

(* 
 * Code expression stringification
 *)

let string_of_code_var_list vl =
    List.fold_left
	(fun acc v -> (if (String.length acc) = 0 then "" else acc^", ")^v)
	"" vl

let string_of_field_list fl =
    List.fold_left
	(fun acc (id, typ) ->
	    (if (String.length acc) = 0 then "" else acc^", ")^
		typ^" "^id)
	"" fl

let string_of_map_key (mid, keys) = mid^"["^(ctype_of_code_var_list keys)^"]"

let string_of_domain_key (did, keys) = did^"["^(ctype_of_code_var_list keys)^"]"

let string_of_datastructure =
    function | `Tuple(n,f) | `Map (n,f,_) | `Set (n,f) -> n | `Multiset (n,f) -> n

let string_of_datastructure_fields =
    function
	| `Tuple (n,f) | `Map (n,f,_) | `Set(n,f) | `Multiset (n,f) ->
              string_of_field_list f

let string_of_declaration =
    function 
	| `Declare(`Variable(n, typ)) -> n^" : "^typ
	| `Declare(`Tuple(n, f_rv_l)) -> n^" : <"^(String.concat "," (snd (List.split (fst (List.split f_rv_l)))))^">"
        | `Declare(`Relation (id,f)) -> id^" : rel("^(string_of_field_list f)^")"
        | `Declare(`Map (id,f,d)) -> id^" : map["^(string_of_field_list f)^"]"
        | `Declare(`Domain (id,f)) -> id^" : dom("^(string_of_field_list f)^")"
        | `Declare(`ProfileLocation p) -> p^" : profile"
        | _ -> raise InvalidExpression

let string_of_code_expr_terminal cterm =
    match cterm with 
	| `Int i -> string_of_int i
	| `Float f -> string_of_float f
	| `Long l -> Int64.to_string l
	| `String s -> "\""^s^"\""
	| `Variable v -> v
	| `MapAccess mk -> string_of_map_key mk
	| `MapContains (mid, keys) -> mid^".find("^(ctype_of_code_var_list keys)^")"
	| `MapIterator (`Begin(mid)) -> mid^".begin()"
	| `MapIterator (`End(mid)) -> mid^".end()"
        | `DomainContains (did, keys) -> did^".find("^(ctype_of_code_var_list keys)^")"
	| `DomainIterator (`Begin(did)) -> did^".begin()"
	| `DomainIterator (`End(did)) -> did^".end()"

let rec string_of_arith_code_expression ac_expr =
    match ac_expr with
	| `CTerm e -> string_of_code_expr_terminal e
	| `Sum (l,r) ->
	      "("^(string_of_arith_code_expression l)^" + "^
		  (string_of_arith_code_expression r)^")"

	| `Minus (l,r) ->
	      "("^(string_of_arith_code_expression l)^" - "^
		  (string_of_arith_code_expression r)^")"

	| `Product(l,r) ->
	      "("^(string_of_arith_code_expression l)^" * "^
		  (string_of_arith_code_expression r)^")"

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


let rec remove_resume_code_expression c_expr =
    let remove c = remove_resume_code_expression c in
    match c_expr with
        | `Declare _ 
        | `Assign _ | `AssignMap _ | `EraseMap _
        | `InsertTuple _ | `DeleteTuple _
        | `Eval _ 
                -> Some(c_expr)

        | `IfNoElse (cond, tc) ->
              let ntc = remove tc in
                  begin
                      match ntc with
                          | Some c -> Some(`IfNoElse(cond, c)) | _ ->  None
                  end

        | `IfElse (cond, tc, ec) ->
              let ntc = remove tc in
              let nec = remove ec in
                  begin match (ntc, nec) with
                      | (None, None) -> None
                      | (None, Some x) -> Some(`IfNoElse(`Not(cond), x))
                      | (Some x,None) -> Some(`IfNoElse(cond, x))
                      | (Some x, Some y) -> Some(`IfElse(cond, x, y))
                  end

        | `ForEach (ds,_) | `ForEachResume (ds,_) ->
              raise (CodegenException
                  ("Cannot resume within nested loop: "^(string_of_datastructure ds)))

        | `Resume code_opt -> None

        | `Block cl ->
              let ncl =
                  List.fold_left
                      (fun acc c -> match c with | None -> acc | Some(ce) -> acc@[ce])
                      [] (List.map remove cl)
              in
                  Some(`Block(ncl))

        | `Return _ -> Some(c_expr)

        | `ReturnMap _ -> Some(c_expr)

        | `ReturnMultiple _ -> Some(c_expr)

        | `Handler (hid,_,_,_) ->  raise (CodegenException
              ("Cannot resume within handler: "^hid))

        | `Profile (stat_type, loc, c) ->
              let nc = remove c in
                  match nc with
                      | None -> None 
                      | Some x -> Some(`Profile(stat_type, loc, x))

let rec bind_resume_code_expression c_expr resume_code =
    let bind c = bind_resume_code_expression c resume_code in
    match c_expr with
        | `Declare _ 
        | `Assign _ | `AssignMap _ | `EraseMap _
        | `InsertTuple _ | `DeleteTuple _
        | `Eval _ 
                -> c_expr

        | `IfNoElse (cond, tc) -> `IfNoElse(cond, bind tc)
        | `IfElse (cond, tc, ec) -> `IfElse(cond, bind tc, bind ec)

        | `ForEach (ds,_) | `ForEachResume (ds,_) ->
              raise (CodegenException
                  ("Cannot resume within nested loop: "^(string_of_datastructure ds)))

        | `Resume code_opt ->
              if code_opt = None then `Resume(Some(resume_code))
              else raise (CodegenException ("Found existing resume code"))

        | `Block cl -> `Block(List.map bind cl)

        | `Return _ -> c_expr

        | `ReturnMap _ -> c_expr

        | `ReturnMultiple _ -> c_expr

        | `Handler (hid,_,_,_) -> raise (CodegenException
              ("Cannot resume within handler: "^hid))

        | `Profile (stat_type, loc, c) -> `Profile(stat_type, loc, bind c)


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
                          | (`Tuple _ as y) | (`Relation _ as y) | (`Map _ as y) | (`Domain _ as y) ->
                                let ctype = ctype_of_datastructure (datastructure_of_declaration y) in
                                    ctype^" "^(identifier_of_declaration y)^";"
                          | `ProfileLocation p ->
                                let counter_val = generate_profile_location p in
                                    "const int32_t "^p^" = "^(string_of_int counter_val)^";"
                  end

	    | `Assign(v,ac) ->
		  v^" = "^(string_of_arith_code_expression ac)^";"

	    | `AssignMap(mk, vc) ->
		  (string_of_map_key mk)^" = "^(string_of_arith_code_expression vc)^";"

	    | `EraseMap (mid,mf) ->
		  mid^".erase("^(ctype_of_code_var_list mf)^");"

            | `InsertTuple (ds, cv_list) ->
                  begin match ds with
                      | `Tuple _ -> raise (CodegenException "Cannot insert into tuple.")
                      | `Map _ ->
                            let dsid = identifier_of_datastructure ds in
                            let dskey = (ctype_of_code_var_list cv_list) in
                            let exists_pred = dsid^".find("^dskey^") == "^dsid^".end()" in
                                "if ( "^exists_pred^" ) { "^dsid^"["^dskey^"] = 1; }\n"^
                                "else { ++"^dsid^"["^dskey^"]; }"

                      | `Set _ | `Multiset _ ->
                            (identifier_of_datastructure ds)^
                                ".insert("^(ctype_of_code_var_list cv_list)^");";

                  end

            | `DeleteTuple (ds, cv_list) -> 
                  begin match ds with
                      | `Tuple _ -> raise (CodegenException "Cannot insert into tuple.")
                      | `Map _ ->
                            let dsid = identifier_of_datastructure ds in
                            let dskey = (ctype_of_code_var_list cv_list) in
                                "if ( "^dsid^".find("^dskey^") != "^dsid^".end() ) {\n"^
                                    "--"^dsid^"["^dskey^"];\n"^
                                    "if ( "^dsid^"["^dskey^"] == 0 ) { "^dsid^".erase("^dskey^"); }\n"^
                                "}"

                      | `Set _ | `Multiset _ ->
                            let id = identifier_of_datastructure ds in
                            let (find_it, find_decl) = point_iterator_declaration_of_datastructure ds in
                                find_decl^" = "^id^".find("^(ctype_of_code_var_list cv_list)^");\n"^
                                    id^".erase("^find_it^");"
                  end

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
			  "for(; "^begin_it^" != "^end_it^"; "^(advance_iterator begin_it)^"){"^
			  (field_declarations_of_datastructure m begin_it "")^"\n"^
			  (string_of_code_expression c)^
			  "}"

	    | `ForEachResume(m,c) ->
                  let deref it = match m with
                      | `Tuple _ -> raise (CodegenException "No iterator for tuples!")
                      | `Map _ -> it^"->first"
                      | `Set _ | `Multiset _ -> "*("^it^")"
                  in
		  let (begin_it, end_it, begin_decl, end_decl) =
		      range_iterator_declarations_of_datastructure m
		  in
                  let resume_opt = resume_declarations_of_datastructure m in
                  let (resume_decl, resume_incr_code, resume_bind_code) = 
                      match resume_opt with
                          | None -> ("", "", [])
                          | Some((resume_it_decl, resume_it), (resume_val_decl, resume_val)) ->
                                let rdecl =
                                    resume_it_decl^" = "^begin_it^";\n"^
                                    "if ( "^resume_it^" != "^end_it^" ) { "^(advance_iterator resume_it)^"; }\n"^
                                    resume_val_decl^";\n"^
                                    "if ( "^resume_it^" != "^end_it^" ) { "^resume_val^" = "^(deref resume_it)^"; }\n\n"
                                in
                                let rcode =
                                    (advance_iterator resume_it)^";\n"^
                                    "bool resume_end = ( "^resume_it^" == "^end_it^" );\n"^
                                    "if ( !resume_end ) {\n"^
                                        resume_val^" = "^(deref resume_it)^";\n"^
                                    "}\n\n"
                                in
                                let rbindcode =
                                    let id = identifier_of_datastructure m in
                                        [ ("if ( !resume_end ) {");
                                        ("    "^resume_it^" = "^id^".find("^(resume_val)^");");
                                        "}";
                                        ("else { "^resume_it^" = "^end_it^"; }");
                                        "continue;"]
                                in
                                    (rdecl, rcode, rbindcode)
                  in
                  let code_w_resume_bound =
                      if resume_bind_code = [] then
                          (match remove_resume_code_expression c with
                              | None -> raise (CodegenException "Removing resume deleted all code")
                              | Some x -> x)
                      else (bind_resume_code_expression c resume_bind_code)
                  in
		      "\n"^begin_decl^" = "^(string_of_datastructure m)^".begin();\n"^
			  end_decl^" = "^(string_of_datastructure m)^".end();\n"^
                          resume_decl^
			  "for(; "^begin_it^" != "^end_it^"; "^(advance_iterator begin_it)^"){"^
			  (field_declarations_of_datastructure m begin_it "")^"\n"^
                          resume_incr_code^
			  (string_of_code_expression code_w_resume_bound)^
			  "}"

            | `Resume resume_code_opt ->
                  begin match resume_code_opt with
                      | None -> (* raise (CodegenException ("No resume code found!")) *) "resume"
                      | Some c ->
                            List.fold_left
                                (fun acc l ->
                                    if (String.length acc) = 0 then l else (acc^"\n"^l))
                                "" c
                  end

	    | `Block (c_expr_l) -> "{"^(string_of_code_block c_expr_l)^"}"

            (* DEMO HACK: no need to return values from handler. *)
	    | `Return (ac) ->
                  (*"return "^(string_of_arith_code_expression ac)^";"*)
                  ""

            | `ReturnMap (mid) ->
                  (*"return "^mid^";"*)
                  ""

            | `ReturnMultiple (rv_l, cv_opt) ->
                  (*
                  let string_of_rv rv = 
                      match rv with
                          | `Arith a -> string_of_arith_code_expression a
                          | `Map mid -> mid
                  in
                  let rv = "make_tuple("^
                      (String.concat "," (List.map string_of_rv rv_l))^");"
                  in
                      begin match cv_opt with
                          | None -> "return "^rv
                          | Some cv -> (cv^" = "^rv^"; return "^cv^";")
                  end
                  *)
                  ""

	    | `Handler (name, args, rt, c_expr_l) ->
		  let h_fields =
		      List.fold_left
			  (fun acc (id, typ) ->
			       (if (String.length acc) = 0 then "" else acc^",")^(typ^" "^id))
			  "" args
		  in
                  let (rv, handler_without_rv) =
                      let rev_handler = List.rev c_expr_l in
                          (List.hd rev_handler, List.rev (List.tl rev_handler))
                  in
                  let handler_profile_id = generate_handler_profile_id name in
		      "void "^name^"("^h_fields^") {\n"^
                          "START_HANDLER_SAMPLE(\"cpu\", "^(handler_profile_id)^");\n"^
                          (string_of_code_block handler_without_rv)^"\n"^
                          "END_HANDLER_SAMPLE(\"cpu\", "^(handler_profile_id)^");\n"^
                          (string_of_code_expression rv)^
                          "\n}"

            | `Profile (statsType, prof_id, c_expr) ->
                  ("{ START_PROFILE(\""^statsType^"\", "^prof_id^")\n")^
                      (string_of_code_expression c_expr)^
                      ("END_PROFILE(\""^statsType^"\", "^prof_id^") }\n")


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
                              | (`Tuple _ as y) | (`Relation _ as y) | (`Map _ as y) | (`Domain _ as y) ->
                                    let ctype = ctype_of_datastructure (datastructure_of_declaration y) in
                                        ctype^" "^(identifier_of_declaration y)^";"
                              | `ProfileLocation p ->
                                let counter_val = generate_profile_location p in
                                    "const int32_t "^p^" = "^(string_of_int counter_val)^";"

		      end
			  
		| `Assign(v,ac) ->
		      v^" = "^(string_of_arith_code_expression ac)^";"

		| `AssignMap(mk, vc) ->
		      (string_of_map_key mk)^" = "^(string_of_arith_code_expression vc)^";"
			  
	        | `EraseMap (mid,mf) ->
		      mid^".erase("^(ctype_of_code_var_list mf)^");"

                | `InsertTuple (ds, cv_list) ->
                      begin match ds with
                          | `Tuple _ -> raise (CodegenException "Cannot insert into tuple.")
                          | `Map _ ->
                                let dsid = identifier_of_datastructure ds in
                                let dskey = (ctype_of_code_var_list cv_list) in
                                let exists_pred = dsid^".find("^dskey^") == "^dsid^".end()" in
                                    "if ( "^exists_pred^" ) { "^dsid^"["^dskey^"] = 1; }\n"^
                                    tab^"else { ++"^dsid^"["^dskey^"]; }"

                          | `Set _ | `Multiset _ ->
                                (identifier_of_datastructure ds)^
                                    ".insert("^(ctype_of_code_var_list cv_list)^");";

                      end

                | `DeleteTuple (ds, cv_list) -> 
                      begin match ds with
                          | `Tuple _ -> raise (CodegenException "Cannot delete from tuple.")
                          | `Map _ ->
                                let dsid = identifier_of_datastructure ds in
                                let dskey = (ctype_of_code_var_list cv_list) in
                                    "if ( "^dsid^".find("^dskey^") != "^dsid^".end() ) {\n"^
                                    ch_tab^"--"^dsid^"["^dskey^"];\n"^
                                    ch_tab^"if ( "^dsid^"["^dskey^"] == 0 ) { "^dsid^".erase("^dskey^"); }\n"^
                                    tab^"}"

                          | `Set _ | `Multiset _ ->
                                let id = identifier_of_datastructure ds in
                                let (find_it, find_decl) = point_iterator_declaration_of_datastructure ds in
                                    find_decl^" = "^id^".find("^(ctype_of_code_var_list cv_list)^");\n"^
                                    tab^id^".erase("^find_it^");"
                      end

		| `Eval(ac) -> string_of_arith_code_expression ac

		| `IfNoElse(p,c) ->
		      "if ( "^(string_of_bool_code_expression p)^" ) {\n"^
			  (sce_aux c (level+1))^"\n"^
			  tab^"}\n"
			  
		| `IfElse(p,l,r) ->
		      "if ( "^(string_of_bool_code_expression p)^" ) {\n"^
			  (sce_aux l (level+1))^"\n"^
			  tab^"}\n"^
		      tab^"else {\n"^
			  (sce_aux r (level+1))^"\n"^
			  tab^"}\n"
			  
		| `ForEach(m,c) ->
		      let (begin_it, end_it, begin_decl, end_decl) =
			  range_iterator_declarations_of_datastructure m
		      in
			  "\n"^tab^
			      begin_decl^" = "^(string_of_datastructure m)^".begin();\n"^
			      tab^end_decl^" = "^(string_of_datastructure m)^".end();\n"^
			      tab^"for(; "^begin_it^" != "^end_it^"; "^(advance_iterator begin_it)^")\n"^
			      tab^"{\n"^
			      (field_declarations_of_datastructure m begin_it ch_tab)^"\n"^
			      (sce_aux c (level+1))^"\n"^
			      tab^"}"

	        | `ForEachResume(m,c) ->
                      let deref it = match m with
                          | `Tuple _ -> raise (CodegenException "No iterator for tuples!")
                          | `Map _ -> it^"->first"
                          | `Set _ | `Multiset _ -> "*("^it^")"
                      in
		      let (begin_it, end_it, begin_decl, end_decl) =
		          range_iterator_declarations_of_datastructure m
		      in
                      let resume_opt = resume_declarations_of_datastructure m in
                      let (resume_decl, resume_incr_code, resume_bind_code) = 
                          match resume_opt with
                              | None -> ("", "", [])
                              | Some((resume_it_decl, resume_it), (resume_val_decl, resume_val)) ->
                                    let rdecl =
                                        tab^resume_it_decl^" = "^begin_it^";\n"^
                                        tab^"if ( "^resume_it^" != "^end_it^" ) { "^(advance_iterator resume_it)^"; }\n"^
                                        tab^resume_val_decl^";\n"^
                                        tab^"if ("^resume_it^" != "^end_it^" ) { "^resume_val^" = "^(deref resume_it)^"; }\n\n"
                                    in
                                    let rcode =
                                        ch_tab^(advance_iterator resume_it)^";\n"^
                                        ch_tab^"bool resume_end = ( "^resume_it^" == "^end_it^" );\n"^
                                        ch_tab^"if ( !resume_end ) {\n"^
                                        ch_tab^"   "^resume_val^" = "^(deref resume_it)^";\n"^
                                        ch_tab^"}\n\n"
                                    in
                                    let rbindcode =
                                        let id = identifier_of_datastructure m in
                                            [ ("if ( !resume_end ) {");
                                            ("    "^resume_it^" = "^id^".find("^(resume_val)^");");
                                            "}";
                                            ("else { "^resume_it^" = "^end_it^"; }");
                                            "continue;"]
                                    in
                                        (rdecl, rcode, rbindcode)
                      in
                      let code_w_resume_bound =
                      if resume_bind_code = [] then
                          (match remove_resume_code_expression c with
                              | None -> raise (CodegenException "Removing resume deleted all code")
                              | Some x -> x)
                          else (bind_resume_code_expression c resume_bind_code)
                      in
			  "\n"^tab^
			      begin_decl^" = "^(string_of_datastructure m)^".begin();\n"^
			      tab^end_decl^" = "^(string_of_datastructure m)^".end();\n"^
                              resume_decl^
			      tab^"for(; "^begin_it^" != "^end_it^"; "^(advance_iterator begin_it)^")\n"^
			      tab^"{\n"^
			      (field_declarations_of_datastructure m begin_it ch_tab)^"\n"^
                              resume_incr_code^
			      (sce_aux code_w_resume_bound (level+1))^"\n"^
			      tab^"}"


                | `Resume resume_code_opt ->
                      begin match resume_code_opt with
                          | None -> (* raise (CodegenException ("No resume code found!")) *) "resume"
                          | Some c ->
                                List.fold_left
                                    (fun acc l ->
                                        if (String.length acc) = 0 then l else (acc^"\n"^tab^l))
                                    "" c
                      end

		| `Block (c_expr_l) ->
		      "{\n"^(string_of_code_block c_expr_l)^"\n"^
			  tab^"}\n"

		| `Return (ac) ->
                      (*"return "^(string_of_arith_code_expression ac)^";"*)
                      ""

                | `ReturnMap (mid) ->
                      (*"return "^mid^";"*)
                      ""

                | `ReturnMultiple (rv_l, cv_opt) ->
                      (*
                      let string_of_rv rv = 
                          match rv with
                              | `Arith a -> string_of_arith_code_expression a
                              | `Map mid -> mid
                      in
                      let rv = "make_tuple("^
                          (String.concat "," (List.map string_of_rv rv_l))^");"
                      in
                          begin match cv_opt with
                              | None -> "return "^rv
                              | Some cv -> cv^" = "^rv^";\n"^tab^"return "^cv^";"
                          end
                      *)
                      ""
		      
		| `Handler (name, args, rt, c_expr_l) ->
		      let h_fields =
			  List.fold_left
			      (fun acc (id, typ) ->
				   (if (String.length acc) = 0 then "" else acc^",")^(typ^" "^id))
			      "" args
		      in
			  (* TODO: handler return type should be same as last code expr*)
			  (* Hacked for now... *)
                          (* DEMO HACK *)
		          (*rt^" "^name^"("^h_fields^") {\n"^*)
		          "void "^name^"("^h_fields^") {\n"^
			      (string_of_code_block c_expr_l)^
			      tab^"\n}"

                | `Profile (statsType, prof_id, c_expr) ->
                      ("{ START_PROFILE(\""^statsType^"\", "^prof_id^")")^"\n"^
                          (sce_aux c_expr level)^"\n"^
                          tab^("END_PROFILE(\""^statsType^"\", "^prof_id^") }\n")

	in
	    tab^out 
    in
	sce_aux c_expr 0


(* Code helpers *)
let get_block_last c_expr =
    match c_expr with
	| `Block cl ->
              let cl_len = List.length cl in
                  begin match List.nth cl (cl_len-1) with
                      | `Resume _ -> List.nth cl (cl_len-2)
                      | x -> x
                  end
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

(* TODO: think about `DeleteTuple, since this may occur on IncrPlan(_,`Minus,_,_)
 *)
let rec get_return_val c_expr =
    match c_expr with
        | `Eval _ -> c_expr
        | `Assign(v, _) -> `Eval(`CTerm(`Variable(v)))
        | `AssignMap(mk,_) -> `Eval(`CTerm(`MapAccess(mk)))
        | `EraseMap((mid,mf) as mk) ->
              print_endline ("Found erase map as return val with keys "^(string_of_code_var_list mf));
              `Eval(`CTerm(`MapAccess(mk)))
        | `IfNoElse(_, c) -> get_return_val c
        | `ForEach(_,c) -> get_return_val c 
        | `ForEachResume(_,c) -> get_return_val c 
        | `Block(y) -> get_return_val (get_block_last c_expr)
        | `Profile(_,_,c) -> get_return_val c
        | _ ->
              print_endline ("get_return_val: "^(indented_string_of_code_expression c_expr));
              raise InvalidExpression

(* TODO: see above note on `DeleteTuple *)
let remove_return_val c_expr =
    let rec remove_aux c =
        match c with
        | `Eval _ -> None
        | `Assign _ -> Some(c)
        | `AssignMap _ -> Some(c)
        | `EraseMap _ -> Some(c)
        | `IfNoElse (p, cc) ->
              begin match remove_aux cc with
                  | None -> None
                  | Some x -> Some(`IfNoElse(p,x))
              end

        | `ForEach(ds,c)
        | `ForEachResume(ds,c) ->
              begin match remove_aux c with
                  | None -> None
                  | Some x -> Some(`ForEach(ds,x))
              end

        | `Block x ->
              let last = get_block_last c in
              let block_without_last = remove_block_last c in
                  begin
                      match remove_aux last with
                          | None -> Some(block_without_last)
                          | Some y -> Some(append_to_block block_without_last y)
                  end

        | `Profile(st, prof_id,c) ->
              begin
                  match remove_aux c with
                      | None -> None
                      | Some x -> Some(`Profile(st, prof_id,x))
              end
        | _ ->
              print_endline ("remove_return_val: "^(indented_string_of_code_expression c));
              raise InvalidExpression                
    in
        match (remove_aux c_expr) with
            | None -> raise (RewriteException "Attempted to remove top level expression")
            | Some x -> x

(* TODO: see above note on `DeleteTuple *)
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
        | `Profile(st,p,c) -> `Profile(st, p, replace_return_val c new_rv)
        (* Top level case *)
        | `ForEach(i,c) 
        | `ForEachResume(i,c)
            -> 
              let last = get_return_val c_expr in 
              let new_id = 
                  match new_rv with
                      | `Assign(v, _) -> v
                      | _ -> raise InvalidExpression
              in
              begin
                  match last with 
                      | `Eval (`CTerm  (`MapAccess(id, _))) -> 
                            `Block [c_expr; `Assign(new_id, `CTerm (`Variable id))]
                      | _ -> `Block [c_expr; new_rv] 
              end
        | _ ->
              print_endline ("replace_return_val: "^(indented_string_of_code_expression c_expr));
              raise InvalidExpression

(* indicates whether the given code is a return value, and whether it is replaceable *)
(* TODO: see above note on `DeleteTuple *)
let rec is_local_return_val code rv =
    match code with
        | `Eval _ -> (code = rv, false)
        | `Assign (v, _) -> (rv = `Eval(`CTerm(`Variable(v))), false)
        | `AssignMap (mk, _) -> (rv = `Eval(`CTerm(`MapAccess(mk))), false)
        | `Block cl -> 
              let block_last = get_block_last code in
              let (local, _) = is_local_return_val (get_block_last code) rv in
                  (local, match block_last with | `Eval _ -> true | _ -> false)
        | `ForEach (_, c) | `ForEachResume(_, c) -> is_local_return_val c rv
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
		      let decls = List.map (fun x -> `Declare x) decl in
                          (replace_return_val block (`Assign(var, arith_rv)),
                          `CTerm(`Variable(var)), (`Variable(var, type_inf_arith_expr arith_rv decls))::decl)
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


let rec get_last_code_expr c_expr = 
    match c_expr with
	| `Declare _ | `Assign _ 
        | `AssignMap _  | `EraseMap _
        | `InsertTuple _ | `DeleteTuple _
        | `Eval _ -> c_expr 
	| `IfNoElse (b_expr, c_expr) -> get_last_code_expr c_expr
	| `IfElse (b_expr, c_expr_l, c_expr_r) -> get_last_code_expr c_expr_r
	| `ForEach (ds, c_expr) -> get_last_code_expr c_expr
	| `ForEachResume (ds, c_expr) -> get_last_code_expr c_expr
        | `Resume _ -> raise InvalidExpression
	| `Block cl -> get_last_code_expr (List.nth cl ((List.length cl) - 1))
        | `Return _ | `ReturnMap _ | `ReturnMultiple _ -> c_expr
	| `Handler (_, args, _, cl) -> get_last_code_expr (List.nth cl ((List.length cl) - 1))
        | `Profile(_,_,c) -> get_last_code_expr c


(* shared code helpers *)
let rec filter_declarations c_expr decl_l =
    match c_expr with
	| `Declare _ ->
              if List.mem c_expr decl_l then None else Some(c_expr)

        | `Assign _ | `AssignMap _  | `EraseMap _
        | `InsertTuple _ | `DeleteTuple _
        | `Eval _
            -> Some(c_expr)

	| `IfNoElse (cond, tc) ->
              let r = filter_declarations tc decl_l in
                  begin match r with
                      | None -> None
                      | Some(ntc) -> Some(`IfNoElse(cond, ntc))
                  end

	| `IfElse (cond, tc, ec) ->
              let rt = filter_declarations tc decl_l in
              let re = filter_declarations ec decl_l in
                  begin match (rt, re) with
                      | (None, None) -> None
                      | (Some(ntc), None) -> Some(`IfNoElse(cond, ntc))
                      | (None, Some(nec)) -> Some(`IfNoElse(`Not(cond), nec))
                      | (Some(ntc), Some(nec)) -> Some(`IfElse(cond, ntc, nec))
                  end

	| `ForEach (ds, fc) ->
              let r = filter_declarations fc decl_l in
                  begin match r with
                      | None -> None
                      | Some(nfc) -> Some(`ForEach(ds,nfc))
                  end

	| `ForEachResume (ds, fc) ->
              let r = filter_declarations fc decl_l in
                  begin match r with
                      | None -> None
                      | Some(nfc) -> Some(`ForEachResume(ds,nfc))
                  end

        | `Resume _ -> Some(c_expr)

	| `Block cl ->
              let ncl =
                  List.fold_left
                      (fun acc opt -> match opt with | None -> acc | Some(c) -> acc@[c])
                      [] (List.map (fun c -> filter_declarations c decl_l) cl)
              in
                  Some(`Block(ncl))

        | `Return _ | `ReturnMap _ | `ReturnMultiple _ -> Some(c_expr)

	| `Handler (id, args, rt, cl) ->
              let ncl =
                  List.fold_left
                      (fun acc opt -> match opt with | None -> acc | Some(c) -> acc@[c])
                      [] (List.map (fun c -> filter_declarations c decl_l) cl)
              in
                  Some(`Handler(id, args, rt, ncl))

        | `Profile(st,loc,c) ->
              let r = filter_declarations c decl_l in
                  begin match r with
                      | None -> None
                      | Some(nc) -> Some(`Profile(st,loc,nc))
                  end

(*
 * Code expression simplification
 *)
let rec substitute_arith_code_vars assignments ac_expr =
    let sa = substitute_arith_code_vars assignments in
    let get_var_matches v = 
	List.filter
	    (fun a -> match a with
		| `Assign(y,_) -> v = y
		| _ -> raise InvalidExpression)
	    assignments
    in
    let substitute_cv_list vl = 
	List.map
	    (fun v ->
		let v_matches = get_var_matches v in
		    begin match v_matches with
			| [] -> v
			| [`Assign(_,`CTerm(`Variable(z)))] -> z
			| _ -> raise DuplicateException
		    end)
	    vl
    in
	match ac_expr with
	    | `CTerm(`Variable(x)) ->
		  let x_matches = get_var_matches x in
		      begin match x_matches with
			  | [] -> ac_expr
			  | [`Assign(_,z)] -> z
			  | _ -> raise DuplicateException
		      end

	    | `CTerm(`MapAccess(mid,kf)) -> `CTerm(`MapAccess(mid, substitute_cv_list kf))
	    | `CTerm(`MapContains(mid, kf)) -> `CTerm(`MapContains(mid, substitute_cv_list kf))
	    | `CTerm(`DomainContains(did, kf)) -> `CTerm(`DomainContains(did, substitute_cv_list kf))

	    | `CTerm(_) -> ac_expr

	    | `Sum (l,r) -> `Sum(sa l, sa r) 
	    | `Minus (l,r) -> `Minus(sa l, sa r) 
	    | `Product (l,r) -> `Product(sa l, sa r) 
	    | `Min (l,r) -> `Min(sa l, sa r)
	    | `Max (l,r) -> `Max(sa l, sa r)



let rec substitute_bool_code_vars assignments bc_expr =
    let sa = substitute_arith_code_vars assignments in
    let sb = substitute_bool_code_vars assignments in
	match bc_expr with
	    | `BCTerm (x) -> `BCTerm(
		  begin match x with
		      | `True -> `True | `False -> `False
		      | `LT (l,r) -> `LT(sa l, sa r)
		      | `LE (l,r) -> `LE(sa l, sa r)
		      | `GT (l,r) -> `GT(sa l, sa r)
		      | `GE (l,r) -> `GE(sa l, sa r)
		      | `EQ (l,r) -> `EQ(sa l, sa r)
		      | `NE (l,r) -> `NE(sa l, sa r)
		  end)

	    | `Not (e) -> `Not(sb e)
	    | `And (l,r) -> `And(sb l, sb r)
	    | `Or (l,r) -> `Or (sb l, sb r)

let rec substitute_code_vars assignments c_expr =
    let sa = substitute_arith_code_vars assignments in
    let sb = substitute_bool_code_vars assignments in
    let sc = substitute_code_vars assignments in
    let get_var_matches v = 
	List.filter
	    (fun a -> match a with
		| `Assign(y,_) -> v = y
		| _ -> raise InvalidExpression)
	    assignments
    in
    let substitute_cv_list vl = 
	List.map
	    (fun v ->
		let v_matches = get_var_matches v in
		    begin match v_matches with
			| [] -> v
			| [`Assign(_,`CTerm(`Variable(z)))] -> z
			| _ -> raise DuplicateException
		    end)
	    vl
    in
	match c_expr with
	    | `Declare d ->
		  begin
		      match d with
			  | `Variable (n,f) ->
				let n_is_asgn =
				    List.exists
					(fun a -> match a with
					    | `Assign(x,_) -> x = n
					    | _ -> raise InvalidExpression)
					assignments
				in
				    if n_is_asgn then
                                        begin
                                            print_endline ("Attempted to substitute assigned variable "^n);
                                            print_endline (indented_string_of_code_expression c_expr);
                                            raise (CodegenException
                                                ("Attempted to substitute assigned variable "^n))
                                        end
				    else `Declare(d)

			  | _ -> `Declare(d)
		  end

	    | `Assign(x, c) ->
		  if not(List.mem c_expr assignments) then
		      `Assign(x, sa c)
		  else c_expr
		      
	    | `AssignMap((mid, kf), c) ->
		  `AssignMap((mid, substitute_cv_list kf), sa c)

	    | `EraseMap (mid, kf) ->
		  `EraseMap (mid, substitute_cv_list kf)

            | `InsertTuple(ds, cvl) -> `InsertTuple(ds, substitute_cv_list cvl)
            | `DeleteTuple(ds, cvl) -> `DeleteTuple(ds, substitute_cv_list cvl)

	    | `IfNoElse(p, c) -> `IfNoElse(sb p, sc c)
	    | `IfElse(p, l, r) -> `IfElse(sb p, sc l, sc r)
	    | `ForEach(ds, c) -> `ForEach(ds, sc c)
	    | `ForEachResume(ds, c) -> `ForEachResume(ds, sc c)
	    | `Resume(s) -> `Resume(s)
	    | `Eval x -> `Eval (sa x)
	    | `Block cl -> `Block(List.map sc cl)
	    | `Return (ac) -> `Return (sa ac)
            | `ReturnMap mid -> `ReturnMap mid
            | `ReturnMultiple (rv_l, cv_opt) ->
                  `ReturnMultiple
                      ((List.map
                          (function | `Arith a -> `Arith(sa a) | `Map mid -> `Map mid)
                          rv_l),
                      cv_opt)

	    | `Handler(n, args, rt, cl) -> `Handler(n, args, rt, List.map sc cl)
            | `Profile(st, p,c) -> `Profile(st, p, sc c)

let rec merge_block_code cl acc =
    match cl with
	| [] -> acc
	| (`Block a)::((`Block b)::t) -> merge_block_code ((`Block(a@b))::t) acc
	| h::t -> merge_block_code t (acc@[h])

let rec simplify_code c_expr =

(*
    print_endline "Simplify code:";
    print_endline (indented_string_of_code_expression c_expr);
    print_endline (String.make 50 '-');

    let r =
*)

    match c_expr with
	| `IfNoElse (p,c) -> `IfNoElse(p, simplify_code c)
	| `IfElse (p,l,r) -> `IfElse(p, simplify_code l, simplify_code r)
	| `ForEach(ds, c) -> `ForEach(ds, simplify_code c)
	| `ForEachResume(ds, c) -> `ForEachResume(ds, simplify_code c)

	(* flatten blocks *)
        | `Block ([x; `Block(y)]) when not(is_block x) ->
              simplify_code (`Block([x]@y))
	| `Block ([`Block(x)]) -> `Block (List.map simplify_code x)
	| `Block ([x]) -> simplify_code x

	| `Block(x) ->
	      (* reorder locally scoped vars to beginning of block
               *  Note: assumes unique variables 
               *)
	      let (decls, code) =
		  List.partition
		      (fun y -> match y with
			  | `Declare(z) -> true | _ -> false) x
	      in
	      let simplified_non_decls = List.map simplify_code code in
              let collapsed_non_decls =
                  List.fold_left
                      (fun acc ce -> match ce with
                          | `Block((h::t) as y) ->
                                begin match h with | `Declare _ -> acc@[ce] | _ -> acc@y end
                          | _ -> acc@[ce])
                      [] simplified_non_decls
              in
	      let non_decls = collapsed_non_decls in

	      (* substitute redundant vars *)
	      let (new_decls, substituted_code) =
		  let assigned_by_vars =
		      List.filter
			  (fun c ->
			      match c with
				  | `Assign(_, `CTerm(`Variable(_))) -> true
				  | _ -> false)
			  non_decls
		  in
		  let decls_assigned_by_vars =
		      List.filter
			  (fun d -> match d with
			      | `Declare(`Variable(v1,_)) ->
				    List.exists
					(fun a -> 
					    match a with
						| `Assign(v2, _) -> v2 = v1
						| _ -> raise InvalidExpression)
					assigned_by_vars
			      | _ -> false) decls
		  in
		  let filtered_decls =
		      List.filter (fun c -> not (List.mem c decls_assigned_by_vars)) decls
		  in
		  let filtered_code =
                      (* substitute LHS vars assigned by RHS vars in code, and
                         remove assignments to corresponding LHS vars *)
		      List.filter
			  (fun c -> not (List.mem c assigned_by_vars))
			  (List.map
			      (substitute_code_vars assigned_by_vars)
			      non_decls)
		  in
		      (filtered_decls, filtered_code)
	      in
	      let reordered_code = new_decls@substituted_code in
	      let merged_code = merge_block_code reordered_code [] in
                  begin match merged_code with
		      | [] -> raise InvalidExpression
		      | [x] -> simplify_code x
		      | h::t ->
                            if merged_code = x then
                                `Block(merged_code)
                            else
                                simplify_code (`Block(merged_code))
		  end

	| `Handler(n, args, rt, c) ->
	      `Handler(n, args, rt, merge_block_code (List.map simplify_code c) [])

        | `Profile(st,p,c) -> `Profile(st, p, simplify_code c)

	| _ -> c_expr

(*
      in
      print_endline "Result:";
      print_endline (indented_string_of_code_expression r);
      print_endline (String.make 50 '-');
      r
*)


(*
 * Code generation from map expressions
 *)

let gc_assign_state_var code var =
    let rv = get_return_val code in
        match rv with
            | `Eval x ->
                  let new_code =
                      if rv = code then `Assign(var, x)
                      else
                          (replace_return_val code (`Assign (var, x)))
                  in
		      new_code
            | _ ->
                  print_endline ("Invalid return val: "^(indented_string_of_code_expression rv));
                  raise (RewriteException "get_assign_state_var: invalid return val")


let gc_incr_state_var code var oplus diff =
    let oper op = function x -> match op with 
	| `Plus -> `Sum (`CTerm(`Variable(var)), x) 
	| `Minus -> `Minus (`CTerm(`Variable(var)), x)
	| `Min -> `Min (`CTerm(`Variable(var)), x)
	| `Max -> `Max (`CTerm(`Variable(var)), x)

        (* Decrmin/max handled separately *)
        | `Decrmin _ | `Decrmax _ -> raise InvalidExpression
    in
    let rv = get_return_val code in
        match rv with
            | `Eval x ->
                  let new_code =
                      match (rv = code, diff) with
                          | (true, true) -> `Block([`Assign(var, oper oplus x); rv])
                          | (true, false) -> `Assign(var, oper oplus x)
                          | (false, true) ->
                                `Block([replace_return_val code (`Assign (var, oper oplus x)); rv])
                          | (false, false) ->
                                replace_return_val code (`Assign (var, oper oplus x))
                  in
		      new_code
            | _ ->
                  print_endline ("Invalid return val: "^
                      (indented_string_of_code_expression rv));
                  raise (RewriteException
                      ("get_incr_state_var: invalid return val"))


let gc_assign_state_map code map_key =
    let rv = get_return_val code in
        match rv with
            | `Eval x ->
                  let new_code =
                      if rv = code then `AssignMap(map_key, x)
                      else
                          replace_return_val code (`AssignMap (map_key, x))
                  in
		      new_code
            | _ ->
                  print_endline ("Invalid return val: "^(indented_string_of_code_expression rv));
                  raise (RewriteException "get_assign_state_map: invalid return val")



let gc_incr_state_map code map_key oplus diff =
    let map_access_code = `CTerm(`MapAccess(map_key)) in
    let oper op = function x -> match op with 
	| `Plus -> `AssignMap(map_key, `Sum (map_access_code, x))
	| `Minus -> `AssignMap(map_key, `Minus(map_access_code, x))
	| `Min -> `AssignMap(map_key, `Min (map_access_code, x))
	| `Max -> `AssignMap(map_key, `Max (map_access_code, x))

        (* Decrmin/max handled separately *)
	| `Decrmin _ | `Decrmax _ -> raise InvalidExpression
    in		
    let rv = get_return_val code in
        match rv with
            | `Eval x ->
                  let new_code =
                      match (rv = code, diff) with
                          | (true, true) -> `Block([(oper oplus x); rv])
                          | (true, false) -> (oper oplus x)
                          | (false, true) ->
                                `Block([replace_return_val code (oper oplus x); rv])
                          | (false, false) ->
                                replace_return_val code (oper oplus x)
                  in
		      new_code
            | _ ->
                  print_endline ("Invalid return val: "^(indented_string_of_code_expression rv));
                  raise (RewriteException "get_incr_state_map: invalid return val")


(*
 * Decrmin/max
 *)
let gc_decr_cmp_agg_check_datastructure dsq_decl dsq_key =
    let ds = datastructure_of_declaration dsq_decl in
    match dsq_decl with
        | `Domain (did,f) ->
              (ds, `CTerm(`DomainContains(dsq_key)), `CTerm(`DomainIterator(`End(did))))
        | _ ->
              let msg = "Invalid decr agg domain: "^(string_of_declaration (`Declare (dsq_decl))) in
                  print_endline msg;
                  raise (CodegenException ("gc_decr_cmp_agg_check_datastructure: "^msg))

let gc_decr_cmp_agg_foreach_body ds recomp_rv recomp_code =
    let tmpvar = gen_var_sym() in
    let foreach_body = 
        if recomp_code = `Eval(recomp_rv) then
            `Assign(tmpvar, recomp_rv)
        else
            replace_return_val recomp_code (`Assign(tmpvar, recomp_rv))
    in
        (tmpvar, `ForEach(ds, foreach_body))

let gc_decr_cmp_agg_validate_rv decr_code recomp_code =
    let decr_rv = get_return_val decr_code in
    let recomp_rv = get_return_val recomp_code in
        begin match (decr_rv, recomp_rv) with
            | (`Eval x, `Eval y) -> (x, y)
            | _ ->
                  let msg = "Invalid return val:\ndecr\n"^
                      (indented_string_of_code_expression decr_code)^"\nrecomp\n"^
                      (indented_string_of_code_expression decr_code)

                  in
                      print_endline msg;
                      raise (RewriteException ("gc_decr_validate_rv: "^msg))
        end


let gc_decr_cmp_agg_state_var decr_code recomp_code dsq_decl dsq_key var var_type =
    let gc_aux decr_rv recomp_rv =
        let (ds, dsq_contains, dsq_end) = gc_decr_cmp_agg_check_datastructure dsq_decl dsq_key in
        let (tmpvar, decr_foreach) = gc_decr_cmp_agg_foreach_body ds recomp_rv recomp_code in
        let decr_code =
            `IfNoElse(`And(
                `BCTerm(`EQ(`CTerm(`Variable(var)), decr_rv)),
                `BCTerm(`EQ(dsq_contains, dsq_end))), 
            `Block(
                [`Declare(`Variable(tmpvar, var_type));
                decr_foreach;
                `Assign(var, (`CTerm(`Variable(tmpvar))));]))
                
        in 
            `Block([decr_code; `Eval(`CTerm(`Variable(var)))])
    in
    let (decr_rv, recomp_rv) = gc_decr_cmp_agg_validate_rv decr_code recomp_code in
        gc_aux decr_rv recomp_rv


let gc_decr_cmp_agg_state_map decr_code recomp_code dsq_decl dsq_key map_key map_ret_type =
    let gc_aux decr_rv recomp_rv =
        let (ds, dsq_contains, dsq_end) = gc_decr_cmp_agg_check_datastructure dsq_decl dsq_key in
        let (tmpvar, decr_foreach) = gc_decr_cmp_agg_foreach_body ds recomp_rv recomp_code in
        let decr_code =
            `IfNoElse(`And(
                `BCTerm(`EQ(`CTerm(`MapAccess(map_key)), decr_rv)),
                `BCTerm(`EQ(dsq_contains, dsq_end))), 
            `Block(
                [`Declare(`Variable(tmpvar, map_ret_type));
                decr_foreach;
                `AssignMap(map_key, (`CTerm(`Variable(tmpvar))));]))
                
        in 
            `Block([decr_code; `Eval(`CTerm(`MapAccess(map_key)))])
    in
    let (decr_rv, recomp_rv) = gc_decr_cmp_agg_validate_rv decr_code recomp_code in
        gc_aux decr_rv recomp_rv


(* Helper for dealing with `MaintainMap *)
let gc_insdel_event m_expr decl event =
    let br = get_base_relations m_expr in
    let event_vars = match event with 
        | `Insert(_, f) | `Delete(_, f) -> List.map (fun (id, ty) -> id) f
    in
    let event_rel = get_bound_relation event in
    let event_rel_decl =
        (* Check event relation is in the recomputed expression,
           and is already declared *)
        let check_base_relations =
            List.exists
                (fun y -> match y with
                    | `Relation(n,_) when n = event_rel -> true
                    | _ -> false) br
        in
            if check_base_relations then
                List.filter
                    (fun y -> match y with
                        | `Relation(n,_) when n = event_rel -> true
                        | _ -> false) decl
            else []
    in
        match (event, event_rel_decl) with
            | (`Insert _, []) -> None
            | (`Insert _, er_decl::t) ->
                  let insert_code =
                      `InsertTuple(datastructure_of_declaration er_decl, event_vars)
                  in
                      Some(insert_code)

            | (`Delete _, []) -> None
            | (`Delete _, er_decl::t) ->
                  let delete_code =
                      `DeleteTuple(datastructure_of_declaration er_decl, event_vars)
                  in
                      Some(delete_code)
                  (*
                  print_endline ("gc_insert_event: called on "^(string_of_delta event)^
                      " with map expr:\n"^(indented_string_of_map_expression m_expr 0));
                  raise (CodegenException ("gc_insert_event: called on "^(string_of_delta event)))
                  *)

let gc_declare_state_for_map_expression m_expr decl unbound_attrs_vars state_id type_list =
    print_endline (string_of_map_expression m_expr);
    let new_type = type_inf_mexpr m_expr type_list decl in

    match unbound_attrs_vars with
	| [] ->
	      let state_var = state_id in
              let r_decl = `Variable(state_var, new_type) in
                  print_endline ("Declaring var state "^state_var^" for "^
                      (string_of_map_expression m_expr));
		  (r_decl, decl@[r_decl])

	| e_uba ->
	      let state_mid = state_id in
	      let (state_map, existing) =
		  let existing_maps =
		      List.filter
			  (fun x -> match x with
			      | `Map(mid, _,_) -> mid = state_mid
			      | _ -> false) decl
		  in
		      match existing_maps with
			  | [] ->
                                let fields = List.map
                                    (fun x -> (field_of_attribute_identifier x,
                                    type_inf_mexpr (`METerm (`Attribute(x))) type_list decl )) e_uba
                                in
				    (`Map(state_mid, fields, new_type), false)
			  | [m] -> (m, true)
			  | _ -> raise (RewriteException "Multiple matching maps.")
	      in
                  print_endline ("Declaring map state "^state_mid^" for "^
                      (string_of_map_expression m_expr));
                  if existing then (state_map, decl) else (state_map, decl@[state_map])


let gc_declare_handler_state_for_map_expression m_expr decl unbound_attrs_vars type_list =
    let global_decfn =
        fun x ->
            gc_declare_state_for_map_expression
                m_expr decl unbound_attrs_vars x type_list
    in
    match m_expr with
	| `Incr (sid,_,_,_,_) | `IncrDiff(sid,_,_,_,_) | `MaintainMap(sid,_,_,_) ->
              begin
                  match unbound_attrs_vars with
                      | [] -> global_decfn  (gen_var_sym())
                      | _ -> global_decfn (gen_map_sym sid)
              end

        | _ ->
              print_endline ("Invalid map expr for declaration:\n"^
                  (indented_string_of_map_expression m_expr 0));
              raise InvalidExpression

let rec gc_ifstmt_map ba = 
    match ba with
        | [(b,a)] -> `BCTerm (`EQ(`CTerm(`Variable(b)), `CTerm (`Variable(a))))
        | (b,a)::tl -> `And (`BCTerm (`EQ(`CTerm (`Variable(b)), `CTerm (`Variable(a)))), gc_ifstmt_map tl)
        | _ -> raise InvalidExpression 

(* TODO: support sliced access for partially bound keys in M-D maps
 * -- requires extension to `ForEach to support partial iteration *)
let gc_foreach_map e e_code e_decl e_vars e_uba_fields mk op diff recursion_decls ba=
    let (mid, mf) = mk in
    let mf_len = List.length mf in
    let bound_f = List.filter (fun f -> List.mem f e_vars) mf in
    let incr_code = gc_incr_state_map e_code mk op diff in
        begin
            print_endline ("mf: "^(string_of_code_var_list mf)^
                " uba: "^(string_of_code_var_list e_uba_fields));
            print_endline ("Accessing "^(string_of_map_key mk)^
                " in expression "^(string_of_map_expression e));
            print_endline ("bound_f: "^(string_of_code_var_list bound_f));
            print_endline ("e_vars: "^(string_of_code_var_list e_vars));
            print_endline ("test: "^(string_of_int (List.length bound_f))^" "^(string_of_int mf_len));
            print_endline ("incr_code: "^(indented_string_of_code_expression incr_code));
            if (List.length bound_f) = mf_len then
                incr_code
            else
                begin
                    let decl_matches =
                        List.filter
                            (fun d -> (identifier_of_declaration d) = mid)
                            (List.map (function
                                | `Declare (`Tuple _) -> raise InvalidExpression
                                | `Declare d -> d) recursion_decls)
                    in
                    let ds = match decl_matches with
                        | [x] ->
                              begin match x with
                                  | `Tuple _ -> raise InvalidExpression
                                  | y -> datastructure_of_declaration y
                              end
                        | [] ->
                              print_endline ("Could not find declaration for "^mid);
                              raise InvalidExpression
                        | _ -> raise DuplicateException
                    in
                    let conditions = 
                        List.filter
                            (fun (b,_) -> List.mem b bound_f) ba;
                    in 
                        if List.length conditions = 0 then 
                            `ForEach(ds, incr_code)
                        else 
                            let ifst = gc_ifstmt_map conditions 
                            in
                                `ForEach(ds, `IfNoElse(ifst, incr_code))
                end
        end


let gc_recursive_state_accessor_code par_decl bindings
        e e_code e_decl e_vars e_uba_fields op diff recursion_decls
=
    match par_decl with
        | `Variable(v,_) ->
                  if (List.mem v e_uba_fields) then
                      begin
                          print_endline ("Could not find declaration for "^v);
                          raise InvalidExpression
                      end
                  else
                      (gc_incr_state_var e_code v op diff, e_decl)

        | `Map(n,f,_) ->
              (* Building map accessors from bindings *)
              let (mk,ba) =
                  let (access_fields, new_bindings) =
                      List.fold_left (fun (acc, bacc ) (id, _) ->
                          let bound_f =
                              List.filter (fun (a,_) ->
                                  (field_of_attribute_identifier a) = id) bindings
                          in
                              match bound_f with
                                  | [] -> (id::acc, bacc)
                                  | [(a, e_opt)] ->
                                        begin
                                            match e_opt with
                                                | Some(`ETerm(`Variable(b))) -> (b::acc, (b,id)::bacc)
                                                | Some(e) ->
                                                      print_endline ("Invalid map access field: "^(string_of_expression e));
                                                      raise (CodegenException
                                                          "Map access fields must be variables!")
                                                | None -> (id::acc, bacc)
                                        end
                                  | _ -> raise DuplicateException)
                          ([], []) f
                  in
                      ((n, List.rev access_fields), new_bindings)
              in
                  (gc_foreach_map e e_code e_decl e_vars e_uba_fields mk op diff recursion_decls ba, e_decl)

let gc_declare_and_incr_state incr_e e_code e_decl e_uba op diff type_list =
    let (state_decl, new_decl) =
        gc_declare_handler_state_for_map_expression incr_e e_decl e_uba type_list
    in
	begin match e_uba with
	    | [] ->
                  let state_var = match state_decl with
                      | `Variable(id, _) -> id | _ -> raise InvalidExpression
                  in
                      (gc_incr_state_var e_code state_var op diff, new_decl)

	    | _ ->
		  let map_key =
                      let mid = match state_decl with
                          | `Map(id,_,_) -> id | _ -> raise InvalidExpression
                      in
		      let mf = List.map field_of_attribute_identifier e_uba in
			  (mid, mf)
		  in
                      print_endline ("Referencing "^(string_of_map_key map_key)^" for "^
                          (string_of_map_expression incr_e));
		      (gc_incr_state_map e_code map_key op diff, new_decl)
	end

let gc_declare_and_assign_state init_e e_code e_decl e_uba type_list=
    let print_debug orig_expr code map_key =
        print_endline "Assigning state map for init";
        print_endline ("expr: "^(string_of_map_expression orig_expr));
        print_endline ("code: "^(indented_string_of_code_expression code));
        print_endline ("Referencing "^(string_of_map_key map_key)^" for "^
            (string_of_map_expression orig_expr))
    in

    let (state_decl, new_decl) =
        gc_declare_handler_state_for_map_expression init_e e_decl e_uba type_list
    in
	begin match e_uba with
	    | [] -> 
                  let state_var = match state_decl with
                      | `Variable(id, _) -> id | _ -> raise InvalidExpression
                  in
                      (gc_assign_state_var e_code state_var, new_decl)

	    | _ ->
		  let map_key =
                      let mid = match state_decl with
                          | `Map(id,_,_) -> id | _ -> raise InvalidExpression
                      in
		      let mf = List.map field_of_attribute_identifier e_uba in
			  (mid, mf)
                  in
                      begin
                          print_debug init_e e_code map_key;
                          (gc_assign_state_map e_code map_key, new_decl)
                      end
	end


let gc_declare_domain sid domain decl type_list =
    let print_debug dom_decl = 
        print_endline ("Declaring domain for "^sid^", "^
            (string_of_declaration (`Declare(dom_decl))));
        List.iter
            (function
                | `Domain(id,f) as x ->
                      print_endline ("existing dom decl: "^(string_of_declaration (`Declare(x))))
                | _ -> raise InvalidExpression)
            (List.filter (function | `Domain _ -> true | _ -> false) decl)
    in
    let dom_id = gen_dom_sym (sid) in
    let dom_field_names = List.map field_of_attribute_identifier domain in
    let dom_fields =
        List.map
            (fun x ->
                (field_of_attribute_identifier x, 
		type_inf_mexpr (`METerm (`Attribute(x))) type_list decl)) domain
    in
    let dom_decl = `Domain(dom_id, dom_fields) in
    let dom_ds = datastructure_of_declaration dom_decl in
        print_debug dom_decl;
        (dom_decl, dom_ds, dom_field_names)


(* TODO: no need for return val -- remove. *)
let gc_finalise_binding_map m_expr binding_map_id
        binding_map_key finalise_map_key binding_var finalise_var
=
    let m_init =
        let br = get_base_relations m_expr in
            simplify_map_expr_constants
                (List.fold_left
                    (fun expr_acc r -> splice expr_acc (`Plan r) (`Plan `FalseRelation))
                    m_expr br)
    in
    let (compare_init, init_code) =
        match m_init with
            | `METerm ((`Int _) as k)
            | `METerm ((`Float _) as k)
            | `METerm ((`String _) as k)
            | `METerm ((`Long _) as k)
                -> (`BCTerm(`EQ(`CTerm(`MapAccess(finalise_map_key)), `CTerm(k))), `CTerm(k))

            | _ -> raise (CodegenException
                  ("Found non-constant initial value for "^
                      (string_of_map_expression m_expr)))
    in
    let finalise_pred =
        `And(`BCTerm(
            `EQ(`CTerm(`Variable(finalise_var)), `CTerm(`Variable(binding_var)))),
        compare_init)
    in
    let finalise_code =      
        `IfNoElse(finalise_pred,
            `Block[`EraseMap(finalise_map_key); `Resume(None)])
    in
    let retval_code =
        let then_code = `Eval(init_code) in
        let else_code = `Eval(`CTerm(`MapAccess(binding_map_key))) in
        `IfElse(
            `BCTerm(`EQ(`CTerm(`MapContains(binding_map_key)),
                `CTerm(`MapIterator(`End(binding_map_id))))),
            then_code, else_code)
    in
        (finalise_code, retval_code)


let gc_finalise_state m_expr sid decl e_uba type_list =
    match e_uba with
        | [] -> raise (CodegenException "gc_finalize_state: invoked on variable.")
        | _  ->
              (* Declare map as necessary, since this may be the first encounter
               * of sid in this handler *) 
              let state_mid = gen_map_sym sid in
              let (new_map, new_decl) = 
                  gc_declare_state_for_map_expression
                      m_expr decl e_uba state_mid type_list
              in
	      let map_key =
                  let mid = match new_map with
                      | `Map(id,_,_) -> id | _ -> raise InvalidExpression
                  in
		  let mf = List.map field_of_attribute_identifier e_uba in
		      (mid, mf)
              in
                  (* Erase from map *)
                  (`EraseMap(map_key), new_decl)



(* map_expression -> binding list -> delta -> boolean
   -> declaration list
   -> (var id * code terminal) list -> (var id list * code terminal) list
   -> declaration list -> (string * string) list ->
   -> declaration list * code_expression  *)
let generate_code handler bindings event body_only event_handler_decls
        map_var_accessors state_p_decls recursion_decls type_list
=
    print_endline ("Generating code for: "^(string_of_map_expression handler));

    (* map_expression -> declaration list -> bool * binding list
         -> code_expression * declaration list *)
    let rec gc_aux e decl bind_info : code_expression * (declaration list) =
        let gc_binary_expr l r decl bind_info merge_rv_fn =
	    let (l_code, l_decl) = gc_aux l decl bind_info in
	    let (r_code, r_decl) = gc_aux r l_decl bind_info in
	        begin
		    match (l_code, r_code) with
		        | (`Eval x, `Eval y) ->  (merge_rv_fn x y, r_decl)
			      
		        | (`Block x, `Eval y) | (`Eval y, `Block x) ->
                              merge_with_block (`Block x) (merge_rv_fn y) r_decl
                                  
		        | (`Block x, `Block y) ->
			      merge_blocks l_code r_code merge_rv_fn r_decl
				  
		        | _ ->
			      print_endline ("gc_binary_expr: "^(string_of_code_expression l_code));
			      print_endline ("gc_binary_expr: "^(string_of_code_expression r_code));
			      raise InvalidExpression
	        end
        in
	match e with
	    | `METerm(x) ->
		  begin
		      (`Eval(`CTerm(
			  match x with 
			      | `Attribute y -> `Variable(field_of_attribute_identifier y) 
			      | `Int y -> `Int y
			      | `Float y -> `Float y 
			      | `Long y -> `Long y
			      | `String y -> `String y
			      | `Variable y ->
                                    if (List.mem_assoc y map_var_accessors) then
                                        List.assoc y map_var_accessors
                                    else
                                        `Variable y)),
                      decl)
		  end

	    | `Sum(l,r) ->
                  gc_binary_expr l r decl bind_info (fun a b -> `Eval(`Sum(a,b)))

	    | `Minus(l,r) ->
                  gc_binary_expr l r decl bind_info (fun a b -> `Eval(`Minus(a,b)))

	    | `Product(l,r) ->
                  gc_binary_expr l r decl bind_info (fun a b -> `Eval(`Product(a,b)))

	    | `Min (l,r) ->
                  gc_binary_expr l r decl bind_info (fun a b -> `Eval(`Min(a,b)))

            | `Max (l,r) ->
                  gc_binary_expr l r decl bind_info (fun a b -> `Eval(`Max(a,b)))

	    | `MapAggregate(fn, f, q) ->
		  let running_var = gen_var_sym() in
		  let (f_code, f_decl) = gc_aux f decl bind_info in
		  let new_type = type_inf_mexpr f type_list decl in
		  print_endline ("!!map aggregate in gc_aux "^string_of_map_expression f ^" "^new_type);
                  let incr_code x =
                      match fn with
                          | `Sum -> `Sum (`CTerm(`Variable(running_var)), x)
                          | `Min -> `Min (`CTerm(`Variable(running_var)), x)
                          | `Max -> `Max (`CTerm(`Variable(running_var)), x)
                  in
		      begin
                          (* TODO: handle nested aggregates that will not produce a single
                           * arith value from compiling f *)
			  match f_code with
			      | `Eval (x) ->
				    let (agg_block, agg_decl) =
					gc_plan_aux q  
					    (`Assign(running_var, incr_code x))
					    f_decl bind_info 
				    in
					(`Block(
					    [`Declare(`Variable(running_var, new_type));
					    agg_block;
					    `Eval(`CTerm(`Variable(running_var)))]), agg_decl)

			      | _ -> raise InvalidExpression
		      end

	    | (`Incr (sid, op, re, bd, e) as oe) | (`IncrDiff(sid, op, re, bd, e) as oe) ->
                  let diff = match oe with `Incr _ -> false | _ -> true in
		  let (e_code, e_decl) = gc_aux e decl bind_info in
		  let (e_is_bound, rc_l) = 
		      match bind_info with
                          | (true, bb) -> is_binding_used e bb
			  | _ -> (false, [])
		  in
                      print_endline ("is_bound "^
                          (string_of_map_expression oe)^" "^(string_of_bool e_is_bound));

                      (* test for rule 67-70 *)
		      if e_is_bound then
                          (* TODO: support multiple bindings for rules 67-70 *)
                          (* Note bound variables should already be declared and assigned.
                           * This currently happens in compile_target *)
                          let (rc, rc_m) = List.hd rc_l in
			  let rc_type = type_inf_mexpr rc_m type_list decl in
 		          let mid = gen_map_sym sid in
		  	  let mid_type = type_inf_mexpr e type_list decl in	
                          let bd_fields = List.map
                              (fun (a, e_opt) ->
                                  let f = field_of_attribute_identifier a in
                                  let ftyp = match e_opt with
                                      | Some(e) -> type_inf_expr e type_list decl
                                      | None -> raise (CodegenException ("Unconstrained binding "^f))
                                  in
                                      (f, ftyp)) bd
                          in
                          let bd_vars = List.map
                              (fun (a, e_opt) ->
                                  let f = field_of_attribute_identifier a in
                                      match e_opt with
                                          | Some(`ETerm(`Variable(v))) -> v
                                          | None | _ ->
                                                raise (CodegenException ("Unconstrained binding "^f)))
                              bd
                          in
		          let c = gen_var_sym() in
                          let map_fields = [(c, rc_type)]@bd_fields in
		          let map_decl = `Map(mid, map_fields, mid_type) in
		          let map_key = (mid, [rc]@bd_vars) in
                          let update_map_key = (mid, [c]@bd_vars) in
                          let new_decl =
                              if List.mem map_decl e_decl then e_decl else e_decl@[map_decl]
                          in
                          let return_val_code decls =
                              let rv = `Eval(`CTerm(`MapAccess(map_key))) in
                              if List.mem_assoc sid state_p_decls then
                                  let (e_vars, _) = List.split(match event with
                                      | `Insert(_,vars) | `Delete(_,vars) -> vars)
                                  in
		                  let e_uba = List.filter
			              (fun uaid -> not(List.mem (field_of_attribute_identifier uaid) e_vars))
			              (get_unbound_attributes_from_map_expression e true)
		                  in
                                  let e_uba_fields = List.map field_of_attribute_identifier e_uba in

                                  let par_decl = List.assoc sid state_p_decls in
                                  let (new_rv,_) = 
                                      gc_recursive_state_accessor_code
                                          par_decl bd e rv decls e_vars e_uba_fields op diff recursion_decls
                                  in
                                      new_rv
                              else
                                  rv
                          in
		              match event with 
			          | `Insert (_,_) ->
                                        print_endline ("new map: "^mid^" rc: "^rc^" c: "^c);
                                        print_endline ("e: \n"^(indented_string_of_map_expression e 0));
                                        let update_code =
                                            (* substitute binding var for map local var *)
                                            let substituted_code = 
                                                substitute_code_vars ([`Assign(rc, `CTerm(`Variable(c)))]) e_code
                                            in
                                                gc_incr_state_map substituted_code update_map_key op diff
                                        in
                                        let (insert_code, insert_decl) =
                                            (* use recomputation code rather than delta code *)
                                            let (re_code, re_decl) = gc_aux re new_decl (false, []) in
                                            let substituted_code = 
                                                substitute_code_vars
                                                    (List.fold_left
                                                        (fun sub_acc (a, e_opt) ->
                                                            match e_opt with
                                                                | None -> sub_acc
                                                                | Some (`ETerm(`Variable(v))) ->
                                                                      let new_sub = 
                                                                          let f = field_of_attribute_identifier a in
                                                                              `Assign(f, `CTerm(`Variable(v)))
                                                                      in
                                                                          sub_acc@[new_sub]
                                                                | _ -> sub_acc)
                                                        [] bd)
                                                    re_code
                                            in
                                                (gc_assign_state_map substituted_code map_key, re_decl)
                                        in
				        let (update_and_init_binding_code, insert_wprof_decl) = 
                                            let prof_loc = generate_profile_id event in
			     	                (`Profile("cpu", prof_loc, 
                                                `Block ([
					            `ForEach (map_decl, update_code);
				 	            `IfNoElse (
                                                        `BCTerm (
                                                            `EQ(`CTerm(`MapContains(map_key)),
                                                            `CTerm(`MapIterator(`End(mid))))),
                                                        insert_code);
                                                    return_val_code insert_decl])),
                                                (`ProfileLocation prof_loc)::insert_decl)
				        in
                                            (update_and_init_binding_code, insert_wprof_decl)

			          | `Delete _ -> 
                                        begin
                                            match op with
                                                | `Decrmin (me,sid)
                                                | `Decrmax (me,sid)
                                                        ->
                                                      (* TODO: generate recomp code and data structure metadata *)
                                                      (*
                                                      let (recomp_code, recomp_decl) = gc_aux me e_decl (false, []) in
                                                      let dsq_id = gen_dom_sym sid in
                                                      let dsq_decl =
                                                          let existing_decl =
                                                              List.filter
                                                                  (fun ds -> match ds with
                                                                      | `Domain(did,_) -> did = dsq_id | _ -> false)
                                                                  recomp_decl
                                                          in
                                                              match existing_decl with
                                                                  | [] -> raise (CodegenException
                                                                        ("No declaration found for agg decr state "^dsq_id))
                                                                  | [x] -> x
                                                                  | _ -> raise DuplicateException
                                                      in
                                                      let dsq_key = in
                                                          gc_decr_cmp_agg_state_map
                                                              e_code recomp_code dsq_decl dsq_key update_map_key mid_type
                                                      *)
                                                      raise InvalidExpression

                                                | otherop ->
                                                      let update_code = gc_incr_state_map e_code update_map_key otherop diff in
                                                      let (finalise_code, retval_code) =
                                                          gc_finalise_binding_map re mid map_key update_map_key rc c
                                                      in
                                                      let update_and_finalise_code = `Block([update_code; finalise_code]) in
                                                          
				                      let (delete_and_update_binding_code, delete_wprof_decl) = 
                                                          let prof_loc = generate_profile_id event in
                                                              (`Profile("cpu", prof_loc,
				                              `Block ([
	 				                          `ForEachResume (map_decl, update_and_finalise_code);
                                                                  return_val_code new_decl])),
                                                              (`ProfileLocation prof_loc)::new_decl)
				                      in
                                                          (delete_and_update_binding_code, delete_wprof_decl)

                                        end

		      else
                          (* Non-binding state update *)
                          let (e_vars, _) = List.split(match event with
                              | `Insert(_,vars) | `Delete(_,vars) -> vars)
                          in
		          let e_uba = List.filter
			      (fun uaid -> not(List.mem (field_of_attribute_identifier uaid) e_vars))
			      (get_unbound_attributes_from_map_expression e true)
		          in
                          let e_uba_fields = List.map field_of_attribute_identifier e_uba in

                              begin match op with
                                  | `Decrmin (me,sid) | `Decrmax(me,sid) -> raise InvalidExpression
                                  | otherop ->
                                        (* Recursive state update *)
                                        if List.mem_assoc sid state_p_decls then
                                            let par_decl = List.assoc sid state_p_decls in
                                                gc_recursive_state_accessor_code par_decl bd
                                                    e e_code e_decl e_vars e_uba_fields otherop diff recursion_decls

                                        (* Local state update *)
                                        else
                                            gc_declare_and_incr_state
                                                oe e_code e_decl e_uba otherop diff type_list
                              end

	    | (`MaintainMap (sid, iop, bd, e) as oe) ->
                  begin
                      (* Note: no need to filter handler args, since e should be a recomputation,
                       * i.e. a map_expression where deltas have not been applied *)
		      let e_uba = get_unbound_attributes_from_map_expression e true in
		      let (e_code, e_decl) = gc_aux e decl bind_info in
                          match iop with
                              | `Init d -> gc_declare_and_assign_state oe e_code e_decl e_uba type_list
                              | `Final d -> gc_finalise_state e sid e_decl e_uba type_list
                  end

	    | _ -> 
		  print_endline("gc_aux: "^(string_of_map_expression e));
		  raise InvalidExpression

    (* plan -> code_expression list -> code_expression * declaration list *)
    and gc_plan_aux q iter_code decl bind_info : code_expression * (declaration list) =
	match q with
	    | `Relation (n,f) ->
		  let (r_decl, new_decl) =
                      (* Use name based matching, since columns are unique *)
                      let existing_decl =
                          List.filter
                              (fun d -> match d with
                                  | `Relation (n2,f2) -> n = n2 | _ -> false)
                              decl
                      in
                          match existing_decl with
                              | [] -> let y = `Relation(n,f) in (y, decl@[y])
                              (* Note: local renaming of fields *)
                              | [`Relation(n2,f2)] -> (`Relation(n2,f), decl)
                              | _ -> raise DuplicateException
		  in
		      (`ForEach(datastructure_of_declaration r_decl, iter_code), new_decl)

            | `Domain (sid, attrs) ->
                  let (domain_decl, new_decl) =
		      let dom_id = gen_dom_sym (sid) in
                      let dom_fields =
                          List.map
                              (fun x ->
                                  (field_of_attribute_identifier x,
                                  type_inf_mexpr (`METerm (`Attribute(x))) type_list decl))
                              attrs
                      in
                      let dom_decl =  `Domain(dom_id, dom_fields) in
                          (dom_decl, if List.mem dom_decl decl then decl else (decl@[dom_decl]))
                  in
                      (`ForEach(datastructure_of_declaration domain_decl, iter_code), new_decl)

            | `TrueRelation -> (iter_code, decl)

	    | `Project (a, `TrueRelation) ->
                  let local_decl =
                      List.concat (List.map
                          (fun (aid,expr) ->
                              let new_var = field_of_attribute_identifier aid in
			      let new_type = type_inf_mexpr (`METerm (`Attribute(aid))) type_list decl
                              in
                                  [`Declare(`Variable(new_var, new_type));
                                  `Assign(new_var, create_code_expression expr)])
                          a)
                  in
                      begin
                          match iter_code with
                              | `Block y -> (`Block(local_decl@y), decl)
                              | _ -> (`Block(local_decl@[iter_code]), decl)
                      end


	    | `Select (pred, cq) ->
		  begin
		      match pred with 
			  | `BTerm(`MEQ(m_expr)) | `BTerm(`MNEQ(m_expr))
			  | `BTerm(`MLT(m_expr)) | `BTerm(`MLE(m_expr))
			  | `BTerm(`MGT(m_expr)) | `BTerm(`MGE(m_expr))
                                ->
                                let print_debug_return_expr pred_var pred_var_code rv_expr =
                                    print_endline ("Declaring pred var "^pred_var^" in "^(string_of_plan q));
                                    print_endline ("pv_code:\n"^
                                        (indented_string_of_code_expression pred_var_code));
                                    print_endline ("rv_code: "^(string_of_arith_code_expression rv_expr));
                                in

				let (pred_var_code, new_decl) = gc_aux m_expr decl bind_info in
				let pred_var_type = type_inf_mexpr m_expr type_list decl in

                                let compute_insert_pred_code () =

                                    (* Compute `Init pred code *)
				    let (pred_cterm, assign_var_code, pred_decl) = 
                                        let pred_var_rv = get_return_val pred_var_code in
                                        let code_wo_rv =
                                            if pred_var_code = pred_var_rv then None
                                            else Some(remove_return_val pred_var_code)
                                        in
                                            match pred_var_rv with
                                                | `Eval(`CTerm(`Variable(x))) ->
						      (* (`Variable(x), code_wo_rv, new_decl) *)
                                                      let error = "Found independent nested map expression"^
                                                          "(should have been lifted.)"
                                                      in
                                                          print_endline ("gc_plan_aux: "^error^":\n"^
                                                              (indented_string_of_code_expression pred_var_code));
                                                          raise (CodegenException error);


                                                | `Eval(`CTerm(`MapAccess(mf))) ->
                                                      (`MapAccess(mf), code_wo_rv, new_decl)

                                                | `Eval(x) ->
						      let pv = gen_var_sym() in 
                                                      let av_code = replace_return_val pred_var_code (`Assign(pv,x)) in
                                                          print_debug_return_expr pv pred_var_code x;
						          (`Variable(pv), Some(av_code), (`Variable(pv, pred_var_type))::new_decl)

                                                | _ ->
                                                      print_endline ("Invalid return val:\n"^
                                                          (indented_string_of_code_expression pred_var_code));
                                                      raise InvalidExpression
				    in
				    let pred_test_code =
                                        let pz_pair = (`CTerm(pred_cterm), `CTerm(`Int 0)) in
                                        let np =
					    match pred with
					        | `BTerm(`MEQ _) -> `EQ(pz_pair) | `BTerm(`MNEQ _) -> `NE(pz_pair)
					        | `BTerm(`MLT _) -> `LT(pz_pair) | `BTerm(`MLE _) -> `LE(pz_pair)
					        | `BTerm(`MGT _) -> `GT(pz_pair) | `BTerm(`MGE _) -> `GE(pz_pair)
					        | _ -> raise InvalidExpression
                                        in
                                            `BCTerm(np)
				    in
                                        (pred_cterm, assign_var_code, pred_decl, pred_test_code)
                                in
                                (* Note: inserts/deletes have `MaintainMap expressions.
                                 * -- Generate insert/delete into base relations before recomputing as necessary
                                 * -- Note this does not generalize for arbitrarily deep nested map expressions yet
                                 *    if we want to share base relations across all recomputations *)

                                (* Code generated and where:
                                 * -- local: insert or del into base relation for recomputation
                                 *    `MaintainMap `Init: insdel code,
                                 *         maintain test code for `Init { pred code for `Init { upper iter code } }
                                 *    `MaintainMap `Final: insdel code,
                                 *         maintain test code for `Final { pred code for `Final }
                                 * -- map_expr recursion: 
                                 *     ++ `MaintainMap `Init: check map decl, recompute code, insert into map
                                 *     ++ `MaintainMap `Final: map decl, erase from map
                                 *     ++ `Incr: map decl (for `Init), check map decl (for `Final),
                                 *               incr code, pred code { upper iter code }
                                 *     ++ Non-incr??
                                 * -- plan recursion:
                                 *     ++ `IncrPlan/IncrDiffPlan: domain decl, domain insert/del
                                 *)

				let (new_iter_code, new_iter_decl) =
                                    match m_expr with
                                        | `MaintainMap (sid, `Init d, bd, e) ->
                                              begin
                                                  let (pred_cterm,assign_var_code,pred_decl,pred_test_code) =
                                                      compute_insert_pred_code()
                                                  in
                                                  let insdel_code = gc_insdel_event e pred_decl event in
                                                  let nc =
                                                      match pred_cterm with
                                                          | `MapAccess(mf) ->
					                        let (mid, _) = mf in
					                        let map_contains_code = `CTerm(`MapContains(mf)) in
                                                                let init_code =
						                    `IfNoElse(
						                        `BCTerm(`EQ(map_contains_code, `CTerm(`MapIterator(`End(mid))))),
                                                                        let pc = `IfNoElse(pred_test_code, iter_code) in
                                                                            match assign_var_code with
                                                                                | None ->  pc
                                                                                | Some av -> `Block([av;pc]))
                                                                in
                                                                    begin match insdel_code with
                                                                        | None -> init_code
                                                                        | Some(ic) -> `Block([ic; init_code])
                                                                    end

                                                          | `Variable _ ->
                                                                let pc = `IfNoElse(pred_test_code, iter_code) in
                                                                    begin match (insdel_code, assign_var_code) with
                                                                        | (None, None) -> pc
                                                                        | (None, Some av) -> `Block([av; pc])
                                                                        | (Some ins, None) -> `Block([ins; pc])
                                                                        | (Some ins, Some av) -> `Block([ins; av; pc])
                                                                    end
                                                  in
                                                      (nc, pred_decl)

                                              end

                                        | `MaintainMap (sid, `Final d, bd, e) ->
                                              begin
                                                  let (pred_cterm,assign_var_code,pred_decl,_) =
                                                      compute_insert_pred_code()
                                                  in
                                                  let insdel_code = gc_insdel_event e pred_decl event in
                                                  let (nc, nc_decl) =
                                                      match pred_cterm with
                                                          | `MapAccess(mf) ->
                                                                let (final_code, final_decl) =
                                                                    let d_sid =
                                                                        match cq with
                                                                            | `IncrPlan(dsid, _, _, _, _)
                                                                            | `IncrDiffPlan(dsid, _, _, _, _) -> dsid
                                                                            | _ -> raise (CodegenException
                                                                                  ("Invalid finalisation plan: "^(string_of_plan cq)))
                                                                    in

                                                                    (* Erase test: if A not in dom *)
                                                                    let (dom_decl, dom_ds, dom_field_names) =
                                                                        gc_declare_domain d_sid d pred_decl type_list in

                                                                    (* Erase code: pred_var_code *)
                                                                    let dom_id = identifier_of_declaration dom_decl in
                                                                    let dom_contains_code =
                                                                        `CTerm(`DomainContains(dom_id, dom_field_names))
                                                                    in
                                                                    let new_decl =
                                                                        if List.mem dom_decl pred_decl then decl else dom_decl::pred_decl
                                                                    in
                                                                        (`IfNoElse(
                                                                            `BCTerm(`EQ(dom_contains_code,
                                                                                `CTerm(`DomainIterator(`End(dom_id))))),
                                                                            pred_var_code),
                                                                        new_decl)
                                                                in 
                                                                    begin match insdel_code with
                                                                        | None -> (final_code, final_decl)
                                                                        | Some(ic) -> (`Block([ic; final_code]), final_decl)
                                                                    end

                                                          | `Variable _ ->
                                                                begin match insdel_code with
                                                                    | None -> (pred_var_code, pred_decl)
                                                                    | Some ins -> (`Block([ins; pred_var_code]), pred_decl)
                                                                end
                                                  in
                                                      (nc, nc_decl)

                                              end

                                        | `Incr _ ->
                                              let (_,assign_var_code,pred_decl,pred_test_code) = compute_insert_pred_code() in
                                              let nc =
                                                  match assign_var_code with
                                                      | None -> `IfNoElse(pred_test_code, iter_code)
                                                      | Some av -> `Block([av; `IfNoElse(pred_test_code, iter_code)])
                                              in
                                                  (nc, pred_decl)

                                        (* We should not have `IncrDiff on the LHS *)
                                        | `IncrDiff _ -> raise (CodegenException
                                              ("Invalid nested select map expression: "^(string_of_map_expression m_expr)))

                                        | _ -> raise (CodegenException
                                              ("Invalid nested select: "^(string_of_map_expression m_expr)))
                                in

                                let (profiled_new_iter_code, pred_wprof_decl) =
                                    let prof_loc = generate_profile_id event in
                                        (`Profile("cpu", prof_loc, new_iter_code),
                                        (`ProfileLocation prof_loc)::new_iter_decl)
                                in
				    gc_plan_aux cq profiled_new_iter_code pred_wprof_decl bind_info 
 
			  | _ ->
				let new_iter_code =
                                    `IfNoElse(create_code_predicate pred, iter_code)
				in
				    gc_plan_aux cq new_iter_code decl bind_info 
		  end
		      
	    | `Union ch ->
		  let (ch_code, ch_decl) = 
		      List.split (List.map (fun c -> gc_plan_aux c iter_code decl bind_info ) ch)
		  in
		      (`Block(ch_code), List.flatten ch_decl)
			  
	    | `Cross(l,r) ->
		  let (inner_code, inner_decl) = gc_plan_aux r iter_code decl bind_info in
		      gc_plan_aux l inner_code inner_decl bind_info 
	                  
	    (* to handle rule 39, 41 *)
            (* TODO: handle unbound attributes in bindings bd *)
	    | `IncrPlan(sid, op, d, bd, nq) | `IncrDiffPlan(sid, op, d, bd, nq) ->
                  let print_incrp_debug q_code incrp_dom decl =
		      print_endline ("rule 39,41: incrp_dom "^(string_of_code_expression (`Declare incrp_dom)));
                      print_endline ("rule 39,41: iter_code "^(string_of_plan nq));
                      print_endline ("rule 39,41: q_code "^(indented_string_of_code_expression q_code));
                      List.iter
                          (function
                              | `Domain (id,f) ->
                                    print_endline ("rule 39,41: decls "^
                                        (string_of_declaration (`Declare(`Domain(id,f)))))
                              | _ -> raise InvalidExpression)
                          (List.filter (function | `Domain _ -> true | _ -> false) decl)

                  in
                  let (dom_decl, dom_ds, dom_field_names) =
                      gc_declare_domain sid d decl type_list in
                  let (new_iter_code, new_decl) =
                      let incr_code = match op with
                          | `Union -> `InsertTuple(dom_ds, dom_field_names)
                          | `Diff -> `DeleteTuple(dom_ds, dom_field_names)
                      in
                          (`Block([incr_code; iter_code]),
                           if List.mem dom_decl decl then decl else dom_decl::decl)
                  in
		  let (q_code, q_decl) = gc_plan_aux nq new_iter_code new_decl bind_info in 
                      print_incrp_debug q_code dom_decl q_decl;
                      (*
                        print_endline ("Referencing "^dom_id^" for "^(string_of_plan q));
		        gc_incr_state_dom q_code q_decl dom_var dom_relation op diff
                      *)
                      (q_code, q_decl)

	    | _ ->
		  print_endline ("gc_plan_aux: "^(string_of_plan q));
		  raise InvalidExpression
    in
	
    let handler_fields = 
        match event with
            | `Insert (_, fields) | `Delete (_, fields) ->
	          print_endline (
	              "Handler("^(string_of_schema fields)^"):\n"^
	                  (indented_string_of_map_expression handler 0));
	          fields
    in

    print_endline (String.make 50 '-');
    print_endline "Generating binding bodies.";

    let (binding_bodies, binding_decls, event_decls_used) =
        let match_binding b = function
            | `Declare(d) -> (identifier_of_declaration d) = b
            | _ -> raise InvalidExpression
        in
        List.fold_left
            (fun (code_acc, decl_acc, used_acc) b ->
                match b with
                    | `BindExpr (v, expr) ->
                          begin
                              if not (List.exists (match_binding v) event_handler_decls) then
                                  (code_acc@[`Assign(v, create_code_expression expr)],
                                  decl_acc@[`Variable(v, type_inf_expr expr type_list decl_acc)],
                                  used_acc)
                              else
                                  (code_acc, decl_acc, used_acc@[List.find (match_binding v) event_handler_decls])
                          end

                    | `BindMapExpr (v, m_expr) ->
                          begin
                              if not (List.exists (match_binding v) event_handler_decls) then
                                  let (binding_code,d) = gc_aux m_expr decl_acc (false, []) in
			          let binding_type = type_inf_mexpr m_expr type_list decl_acc in
                                  let rv = get_return_val binding_code in
                                      match rv with 
                                          | `Eval x ->
                                                (code_acc@[(replace_return_val binding_code (`Assign(v, x)))],
                                                d@[`Variable(v, binding_type)],
                                                used_acc)
                                          | _ ->
                                                print_endline ("Invalid return val: "^(string_of_code_expression rv));
                                                raise InvalidExpression
                              else
                                  (code_acc, decl_acc, used_acc@[List.find (match_binding v) event_handler_decls])
                          end)
            ([], [], []) bindings
    in
    
    print_endline (String.make 50 '-');
    print_endline "Generating handler bodies.";

    let reused_binding_decls =
        List.map
            (function | `Declare(d) -> d | _ -> raise InvalidExpression)
            event_decls_used
    in

    let (handler_body, handler_decl) =
        gc_aux handler (binding_decls@reused_binding_decls) (true, bindings)
    in

    print_endline (String.make 50 '-');

    (* Note: binding declarations are included in declaration_code
     * Filter out reused declarations, since these have already been declared
     * in some other part of the event handler. *)
    let declaration_code =
        List.map
            (fun x -> `Declare(x))
            (List.filter (fun x -> not(List.mem x reused_binding_decls)) handler_decl)
    in
    let separate_global_declarations code =
	List.partition
	    (fun x -> match x with
                | `Declare(`Tuple _)
		| `Declare(`Map _) | `Declare(`Relation _)
                | `Declare(`Domain _) | `Declare(`ProfileLocation _)
                      -> true
		| _ -> false)
            code
    in
        if body_only then
                let (global_decls, handler_code) =
                    separate_global_declarations
	                (declaration_code@binding_bodies@[handler_body])
                in
	            (global_decls,
                    List.map (fun d -> `Declare d) binding_decls, event_decls_used,
                    simplify_code (`Block(handler_code)))
        else
            begin
                let (handler_body_with_rv, result_type) = 
	            match get_return_val handler_body with
	                | `Eval x ->
                              let ret_type = type_inf_arith_expr
                                  x (declaration_code@binding_bodies@[handler_body])
                              in
                                  (replace_return_val handler_body (`Return x), ret_type)

	                | _ ->
                              print_endline ("Invalid return val:\n"^
                                  (indented_string_of_code_expression handler_body));
                              raise InvalidExpression
                in
                let (global_decls, handler_code) =
                    separate_global_declarations
	                (declaration_code@binding_bodies@[handler_body_with_rv])
                in
	            (global_decls,
                    List.map (fun d -> `Declare(d)) binding_decls, event_decls_used,
	            simplify_code
	                (`Handler(handler_name_of_event event,
	                handler_fields, result_type, handler_code)))
            end



(* map expression list ->
   (int * variable identifier) list -> (state_identifier * int) list ->
   (declaration list) * (variable_identifier * code_term) list * (state_identifier * declaration) list *)
let generate_map_declarations maps map_vars state_parents type_list =

    List.iter (fun me ->
        print_endline ("gmd: "^
            " hash:"^(string_of_int (dbt_hash me))^
            " map: "^(string_of_map_expression me)))
        maps;

    List.iter (fun (hv, var) ->
        print_endline ("gmd var: "^var^" hash: "^(string_of_int hv)))
        map_vars;

    List.iter (fun (sid, par, _) ->
        print_endline ("gmd sid: "^sid^" hash: "^(string_of_int par)))
        state_parents;

    print_endline ("# maps: "^(string_of_int (List.length maps))^
        ", #vars: "^(string_of_int (List.length map_vars)));

    let get_map_id m_expr me_uba_and_vars =
        let string_of_attrs attrs =
            List.fold_left (fun acc f -> acc^f)
                "" (List.map field_of_attribute_identifier attrs)
        in
        let (agg_prefix, agg_attrs) = match m_expr with
            | `MapAggregate(fn, f, q) ->
                  let f_uba = get_unbound_attributes_from_map_expression f false in
                      (begin match fn with | `Sum -> "s" | `Min -> "mn" | `Max -> "mx" end,
                      f_uba)
            | _ -> raise InvalidExpression
        in
            agg_prefix^"_"^
                (let r = string_of_attrs agg_attrs in
                    if (String.length r) = 0 then "1" else r)^
                (if List.length me_uba_and_vars = 0 then ""
                else ("_"^(string_of_attrs me_uba_and_vars)))
    in
    let maps_and_hvs = List.combine maps (List.map dbt_hash maps) in
    let (map_decls_and_cterms, var_cterms) =
        List.fold_left
            (fun (decl_acc, accessor_acc) (me, me_hv) ->
                let me_uba_w_vars = get_unbound_attributes_from_map_expression me true in
                let me_id = get_map_id me me_uba_w_vars in
                let (new_decl, _) =
                    gc_declare_state_for_map_expression me [] me_uba_w_vars me_id type_list
                in
                (* Handle mulitple uses of this map *)
                let vars_using_map =
                    let vum = List.map (fun (_,v) -> v)
                        (List.filter (fun (hv, _) -> hv = me_hv) map_vars)
                    in
                        print_endline ("Vars using map "^(string_of_int me_hv)^": "^
                            (List.fold_left (fun acc var ->
                                (if (String.length acc) = 0 then "" else acc^", ")^var) "" vum));
                        vum
                in
                let (actual_new_decl, new_cterm, new_accessors) =
                    match new_decl with
                        | `Map(id,f,_) ->
                              let new_cterm = `MapAccess(id, let (r,_) = List.split f in r) in
                                  (new_decl, new_cterm,
                                  List.map (fun v -> (v, new_cterm)) vars_using_map)

                        | `Variable(n,typ) ->
                              let new_var_sym = gen_var_sym() in 
                              let new_var_decl = `Variable(new_var_sym, typ) in
                              let new_cterm = `Variable(new_var_sym) in
                                  (new_var_decl, new_cterm,
                                  List.map (fun v -> (v, new_cterm)) vars_using_map)
                in
                    (decl_acc@[(me_hv, (actual_new_decl, new_cterm))], accessor_acc@new_accessors))

                (* Associate map key with each var using map
                let decl_map_key =
                    match new_decl with
                        | `Map(id,f,_) -> (id, let (r,_) = List.split f in r)
                        | `Variable(n,typ) ->
                        | _ -> raise (CodegenException ("Invalid map declaration:"^
                              (identifier_of_declaration new_decl)))
                in
                let new_accessors = List.map
                    (fun v -> (v, `MapAccess(decl_map_key))) vars_using_map
                in
                    (decl_acc@[(me_hv, (new_decl, `MapAccess(decl_map_key)))],
                        accessor_acc@new_accessors))
                *)
            ([], []) maps_and_hvs
    in
    let (new_stp_decls, stp_decls) = List.fold_left
        (fun (decl_acc, stp_acc) (sid, par, m) ->
            let is_map = List.mem_assoc par map_decls_and_cterms in
            let is_decl = if is_map then false else (List.mem_assoc par decl_acc) in
                match (is_map, is_decl) with
                    | (true, false) | (false, true) -> 
                          let decl =
                              if is_map then
                                  let (r, _) = List.assoc par map_decls_and_cterms in r
                              else
                                  List.assoc par decl_acc
                          in
                              (decl_acc, stp_acc@[(sid, decl)])

                    | (false, false) ->
                          begin match m with
                              | `Incr(_,_,_,bd,_) | `IncrDiff(_,_,_,bd,_) when (List.length bd > 0) ->
                                    let new_map_id = gen_map_sym sid in
			            let new_type = type_inf_mexpr_map m type_list maps map_vars in
                                    let new_fields =
                                        List.map
                                            (fun (a, e_opt) ->
                                                let f = field_of_attribute_identifier a in
                                                    (f, 
                                                    match e_opt with
                                                        | None ->
                                                              type_inf_mexpr_map
                                                                  (`METerm (`Attribute(a))) type_list maps map_vars
                                                        | Some (e) ->  type_inf_expr e type_list []))
                                            bd
                                    in
                                    let new_decl = `Map(new_map_id, new_fields, new_type) in
                                        print_endline ("Using newly declared map: "^new_map_id^
                                            " for state id: "^sid^" hash: "^(string_of_int par)^
                                            " fields: "^(string_of_schema_fields new_fields)^
                                            " type: "^new_type);
                                        (decl_acc@[(par, new_decl)], stp_acc@[(sid, new_decl)])

                              | _ ->
                                    let new_decl_var = gen_var_sym() in
			            let new_type = type_inf_mexpr_map m type_list maps map_vars in
                                    let new_decl = `Variable(new_decl_var, new_type) in
                                        print_endline ("Using newly declared var: "^new_decl_var^
                                            " for state id: "^sid^" hash: "^(string_of_int par)^" type: "^new_type);
                                        (decl_acc@[(par, new_decl)], stp_acc@[(sid, new_decl)])
                          end

                    | _ -> raise (CodegenException "Invalid parent: multiple declarations found."))

        ([],[]) state_parents
    in
    let all_decls =
        (List.map (fun (_, d) -> d) new_stp_decls)@
        (List.map (fun (_,(d,_)) -> d) map_decls_and_cterms)
    in
        (all_decls, var_cterms, stp_decls)






(*
 * File I/O generation
 *)
let generate_fileio_includes out_chan =
    output_string out_chan "#include <fstream>\n";
    output_string out_chan "#include <boost/tokenizer.hpp>\n\n";
    output_string out_chan "using namespace boost;\n\n";
    output_string out_chan "typedef tokenizer <char_separator<char> > tokeniz;\n\n"


(* parent class to handle file_stream *)
let file_stream_body = 
    "struct file_stream\n"^
	"{\n"^
	"    public:\n"^
	"    string delim;\n"^
	"    int num_fields;\n"^
	"    ifstream *input_file;\n"^
	"\n"^
	"    file_stream(string file_name, string d, int n): num_fields(n), delim(d) {\n"^
 	"        input_file = new ifstream(file_name.c_str());\n"^
	"        if(!(input_file->good()))\n"^
	"            cerr << \"Failed to open file \" << file_name << endl;\n"^
	"    }\n"^
	"\n"^
 	"    tuple<bool, tokeniz> read_inputs(int line_num)\n"^
	"    {\n"^
	"        char buf[256];\n"^
	"        char_separator<char> sep(delim.c_str());\n"^
	"        input_file->getline(buf, sizeof(buf));\n"^
	"        string line = buf;\n"^
	"        tokeniz tokens(line, sep);\n"^
	"\n"^
	"        if(*buf == '\\0')\n"^
	"            return make_tuple(false, tokens);\n"^
	"\n"^
	"        if (is_valid_num_tokens(tokens)) {\n"^
	"            cerr<< \"Failed to parse record at line \" << line_num << endl;\n"^
	"            return make_tuple(false, tokens);\n"^
	"        }\n"^
	"        return make_tuple(true, tokens);\n"^
	"    }\n"^
	"\n"^
	"    inline bool is_valid_num_tokens(tokeniz tok)\n"^
	"    {\n"^
	"        int i = 0;\n"^
	"        for (tokeniz::iterator beg = tok.begin(); beg!=tok.end(); ++beg, i ++)\n"^
	"            ;\n"^
	"        return i == num_fields;\n"^
	"    }\n"^
	"\n"^
	"    void init_stream()\n"^
	"    {\n"^
	"        int line = 0;\n"^
	"        while(input_file -> good())\n"^
	"        {\n"^
	"            tuple<bool, tokeniz> valid_input = read_inputs(line ++);\n"^
	"            if(!get<0> (valid_input))\n"^
	"                break;\n"^
	"            if(!insert_tuple(get<1>(valid_input)))\n"^
	"                cerr<< \"Failed to parse record at line \" << line << endl;\n"^
	"        }\n"^
	"    }\n"^
	"\n"^
	"    virtual bool insert_tuple(tokeniz t) =0;\n"^
	"};\n"^
	"\n"

let make_file_streams `Relation(id, fields) =
    let typename = 
	let ftype = ctype_of_datastructure_fields fields in
	    if (List.length fields) = 1 then ftype else "tuple<"^ftype^">" in
    let classname = "stream_"^id in
    let struct_f_1 = 
	"struct "^classname^" : public file_stream\n"^
	    "{\n"^
	    "    "^classname^"(string filename, string delim, int n)\n"^
	    "        : file_stream(filename, delim, n) { }\n"^
	    "\n"^
	    "    bool insert_tuple(tokeniz tok)\n"^
	    "    {\n"^
	    "        "^typename^" r;\n"^
	    "        int i = 0;\n"^
	    "        for (tokeniz::iterator beg = tok.begin(); beg!=tok.end(); ++beg) {\n"
    in
    let switch_body fields n = 
	let (m, str) = 
	    List.fold_left 
		(fun (n,s) (_,t) ->
		    (n+1, 
		    (* TODO : need to handle string, long, float too *)
		    (if t = "int" then s^
			"            case "^(string_of_int n)^":\n"^ 
			"                get<"^(string_of_int n)^">(r) = atoi((*beg).c_str());\n"^
			"                break;\n" else s))
		) 
		(0,"") fields
	in 	
	    (m, "            switch(i) {\n"^str^"            }\n")
    in let (num_fields, s_body) = switch_body fields 0 
    in let struct_f_2 = 
	    "\n"^
	        "            i ++;\n"^
	        "        }\n"^
	        "\n"^
	        "        "^id^".insert(r);\n"^
	        "        return true;\n"^
	        "    }\n"^
	        "\n"^
	        "};\n"^
	        "\n"
    in let whole_body = struct_f_1 ^ s_body ^ struct_f_2
    in let caller = 
	    let rec arguments acc n =
	        if n = num_fields then acc
	        else if n = 0 then arguments (acc^"get<"^string_of_int n^"> (*front)") (n+1)
	        else arguments (acc^", get<"^string_of_int n^"> (*front)") (n+1)
	    in
	        function x -> 
	            "    {\n"^
		        "        list <"^typename^" >::iterator front = "^id^".begin();\n"^
		        "        list <"^typename^" >::iterator end = "^id^".end();\n"^
		        "        for ( ; front != end; ++front) {\n"^
		        "            "^x^"( "^(arguments "" 0) ^");\n"^
		        "        }\n"^
		        "    }\n" 
    in (classname, whole_body, caller)

let config_handler global_decls = 
    let c_handler_1 = 
	"list<file_stream *> initialize(ifstream *config)\n"^
	    "{\n"^
	    "    char buf[256];\n"^
	    "    string line;\n"^
	    "    char_separator<char> sep(\" \");\n"^
	    "    int i;\n"^
   	    "    list <file_stream *> files;\n"^
	    "    file_stream * f_stream;\n"^
	    "\n"^
	    "    while(config->good()) {\n"^
	    "        string param[4];\n"^
	    "        config->getline(buf, sizeof(buf));\n"^
	    "        line = buf;\n"^
	    "        tokeniz tok(line, sep);\n"^
	    "        i = 0;\n"^
	    "\n"^
	    "        for (tokeniz::iterator beg = tok.begin() ; beg != tok.end(); ++beg, i ++) {\n"^
	    "            param[i] = *beg;\n"^
	    "        }\n"^ 
	    "\n" in
    let c_handler_2 =
	"\n"^
	    "        f_stream->init_stream();\n"^
	    "        files.push_back(f_stream);\n"^
	    "    }\n"^
	    "    return files;\n"^
	    "}\n\n" in
    let if_stmts = 
	let param = "param[1], param[2], atoi(param[3].c_str())" in
	    List.fold_left
	        (fun acc x -> match x with
		        `Declare d ->
		            begin
			        match d with
			                `Relation(i,f) -> if acc = "" then 
				            acc^"        if(param[0] == \""^i^"\")\n"^
				                "            f_stream = new stream_"^i^"("^param^");\n"
				        else 
				            acc^"        else if(param[0] == \""^i^"\")\n"^
				                "            f_stream = new stream_"^i^"("^param^");\n"
			            | _ -> acc
		            end
		    | _ -> raise InvalidExpression ) "" global_decls 
    in c_handler_1 ^ if_stmts ^ c_handler_2

