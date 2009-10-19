%{
    open Algebra

    let parse_error s =
        let lex_pos = symbol_end_pos () in
        let line_pos_str = string_of_int (lex_pos.Lexing.pos_lnum) in
        let char_pos_str = string_of_int
            (lex_pos.Lexing.pos_cnum - lex_pos.Lexing.pos_bol)
        in
            print_endline
                ("Parsing error at line: "^line_pos_str^
                    " char: "^(char_pos_str));
            flush stdout

    (* Parser debugging helpers *)
    let string_of_relation_list rl =
        String.concat ","
            (List.map
                (function
                    | Rel(n,_) -> n
                    | _ -> raise (Failure "Invalid relation."))
                rl)

    (* SQL->DBToaster typing *)

    let create_type t =
        match t with
            | "int" | "integer" -> TInt
            | "int8" | "bigint" -> TLong
            | "float" | "double" -> TDouble
            | "text" -> TString
            | _ -> raise Parse_error

    (* Aggregate function definitions *)

    let aggregate_functions = Hashtbl.create 4
    let _ = List.iter
        (fun (agg, ast) -> Hashtbl.add aggregate_functions agg ast)
        [ ("SUM", `Sum); (*("MIN", `Min); ("MAX", `Max)*) ]


    (* Table definitions *)

    (* table name -> fields *)
    let relations = Hashtbl.create 10

    (* table name -> (file name, delimiter) *)
    let relation_sources = Hashtbl.create 10

    let add_relation name fields =
        Hashtbl.replace relations (String.uppercase name) fields

    (* See Runtime.ml for contents of source info. *)
    let add_source name source_info =
        Hashtbl.replace relation_sources
            (String.uppercase name) (name, source_info)

    (* Table aliasing in queries *)

    (* table alias name -> table name *)
    let alias_relations = Hashtbl.create 10

    let add_alias name alias = Hashtbl.replace alias_relations alias name

    let get_original alias =
        try Hashtbl.find alias_relations alias
        with Not_found -> alias

    let clear_aliases () = Hashtbl.clear alias_relations


    (* Nested query scoping *)

    (* Stack of relations bound above subqueries *)
    let outer_relations = ref []
    let current_relations = ref []

    let push_relation r =
        current_relations := (!current_relations)@[r]

    let pop_relations rl =
        (* print_endline ("Popping: "^(string_of_relation_list rl)); *)
        current_relations :=
            List.filter (fun r -> not(List.mem r rl)) (!current_relations)

    let clear_relations () = current_relations := []

    let get_outer_relations () = !current_relations

    (* Scoping helpers *)

    (* readable_relalg_t -> readable_relalg_lf_t list *)
    let rec get_base_relations_plan r =
        let relalg_lf lf = match lf with
            | Rel(_) -> [lf]
            | AtomicConstraint(_, t1, t2) ->
                  (get_base_relations t1)@(get_base_relations t2)
            | _ -> []
        in
            fold_relalg Util.ListAsSet.multiunion 
                Util.ListAsSet.multiunion relalg_lf r

    (* readable_term_t -> readable_relalg_lf_t list *)
    and get_base_relations t =
        let term_lf lf = match lf with
            | AggSum(f,r) ->
                  (get_base_relations f)@(get_base_relations_plan r)
            | _ -> []
        in
            fold_term Util.ListAsSet.multiunion 
                Util.ListAsSet.multiunion term_lf t

    (* readable_relalg_t -> readable_term_lf_t list *)
    let get_free_variables_from_plan r =
        (*
        let debug_free_vars bound_vars vars =
            print_endline ("RelAlg xx : "^
                (Algebra.relalg_as_string (make_relalg r) []));
            print_endline ("Bound vars: "^
                (String.concat "," (List.map fst bound_vars)));
            print_endline ("Used vars: "^
                (String.concat "," (List.map fst vars)));
        in
        *)
        let bound_vars = List.flatten (List.map
            (function
                | Rel(r,v) ->
                      v@(List.map (fun (n,t) -> (r^"."^n, t)) v)
                | _ -> 
                      let msg =  "Internal error: "^
                          "get_free_variables_from_plan expected a relation"
                      in
                          raise (Failure (msg)))
            (get_base_relations_plan r))
        in
        let vars = relalg_vars (make_relalg r) in
            (*debug_free_vars bound_vars vars;*)
            Util.ListAsSet.diff vars bound_vars

    (* readable_term_t -> readable_term_lf_t list *)
    let get_free_variables_from_map_term t =
        (*
        let debug_free_vars bound_vars vars =
            print_endline ("Term : "^
                (Algebra.term_as_string (make_term t) []));
            print_endline ("Bound vars: "^
                (String.concat "," (List.map fst bound_vars)));
            print_endline ("Used vars: "^
                (String.concat "," (List.map fst vars)));

        in
        *)
        let bound_vars = List.flatten (List.map
            (function
                | Rel(r,v) ->
                      v@(List.map (fun (n,t) -> (r^"."^n, t)) v)
                | _ -> 
                      let msg =  "Internal error: "^
                          "get_free_variables_from_map_term expected a relation"
                      in
                          raise (Failure (msg)))
            (get_base_relations t))
        in
        let vars = term_vars (make_term t) in
            (*debug_free_vars bound_vars vars;*)
            Util.ListAsSet.diff vars bound_vars

    (* Attribute name qualification *)

    (* TODO: generalize for arbitrary group-by expressions,
     * not just for columns *)
    (* TODO: handle schema names *)

    (* string -> string * string *)
    let get_field_and_relation name =
        let field_name_pos =
            try (String.rindex name '.')+1 with Not_found -> 0 in
        let field_name =
            String.sub name field_name_pos
                ((String.length name) - field_name_pos) in
        let relation_name =
            if field_name_pos = 0 then ""
            else String.sub name 0 (field_name_pos-1)
        in
            (*print_endline ("Found field "^relation_name^"."^field_name);*)
            (relation_name, field_name)

    (* (string, (string * type_t) list) Hashtbl.t -> string
     *     -> (string * type_t) list *)
    let get_field_relations fields field_name =
        if Hashtbl.mem fields field_name
        then Hashtbl.find fields field_name
        else raise (Failure ("No field "^field_name^" found in any table."))

    (* readable_relalg_lf_t list
     *    -> (string, (string * type_t) list) Hashtbl.t *)
    let get_field_relations_map base_relations =
        (*
        let debug_field_rels global_relations =
            print_endline ("Base relations: "^
                (string_of_relation_list base_relations));
            print_endline ("Resolving with global relations: "^
                (string_of_relation_list global_relations));
        in
        *)
        let m = Hashtbl.create 10 in
        let add_fields h n f =
            List.iter (fun (id, ty) ->
                let rels_and_types =
                    if Hashtbl.mem h id then Hashtbl.find h id else []
                in
                    Hashtbl.replace h id (rels_and_types@[(n,ty)]))
                f
        in
        let global_relations =
            List.fold_left
                (fun acc r -> if List.mem r acc then acc else (acc@[r]))
                (get_outer_relations()) base_relations
        in
            (*debug_field_rels global_relations;*)

            (* Add all base relation fields for validation *)
            List.iter
                (function
                    | Rel(n,f) -> add_fields m n f
                    | _ -> raise (Failure
                          ("Internal error: expected a relation.")))
                global_relations;
            m

    (* string -> (string , (string * type_t) list) Hashtbl.t
     *     -> string * string * type_t *)
    let qualify_field name fields =
        let (relation_name, field_name) = get_field_and_relation name in
        let (field_relations, field_types) =
            List.split (get_field_relations fields field_name) in
        let single_relation =
            (List.length field_relations = 1) &&
                (relation_name = "" ||
                        relation_name = (List.hd field_relations))
        in
        let valid_relation =
            List.mem relation_name field_relations in
        let valid_field = single_relation || valid_relation in
            if not valid_field then
                let rel_names = String.concat "," field_relations in
                let msg =
                    if List.length field_relations > 1 then
                        ("Ambiguous field "^
                            (if relation_name = "" then ""
                            else relation_name^".")^
                            field_name^" in relations "^rel_names)
                    else
                        ("Invalid field '"^relation_name^"."^field_name^"'")
                in
                    raise (Failure msg)
            else
                let r =
                    if relation_name = "" then List.hd field_relations
                    else relation_name
                in
                let field_type =
                    List.assoc r (List.combine field_relations field_types)
                in
                    (r, field_name, field_type)

    (* readable_relalg_lf_t list -> string list
     *     -> (string * string * type_t) list *)
    let qualify_fields base_relations fields =
        let br_fields = get_field_relations_map base_relations in
            List.map (fun name -> qualify_field name br_fields) fields


    (* readable_relalg_t -> readable_relalg_t -> readable_relalg_t *)
    let qualify_relational_attributes ra_to_qualify ra =
        (*
        let debug_qualify free_var_names r =
            print_endline ("RelAlg: "^
                (relalg_as_string (make_relalg ra_to_qualify) []));
            print_endline ("Free vars: "^
                (String.concat "," free_var_names));
            print_endline ("Qual RelAlg: "^
                (relalg_as_string (make_relalg r) []));
        in
        *)
        let relations = get_base_relations_plan ra in
        let free_vars = get_free_variables_from_plan ra_to_qualify in
        let free_var_names = List.map fst free_vars in
        let free_var_fields = qualify_fields relations free_var_names in
        let free_var_mappings =
            List.map 
                (fun ((id,t), (nr, nid, nt)) -> ((id,t), (nr^"."^nid, nt)))
                (List.combine free_vars free_var_fields)
        in
        let r = 
            readable_relalg (apply_variable_substitution_to_relalg
                free_var_mappings (make_relalg ra_to_qualify))
        in
            (*debug_qualify_free_var_names r;*)
            r

    (* readable_term_t -> readable_relalg_t -> readable_term_t *)
    let qualify_aggregate_arguments term plan =
        (*
        let debug_qualify free_var_names =
            print_endline ("Free agg arg vars: "^
                (String.concat "," free_var_names));
        in
        *)
        let relations = get_base_relations_plan plan in
        let free_vars = get_free_variables_from_map_term term in
        let free_var_names = List.map fst free_vars in
        let free_var_fields = qualify_fields relations free_var_names in
        let free_var_mappings =
            List.map 
                (fun ((id,t), (nr, nid, nt)) -> ((id,t), (nr^"."^nid, nt)))
                (List.combine free_vars free_var_fields)
        in
            (*debug_qualify free_var_names;*)
            readable_term
                (apply_variable_substitution_to_term
                    free_var_mappings (make_term term))


    (* [< `Column of string] -> (string, (string * type_t) list) Hashtbl.t ->
           (string * string * type_t) list -> (string * string * type_t) *)
    let qualify_group_by_column c fields group_bys =
        match c with
            | `Column id ->
                  let (rel, field_name) = get_field_and_relation id in
                  let field_relations = get_field_relations fields field_name in
                  let (relation_name, field_type) =
                      if rel = "" then List.hd field_relations
                      else 
                          List.find (fun (r,_) -> r = rel) field_relations
                  in
                      if not(List.mem
                          (relation_name, field_name, field_type) group_bys)
                      then
                          raise (Failure
                              ("Invalid group by field "^
                                  relation_name^"."^field_name))
                      else
                          (relation_name, field_name, field_type)

            | _ -> raise (Failure
                  ("Internal error: expected a `Column for a group-by column"))

    (* relalg_lf_t -> string list -> [< `Column of string] list
     *     -> (string * string * type_t) list *)
    let qualify_group_by_fields base_relations group_bys columns =
        let br_fields = get_field_relations_map base_relations in
        let valid_group_by_fields =
            List.map (fun name -> qualify_field name br_fields) group_bys
        in
        let valid_group_by_columns =
            List.map
                (fun c ->
                    qualify_group_by_column
                        c br_fields valid_group_by_fields)
                columns
        in
            valid_group_by_columns

    (* Relalg and term constructors *)
    let create_constraint op_token l r =
        let c = match op_token with
            | `EQ -> AtomicConstraint(Eq, l, r)
            | `NE -> AtomicConstraint(Neq, l, r)
            | `LT -> AtomicConstraint(Lt, l, r)
            | `LE -> AtomicConstraint(Le, l, r)
            | `GT -> AtomicConstraint(Lt, r, l)
            | `GE -> AtomicConstraint(Le, r, l)
        in
            RA_Leaf(c)


    let create_binary_map_term op_token l r =
        match op_token with
            | `Sum -> RSum([l;r])
            | `Product -> RProd([l;r])
            | `Minus -> raise Parse_error
            | `Divide -> raise Parse_error

    let create_map_term_list aggregate_list table join_list predicate_opt =
        let relational =
            let untyped_relalg = RA_MultiNatJoin([table]@join_list) in
                qualify_relational_attributes untyped_relalg untyped_relalg
        in
        let plan = match predicate_opt with
            | None -> relational
            | Some (p) ->
                  let valid_p = qualify_relational_attributes p relational in
                      RA_MultiNatJoin([relational; valid_p])
        in
        (*
        let debug_result r =
            List.iter (fun t -> print_endline ("Created terms: "^
                (term_as_string (make_term t) []))) r;
        in
        *)
        let r =
            List.map
                (fun agg_params ->
                    match agg_params with
                        | `Aggregate(agg_fn, agg_f_l) ->
                              begin match agg_f_l with
                                  | [f] ->
                                        let new_f =
                                            qualify_aggregate_arguments f plan
                                        in
                                            RVal(AggSum(new_f, plan))
                                  | _ -> raise (Failure
                                        ("Found multiple aggregate arguments "^
                                            "for standard aggregates."))
                              end
                        | _ -> raise (Failure
                              ("Found non-aggregate in select list.")))
                aggregate_list
        in
            (*debug_result r;*)
            r

    let rec replace_relations_in_relalg fields r =
        let replace_relalg_lf lf = match lf with
            | Rel(n,f) ->
                  let original_n = get_original n in
                  let new_f =
                      List.map
                          (fun (id, ty) ->
                              if original_n = n then
                                  let rels = get_field_relations fields id in
                                      if List.length rels = 1 then (id, ty)
                                      else (n^"__"^id, ty)
                              else (n^"__"^id, ty))
                          f
                  in
                      RA_Leaf(Rel(original_n, new_f))
            | AtomicConstraint(op, t1, t2) ->
                  RA_Leaf(AtomicConstraint(op,
                      replace_relations_in_term fields t1,
                      replace_relations_in_term fields t2))
            | _ -> RA_Leaf(lf)
        in
            fold_relalg (fun x -> RA_MultiUnion(x))
                (fun x -> RA_MultiNatJoin(x)) replace_relalg_lf r

    and replace_relations_in_term fields t =
        let replace_term_lf lf = match lf with
            | AggSum(f,r) ->
                  RVal(AggSum(replace_relations_in_term fields f,
                      (replace_relations_in_relalg fields r)))
            | _ -> RVal(lf)
        in
            fold_term (fun x -> RSum(x))
                (fun x -> RProd(x)) replace_term_lf t

    let rename_var name fields = 
        let (rel, field_name) = get_field_and_relation name in
        let base_rel_name = get_original rel in
            if base_rel_name = rel then
                let rels = get_field_relations fields field_name in
                    if List.length rels = 1 then field_name
                    else rel^"__"^field_name
            else rel^"__"^field_name

    let rename_map_term t =
        let fields = get_field_relations_map (get_base_relations t) in
        let mappings =
            List.map
                (fun ((n,t) as v) -> (v, (rename_var n fields, t)))
                (term_vars (make_term t))
        in
        let new_t = replace_relations_in_term fields t in
            readable_term
                (apply_variable_substitution_to_term
                    mappings (make_term new_t))

    let rename_group_bys group_bys base_relations =
        let fields = get_field_relations_map base_relations in
            List.map (fun (r,f,t) ->
                (rename_var (r^"."^f) fields, t)) group_bys

    let concat_stmt (t,s,p) n =
        let r = if t = [] then n else [(t,s,p)]@n in
            (r, relation_sources)

    let get_db_schema t_l =
        let br = get_base_relations (List.hd t_l) in
            List.fold_left
                (fun acc r -> match r with
                    | Rel(r, v) ->
                          if List.mem_assoc r acc then acc else acc@[(r,v)]
                    | _ -> raise (Failure
                          "Internal error: expected a relation."))
            [] br

    let get_source_info stream_type source_type
            source_args source_instance tuple_type adaptor_type adaptor_bindings_str
            =
        let adaptor_bindings =
            let bindings_l = Str.split (Str.regexp ",") adaptor_bindings_str in
            let (crf_l, ctf_l) =
                List.partition
                    (fun (c, _) -> (c mod 2) = 0)
                    (snd (List.fold_left
                        (fun (c, acc) id -> (c+1, acc@[(c, id)]))
                        (0, []) bindings_l))
            in
            let (_, rel_fields) = List.split crf_l in
            let (_, tuple_fields) = List.split ctf_l in
                List.combine
                    (List.map String.uppercase rel_fields) tuple_fields
        in
        let thrift_ns = "" in
            (stream_type, source_type, source_args, tuple_type,
            adaptor_type, adaptor_bindings, thrift_ns, source_instance)

%}

%token <string> ID STRING
%token <int> INT   
%token <float> FLOAT
%token SUM MINUS PRODUCT DIVIDE
%token EQ NE LT LE GT GE
%token AND OR NOT BETWEEN
%token COMMA LPAREN RPAREN
%token AS
%token JOIN INNER OUTER LEFT RIGHT ON   
%token CREATE TABLE FROM USING DELIMITER SELECT WHERE GROUP BY HAVING ORDER
%token ASC DESC
%token SOURCE ARGS INSTANCE TUPLE ADAPTOR BINDINGS
%token EOSTMT
%token EOF

// start   
%start dbtoasterSqlList
// (terms, db_schema, params) list, relation_sources
%type <(Algebra.readable_term_t list * (string * Algebra.var_t list) list * Algebra.var_t list) list * (string, Runtime.source_info) Hashtbl.t> dbtoasterSqlList
    
%%

dbtoasterSqlList:
| dbtoasterSqlStmt EOF    { concat_stmt $1 [] }
| dbtoasterSqlStmt EOSTMT EOF    { concat_stmt $1 [] }
| dbtoasterSqlStmt EOSTMT dbtoasterSqlList
          { clear_aliases(); concat_stmt $1 (fst $3) }

dbtoasterSqlStmt:
| createAnonTableStmt    { let _ = $1 in ([],[],[]) }
| createTableStmt        { let _ = $1 in ([],[],[]) }
| selectStmt             { $1 }

//
// Create table statements
createAnonTableStmt:
    CREATE TABLE ID LPAREN fieldList RPAREN
    {
        let name = $3 in
        let fields = $5 in
            add_relation name fields
    }

createTableStmt:
|   CREATE TABLE ID LPAREN fieldList RPAREN
    FROM STRING SOURCE STRING TUPLE STRING
    {
        let name = $3 in
        let fields = $5 in
        let source_info = 
            let tuple_type = $12 in
            let adaptor_type =
                "DBToaster::Adaptors::InsertAdaptor<"^tuple_type^">" in
            let adaptor_bindings_str =
                String.concat "," (List.map
                    (fun (id,_) -> id^","^(String.lowercase id)) fields)
            in
                get_source_info "file" $10 $8 (name^"Source")
                    tuple_type adaptor_type adaptor_bindings_str
        in
            add_relation name fields;
            add_source name source_info
    }

|   CREATE TABLE ID LPAREN fieldList RPAREN
    FROM STRING
    SOURCE STRING ARGS STRING INSTANCE STRING
    TUPLE STRING
    ADAPTOR STRING
    {
        let name = $3 in
        let fields = $5 in
        let source_info =
            let adaptor_bindings_str =
                String.concat "," (List.map
                    (fun (id, ty) -> id^","^(String.lowercase id)) fields)
            in
                get_source_info $8 $10 $12 $14 $16 $18 adaptor_bindings_str
        in
            add_relation name fields;
            add_source name source_info
    }

|   CREATE TABLE ID LPAREN fieldList RPAREN
    FROM STRING
    SOURCE STRING ARGS STRING INSTANCE STRING
    TUPLE STRING
    ADAPTOR STRING BINDINGS STRING
    {
        let name = $3 in
        let fields = $5 in
        let source_info = get_source_info $8 $10 $12 $14 $16 $18 $20
        in
            add_relation name fields;
            add_source name source_info
    }

fieldList:
| ID ID                    { [String.uppercase $1, create_type $2] }
| ID ID COMMA fieldList    { (String.uppercase $1, create_type $2)::$4 }

//
// Select statements

selectStmt: 
    SELECT selectList   
    FROM tableExpression   
    joinList   
    whereClause
    groupByClause
    orderByClause
    {   
        let (aggregate_list, group_by_columns) =
            let (al,cl) =
                List.partition
                    (fun si -> match si with
                        | `Aggregate _ -> true | _ -> false) $2
            in
                (List.rev al, List.rev cl)
        in
        let table = $4 in
        let join_list = $5 in
        let predicate_opt = $6 in
        let group_by_fields = $7 in
        let order_by = $8 in
            match order_by with
                | [] ->
                      let t_l =
                          create_map_term_list
                              aggregate_list table join_list predicate_opt
                      in
                      let base_relations = get_base_relations (List.hd t_l) in
                      let valid_group_bys =
                          qualify_group_by_fields
                              base_relations group_by_fields group_by_columns
                      in

                      pop_relations base_relations;

                      let renamed_t_l = List.map rename_map_term t_l in
                      let db_schema = get_db_schema renamed_t_l in
                      let params =
                          rename_group_bys valid_group_bys base_relations
                      in
                          (renamed_t_l, db_schema, params)

                | _ -> raise Parse_error
    }   

scalarSelectStmt:
    SELECT selectList
    FROM tableExpression
    joinList
    whereClause
    {
        let aggregate_list =
            let (al, cl) =
                List.partition
                    (fun si -> match si with
                        | `Aggregate _ -> true | _ -> false) $2
            in
                match cl with
                    | [] -> List.rev al
                    | _ -> raise Parse_error
        in
        let table = $4 in
        let join_list = $5 in
        let predicate_opt = $6 in
        let t_l = create_map_term_list
            aggregate_list table join_list predicate_opt
        in
            pop_relations (get_base_relations (List.hd t_l));
            (t_l, [], [])
    }

//
// Select list

selectList:
| selectItem                     { [$1] }
| selectItem COMMA selectList    { $1 :: $3 }   

// TODO: how do we allow nested aggregates in the select list?
// TODO: write rule for top level map expression, e.g. sum/product of aggregates
selectItem:
| ID               { `Column (String.uppercase $1) }
| ID LPAREN mapTermList RPAREN
          {
              try `Aggregate(
                  Hashtbl.find aggregate_functions (String.uppercase $1), $3)
              with Not_found -> raise Parse_error
          }

// 
// From clause

tableExpression:
| ID
        {
            let r = String.uppercase $1 in
            let r_fields =
                try Hashtbl.find relations r
                with Not_found ->
                    print_endline ("Could not find relation "^r);
                    raise Parse_error
            in
            let relation = Rel(r, r_fields) in
                push_relation relation;
                RA_Leaf(relation)
        }
| ID ID
        {
            let r = String.uppercase $1 in
            let new_r = String.uppercase $2 in
            let r_fields =
                try Hashtbl.find relations r
                with Not_found ->
                    print_endline ("Could not find relation "^r);
                    raise Parse_error
            in
            let relation = Rel(new_r, r_fields) in
                add_alias r new_r;
                push_relation relation;
                RA_Leaf(relation)

        }
| LPAREN selectStmt RPAREN ID
       {
           (* TODO: nested table expression
           let subplan = $2 in
           let rel_name = $4 in
           *)
           raise Parse_error
       }

//
// Join clause

joinList:   
|                               { [] }   
| joinClause                    { $1 }   
| joinClause joinList           { $1 @ $2 }       
          
joinClause: 
| COMMA tableExpression                      { [$2] }
| JOIN tableExpression joinOnClause          { [$2] @ $3 }
| INNER JOIN tableExpression joinOnClause    { [$3] @ $4 }
| OUTER JOIN tableExpression joinOnClause    { raise Parse_error }
| LEFT  JOIN tableExpression joinOnClause    { raise Parse_error }   
| RIGHT JOIN tableExpression joinOnClause    { raise Parse_error }   

// TODO: where do we check the join predicate to ensure no nested aggregates?
joinOnClause:   
|                      { [] }
| ON constraintList    { [$2] }

//
// Where clause

whereClause:   
|                         { None }   
| WHERE constraintList    { Some($2) }

// Join, where clause constraints
// Note: these are constraint-only relational algebra expressions, thus can
// be safely complemented.
constraintList:
| atomicConstraint                      { $1 }
| NOT atomicConstraint
          { readable_relalg (Algebra.complement(make_relalg $2)) }
| atomicConstraint AND constraintList   { RA_MultiNatJoin([$1; $3]) }   
| atomicConstraint OR  constraintList   { RA_MultiUnion([$1; $3]) }
| LPAREN constraintList RPAREN          { $2 }

atomicConstraint:
| mapTerm cmp_op mapTerm                 { create_constraint $2 $1 $3 }
| mapTerm BETWEEN mapTerm AND mapTerm    { raise Parse_error }
| LPAREN atomicConstraint RPAREN         { $2 }

//
// Expressions

mapTermList:
| mapTerm                      { [$1] }
| mapTerm COMMA mapTermList    { $1 :: $3 }

mapTerm:
| value                       { RVal($1) }
| scalarSelectStmt
          {
              let (t_l,_,_) = $1 in
                  if List.length t_l = 1 then List.hd t_l
                  else raise Parse_error
          }
| LPAREN mapTerm RPAREN       { $2 }
| mapTerm arith_op mapTerm    { create_binary_map_term $2 $1 $3 }


//
// Terminals

// Note we use polymorphic variants for operators to defer 
// construction of actual map algebra expressions
cmp_op:
| EQ { `EQ } | NE { `NE }
| LT { `LT } | LE { `LE }
| GT { `GT } | GE { `GE }   

arith_op:
| SUM        { `Sum }
| MINUS      { `Minus }
| PRODUCT    { `Product }
| DIVIDE     { `Divide }

value:
| INT       { Const(Int($1)) }
| FLOAT     { Const(Double($1)) }
| STRING    { Const(String($1)) }
| ID       
          {
              (* Note: we use any old type here, since types are replaced
               * when qualifying fields *)
              Var(String.uppercase $1, TInt)
          }


//
// Group by clause

groupByClause:
|                         { [] }
| GROUP BY groupByList    { $3 }

groupByList:
| ID                      { [String.uppercase $1] }
| ID COMMA groupByList    { (String.uppercase $1)::$3 }


//
// Order by clause

orderByClause:   
|                         { [] }   
| ORDER BY orderByList    { $3 }   
                                        
orderByList:   
| orderBy                       { [$1] }   
| orderBy COMMA orderByList     { $1 :: $3 }   

// TODO: add ascending, descending to DBToaster AST.
orderBy:   
| ID         { $1 }
| ID ASC     { $1 }
| ID DESC    { $1 }
          
%%
