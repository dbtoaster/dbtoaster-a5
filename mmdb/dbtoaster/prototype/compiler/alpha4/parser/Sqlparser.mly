%{
    open Calculus
    
    exception SQLParseError of string
    exception FeatureUnsupported of string
    
    (* Calculus construction utilitie *)
    let fold_calc (sum_f: 'a list -> 'a) (prod_f: 'a list -> 'a) 
                  (neg_f: 'a -> 'a) (leaf_f: readable_relcalc_lf_t -> 'a) 
                  (calc: readable_relcalc_t): 'a =
      let rec fold_aux rr = 
          match rr with
            | RA_Leaf(x)         -> leaf_f x
            | RA_Neg(x)          -> neg_f (fold_aux x)
            | RA_MultiUnion(l)   -> sum_f (List.map fold_aux l)
            | RA_MultiNatJoin(l) -> prod_f (List.map fold_aux l)
      in fold_aux calc;;
    
    let fold_term (sum_f: 'a list -> 'a) (prod_f: 'a list -> 'a) 
                  (neg_f: 'a -> 'a) (leaf_f: readable_term_lf_t -> 'a) 
                  (term: readable_term_t): 'a =
      let rec fold_aux rr = 
          match rr with
            | RVal(x)  -> leaf_f x
            | RNeg(x)  -> neg_f (fold_aux x)
            | RSum(l)  -> sum_f (List.map fold_aux l)
            | RProd(l) -> prod_f (List.map fold_aux l)
      in fold_aux term;;
    
    let parse_error s =
        let lex_pos = symbol_end_pos () in
        let line_pos_str = string_of_int (lex_pos.Lexing.pos_lnum) in
        let char_pos_str = string_of_int
            (lex_pos.Lexing.pos_cnum - lex_pos.Lexing.pos_bol)
        in
            print_endline
                ("Parsing error '"^s^"' at line: "^line_pos_str^
                    " char: "^(char_pos_str));
            flush stdout

    (* Parser debugging helpers *)
    let string_of_relation_list rl =
        String.concat ","
            (List.map
                (function
                    | Rel(n,_) -> n
                    | _ -> raise (SQLParseError "Invalid relation."))
                rl)

    (* SQL->DBToaster typing *)

    let create_type t =
        match t with
            | "int" | "integer" -> TInt
            | "int8" | "bigint" -> TLong
            | "float" | "double" -> TDouble
            | "text" -> TString
            | _ -> raise (SQLParseError("Unknown Type '"^t^"'"))

    (* Aggregate function definitions *)

    let aggregate_functions = Hashtbl.create 4
    let _ = List.iter
        (fun (agg, ast) -> Hashtbl.add aggregate_functions agg ast)
        [ ("SUM", `Sum); ("AVG", `Avg)
          (*("MIN", `Min); ("MAX", `Max)*) ]


    (* Table definitions *)

    (* table name -> fields *)
    let relations:((string, Calculus.var_t list) Hashtbl.t) = Hashtbl.create 10

    (* table name -> source * framing * adaptor *)
    let relation_sources:((string,M3.relation_input_t) Hashtbl.t)
      = Hashtbl.create 10

    let add_relation name fields =
        Hashtbl.replace relations (String.uppercase name) fields

    let add_source name source_info =
        Hashtbl.replace relation_sources
            (String.uppercase name) (source_info)

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
        in let noop i = i in
          fold_calc Util.ListAsSet.multiunion 
                    Util.ListAsSet.multiunion 
                    noop relalg_lf r

    (* readable_term_t -> readable_relalg_lf_t list *)
    and get_base_relations t =
        let term_lf lf = match lf with
            | AggSum(f,r) ->
                  (get_base_relations f)@(get_base_relations_plan r)
              (* OK: We can skip External here, right? Externals are references
                 to subqueries, and not relations... I think. *)
            | _ -> []
        in let noop i = i in
          fold_term Util.ListAsSet.multiunion
                    Util.ListAsSet.multiunion
                    noop term_lf t

    (* readable_relalg_t -> readable_term_lf_t list *)
    let get_free_variables_from_plan r =
        (*
        let debug_free_vars bound_vars vars =
            print_endline ("RelAlg xx : "^
                (Calculus.relalg_as_string (make_relalg r) []));
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
                          failwith msg)
            (get_base_relations_plan r))
        in
        let vars = relcalc_vars (make_relcalc r) in
            (*debug_free_vars bound_vars vars;*)
            Util.ListAsSet.diff vars bound_vars

    (* readable_term_t -> readable_term_lf_t list *)
    let get_free_variables_from_map_term t =
        (*
        let debug_free_vars bound_vars vars =
            print_endline ("Term : "^
                (Calculus.term_as_string (make_term t) []));
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
                          failwith msg)
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
        else raise (SQLParseError ("No field "^field_name^" found in any table."))

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
                    | _ -> (failwith 
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
                    raise (SQLParseError msg)
            else
                let r =
                  try
                    if relation_name = "" then List.hd field_relations
                    else relation_name
                  with Failure("hd") -> 
                    raise (SQLParseError("Invalid field '"^
                           relation_name^"."^field_name^"'"))
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
            readable_relcalc (apply_variable_substitution_to_relcalc
                free_var_mappings (make_relcalc ra_to_qualify))
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
                    try
                      if rel = "" then List.hd field_relations
                      else 
                          List.find (fun (r,_) -> r = rel) field_relations
                    with Failure("hd") -> 
                      raise (SQLParseError("Invalid field '"^
                             rel^"."^field_name^"'"))

                  in
                      if not(List.mem
                          (relation_name, field_name, field_type) group_bys)
                      then
                          raise (SQLParseError
                              ("Invalid group by field "^
                                  relation_name^"."^field_name))
                      else
                          (relation_name, field_name, field_type)

            | _ -> raise (SQLParseError
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
            | `Minus -> raise (FeatureUnsupported("Subtraction"))
            | `Divide -> raise (FeatureUnsupported("Division"))

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
                                  | _ -> raise (SQLParseError
                                        ("Found multiple aggregate arguments "^
                                            "for standard aggregates."))
                              end
                        | _ -> raise (SQLParseError
                              ("Found non-aggregate in select list.")))
                aggregate_list
        in
            (*debug_result r;*)
            r

    let rec replace_relations_in_relalg fields r =
        let replace_relalg_lf lf = 
          match lf with
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
            fold_calc (fun x -> RA_MultiUnion(x))
                (fun x -> RA_MultiNatJoin(x)) 
                (fun x -> x) replace_relalg_lf r

    and replace_relations_in_term fields t =
        let replace_term_lf lf = match lf with
            | AggSum(f,r) ->
                  RVal(AggSum(replace_relations_in_term fields f,
                      (replace_relations_in_relalg fields r)))
            | _ -> RVal(lf)
        in
            fold_term (fun x -> RSum(x))
                (fun x -> RProd(x)) 
                (fun x -> x) replace_term_lf t

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
    
    let extract_relation_sources 
      (sources:(string,M3.relation_input_t) Hashtbl.t): 
      M3.relation_input_t list =
        (Hashtbl.fold 
          (fun _ (source, framing, relation, adaptor) list -> 
            (source, framing, relation, adaptor)::list) 
          sources []
        )

    let concat_stmt (t,s,p) (n,old_sources) =
        let r = if t = [] then n else [(t,s,p)]@n in
            (r, (extract_relation_sources relation_sources))

    let get_db_schema t_l =
      if t_l == [] then
        raise (SQLParseError("Looking up schema for empty term list: get_db_schema"))
      else
        let br = get_base_relations (List.hd t_l) in
            List.fold_left
                (fun acc r -> match r with
                    | Rel(r, v) ->
                          if List.mem_assoc r acc then acc else acc@[(r,v)]
                    | _ -> raise (Failure
                          "Internal error: expected a relation."))
            [] br

%}

%token <string> ID STRING
%token <int> INT   
%token <float> FLOAT
%token SUM MINUS PRODUCT DIVIDE
%token EQ NE LT LE GT GE
%token AND OR NOT BETWEEN
%token COMMA LPAREN RPAREN PERIOD
%token AS
%token JOIN INNER OUTER LEFT RIGHT ON   
%token CREATE TABLE FROM USING DELIMITER SELECT WHERE GROUP BY HAVING ORDER
%token SOCKET FILE FIXEDWIDTH VARSIZE OFFSET ADJUSTBY SETVALUE LINE DELIMITED
%token POSTGRES RELATION PIPE
%token ASC DESC
%token SOURCE ARGS INSTANCE TUPLE ADAPTOR BINDINGS
%token EOSTMT
%token EOF

// start   
%start dbtoasterSqlList
/* 
  List of
    List of Query Expressions       : target terms
    List of Input Relations         : db schema
    List of Output Vars / Group Bys : query parameters
  List of                           : relation sources
    Relation Name
    Input Source
    Framing Method
    Read Adaptor
*/
%type < ( Calculus.readable_term_t list * (string * Calculus.var_t list) list * Calculus.var_t list) list * M3.relation_input_t list > dbtoasterSqlList
    
%%

dbtoasterSqlList:
| dbtoasterSqlStmt EOF    { concat_stmt $1 ([],[]) }
| dbtoasterSqlStmt EOSTMT EOF    { concat_stmt $1 ([],[]) }
| dbtoasterSqlStmt EOSTMT dbtoasterSqlList
          { clear_aliases(); concat_stmt $1 $3 }

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

pipeArgs:
|   STRING                { [$1] }
|   STRING COMMA pipeArgs { $1::$3 }

sourceStmt:
|   FILE STRING        { M3.FileSource($2) }
|   SOCKET STRING INT  { M3.SocketSource(Unix.inet_addr_of_string $2, $3) }
|   SOCKET INT         { M3.SocketSource(Unix.inet_addr_any, $2) }
|   PIPE STRING        { M3.PipeSource($2) }

framingStmt:
|   FIXEDWIDTH INT                  { M3.FixedSize($2) }
|   LINE DELIMITED                  { M3.Delimited("\n") }
|   STRING DELIMITED                { M3.Delimited($1) }
|   VARSIZE                         { M3.VarSize(0,0) }
|   VARSIZE OFFSET INT              { M3.VarSize($3,0) }
|   VARSIZE OFFSET INT ADJUSTBY INT { M3.VarSize($3,$5) }

adaptorParams:
|   ID SETVALUE STRING                     { [(String.lowercase $1,$3)] }
|   ID SETVALUE STRING COMMA adaptorParams { (String.lowercase $1,$3)::$5 }

adaptorStmt:
|   ID LPAREN RPAREN               { (String.lowercase $1, []) }
|   ID LPAREN adaptorParams RPAREN { (String.lowercase $1, $3) }
|   ID                             { (String.lowercase $1, []) }

postgresFieldList:
|   ID ID                          { ($1, $2) }
|   ID ID COMMA postgresFieldList  { let (n,t) = $4 in ($1^","^n, $2^","^t) }

relationInputStmt:
|   sourceStmt framingStmt adaptorStmt { ($1,$2,$3) }
|   POSTGRES ID LPAREN postgresFieldList RPAREN {
        let (names,types) = $4 in
        let db_rel = Str.split (Str.regexp "\\.") $2 in
        let (db,rel) = 
          if List.length db_rel > 1 
          then (List.nth db_rel 0, List.nth db_rel 1)
          else ("", List.nth db_rel 0)
        in
          (
            M3.PipeSource("psql "^db^" -A -t -c \"SELECT "^names^
                          " FROM "^rel^"\""),
            M3.Delimited("\n"),
            ("csv", [("fields","|");("schema",types)])
            (* ;("skiplines","2");("trimwhitespace","") *)
          )
    }

createTableStmt:
|   CREATE TABLE ID LPAREN fieldList RPAREN FROM relationInputStmt { 
        let name = $3 in
        let fields = $5 in
        let (source,framing,adaptor) = $8 in
        let source_info = (source,framing,name,adaptor) in
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
                      let base_relations = 
                        try
                          get_base_relations (List.hd t_l)
                        with Failure("hd") -> 
                          raise (SQLParseError("Looking up relations for empty term list: selectStmt"))
                      in
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

                | _ -> raise (FeatureUnsupported("ORDER BY"))
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
                    | `Column(a)::l -> 
                        raise (SQLParseError(
                          "Found non-aggregate "^a^" in select list."
                        ))
                    | _ -> failwith "Non-aggregate, non-column type in select list (Bug in Sqlparser.mly)"
        in
        let table = $4 in
        let join_list = $5 in
        let predicate_opt = $6 in
        let t_l = create_map_term_list
            aggregate_list table join_list predicate_opt
        in
          try
            pop_relations (get_base_relations (List.hd t_l));
            (t_l, [], [])
          with Failure("hd") ->
            raise (SQLParseError("Looking up schema for empty term list"))

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
              with Not_found -> 
                  raise (SQLParseError("Unknown aggregate '"^$1^"'"))
          }

// 
// From clause

tableExpression:
| ID
        {
            let r = String.uppercase $1 in
            let r_fields : Calculus.var_t list =
                try (Hashtbl.find relations r)
                with Not_found ->
                    raise (SQLParseError("Undefined Relation '"^r^"'"))
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
                    raise (SQLParseError("Undefined Relation '"^r^"'"))
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
           raise (FeatureUnsupported ("Nested SELECT Statements in Target"))
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
| OUTER JOIN tableExpression joinOnClause
                              { raise (FeatureUnsupported("OUTER JOIN")) }
| LEFT  JOIN tableExpression joinOnClause     
                              { raise (FeatureUnsupported("LEFT JOIN")) } 
| RIGHT JOIN tableExpression joinOnClause
                              { raise (FeatureUnsupported("RIGHT JOIN")) }  

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
| NOT atomicConstraint                  { RA_Neg($2) }
| atomicConstraint AND constraintList   { RA_MultiNatJoin([$1; $3]) }   
| atomicConstraint OR  constraintList   { RA_MultiUnion([$1; $3]) }
| LPAREN constraintList RPAREN          { $2 }

atomicConstraint:
| mapTerm cmp_op mapTerm               { create_constraint $2 $1 $3 }
| mapTerm BETWEEN mapTerm AND mapTerm  { raise (FeatureUnsupported("BETWEEN")) }
| LPAREN atomicConstraint RPAREN       { $2 }

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
        try
          if List.length t_l = 1 then List.hd t_l
          else raise (FeatureUnsupported ("Nested SELECT Statements in Target"))
        with Failure("hd") -> 
          raise (SQLParseError("Looking up schema for empty term list"))
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
