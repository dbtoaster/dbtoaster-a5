%{   
    (*
     * TODO: resolve inconsistency between arithmetic operators for
     * map expressions and non-nested expressions
     *)
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


    let string_of_relation_list rl =
        String.concat ","
            (List.map
                (function
                    | `Relation (n,_) -> n
                    | _ -> raise InvalidExpression)
                rl)

    (* Aggregate function definitions *)

    let aggregate_functions = Hashtbl.create 4
    let _ = List.iter
        (fun (agg, ast) -> Hashtbl.add aggregate_functions agg ast)
        [ ("SUM", `Sum); ("MIN", `Min); ("MAX", `Max)]


    (* Table definitions *)

    (* table name -> fields *)
    let relations = Hashtbl.create 10

    (* table name -> (file name, delimiter) *)
    let relation_sources = Hashtbl.create 10

    let add_relation name fields =
        Hashtbl.replace relations (String.uppercase name) fields

    (* See dbtoaster.ml for contents of source info. *)
    let add_source name source_info =
        Hashtbl.replace relation_sources (String.uppercase name) (name, source_info)


    (* Table aliasing in queries *)

    (* table alias name -> table name *)
    let alias_relations = Hashtbl.create 10

    let add_alias name alias = Hashtbl.replace alias_relations alias name

    let get_original alias = try Hashtbl.find alias_relations alias with Not_found -> alias

    let clear_aliases () = Hashtbl.clear alias_relations


    (* Nested query scoping *)

    (* Stack of relations bound above subqueries *)
    let outer_relations = ref []
    let current_relations = ref []

    let push_relation r =
        current_relations := (!current_relations)@[r]

    let pop_relations rl =
        print_endline ("Popping: "^(string_of_relation_list rl));
        current_relations :=
            List.filter (fun r -> not(List.mem r rl)) (!current_relations)

    let clear_relations () = current_relations := []

    let get_outer_relations () = !current_relations


    (* SQL->DBToaster typing *)

    let check_type t =
        match t with
            | "int" | "integer" -> "int"
            | "int8" | "bigint" -> "long"
            | "real" -> "float"
            | "float" -> "double"
            | "text" -> "string"
            | _ -> raise Parse_error


    (* Attribute name qualification *)

    (* TODO: generalize for arbitrary group-by expressions, not just for columns *)
    (* TODO: handle schema names *)
    let get_field_and_relation name =
        let field_name_pos = try (String.rindex name '.')+1 with Not_found -> 0 in
        let field_name = String.sub name field_name_pos ((String.length name) - field_name_pos) in
        let relation_name = if field_name_pos = 0 then "" else String.sub name 0 (field_name_pos-1) in
            print_endline ("Found field "^relation_name^"."^field_name);
            (relation_name, field_name)

    let get_field_relations fields field_name =
        if Hashtbl.mem fields field_name then Hashtbl.find fields field_name
        else raise (Failure ("No field "^field_name^" found in any table."))

    let get_field_relations_map base_relations =
        let m = Hashtbl.create 10 in
        let add_fields h n f =
            List.iter
                (fun (id, ty) ->
                    let rels = if Hashtbl.mem h id then Hashtbl.find h id else [] in
                        Hashtbl.replace h id (rels@[n]))
                f
        in
        let global_relations =
            List.fold_left
                (fun acc r -> if List.mem r acc then acc else (acc@[r]))
                (get_outer_relations()) base_relations
        in
            print_endline ("Base relations: "^(string_of_relation_list base_relations));
            print_endline ("Resolving with global relations: "^
                (string_of_relation_list global_relations));

            (* Add all base relation fields for validation *)
            List.iter
                (function
                    | `Relation(n,f) -> add_fields m n f
                    | _ -> raise InvalidExpression)
                global_relations;
            m

    let qualify_field name fields =
        let (relation_name, field_name) = get_field_and_relation name in
        let field_relations = get_field_relations fields field_name in
        let single_relation =
            (List.length field_relations = 1) &&
                (relation_name = "" || relation_name = (List.hd field_relations))
        in
        let valid_relation = List.mem relation_name field_relations in
        let valid_field = single_relation || valid_relation in
            if not valid_field then
                let rel_names = String.concat "," field_relations in
                let msg =
                    if List.length field_relations > 1 then
                        ("Ambiguous field "^
                            (if relation_name = "" then "" else relation_name^".")^
                            field_name^" in relations "^rel_names)
                    else
                        ("Invalid field '"^relation_name^"."^field_name^"'")
                in
                    raise (Failure msg)
            else
                let r = if relation_name = "" then List.hd field_relations else relation_name in
                    (r, field_name)
            
    let qualify_fields base_relations fields =
        let br_fields = get_field_relations_map base_relations in
            List.map (fun name -> qualify_field name br_fields) fields

    let qualify_predicate_attributes predicate plan =
        let relations = get_base_relations_plan plan in
        let pred_attrs = get_unbound_attributes_from_predicate predicate false in
        let pred_attr_names =
            List.map
                (fun attr -> match attr with
                    | `Unqualified aid -> aid
                    | `Qualified(n,id) -> n^"."^id)
                pred_attrs
        in
        print_endline ("Pred: "^(indented_string_of_bool_expression predicate 0));
        print_endline ("Unbound pred attrs: "^(String.concat "," pred_attr_names));
        let pred_attr_fields = qualify_fields relations pred_attr_names in
        let pred_attr_mappings = List.combine pred_attr_names pred_attr_fields in
        let replace_eterm_attr = function
            | (`ETerm(`Attribute(`Unqualified(x)))) as y ->
                  if List.mem_assoc x pred_attr_mappings then
                      `ETerm(`Attribute(`Qualified(List.assoc x pred_attr_mappings)))
                  else y
                      
            | x -> x
        in
        let replace_meterm_attr = function
            | (`METerm(`Attribute(`Unqualified(x)))) as y ->
                  if List.mem_assoc x pred_attr_mappings then
                      `METerm(`Attribute(`Qualified(List.assoc x pred_attr_mappings)))
                  else y

            | x -> x
        in
            map_bool_expr
                replace_eterm_attr (fun x -> x)
                replace_meterm_attr (fun x -> x)
                predicate

    let qualify_aggregate_arguments m_expr plan =
        let relations = get_base_relations_plan plan in
        let uba = get_unbound_attributes_from_map_expression m_expr false in
        let ub_attr_names =
            List.map
                (fun attr -> match attr with
                    | `Unqualified aid -> aid
                    | `Qualified(n,id) -> n^"."^id)
                uba
        in
        print_endline ("Unbound agg args: "^(String.concat "," ub_attr_names));
        let ub_attr_fields = qualify_fields relations ub_attr_names in
        let ub_attr_mappings = List.combine ub_attr_names ub_attr_fields in
        let replace_eterm_attr = function
            | (`ETerm(`Attribute(`Unqualified(x)))) as y ->
                  if List.mem_assoc x ub_attr_mappings then
                      `ETerm(`Attribute(`Qualified(List.assoc x ub_attr_mappings)))
                  else y
                      
            | x -> x
        in
        let replace_meterm_attr = function
            | (`METerm(`Attribute(`Unqualified(x)))) as y ->
                  if List.mem_assoc x ub_attr_mappings then
                      `METerm(`Attribute(`Qualified(List.assoc x ub_attr_mappings)))
                  else y

            | x -> x
        in
            map_map_expr
                replace_eterm_attr (fun x -> x)
                replace_meterm_attr (fun x -> x)
                m_expr

    let qualify_group_by_column c fields group_bys =
        match c with
            | `Column (e, id) ->
                  let (rel, field_name) = get_field_and_relation id in
                  let relation_name =
                      if rel = "" then List.hd (get_field_relations fields field_name)
                      else rel
                  in
                      if not(List.mem (relation_name, field_name) group_bys) then
                          raise (Failure ("Invalid group by field "^relation_name^"."^field_name))
                      else
                          (relation_name, field_name)

            | _ -> raise (Failure ("Internal error: expected a `Column for a group-by column"))

    let qualify_group_by_fields base_relations group_bys columns =
        let br_fields = get_field_relations_map base_relations in 
        let valid_group_by_fields =
            List.map (fun name -> qualify_field name br_fields) group_bys
        in
        let valid_group_by_columns =
            List.map (fun c -> qualify_group_by_column c br_fields valid_group_by_fields) columns
        in
            valid_group_by_columns


    (* Map expression construction *)

    let rec push_cross subplan relplan =
        match subplan with
            | `Select (p, sp) -> `Select(p, push_cross sp relplan)
            | `Cross(l,r) -> `Cross(subplan, relplan)
            | `Relation r -> `Cross(subplan, relplan)
            | _ -> raise Parse_error

    let rec create_join_tree tbl jl =
        let create_join_plan pred subplan =
            match pred with
                | None -> subplan
                | Some(p) -> `Select(p, subplan)
        in
        match (tbl,jl) with
            | (_, []) -> tbl

            | (`Relation rf, (jc, jtbl)::t)
                -> create_join_tree (create_join_plan jc (`Cross(tbl, jtbl))) t

            | (`Select _, (jc, jtbl)::t)
            | (`Cross _, (jc, jtbl)::t)
                -> create_join_tree (create_join_plan jc (push_cross tbl jtbl)) t

            | (_, _) ->
                  print_endline ("tbl: "^(string_of_plan tbl));
                  print_endline ("jl: "^(string_of_int (List.length jl)));
                  raise Parse_error

    (* Op expression helpers *)
    let create_binary_expression op_token l r =
        match op_token with
            | `Sum -> `Sum(l,r)
            | `Product -> `Product(l,r)
            | `Minus -> `Minus(l,r)
            | `Divide -> `Divide(l,r)

    let create_binary_map_expression op_token l r =
        match op_token with
            | `Sum -> `Sum(l,r)
            | `Product -> `Product(l,r)
            | `Minus -> `Minus(l,r)

    (* Non-nested map expression -> expression construction *)
    let rec create_non_nested_expression m_expr =
        let recur = create_non_nested_expression in
            match m_expr with
                | `METerm(x) -> `ETerm(x)
                | `Sum(l,r) -> `Sum(recur l, recur r)
                | `Product(l,r) -> `Product(recur l, recur r)
                | `Minus(l,r) -> `Minus(recur l, recur r)
                | _ -> raise InvalidExpression

    (* Expression -> map expression construction *)
    let rec create_map_expression expr =
        match expr with
            | `ETerm(x) -> `METerm(x)
            | `Sum (l,r) -> `Sum (create_map_expression l, create_map_expression r)
            | `Minus (l,r) -> `Minus (create_map_expression l, create_map_expression r)
            | `Product (l,r) -> `Product (create_map_expression l, create_map_expression r)
            | _ -> raise Parse_error

    let create_bterm op_token l r =
        let bt = 
            match op_token with
                | `EQ -> `EQ(l,r) | `NE -> `NE(l,r)
                | `LT -> `LT(l,r) | `LE -> `LE(l,r)
                | `GT -> `GT(l,r) | `GE -> `GE(l,r)
        in
            `BTerm(bt)

    let create_nested_bterm op_token l r =
        let create_homogenous nested value =
            match value with
                | `ETerm(`Int 0) | `ETerm(`Float 0.0) -> nested
                | `ETerm(`String _) -> raise Parse_error
                | _ -> `Minus(nested, create_map_expression value)
        in
        let create_op e =
            match op_token with
            | `EQ -> `MEQ(e) | `NE -> `MNEQ(e)
            | `LT -> `MLT(e) | `LE -> `MLE(e)
            | `GT -> `MGT(e) | `GE -> `MGE(e)
        in
            match (l,r) with
                | (`Value x, `Value y) ->

                      print_endline ("Creating value bterm:\nl: "^
                          (string_of_expression x)^
                          "\nr: "^(string_of_expression y));

                      create_bterm op_token x y

                | (`Nested x, `Nested y) ->

                      print_endline ("Creating nested bterm:\nl: "^
                          (indented_string_of_map_expression x 0)^
                          "\nr: "^(indented_string_of_map_expression y 0));

                      `BTerm(create_op (`Minus(x,y)))
                | (`Nested x, `Value y) | (`Value y, `Nested x) ->
                      `BTerm(create_op (create_homogenous x y))
                | _ -> raise Parse_error

    let create_map_expression_list aggregate_list table join_list predicate_opt =
        let relational = create_join_tree table join_list in
        let plan =
            match predicate_opt with
                | None -> relational
                | Some p ->
                      `Select(qualify_predicate_attributes p relational, relational)
        in
            List.map
                (fun agg_params ->
                    match agg_params with
                        | `Aggregate(agg_fn, agg_f_l) ->
                              begin match agg_f_l with
                                  | [f] ->
                                        let new_f = qualify_aggregate_arguments f plan in
                                            `MapAggregate(agg_fn, new_f, plan)
                                  | _ -> raise (Failure
                                        ("Found multiple aggregate arguments for standard aggregates."))
                              end
                        | _ -> raise (Failure ("Found non-aggregate in select list.")))
                aggregate_list


    let rename_map_expression m_expr =
        let get_new_unqualified_name name =
            let (rel, field_name) = get_field_and_relation name in
            let base_rel_name = get_original rel in
                if base_rel_name = rel then base_rel_name^"."^field_name
                else base_rel_name^"."^rel^"_"^field_name
        in
        let get_new_qualified_name rel name =
            let base_rel_name = get_original rel in
                if base_rel_name = rel then (base_rel_name, name)
                else (base_rel_name, rel^"_"^name)
        in
        let replace_eterm_attr = function
            | `ETerm(`Attribute(`Unqualified(x))) ->
                  let new_name = get_new_unqualified_name x in
                      `ETerm(`Attribute(`Unqualified(new_name)))

            | `ETerm(`Attribute(`Qualified(x,y))) ->
                  let new_name = get_new_qualified_name x y in
                      `ETerm(`Attribute(`Qualified(new_name)))

            | x -> x
        in
        let replace_meterm_attr = function
            | `METerm(`Attribute(`Unqualified(x))) ->
                  let new_name = get_new_unqualified_name x in
                      `METerm(`Attribute(`Unqualified(new_name)))

            | `METerm(`Attribute(`Qualified(x,y))) ->
                  let new_name = get_new_qualified_name x y in
                      `METerm(`Attribute(`Qualified(new_name)))

            | x -> x
        in
        let replace_plan_relation = function
            | `Relation(n,f) ->
                  let base_rel_name = get_original n in
                  let new_fields =
                      if base_rel_name = n then f
                      else
                          List.map
                              (fun (id, ty) -> let new_id = n^"_"^id in (new_id, ty))
                              f
                  in
                      `Relation(base_rel_name, new_fields)
            | x -> x
        in
            map_map_expr
                replace_eterm_attr (fun x -> x)
                replace_meterm_attr replace_plan_relation
                m_expr
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
%type <Algebra.map_expression list * (string, Algebra.source_info) Hashtbl.t> dbtoasterSqlList
    
%%

dbtoasterSqlList:
| dbtoasterSqlStmt EOF                        { $1, relation_sources }
| dbtoasterSqlStmt EOSTMT EOF                 { $1, relation_sources }
| dbtoasterSqlStmt EOSTMT dbtoasterSqlList    { clear_aliases(); ($1@(fst $3)), relation_sources }

dbtoasterSqlStmt:
| createAnonTableStmt    { let _ = $1 in [] }
| createTableStmt        { let _ = $1 in [] }
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
            let stream_type = "file" in
            let source_type = $10 in
            let source_args = $8 in
            let source_instance = name^"Source" in
            let tuple_type = $12 in
            let adaptor_type = "DBToaster::Adaptors::InsertAdaptor<"^tuple_type^">" in
            let adaptor_bindings = List.map (fun (id, ty) -> (id, id)) fields in
            let thrift_ns = "" in
                (stream_type, source_type, source_args, tuple_type,
                adaptor_type, adaptor_bindings, thrift_ns, source_instance)
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
        let source_info =
            let stream_type = "file" in
            let source_type = $10 in
            let source_args = $12 in
            let source_instance = $14 in
            let tuple_type = $16 in
            let adaptor_type = $18 in
            let adaptor_bindings =
                let bindings_l = Str.split (Str.regexp ",") $20 in
                let (crf_l, ctf_l) =
                    List.partition
                        (fun (c, _) -> (c mod 2) = 0)
                        (snd (List.fold_left
                            (fun (c, acc) id -> (c+1, acc@[(c, id)]))
                            (0, []) bindings_l))
                in
                let (_, rel_fields) = List.split crf_l in
                let (_, tuple_fields) = List.split ctf_l in
                    List.combine rel_fields tuple_fields
            in
            let thrift_ns = "" in
                (stream_type, source_type, source_args, tuple_type,
                adaptor_type, adaptor_bindings, thrift_ns, source_instance)
        in
            add_relation name fields;
            add_source name source_info
    }

fieldList:
| ID ID                    { [String.uppercase $1, check_type $2] }
| ID ID COMMA fieldList    { (String.uppercase $1, check_type $2)::$4 }


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
                    (fun si -> match si with | `Aggregate _ -> true | _ -> false) $2
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
                      let r =
                          create_map_expression_list
                              aggregate_list table join_list predicate_opt
                      in
                      let base_relations = get_base_relations (List.hd r) in
                          ignore(qualify_group_by_fields base_relations group_by_fields group_by_columns);
                          pop_relations base_relations;
                          List.map rename_map_expression r

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
                    (fun si -> match si with | `Aggregate _ -> true | _ -> false) $2
            in
                match cl with
                    | [] -> List.rev al
                    | _ -> raise Parse_error
        in
        let table = $4 in
        let join_list = $5 in
        let predicate_opt = $6 in
        let r =
            create_map_expression_list
                aggregate_list table join_list predicate_opt
        in
            pop_relations (get_base_relations (List.hd r));
            r
    }


//
// Select list

selectList:
| selectItem                     { [$1] }
| selectList COMMA selectItem    { $3 :: $1 }   

selectItem:
| ID      {
              `Column (
                  `METerm(`Attribute(`Unqualified(String.uppercase $1))),
                  String.uppercase $1)
          }
| nonNestedMapExpression                     { `Column ($1, "") }
| nonNestedMapExpression AS ID               { `Column ($1, String.uppercase $3) }
| ID LPAREN nonNestedMapExpressionList RPAREN
          {
              try `Aggregate(Hashtbl.find aggregate_functions (String.uppercase $1), $3)
              with Not_found -> raise Parse_error
          }


// 
// From clause

tableExpression:
| ID
        {
            let r = String.uppercase $1 in
            let r_fields =
                print_endline ("table expr: "^r);
                try Hashtbl.find relations r
                with Not_found ->
                    print_endline ("Could not find relation "^r);
                    raise Parse_error
            in
            let relation = `Relation(r, r_fields) in
                push_relation relation;
                relation
        }
| ID ID
        {
            let r = String.uppercase $1 in
            let new_r = String.uppercase $2 in
            let r_fields =
                print_endline ("table expr: "^r);
                try Hashtbl.find relations r
                with Not_found ->
                    print_endline ("Could not find relation "^r);
                    raise Parse_error
            in
            let relation = `Relation(new_r, r_fields) in
                add_alias r new_r;
                push_relation relation;
                relation

        }
| LPAREN selectStmt RPAREN ID
       {
           let subplan = $2 in
           let rel_name = $4 in
               raise Parse_error
       }


//
// Join clause

joinList:   
|                               { [] }   
| joinClause                    { [$1] }   
| joinClause joinList           { $1 :: $2 }       
          
joinClause: 
| COMMA tableExpression                      { None, $2 }
| JOIN tableExpression joinOnClause          { $3, $2 }
| INNER JOIN tableExpression joinOnClause    { $4, $3 }
| OUTER JOIN tableExpression joinOnClause    { raise Parse_error }
| LEFT  JOIN tableExpression joinOnClause    { raise Parse_error }   
| RIGHT JOIN tableExpression joinOnClause    { raise Parse_error }   
          
joinOnClause:   
|                            { None }   
| ON simpleConstraintList    { Some($2) }
                                        

//
// Where clause

whereClause:   
|                               { None }   
| WHERE nestedConstraintList    { Some($2) }

// Join clause constraints
simpleConstraintList:
| simpleConstraintExpression                            { $1 }
| NOT simpleConstraintExpression                        { `Not($2) }
| simpleConstraintExpression AND simpleConstraintList   { `And($1, $3) }   
| simpleConstraintExpression OR  simpleConstraintList   { `Or($1, $3) }

simpleConstraintExpression:
| expression cmp_op expression                    { create_bterm $2 $1 $3 }
| expression BETWEEN expression AND expression    { raise Parse_error }
| LPAREN simpleConstraintExpression RPAREN        { $2 }

// Where clause constraints
nestedConstraintList:
| nestedConstraintExpression                            { $1 }
| NOT nestedConstraintExpression                        { `Not($2) }
| nestedConstraintExpression AND nestedConstraintList   { `And($1, $3) }   
| nestedConstraintExpression OR  nestedConstraintList   { `Or($1, $3) }

nestedConstraintExpression:
| nestedValue cmp_op nestedValue              { create_nested_bterm $2 $1 $3 }
| value BETWEEN value AND value               { raise Parse_error }
| LPAREN nestedConstraintExpression RPAREN    { $2 }

nestedValue:
| mapExpression
        {
            let (is_map_expr, expr) = (find_aggregate $1, $1) in
                if is_map_expr then `Nested(expr)
                else `Value(create_non_nested_expression expr)
        }

//
// Expressions

// Note: define expressions first to ensure matching prior to map expressions
expressionList:
| expression                         { [$1] }
| expression COMMA expressionList    { $1 :: $3 }

// TODO: function id lookup in registered functions.
expression:
| value                              { $1 }
| ID LPAREN expressionList RPAREN    { `Function($1, $3) }
| LPAREN expression RPAREN           { $2 }
| expression arith_op expression     { create_binary_expression $2 $1 $3 }

nonNestedMapExpressionList:
| nonNestedMapExpression                                     { [$1] }
| nonNestedMapExpression COMMA nonNestedMapExpressionList    { $1 :: $3 }

nonNestedMapExpression:
| value                                                         { create_map_expression $1 }
| LPAREN nonNestedMapExpression RPAREN                          { $2 }
| nonNestedMapExpression map_arith_op nonNestedMapExpression    { create_binary_map_expression $2 $1 $3 }

mapExpression:
| value                                       { create_map_expression $1 }
| scalarSelectStmt
          {
              let m_expr_l = $1 in
                  if List.length m_expr_l = 1 then
                      List.hd m_expr_l
                  else raise Parse_error
          }
| LPAREN mapExpression RPAREN                 { $2 }
| mapExpression map_arith_op mapExpression    { create_binary_map_expression $2 $1 $3 }

//
// Terminals
cmp_op:
| EQ { `EQ } | NE { `NE }
| LT { `LT } | LE { `LE }
| GT { `GT } | GE { `GE }   

// Note inconsistency between map arith ops and expr arith ops
// where map exprs do not have a division operator
map_arith_op:
| SUM        { `Sum }
| MINUS      { `Minus }
| PRODUCT    { `Product }

arith_op:
| SUM        { `Sum }
| MINUS      { `Minus }
| PRODUCT    { `Product }
| DIVIDE     { `Divide }

value:
| INT       { `ETerm(`Int($1)) }
| FLOAT     { `ETerm(`Float($1)) }
| STRING    { `ETerm(`String($1)) }
| ID        { `ETerm(`Attribute(`Unqualified(String.uppercase $1))) }


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
