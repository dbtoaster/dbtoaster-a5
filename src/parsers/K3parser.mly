%{    
   open Calculus
   open Types
   open K3
   exception K3ParseError of string
   exception K3TypeError of string
   exception K3FeatureUnsupported of string
    
   type k3_statement_t = K3.statement_t
   type k3_trigger_t = K3.trigger_t
   type k3_program_t = K3.prog_t
   type m3_map_t = string * (Types.type_t list) * (Types.type_t list) * Types.type_t
   type map_schema_t = ((string * Types.type_t) list * (string * Types.type_t) list)

    
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

   let empty_output_k3 = ([], [], [])

   let maps:((string, m3_map_t) Hashtbl.t) = Hashtbl.create 10
   let maps_schema:((string, map_schema_t) Hashtbl.t) = Hashtbl.create 10

   let add_map name (new_map: m3_map_t) =
      Hashtbl.replace maps (String.uppercase name) new_map

   let add_map_schema name (new_schema: map_schema_t) =
      Hashtbl.replace maps_schema (String.uppercase name) new_schema

   let get_map name : m3_map_t= 
      try Hashtbl.find maps (String.uppercase name)
         with _ -> raise (K3TypeError("Map '"^name^"' is not defined!"))

   let get_map_schema name : map_schema_t= 
      try Hashtbl.find maps_schema (String.uppercase name)
         with _ -> raise (K3TypeError("Map '"^name^"' is not defined!"))


(*   let patterns:((string, Patterns.pattern list) Hashtbl.t) = Hashtbl.create 10

   let get_patterns name : Patterns.pattern list= 
      let map_name = (String.uppercase name) in
         try 
            Hashtbl.find patterns map_name 
         with 
            _ -> raise (K3TypeError("Map '"^name^"' is not defined!"))

   let add_pattern name (new_pattern: Patterns.pattern) =
      let pattern_list =
         try 
            get_patterns name
         with 
            _ -> []
      in
         let new_pattern_list = 
            if List.exists (fun x -> x == new_pattern) pattern_list then 
               pattern_list
            else
               new_pattern::pattern_list
         in
            Hashtbl.replace patterns (String.uppercase name) new_pattern_list
*)
   let patterns:(Patterns.pattern_map ref) = ref []

   let add_pattern name (new_pattern: Patterns.pattern) = 
      patterns := Patterns.add_pattern !patterns (name, new_pattern)



   let create_map name input_var_types output_var_types map_type =
        let f = fun l -> List.map snd l      (* was (fun x -> ("", x)) *)
         in
            let input_types = f input_var_types in
            let output_types = f output_var_types
            in
                let new_map = (name, input_types, output_types, map_type)
                    in
                        add_map name new_map; 
                        add_map_schema name (input_var_types, output_var_types);
                        (name, input_var_types, input_var_types, map_type) 
   
   let concat_stmt (m,t) (map_list,pat_list,trig_list) =
				let new_map = if m = [] then map_list else m @ map_list
					in
						let new_trig = if t = [] then trig_list else t @ trig_list
							in
								(Schema.empty_db (), (new_map, !patterns), new_trig, []) 

   let slice_infering statement var_bind_list = 
      let map_name = match statement with 
         | PC(n, _, _, _) -> n
         | OutPC(n, _, _) -> n
         | InPC(n, _, _)  -> n
         | SingletonPC(n, _) -> n
         | _ -> raise (K3TypeError("First argument of Slice should be a Collection!"))
      in
         let (in_var_types, out_var_types) = get_map_schema map_name in
         let get_vars l = List.map fst l in
         let in_vars = get_vars in_var_types in
         let out_vars = get_vars out_var_types 
         in
            let var_list = get_vars var_bind_list
            in
(*
               let infer_pattern desired_var_list = 
                  let result_pattern: ((string list * int list) ref) = ref ([],[]) in
                  let counter = ref 0 in
                  List.iter 
                  (fun d_var ->
                     (
                     if (List.exists (fun x -> x == d_var) var_list) then
                        let ids = fst !result_pattern in
                        let nums = snd !result_pattern in
                           result_pattern := (ids@[d_var], nums@[!counter])
                     ); counter := !counter + 1
                  ) desired_var_list; !result_pattern;
               in
                  let infer_in_pattern = infer_pattern in_vars in
                  let infer_out_pattern = infer_pattern out_vars 
                  in
                     (
                        if (infer_in_pattern <> ([],[])) then 
                           let result = Patterns.In(infer_in_pattern) in
                              print_endline (Patterns.string_of_pattern result);
                              add_pattern map_name result
                     );
                     (
                        if (infer_out_pattern <> ([],[])) then 
                           let result = Patterns.Out(infer_out_pattern) in
                              print_endline (Patterns.string_of_pattern result);
                              add_pattern map_name result
                     );
*)
               let in_filt = ListAsSet.inter in_vars var_list in
               let out_filt = ListAsSet.inter out_vars var_list in
               let in_pattern = Patterns.make_in_pattern in_vars in_filt in
               let out_pattern = Patterns.make_out_pattern out_vars out_filt in
(*
                  print_endline (Patterns.string_of_pattern in_pattern);
                  print_endline (Patterns.string_of_pattern out_pattern);
*)
                  add_pattern map_name in_pattern;
                  add_pattern map_name out_pattern;
                     in_var_types@out_var_types


   let collections:((string, expr_t) Hashtbl.t) = Hashtbl.create 10

   let add_collection name expr =
      Hashtbl.replace collections name expr

   let get_collection c_id = 
      Hashtbl.find collections c_id

	let types_to_vartypes types = 
      List.map 
         (
            fun (n, t) -> let nt = TBase(t)
            in 
               (n, nt)
         )
      types
%}

%token <int> INTEGER
%token <string> ID CONST_STRING 
%token <float> CONST_FLOAT
%token EQ NE LT LE GT
%token SUM MINUS PRODUCT
%token COMMA LPAREN RPAREN LBRACK RBRACK PERIOD COLON DOLLAR
%token ON
%token IF IF0 ELSE ITERATE LAMBDA APPLY MAP FLATTEN AGGREGATE GROUPBYAGGREGATE MEMBER LOOKUP SLICE
%token CREATE TABLE FROM SOCKET FILE PIPE FIXEDWIDTH DELIMITED LINE VARSIZE OFFSET ADJUSTBY SETVALUE
%token PCUPDATE PCVALUEUPDATE PCELEMENTREMOVE
%token INT UNIT FLOAT COLLECTION
%token EOSTMT
%token EOF
%token LBRACE RBRACE
%token DOLLAR BOUND ARROW

%left EQ NE LT LE GT GE
%left SUM
%left PRODUCT

// start   
%start dbtoasterK3Program statement
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

%type < K3.prog_t > dbtoasterK3Program
%type < K3.expr_t > statement
    
%%

dbtoasterK3Program:
| mapDeclarationList triggerList         
	{ let _ = $1 in
     concat_stmt ($1, $2) empty_output_k3 }
mapDeclarationList:
| mapDeclaration mapDeclarationList                         { $1::$2 }
| mapDeclaration                                            { [$1] }

mapDeclaration:
| ID LPAREN typeItem RPAREN LBRACK mapVarList RBRACK LBRACK mapVarList RBRACK EOSTMT   
	{ let col = PC($1, types_to_vartypes $6, types_to_vartypes $9, TBase($3))
	   in
	      add_collection $1 col;
	         create_map $1 $6 $9 $3}
| ID LPAREN typeItem RPAREN LBRACK RBRACK LBRACK mapVarList RBRACK EOSTMT            
	{ let col = OutPC($1, types_to_vartypes $8, TBase($3))
	   in
	      add_collection $1 col;
	         create_map $1 [] $8 $3}
| ID LPAREN typeItem RPAREN LBRACK mapVarList RBRACK LBRACK RBRACK EOSTMT            
	{ let col = InPC($1, types_to_vartypes $6, TBase($3))
	   in
	      add_collection $1 col;
	         create_map $1 $6 [] $3}
| ID LPAREN typeItem RPAREN LBRACK RBRACK LBRACK RBRACK EOSTMT                     
	{ let col = SingletonPC($1,TBase($3))
	   in
	      add_collection $1 col;
	         create_map $1 [] [] $3}

mapVarList:
| mapVarItem COMMA mapVarList                               { $1::$3 }
| mapVarItem                                                { [$1] }

mapVarItem:
| ID COLON typeItem                                        { ($1, $3) }

typeItem:
| INT                                                       { Types.TInt }
| FLOAT                                                     { Types.TFloat }

triggerList:
| trigger triggerList                                       { $1::$2 }
| trigger                                                   { [$1] }

trigger:
| ON triggerType ID LPAREN argumentList RPAREN COLON LBRACE statementList RBRACE 
                                                            {
                                                            let args = List.map (fun x -> (x, Types.TFloat)) $5
                                                            in
                                                               let r = ($3, args,Schema.StreamRel)
                                                               in
                                                                  let ev = 
                                                                        if $2 then 
                                                                           Schema.InsertEvent(r)
                                                                        else
                                                                           Schema.DeleteEvent(r)
                                                                  in
                                                                     (ev, $9) }
| ON triggerType ID LBRACK argumentList RBRACK COLON LBRACE statementList RBRACE 
                                                            {
                                                            let args = List.map (fun x -> (x, Types.TFloat)) $5
                                                            in
                                                               let r = ($3, args,Schema.StreamRel)
                                                               in
                                                                  let ev = 
                                                                        if $2 then 
                                                                           Schema.InsertEvent(r)
                                                                        else
                                                                           Schema.DeleteEvent(r)
                                                                  in
                                                                     (ev, $9) }

triggerType:
| SUM                                                       { true }
| MINUS                                                     { false }

argumentList:
| ID COMMA argumentList                                     { $1::$3 }
| ID                                                        { [$1] }

statementList:
| statement EOSTMT statementList                            { $1::$3 }
| statement EOSTMT                                          { [$1] }

statement:
| LPAREN statement RPAREN                                   { $2 }
| constStatement                                            { $1 }
| varStatement                                              { $1 }
| tupleStatement                                            { $1 }
| arithmeticStatement                                       { $1 }
| ifThenStatement                                           { $1 }
| ifThenElseStatement                                       { $1 }
| blockStatement                                            { $1 }
| iterateStatement                                          { $1 }
| lambdaStatement                                           { $1 }
| assLmbdStatement                                          { $1 }
| applyStatement                                            { $1 }
| mapStatement                                              { $1 }
| flattenStatement                                          { $1 }
| aggregateStatement                                        { $1 }
| grpByAggStatement                                         { $1 }
| memberStatement                                           { $1 }
| lookupStatement                                           { $1 }
| sliceStatement                                            { $1 }
| collectionStatement                                       { $1 }
| pcUpdateStatement                                         { $1 }
| pcValueUpdateStatement                                    { $1 }
| pcElementRemoveStatement                                  { $1 }

constStatement:
| CONST_FLOAT                                               { Const(Types.CFloat($1)) }
| INTEGER                                                   { Const(Types.CFloat(float_of_int $1)) }

varStatement:
| ID                                                        { Var($1, TBase(Types.TFloat)) }

tupleStatement:
| LT statementElementList GT                                { Tuple($2) }

statementElementList:
| statement EOSTMT statementElementList                     { $1::$3 }
| statement                                                 { [$1] }

arithmeticStatement:
| statement SUM statement                                   { Add($1, $3) }
| statement PRODUCT statement                               { Mult($1, $3) }
| statement EQ statement                                    { Eq($1, $3) }
| statement NE statement                                    { Neq($1, $3) }
| statement LT statement                                    { Lt($1, $3) }
| statement LE statement                                    { Leq($1, $3) }

ifThenStatement:
| IF0 LPAREN statement RPAREN statement                     { IfThenElse0($3, $5) }

ifThenElseStatement:
| IF LPAREN statement RPAREN statement ELSE statement       { IfThenElse($3, $5, $7) }

blockStatement:
| LBRACE statementElementList RBRACE                        { Block($2) }

iterateStatement:
| ITERATE LPAREN statement RPAREN statement                 { Iterate($3, $5) }

lambdaStatement:
| LAMBDA LPAREN lambdaArgument RPAREN statement             { Lambda($3, $5) }

lambdaArgument:
| varItem                                                   { AVar(fst $1, snd $1) }
| LT varList GT                                             { ATuple($2) }
| LPAREN lambdaArgument RPAREN                              { $2 }

varItem:
| ID COLON varType                                          { ($1, $3) }

varList:
| varItem EOSTMT varList                                    { $1::$3 }
| varItem                                                   { [$1] }

varType:
| INT                                                       { TBase(Types.TInt) }
| FLOAT                                                     { TBase(Types.TFloat) }
| UNIT                                                      { TUnit }
| LT varTypeList GT                                         { TTuple($2) }
| COLLECTION LPAREN varType RPAREN                          { Collection($3) }
| LPAREN varTypeList RPAREN ARROW varType                   { Fn($2, $5) }

varTypeList:
| varType EOSTMT varTypeList                                { $1::$3 }
| varType                                                   { [$1] }

assLmbdStatement:
| LAMBDA LPAREN lambdaArgument COMMA lambdaArgument RPAREN statement
                                                            { AssocLambda($3, $5, $7) }

applyStatement:
| APPLY LPAREN statement COMMA statement RPAREN             { Apply($3, $5) }

mapStatement:
| MAP LPAREN statement COMMA statement RPAREN               { Map($3, $5) }

flattenStatement:
| FLATTEN LPAREN statement RPAREN                           { Flatten($3) }

aggregateStatement:
| AGGREGATE LPAREN statement COMMA statement COMMA statement RPAREN               
                                                            { Aggregate($3, $5, $7) }

grpByAggStatement:
| GROUPBYAGGREGATE LPAREN statement COMMA statement COMMA statement COMMA statement RPAREN               
                                                            { GroupByAggregate($3, $5, $7, $9) }

memberStatement:
| MEMBER LPAREN statement COMMA LBRACK statementElementList RBRACK RPAREN
                                                            { Member($3, $6) }

lookupStatement:
| LOOKUP LPAREN statement COMMA LBRACK statementElementList RBRACK RPAREN
                                                            { Lookup($3, $6) }

sliceStatement:
| SLICE LPAREN statement COMMA LBRACK 
         varBindList RBRACK RPAREN                          { let var_list = slice_infering $3 $6 
                                                               in
                                                                  Slice($3, types_to_vartypes var_list, $6) }

varBindList:
| varBindItem EOSTMT varBindList                            { $1::$3 }
| varBindItem                                               { [$1] }
|                                                           { [] }

varBindItem:
| ID BOUND statement                                        { ($1, $3) }

collectionStatement:
| DOLLAR ID                                                 { get_collection $2 }

pcUpdateStatement:
| PCUPDATE LPAREN statement COMMA LBRACK statementElementListOpt RBRACK
   COMMA statement RPAREN                                   { PCUpdate($3, $6, $9) }

pcElementRemoveStatement:
| PCELEMENTREMOVE LPAREN statement COMMA LBRACK statementElementListOpt RBRACK
   COMMA LBRACK statementElementListOpt RBRACK RPAREN       { PCElementRemove($3, $6, $10) }

pcValueUpdateStatement:
| PCVALUEUPDATE LPAREN statement COMMA LBRACK statementElementListOpt RBRACK
   COMMA LBRACK statementElementListOpt RBRACK COMMA statement RPAREN   
                                                            { PCValueUpdate($3, $6, $10, $13) }
statementElementListOpt:
| statementElementList                                      { $1 }
|                                                           { [] }
