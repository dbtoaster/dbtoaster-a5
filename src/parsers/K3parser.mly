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
    type m3_map_t = K3.map_t

    
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

    (* table name -> fields *)
    let maps:((string, m3_map_t) Hashtbl.t) = Hashtbl.create 10

		let add_map name (new_map: m3_map_t) =
        Hashtbl.replace maps (String.uppercase name) new_map

		let get_map name : m3_map_t= 
				try Hashtbl.find maps name
					with _ -> raise (K3TypeError("Map '"^name^"' is not defined!"))


    let create_map name input_types output_types =
        let f = fun l -> List.map (fun x -> x) l      (* was (fun x -> ("", x)) *)
            in
                let new_map = (name, (f input_types), (f output_types))
                    in
                        add_map name new_map; 
                        (name, input_types, output_types) 
   
		let concat_stmt (m,t) (map_list,pat_list,trig_list) =
				let new_map = if m = [] then map_list else m @ map_list
					in
						let new_trig = if t = [] then trig_list else t @ trig_list
							in
								(new_map, pat_list,new_trig) 
		

   let create_k3_statement expr = 
      (Const(Types.CFloat(0.)), expr)

   let collections:((string, expr_t) Hashtbl.t) = Hashtbl.create 10

   let add_collection name expr =
      Hashtbl.replace collections name expr

   let get_collection c_id = 
      Hashtbl.find collections c_id

   let types_to_vartypes types = 
      List.map 
         (
            fun (t) -> let nt = TBase(t)
            in 
               ("", nt)
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
%start dbtoasterK3Program
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
    
%%

dbtoasterK3Program:
| mapDeclarationList triggerList         { let _ = $1 in
                                                               concat_stmt ($1, $2) empty_output_k3 }
mapDeclarationList:
| mapDeclaration mapDeclarationList                         { $1::$2 }
| mapDeclaration                                            { [$1] }

mapDeclaration:
| ID LBRACK typeList RBRACK LBRACK typeList RBRACK EOSTMT   { let col = PC($1, types_to_vartypes $3, types_to_vartypes $6, TBase(Types.TFloat))
                                                               in
                                                                  add_collection $1 col;
                                                                     create_map $1 $3 $6 }
| ID LBRACK RBRACK LBRACK typeList RBRACK EOSTMT            { let col = OutPC($1, types_to_vartypes $5, TBase(Types.TFloat))
                                                               in
                                                                  add_collection $1 col;
                                                                     create_map $1 [] $5 }
| ID LBRACK typeList RBRACK LBRACK RBRACK EOSTMT            { let col = InPC($1, types_to_vartypes $3, TBase(Types.TFloat))
                                                               in
                                                                  add_collection $1 col;
                                                                     create_map $1 $3 [] }
| ID LBRACK RBRACK LBRACK RBRACK EOSTMT                     { let col = SingletonPC($1, TBase(Types.TFloat))
                                                               in
                                                                  add_collection $1 col;
                                                                     create_map $1 [] [] }

typeList:
| typeItem COMMA typeList                                   { $1::$3 }
| typeItem                                                  { [$1] }

typeItem:
| INT                                                       { Types.TInt }
| FLOAT                                                     { Types.TFloat }

triggerList:
| trigger triggerList                                       { $1::$2 }
| trigger                                                   { [$1] }

trigger:
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
| statement EOSTMT statementList                            { (create_k3_statement $1)::$3 }
| statement EOSTMT                                          { [create_k3_statement $1] }

statement:
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
| SLICE LPAREN statement COMMA LBRACK varList RBRACK COMMA LBRACK 
         varBindList RBRACK RPAREN                          { Slice($3, $6, $10) }

varBindList:
| varBindItem EOSTMT varBindList                            { $1::$3 }
| varBindItem                                               { [$1] }

varBindItem:
| ID BOUND statement                                        { ($1, $3) }

collectionStatement:
| DOLLAR ID                                                 { get_collection $2 }

pcUpdateStatement:
| PCUPDATE LPAREN statement COMMA LBRACK statementElementListOpt RBRACK
   COMMA statement RPAREN                                   { PCUpdate($3, $6, $9) }

pcValueUpdateStatement:
| PCVALUEUPDATE LPAREN statement COMMA LBRACK statementElementListOpt RBRACK
   COMMA LBRACK statementElementListOpt RBRACK COMMA statement RPAREN   
                                                            { PCValueUpdate($3, $6, $10, $13) }
statementElementListOpt:
| statementElementList                                      { $1 }
|                                                           { [] }
