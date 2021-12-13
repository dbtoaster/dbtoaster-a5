%{    
   open Calculus
   open Type
   open Constants
   open K3
   exception K3ParseError of string
   exception K3TypeError of string
   exception K3FeatureUnsupported of string
    
   type k3_statement_t = K3.statement_t
   type k3_trigger_t = K3.trigger_t
   type k3_program_t = K3.prog_t
   (*
   type k3_map_t = K3.map_t
   type map_schema_t = (Type.var_t list * Type.var_t list)
   *)

    
   let parse_error s =
      let lex_pos = symbol_end_pos () in
      let line_pos_str = string_of_int (lex_pos.Lexing.pos_lnum) in
      let char_pos_str = string_of_int
         (lex_pos.Lexing.pos_cnum - lex_pos.Lexing.pos_bol)
      in
         print_endline
            ("Parsing error '" ^ s ^ "' at line: " ^ line_pos_str ^
             " char: " ^ (char_pos_str));
         flush stdout

   let addSchema rel old_rels = 
      let (name, schema, reltype, source, adaptor) = rel in
         Schema.add_rel old_rels ~source:source ~adaptor:adaptor
                        (name, schema, reltype);
         old_rels

   let collections:((string, expr_t) Hashtbl.t) = Hashtbl.create 10

   let add_collection name expr =
      Hashtbl.replace collections name expr

   let get_collection c_id = 
      try Hashtbl.find collections c_id
      with _ -> raise (K3TypeError("Collection '"^c_id^"' is not defined!"))
      
   let get_collection_schema name =  begin match get_collection name with
      | SingletonPC(id,t) ->  [],[]
      | OutPC(id,outs,t) ->   [],outs
      | InPC(id,ins,t) ->     ins,[]
      | PC(id,ins,outs,t) ->  ins,outs
      | _ -> failwith "K3 expression must be PC!"
   end
   
   let create_map coll = List.hd (get_expr_map_schema coll)

   let patterns:(M3Patterns.pattern_map ref) = ref []

   let add_pattern name (new_pattern: M3Patterns.pattern) = 
      patterns := M3Patterns.add_pattern !patterns (name, new_pattern)

   let tl_queries: (toplevel_query_t list ref) = ref []

   let add_tl qname qexp = 
      tl_queries := (qname, qexp)::(!tl_queries)
   
   let must_infer_from_slice: (bool ref) = ref true

   let slice_infering statement var_bind_list =
      let calc_result stmt = 
         let map_name = match stmt with 
            | PC(n, _, _, _) -> n
            | OutPC(n, _, _) -> n
            | InPC(n, _, _)  -> n
            | SingletonPC(n, _) -> n
            | _ -> raise (K3TypeError("Incorrect collection!"))
         in
         let (in_var_types, out_var_types) = get_collection_schema map_name in
         if (!must_infer_from_slice) then
         begin
            let get_vars l = List.map fst l in
            let in_vars = get_vars in_var_types in
            let out_vars = get_vars out_var_types in
            let var_list = get_vars var_bind_list in
            let in_filt = ListAsSet.inter in_vars var_list in
            let out_filt = ListAsSet.inter out_vars var_list in
            let in_pattern = M3Patterns.make_in_pattern in_vars in_filt in
            let out_pattern = M3Patterns.make_out_pattern out_vars out_filt in
            if in_filt <> [] then (add_pattern map_name in_pattern) else ();
            if out_filt <> [] then (add_pattern map_name out_pattern) else ()
         end
         else ();
         in_var_types@out_var_types
      in
     
      let rec rcr stmt = match stmt with
         | Map(_, e) -> rcr e         
         | Slice(e, _, _) -> rcr e
         | Block(el) ->
            let rec last_item l = 
               if List.length l = 1 
               then List.hd l
               else last_item (List.tl l)
            in
               rcr (last_item el)
          | PC _ | OutPC _ | InPC _ | SingletonPC _ -> calc_result stmt
          | _ -> raise (
             K3TypeError("First argument of Slice should be a Collection!"))
      in
      rcr statement
      
   let varType_to_varK3Type = List.map (fun (v,t) -> (v,TBase(t)))   
   let varK3Type_to_varType = List.map (fun (v,t) -> (v,base_type_of t))
%}

%token <Type.type_t> TYPE
%token <int> INTEGER
%token <string> ID CONST_STRING 
%token <float> CONST_FLOAT
%token EQ NE LT LE GT
%token SUM MINUS PRODUCT
%token COMMA LPAREN RPAREN LBRACK RBRACK PERIOD COLON DOLLAR
%token ON SYSTEM READY QUERY
%token IF IF0 ELSE ITERATE LAMBDA APPLY MAP FLATTEN AGGREGATE GROUPBYAGGREGATE
%token MEMBER LOOKUP SLICE FILTER SINGLETON COMBINE
%token CREATE TABLE STREAM FROM SOCKET FILE PIPE FIXEDWIDTH DELIMITED LINE
%token VARSIZE OFFSET ADJUSTBY SETVALUE
%token PCUPDATE PCVALUEUPDATE PCELEMENTREMOVE EXTERNALLAMBDA
%token INT UNIT FLOAT COLLECTION STRINGTYPE CHAR VARCHAR DATE
%token IN OUT
%token EOSTMT
%token EOF
%token LBRACE RBRACE
%token DOLLAR BOUND ARROW

%left EQ NE LT LE GT GE
%left SUM
%left PRODUCT

// start   
%start dbtoasterK3Program statement mapDeclaration
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
%type < K3.map_t > mapDeclaration
    
%%

dbtoasterK3Program:
| relStatementList mapDeclarationList queryDeclarationList
  patternDeclarationList triggerList         
   { let _ = $3 in
     ($1, ($2, !patterns), $5, !tl_queries) }
mapDeclarationList:
| mapDeclaration mapDeclarationList                         { $1::$2 }
| mapDeclaration                                            { [$1] }

mapDeclaration:
| ID LPAREN typeItem RPAREN LBRACK varList RBRACK 
  LBRACK varList RBRACK EOSTMT   
   { let col = PC($1, varType_to_varK3Type $6, 
                      varType_to_varK3Type $9, TBase($3))
     in
        add_collection $1 col;
        create_map col}
| ID LPAREN typeItem RPAREN LBRACK RBRACK LBRACK varList RBRACK EOSTMT     
   { let col = OutPC($1, varType_to_varK3Type $8, TBase($3))
     in 
        add_collection $1 col;
        create_map col}
| ID LPAREN typeItem RPAREN LBRACK varList RBRACK LBRACK RBRACK EOSTMT
   { let col = InPC($1, varType_to_varK3Type $6, TBase($3))
     in
        add_collection $1 col;
        create_map col}
| ID LPAREN typeItem RPAREN LBRACK RBRACK LBRACK RBRACK EOSTMT          
   { let col = SingletonPC($1,TBase($3))
     in
        add_collection $1 col;
        create_map col}

queryDeclarationList:
| queryDeclaration EOSTMT queryDeclarationList              { let _ = $1 in () }
|                                                           { () }

queryDeclaration:
| QUERY ID SETVALUE statement                               { add_tl $2 $4 }

patternDeclarationList:
| patternDeclarationItem EOSTMT patternDeclarationList      
   { let _ = $1, $3 in () }
|                                                           { ()}

patternDeclarationItem:
| ID COLON patternList
   { must_infer_from_slice := false;
     let mapname = $1 in
     let patts = $3 in
     List.iter (
        fun p -> add_pattern mapname p
     ) patts
   }

patternList:
| patternItem COMMA patternList                             { $1::$3 }
| patternItem                                               { [$1] }

patternItem:
| OUT LBRACE patternElementList RBRACE                      { M3Patterns.Out($3) }
| IN LBRACE patternElementList RBRACE                       { M3Patterns.In($3) }

patternElementList:
| patternElementItem COMMA patternElementList               
   { ( (fst $1)::(fst $3) ),  ( (snd $1)::(snd $3) )}
| patternElementItem                                        
   { [fst $1], [snd $1] } 
|  { [], [] }

patternElementItem:
| INTEGER COLON ID                                       { $3, $1 }

varList:
| varItem COMMA varList                                  { $1::$3 }
| varItem                                                { [$1] }

varItem:
| ID COLON typeItem                                      { ($1, $3) }
| ID                                                     { ($1, Type.TInt) }

typeItem:
| INT                                                      { Type.TInt }
| FLOAT                                                    { Type.TFloat }
| STRINGTYPE                                               { Type.TString }
| DATE                                                     { Type.TDate }


k3Type:
| INT
   { TBase(Type.TInt) }
| FLOAT
   { TBase(Type.TFloat) }
| DATE
   { TBase(Type.TDate) }
| STRINGTYPE
   { TBase(Type.TString) }
| UNIT                                                     { TUnit }
| LT k3TypeList GT                                         { TTuple($2) }
| COLLECTION LPAREN k3Type RPAREN
   { Collection(Unknown, $3) }
| LPAREN k3TypeList RPAREN ARROW k3Type                    { Fn($2, $5) }

k3TypeList:
| k3Type EOSTMT k3TypeList                                 { $1::$3 }
| k3Type                                                   { [$1] }

k3Var:
| ID COLON k3Type                                          { ($1, $3) }

k3VarList:
| k3Var EOSTMT k3VarList                                   { $1::$3 }
| k3Var                                                    { [$1] }




triggerList:
| trigger triggerList                                       { $1::$2 }
| trigger                                                   { [$1] }

trigger:
| ON triggerType ID LPAREN varList RPAREN LBRACE statementList RBRACE 
   { let args = $5 in
     let r = ($3, args,Schema.StreamRel) in
     let ev = 
        if $2 
        then Schema.InsertEvent(r)
        else Schema.DeleteEvent(r)
     in
        (ev, $8) }
| ON triggerType ID LBRACK varList RBRACK LBRACE statementList RBRACE 
   { let args = $5 in
     let r = ($3, args,Schema.StreamRel) in
     let ev = 
        if $2 
        then Schema.InsertEvent(r)
        else Schema.DeleteEvent(r)
     in
        (ev, $8) }
| ON SYSTEM READY LBRACE statementList RBRACE
   { (Schema.SystemInitializedEvent, $5) }

triggerType:
| SUM                                                       { true }
| MINUS                                                     { false }

statementList:
| statement EOSTMT statementList                            { $1::$3 }
|                                                           { [] }

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
| filterStatement                                           { $1 }
| collectionStatement                                       { $1 }
| pcUpdateStatement                                         { $1 }
| pcValueUpdateStatement                                    { $1 }
| pcElementRemoveStatement                                  { $1 }
| singletonStatement                                        { $1 }
| combineStatement                                          { $1 }
| externalLambdaStatement                                   { $1 }

constStatement:
| CONST_FLOAT
   { Const(Constants.CFloat($1)) }
| INTEGER
   { Const(Constants.CInt($1)) }
| CONST_STRING
   { Const(CString($1)) }
| DATE LPAREN CONST_STRING RPAREN
   { Const(Constants.parse_date $3) }

varStatement:
| ID COLON k3Type   { Var($1, $3 ) }
| ID                { Var($1, TBase(Type.TInt)) }

tupleStatement:
| LT statementElementList GT                                { Tuple($2) }

statementElementList:
| statement EOSTMT statementElementList                     { $1::$3 }
| statement EOSTMT                                          { [$1] }
| statement                                                 { [$1] }

arithmeticStatement:
| statement SUM statement                                   { Add($1, $3) }
| statement PRODUCT statement                               { Mult($1, $3) }
| statement EQ statement                                    { Eq($1, $3) }
| statement NE statement                                    { Neq($1, $3) }
| statement LT statement                                    { Lt($1, $3) }
| statement LE statement                                    { Leq($1, $3) }

ifThenStatement:
| IF0 LPAREN statement RPAREN statement
   { IfThenElse0($3, $5) }

ifThenElseStatement:
| IF LPAREN statement RPAREN statement ELSE statement
   { IfThenElse($3, $5, $7) }

blockStatement:
| LBRACE statementElementList RBRACE                        { Block($2) }

iterateStatement:
| ITERATE LPAREN statement COMMA statement RPAREN           { Iterate($3, $5) }

lambdaStatement:
| LAMBDA LPAREN lambdaArgument RPAREN LBRACE statement RBRACE { Lambda($3, $6) }

lambdaArgument:
| k3Var                                                     
   { AVar(fst $1, snd $1) }
| LT k3VarList GT                                           { ATuple($2) }
| LPAREN lambdaArgument RPAREN                              { $2 }

assLmbdStatement:
| LAMBDA LPAREN lambdaArgument COMMA lambdaArgument RPAREN 
  LBRACE statement RBRACE
   { AssocLambda($3, $5, $8) }

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
| GROUPBYAGGREGATE LPAREN statement COMMA statement 
                   COMMA statement COMMA statement RPAREN
   { GroupByAggregate($3, $5, $7, $9) }

memberStatement:
| MEMBER LPAREN statement COMMA LBRACK statementElementList RBRACK RPAREN
                                                            { Member($3, $6) }

lookupStatement:
| LOOKUP LPAREN statement COMMA LBRACK statementElementList RBRACK RPAREN
                                                            { Lookup($3, $6) }

sliceStatement:
| SLICE LPAREN statement COMMA LBRACK 
        varBindList RBRACK RPAREN
   { let var_list = slice_infering $3 $6 in
     Slice($3, var_list, $6) }

filterStatement:
| FILTER LPAREN statement COMMA statement RPAREN            { Map($3, $5) }

varBindList:
| varBindItem EOSTMT varBindList                            { $1::$3 }
| varBindItem                                               { [$1] }
|                                                           { [] }

varBindItem:
| ID BOUND statement                                        { ($1, $3) }

collectionStatement:
| DOLLAR ID
   { get_collection $2 }

pcUpdateStatement:
| PCUPDATE LPAREN statement COMMA LBRACK statementElementListOpt RBRACK
           COMMA statement RPAREN
   { PCUpdate($3, $6, $9) }

pcElementRemoveStatement:
| PCELEMENTREMOVE LPAREN statement COMMA LBRACK statementElementListOpt RBRACK
   COMMA LBRACK statementElementListOpt RBRACK RPAREN
   { PCElementRemove($3, $6, $10) }

pcValueUpdateStatement:
| PCVALUEUPDATE LPAREN statement COMMA LBRACK statementElementListOpt RBRACK
                COMMA LBRACK statementElementListOpt RBRACK 
                COMMA statement RPAREN
   { PCValueUpdate($3, $6, $10, $13) }
    
singletonStatement:
| SINGLETON LPAREN statement RPAREN                         { Singleton($3) }

combineStatement:
| COMBINE LBRACE statementElementList RBRACE                { Combine($3) }

externalLambdaStatement:
| EXTERNALLAMBDA LPAREN ID COMMA lambdaArgument COMMA typeItem RPAREN
    { ExternalLambda($3, $5, TBase($7)) }

statementElementListOpt:
| statementElementList                                      { $1 }
|                                                           { [] }

relStatementList:
| relStatement EOSTMT relStatementList
    { addSchema $1 $3 }
|   { Schema.empty_db () }

relStatement:
| CREATE tableOrStream ID LPAREN emptyFieldList RPAREN
   { (String.uppercase_ascii $3, $5, $2, Schema.NoSource, ("",[])) }
| CREATE tableOrStream ID LPAREN emptyFieldList RPAREN FROM sourceStmt
   { (String.uppercase_ascii $3, $5, $2, fst $8, snd $8) }

tableOrStream:
| TABLE  { Schema.TableRel  }
| STREAM { Schema.StreamRel }

emptyFieldList:
|                             { [] }
| fieldList                   { $1 }

fieldList:
| ID dbtType                  { [$1,$2] }
| ID dbtType COMMA fieldList  { ($1,$2)::$4 }

sourceStmt:
|   FILE CONST_STRING bytestreamParams 
  { (Schema.FileSource($2, fst $3), snd $3) }
|   SOCKET CONST_STRING INTEGER bytestreamParams
  { (Schema.SocketSource(Unix.inet_addr_of_string $2, $3, fst $4), snd $4) }
|   SOCKET INTEGER bytestreamParams
  { (Schema.SocketSource(Unix.inet_addr_any, $2, fst $3), snd $3) }

bytestreamParams: 
|   framingStmt adaptorStmt { ($1, $2) }

framingStmt:
|   FIXEDWIDTH INTEGER                  { Schema.FixedSize($2) }
|   LINE DELIMITED                  { Schema.Delimited("\n") }
|   CONST_STRING DELIMITED                { Schema.Delimited($1) }

adaptorStmt:
|   ID LPAREN RPAREN               { (String.lowercase_ascii $1, []) }
|   ID LPAREN adaptorParams RPAREN { (String.lowercase_ascii $1, $3) }
|   ID                             { (String.lowercase_ascii $1, []) }

adaptorParams:
|   ID SETVALUE CONST_STRING                     
   { [(String.lowercase_ascii $1,$3)] }
|   ID SETVALUE CONST_STRING COMMA adaptorParams 
   { (String.lowercase_ascii $1,$3)::$5 }

dbtType:
| TYPE                          { $1 }
| typeItem                      { $1 }
| CHAR LPAREN INTEGER RPAREN    { TString }
| VARCHAR LPAREN INTEGER RPAREN { TString }
| VARCHAR                       { TString }
| CHAR                          { TString }
| DATE                          { TDate   }
  
  