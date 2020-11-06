%{
open Type
open Constants
;;

let bail ?(loc = symbol_start_pos ()) msg = 
   raise (Sql.SQLParseError(msg, loc))

let (table_defs:((string * Sql.table_t) list ref)) = Sql.global_table_defs

;;

let re_source_vars name vars =
   List.map (fun (_,v,t) -> ((Some(name)), v, t)) vars

;;
let mk_tbl table_def = 
   let (relname, relsch, reltype, relsource) = table_def in
   if List.mem_assoc relname !table_defs then
      bail ("Duplicate Definition of Relation '"^relname^"'")
   else
      let new_schema = re_source_vars relname relsch in
      let new_def = (relname, new_schema, reltype, relsource) in
         table_defs := (relname, new_def) :: !table_defs;
         new_def
;;
let get_schema table_name source_name = 
   if List.mem_assoc table_name !table_defs then
      let (_,sch,_,_) = List.assoc table_name !table_defs in
         List.map (fun (_,v,vt) -> (Some(source_name),v,vt)) sch
   else
      bail ("Reference to Undefined Table '"^table_name^"'")

let select_schema name q = 
   re_source_vars name (Sql.select_schema ~strict:false
                                          (List.map snd !table_defs) 
                                          q)

let natural_join lhs rhs = 
   let assoc_rhs = List.map (fun (tbl,v,t) -> (v,(tbl,v,t))) rhs in
   let var v = Sql.Var(v) in
   let (cond, sch)  = 
      List.fold_left (fun (cond, rhs) (lhs_tbl, lhs_v, lhs_type) ->
         if List.mem_assoc lhs_v rhs then
            (
               Sql.mk_and 
                  cond 
                  (Sql.Comparison(var(List.assoc lhs_v rhs), Eq, 
                                  var(lhs_tbl, lhs_v, lhs_type))),
               List.remove_assoc lhs_v rhs
            )
         else
            (cond, rhs)
      ) ((Sql.ConstB(true)), assoc_rhs) lhs
   in
      (cond, lhs @ (List.map snd sch))

let rec scan_for_existence (op_name:string) (q:Sql.select_t) (cmp_op:cmp_t) 
                           (expr:Sql.expr_t) =
   match q with
   | Sql.Union(s1, s2) -> 
      let rcr stmt = scan_for_existence op_name stmt cmp_op expr in
      Sql.Or(rcr s1, rcr s2)
   (* XXX: Verify that this is the right way to handle HAVING clauses *)
   | Sql.Select(targets, sources, cond, gb_vars, having, _) ->
      let (_,tgt) = match targets with [tgt] -> tgt | _ -> 
         bail ("Target of "^op_name^" clause should produce a single column")
      in 
      if Sql.is_agg_expr tgt
      then Sql.Comparison(expr, cmp_op, Sql.NestedQ(q))
      else Sql.Exists(Sql.Select(["unused", Sql.Const(CInt(1))], sources, 
            (Sql.And(cond, Sql.Comparison(expr, cmp_op, tgt))), 
             gb_vars, having, []))


let bind_select_vars q =
   Sql.bind_select_vars q (List.map snd !table_defs)

%}

%token <Type.type_t> TYPE
%token <string> ID STRING
%token <int> INT   
%token <float> FLOAT
%token DATE
%token CHAR
%token VARCHAR
%token DECIMAL
%token TRUE FALSE
%token EQ NE LT LE GT GE
%token SUM MINUS
%token PRODUCT DIVIDE
%token AND OR NOT BETWEEN LIKE
%token COMMA LPAREN RPAREN PERIOD
%token AS
%token JOIN INNER OUTER LEFT RIGHT ON NATURAL EXISTS IN SOME ALL UNION
%token CREATE TABLE FROM USING SELECT WHERE GROUP BY HAVING ORDER
%token SOCKET FILE FIXEDWIDTH VARSIZE OFFSET ADJUSTBY SETVALUE LINE DELIMITED
%token EXTRACT LIST DISTINCT HAVING 
%token CASE WHEN ELSE THEN END
%token FUNCTION RETURNS EXTERNAL
%token POSTGRES RELATION PIPE
%token ASC DESC
%token SOURCE ARGS INSTANCE TUPLE ADAPTOR BINDINGS STREAM
%token EOSTMT
%token EOF
%token SUMAGG COUNTAGG AVGAGG MAXAGG MINAGG
%token INCLUDE
%token INTERVAL

%left AND OR NOT
%left EQ NE LT LE GT GE
%left SUM MINUS
%left PRODUCT DIVIDE
%nonassoc UMINUS

// start   
%start dbtoasterSqlFile dbtoasterSqlStmtList expression
%type < Sql.file_t > dbtoasterSqlFile
%type < Sql.t list > dbtoasterSqlStmtList
%type < Sql.expr_t > expression
    
%%

dbtoasterSqlFile:
dbtoasterSqlStmtList   { 
   List.fold_right Sql.add_to_file_first $1 Sql.empty_file 
}

dbtoasterSqlStmtList:
| dbtoasterSqlStmt EOF           { $1 }
| dbtoasterSqlStmt EOSTMT EOF    { $1 }
| dbtoasterSqlStmt EOSTMT dbtoasterSqlStmtList
                                 { $1 @ $3 }
| error {
      bail "Expected ';'"
   }

dbtoasterSqlStmt:
| INCLUDE STRING           { (!Sql.parse_file) $2 }
| createTableStmt          { [Sql.Create_Table($1)] }
| selectStmt               { [Sql.SelectStmt(bind_select_vars $1)] }
| functionDeclarationStmt  { [] }
| error  { 
      bail "Invalid DBT-SQL statement";
   }

functionDeclarationStmt:
| CREATE FUNCTION ID LPAREN fieldList RPAREN RETURNS typeDefn 
  AS functionDefinition {
    Functions.declare_usr_function $3 (List.map (fun (_,_,x)->x) $5) $8 $10 
  }
| error {
      bail "Invalid CREATE FUNCTION declaration"
   }

functionDefinition:
| EXTERNAL STRING { $2 }

//
// Create table statements

framingStmt:
|   FIXEDWIDTH INT                  { Schema.FixedSize($2) }
|   LINE DELIMITED                  { Schema.Delimited("\n") }
|   STRING DELIMITED                { Schema.Delimited($1) }


adaptorParams:
|   ID SETVALUE STRING                     { [(String.lowercase_ascii $1,$3)] }
|   ID SETVALUE STRING COMMA adaptorParams { (String.lowercase_ascii $1,$3)::$5 }

adaptorStmt:
|   ID LPAREN RPAREN               { (String.lowercase_ascii $1, []) }
|   ID LPAREN adaptorParams RPAREN { (String.lowercase_ascii $1, $3) }
|   ID                             { (String.lowercase_ascii $1, []) }
| error {
      bail "Not a valid adaptor declaration"
   }

bytestreamParams: 
|   framingStmt adaptorStmt { ($1, $2) }

sourceStmt:
|   FILE STRING bytestreamParams 
  { (Schema.FileSource($2, fst $3), snd $3) }
|   SOCKET STRING INT bytestreamParams
  { (Schema.SocketSource(Unix.inet_addr_of_string $2, $3, fst $4), snd $4) }
|   SOCKET INT bytestreamParams
  { (Schema.SocketSource(Unix.inet_addr_any, $2, fst $3), snd $3) }
| error {
      bail "Invalid source statement"
   }

typeDefn:
| TYPE                      { $1 }
| CHAR LPAREN INT RPAREN    { if ($3 = 1) then TChar else TString }
| CHAR                      { TChar }
| VARCHAR LPAREN INT RPAREN { TString }
| VARCHAR                   { TString }
| DECIMAL LPAREN INT COMMA INT RPAREN { TFloat }
| DECIMAL                   { TFloat }
| DATE                      { TDate }
| error {
      bail "Invalid type declaration"
   } 

fieldList:
| ID typeDefn                    { [None, String.uppercase_ascii $1, $2] }
| ID typeDefn COMMA fieldList    { (None, String.uppercase_ascii $1, $2)::$4 }

tableOrStream:
| TABLE               { Schema.TableRel }
| STREAM              { Schema.StreamRel }

createTableStmt:
|   CREATE tableOrStream ID LPAREN fieldList RPAREN { 
      mk_tbl (String.uppercase_ascii $3, $5, $2, (Schema.NoSource, ("",[])))
    }
|   CREATE tableOrStream ID LPAREN fieldList RPAREN FROM sourceStmt { 
      mk_tbl (String.uppercase_ascii $3, $5, $2, $8)
    }
| error {
      bail "Invalid CREATE TABLE statement"
   }


//
// Select statements

targetItem:
| expression       { (String.uppercase_ascii (Sql.name_of_expr $1), $1) }
| expression AS ID { (String.uppercase_ascii $3, $1) }
| error {
      bail "Invalid output column declaration"
   }

targetList:
| targetItem                  { [$1] }
| targetItem COMMA targetList { $1 :: $3 }
| error {
      bail "Expected ','"
   }

fromItem: // ((source, name), schema)
| ID        { ((String.uppercase_ascii $1, (Sql.Table(String.uppercase_ascii $1))), 
               get_schema (String.uppercase_ascii $1) (String.uppercase_ascii $1)) }
| ID ID     { ((String.uppercase_ascii $2, (Sql.Table(String.uppercase_ascii $1))), 
               get_schema (String.uppercase_ascii $1) (String.uppercase_ascii $2)) }
| ID AS ID  { ((String.uppercase_ascii $3, (Sql.Table(String.uppercase_ascii $1))), 
               get_schema (String.uppercase_ascii $1) (String.uppercase_ascii $3)) }
| LPAREN selectStmt RPAREN ID
            { ((String.uppercase_ascii $4, (Sql.SubQ($2))), 
               select_schema (String.uppercase_ascii $4) $2) }
| LPAREN selectStmt RPAREN AS ID
            { ((String.uppercase_ascii $5, (Sql.SubQ($2))), 
               select_schema (String.uppercase_ascii $5) $2) }
| error {
      bail "Invalid source declaration in a FROM clause"
   }

fromJoin:
| fromJoin JOIN fromItem ON condition
    { let (lhs_tbl, lhs_cond, lhs_sch) = $1 in
      let (rhs_tbl,           rhs_sch) = $3 in
          (  lhs_tbl @ [rhs_tbl], 
             Sql.mk_and lhs_cond $5,
             lhs_sch @ rhs_sch
          )
    }
| fromJoin JOIN fromItem
    { let (lhs_tbl, lhs_cond, lhs_sch) = $1 in
      let (rhs_tbl,           rhs_sch) = $3 in
          (  lhs_tbl @ [rhs_tbl], 
             lhs_cond,
             lhs_sch @ rhs_sch
          )
    }
| fromJoin NATURAL JOIN fromItem
    { let (lhs_tbl, lhs_cond, lhs_sch) = $1 in
      let (rhs_tbl,           rhs_sch) = $4 in
      let (join_cond, joined_sch) = natural_join lhs_sch rhs_sch in
          (  lhs_tbl @ [rhs_tbl], 
             Sql.mk_and lhs_cond join_cond,
             joined_sch
          )
    }
| fromItem
    { let (tbl,sch) = $1 in ([tbl], (Sql.ConstB(true)), sch) }

fromList:
| fromJoin COMMA fromList
            { let (lhs_tbls, lhs_cond, _) = $1 in
              let (rhs_tbls, rhs_cond) = $3 in
                  (lhs_tbls @ rhs_tbls, Sql.mk_and lhs_cond rhs_cond) }
| fromJoin  { let (tbl, join_cond, _) = $1 in (tbl, join_cond) }
| error {
      bail "Expected ','"
   }

fromClause:
| FROM fromList { $2 }
|               { ([], Sql.ConstB(true)) }

whereClause:
| WHERE condition { $2 }
|                 { Sql.ConstB(true) }
| error {
      bail "Invalid WHERE clause"
   }

havingClause:
| HAVING condition { $2 }
|                  { Sql.ConstB(true) }
| error {
      bail "Invalid HAVING clause"
   }

groupByList:
| variable                   { [$1] }
| variable COMMA groupByList { $1 :: $3 }

groupByClause:
| GROUP BY groupByList { $3 }
|                      { [] }
| error {
      bail "Invalid GROUP BY clause"
   }

selectStmt: 
| selectStmt UNION selectStmt { Sql.Union($1, $3) }
| LPAREN selectStmt RPAREN { $2 }
| SELECT optionalDistinct 
    targetList
    fromClause
    whereClause
    groupByClause
    havingClause
    {
      let (from, join_conds) = $4 in
      Sql.expand_wildcard_targets (List.map snd !table_defs)
         (Sql.Select($3, from, Sql.mk_and join_conds $5, $6, $7, $2))
    }
| error {
      bail "Invalid SELECT statement"
   }

optionalDistinct:
|           { [] }
| DISTINCT  { [Sql.Select_Distinct] }

//
// Expressions

variable:
| ID              { (None, String.uppercase_ascii $1, TAny) }
| ID PERIOD ID    { ((Some(String.uppercase_ascii $1)),String.uppercase_ascii $3, TAny) }

variableList:
| variable COMMA variableList  { $1 :: $3 }
| variable                     { [$1] }
|                              { [] }

op:
| SUM     { Sql.Sum }
| PRODUCT { Sql.Prod }
| MINUS   { Sql.Sub}
| DIVIDE  { Sql.Div }

expression:
| LPAREN expression RPAREN { $2 }
| constant                 { Sql.Const($1) }
| ID           { Sql.Var(None, String.uppercase_ascii $1, TAny) }
| ID PERIOD ID { Sql.Var(Some(String.uppercase_ascii $1), String.uppercase_ascii $3, TAny) }
| PRODUCT      { Sql.Var(None, "*", TAny) }
| PRODUCT PERIOD PRODUCT { Sql.Var(None, "*", TAny) }
| ID PERIOD PRODUCT      { Sql.Var(Some(String.uppercase_ascii $1), "*", TAny) }
| expression op expression      { Sql.SQLArith($1, $2, $3) }
| MINUS expression %prec UMINUS { Sql.Negation($2) }
| LPAREN selectStmt RPAREN      { Sql.NestedQ($2) }
| SUMAGG LPAREN expression RPAREN { Sql.Aggregate(Sql.SumAgg, $3) }
| AVGAGG LPAREN expression RPAREN { Sql.Aggregate(Sql.AvgAgg, $3) }
| COUNTAGG LPAREN countAggParam RPAREN { Sql.Aggregate(Sql.CountAgg($3), 
                                                     Sql.Const(CInt(1))) }
| MAXAGG LPAREN expression RPAREN { bail "MAX is not (yet) supported" }
| MINAGG LPAREN expression RPAREN { bail "MIN is not (yet) supported" }
| ID LPAREN  RPAREN                   { Sql.ExternalFn($1,[]) }
| ID LPAREN functionParameters RPAREN { Sql.ExternalFn($1,$3) }
| EXTRACT LPAREN ID FROM variable RPAREN {
      let field = String.uppercase_ascii $3 in
      match field with
         | "YEAR" | "MONTH" | "DAY" -> 
            Sql.ExternalFn("date_part", [Sql.Const(CString(field)); 
                                         Sql.Var($5)])
         | _ -> bail ("Invalid field '"^field^"' referenced in EXTRACT")
   }
| CASE expression WHEN caseSimpleWhenClauseList caseElseClause END {
      Sql.Case(List.map (fun (cmp,ret) -> 
         (Sql.Comparison($2,Type.Eq,cmp), ret)) $4, $5)
   }
| CASE WHEN caseSearchWhenClauseList caseElseClause END { Sql.Case($3, $4) }
| error {
      bail "Invalid SQL expression"
   }

caseElseClause:
| ELSE expression  { $2 }
|                  { Sql.Const(CInt(0)) }

caseSimpleWhenClauseList:
| expression THEN expression                               { [$1,$3] }
| expression THEN expression WHEN caseSimpleWhenClauseList { ($1,$3)::$5 }

caseSearchWhenClauseList:
| condition THEN expression                               { [$1,$3] }
| condition THEN expression WHEN caseSearchWhenClauseList { ($1,$3)::$5 }

functionParameters: 
| expression COMMA functionParameters { $1::$3 }
| expression                          { [$1] }

countAggParam:
| PRODUCT               { None }
| expression            { None }
|                       { None }
| DISTINCT variableList { Some($2) }

cmpOp:
| EQ { Eq }
| NE { Neq }
| LT { Lt }
| LE { Lte }
| GT { Gt }
| GE { Gte }

conditionAtom:
| expression cmpOp expression { Sql.Comparison($1, $2, $3) }
| LPAREN condition RPAREN     { $2 }

condition: 
| conditionAtom                   { $1 }
| condition AND condition         { Sql.And($1, $3) }
| condition OR condition          { Sql.Or($1, $3) }
| NOT condition                   { Sql.Not($2) }
| TRUE                            { Sql.ConstB(true) }
| FALSE                           { Sql.ConstB(false) }
| EXISTS LPAREN selectStmt RPAREN { Sql.Exists($3) }
| expression LIKE STRING          { Sql.Like($1, $3) }
| expression NOT LIKE STRING      { Sql.Not(Sql.Like($1, $4)) }
| expression BETWEEN expression AND expression 
   { Sql.And(Sql.Comparison($1, Gte, $3), Sql.Comparison($1, Lte, $5)) }
| expression IN LIST LPAREN constantList RPAREN {
      Sql.InList($1, $5)
   }
| expression NOT IN LIST LPAREN constantList RPAREN {
      Sql.Not(Sql.InList($1, $6))
   }
| expression IN LPAREN selectStmt RPAREN { 
      scan_for_existence "IN" $4 Eq $1 
   }
| expression NOT IN LPAREN selectStmt RPAREN { 
      Sql.Not(scan_for_existence "IN" $5 Eq $1)
   }
| expression cmpOp SOME LPAREN selectStmt RPAREN { 
      scan_for_existence "SOME" $5 $2 $1 
   }
| expression cmpOp ALL LPAREN selectStmt RPAREN { 
      Sql.Not(scan_for_existence "ALL" $5 (inverse_of_cmp $2) $1)
   }
| error {
      bail "Invalid boolean predicate"
   }

constantList:
| constant COMMA constantList   { $1 :: $3 }
| constant                      { [$1] }
|                               { [] }

constant:
| INT                           { CInt($1) }
| FLOAT                         { CFloat($1) }
| STRING                        { CString($1) }
| DATE LPAREN STRING RPAREN     { Constants.parse_date $3 }
| INTERVAL STRING ID { 
      Constants.parse_interval (String.uppercase_ascii $3) $2 None
   }
| INTERVAL STRING ID LPAREN INT RPAREN { 
      Constants.parse_interval (String.uppercase_ascii $3) $2 (Some($5))
   }
