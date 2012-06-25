%{
open Types
open Constants
;;

let bail msg = raise (Sql.SQLParseError(msg))

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

let scan_for_existence (op_name:string) (q:Sql.select_t) (cmp_op:cmp_t) 
                       (expr:Sql.expr_t) =
   let (targets, sources, cond, gb_vars) = q in
   let (_,tgt) = match targets with [tgt] -> tgt | _ -> 
      bail ("Target of "^op_name^" clause should produce a single column")
   in 
   if Sql.is_agg_expr tgt
   then Sql.Comparison(expr, cmp_op, Sql.NestedQ(q))
   else Sql.Exists(["unused", Sql.Const(CInt(1))], sources, 
              (Sql.And(cond, Sql.Comparison(expr, cmp_op, tgt))), gb_vars)


let bind_select_vars q =
   Sql.bind_select_vars q (List.map snd !table_defs)

%}

%token <Types.type_t> TYPE
%token <string> ID STRING
%token <int> INT   
%token <float> FLOAT
%token DATE
%token CHAR
%token VARCHAR
%token TRUE FALSE
%token EQ NE LT LE GT GE
%token SUM MINUS
%token PRODUCT DIVIDE
%token AND OR NOT BETWEEN
%token COMMA LPAREN RPAREN PERIOD
%token AS
%token JOIN INNER OUTER LEFT RIGHT ON NATURAL EXISTS IN SOME ALL UNION
%token CREATE TABLE FROM USING DELIMITER SELECT WHERE GROUP BY HAVING ORDER
%token SOCKET FILE FIXEDWIDTH VARSIZE OFFSET ADJUSTBY SETVALUE LINE DELIMITED
%token EXTRACT
%token POSTGRES RELATION PIPE
%token ASC DESC
%token SOURCE ARGS INSTANCE TUPLE ADAPTOR BINDINGS STREAM
%token EOSTMT
%token EOF
%token SUMAGG COUNTAGG AVGAGG
%token INCLUDE

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

dbtoasterSqlStmt:
| INCLUDE STRING         { (!Sql.parse_file) $2 }
| createTableStmt        { [Sql.Create_Table($1)] }
| selectStmt             { [Sql.Select(bind_select_vars $1)] }

//
// Create table statements

framingStmt:
|   FIXEDWIDTH INT                  { Schema.FixedSize($2) }
|   LINE DELIMITED                  { Schema.Delimited("\n") }
|   STRING DELIMITED                { Schema.Delimited($1) }

adaptorParams:
|   ID SETVALUE STRING                     { [(String.lowercase $1,$3)] }
|   ID SETVALUE STRING COMMA adaptorParams { (String.lowercase $1,$3)::$5 }

adaptorStmt:
|   ID LPAREN RPAREN               { (String.lowercase $1, []) }
|   ID LPAREN adaptorParams RPAREN { (String.lowercase $1, $3) }
|   ID                             { (String.lowercase $1, []) }

bytestreamParams: 
|   framingStmt adaptorStmt { ($1, $2) }

sourceStmt:
|   FILE STRING bytestreamParams 
  { (Schema.FileSource($2, fst $3), snd $3) }
|   SOCKET STRING INT bytestreamParams
  { (Schema.SocketSource(Unix.inet_addr_of_string $2, $3, fst $4), snd $4) }
|   SOCKET INT bytestreamParams
  { (Schema.SocketSource(Unix.inet_addr_any, $2, fst $3), snd $3) }

typeDefn:
| TYPE                      { $1 }
| CHAR LPAREN INT RPAREN    { TString }
| VARCHAR LPAREN INT RPAREN { TString }
| VARCHAR                   { TString }
| CHAR                      { TString }
| DATE                      { TDate    }

fieldList:
| ID typeDefn                    { [None, String.uppercase $1, $2] }
| ID typeDefn COMMA fieldList    { (None, String.uppercase $1, $2)::$4 }

tableOrStream:
| TABLE               { Schema.TableRel }
| STREAM              { Schema.StreamRel }

createTableStmt:
|   CREATE tableOrStream ID LPAREN fieldList RPAREN { 
      mk_tbl (String.uppercase $3, $5, $2, (Schema.NoSource, ("",[])))
    }
|   CREATE tableOrStream ID LPAREN fieldList RPAREN FROM sourceStmt { 
      mk_tbl (String.uppercase $3, $5, $2, $8)
    }


//
// Select statements

targetItem:
| expression       { (String.uppercase (Sql.name_of_expr $1), $1) }
| expression AS ID { (String.uppercase $3, $1) }

targetList:
| targetItem                  { [$1] }
| targetItem COMMA targetList { $1 :: $3 }

fromItem: // ((source, name), schema)
| ID        { ((String.uppercase $1, (Sql.Table(String.uppercase $1))), 
               get_schema (String.uppercase $1) (String.uppercase $1)) }
| ID ID     { ((String.uppercase $2, (Sql.Table(String.uppercase $1))), 
               get_schema (String.uppercase $1) (String.uppercase $2)) }
| ID AS ID  { ((String.uppercase $3, (Sql.Table(String.uppercase $1))), 
               get_schema (String.uppercase $1) (String.uppercase $3)) }
| LPAREN selectStmt RPAREN ID
            { ((String.uppercase $4, (Sql.SubQ($2))), 
               select_schema (String.uppercase $4) $2) }
| LPAREN selectStmt RPAREN AS ID
            { ((String.uppercase $5, (Sql.SubQ($2))), 
               select_schema (String.uppercase $5) $2) }

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

fromClause:
| FROM fromList { $2 }
|               { ([], Sql.ConstB(true)) }

whereClause:
| WHERE condition { $2 }
|                 { Sql.ConstB(true) }

groupByList:
| variable                   { [$1] }
| variable COMMA groupByList { $1 :: $3 }

groupByClause:
| GROUP BY groupByList { $3 }
|                      { [] }

selectStmt: 
    SELECT targetList
    fromClause
    whereClause
    groupByClause
    {
      let (from, join_conds) = $3 in
         Sql.expand_wildcard_targets (List.map snd !table_defs)
            ($2, from, Sql.mk_and join_conds $4, $5)
    }

//
// Expressions

variable:
| ID              { (None, String.uppercase $1, TAny) }
| ID PERIOD ID    { ((Some(String.uppercase $1)),String.uppercase $3, TAny) }

op:
| SUM     { Sql.Sum }
| PRODUCT { Sql.Prod }
| MINUS   { Sql.Sub}
| DIVIDE  { Sql.Div }

expression:
| LPAREN expression RPAREN { $2 }
| INT          { Sql.Const(CInt($1)) }
| FLOAT        { Sql.Const(CFloat($1)) }
| STRING       { Sql.Const(CString($1)) }
| ID           { Sql.Var(None, String.uppercase $1, TAny) }
| ID PERIOD ID { Sql.Var(Some(String.uppercase $1), String.uppercase $3, TAny) }
| PRODUCT      { Sql.Var(None, "*", TAny) }
| PRODUCT PERIOD PRODUCT { Sql.Var(None, "*", TAny) }
| ID PERIOD PRODUCT      { Sql.Var(Some(String.uppercase $1), "*", TAny) }
| expression op expression      { Sql.SQLArith($1, $2, $3) }
| MINUS expression %prec UMINUS { Sql.Negation($2) }
| LPAREN selectStmt RPAREN      { Sql.NestedQ($2) }
| SUMAGG LPAREN expression RPAREN { Sql.Aggregate(Sql.SumAgg, $3) }
| AVGAGG LPAREN expression RPAREN { Sql.Aggregate(Sql.AvgAgg, $3) }
| COUNTAGG LPAREN countAggParam RPAREN { Sql.Aggregate(Sql.CountAgg, 
                                                     Sql.Const(CInt(1))) }
| ID LPAREN  RPAREN                   { Sql.ExternalFn($1,[]) }
| ID LPAREN functionParameters RPAREN { Sql.ExternalFn($1,$3) }
| DATE LPAREN STRING RPAREN     { Sql.Const(Constants.parse_date $3) }
| EXTRACT LPAREN ID FROM variable RPAREN {
      let field = String.uppercase $3 in
      match field with
         | "YEAR" | "MONTH" | "DAY" -> 
            Sql.ExternalFn("date_part", [Sql.Const(CString(field)); 
                                         Sql.Var($5)])
         | _ -> bail ("Invalid field '"^field^"' referenced in EXTRACT")
   }

functionParameters: 
| expression COMMA functionParameters { $1::$3 }
| expression                          { [$1] }

countAggParam:
| PRODUCT    { () }
| expression { () }
|            { () }

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
| condition AND conditionAtom     { Sql.And($1, $3) }
| condition OR conditionAtom      { Sql.Or($1, $3) }
| NOT condition                   { Sql.Not($2) }
| TRUE                            { Sql.ConstB(true) }
| FALSE                           { Sql.ConstB(false) }
| EXISTS LPAREN selectStmt RPAREN { Sql.Exists($3) }
| expression BETWEEN expression AND expression 
   { Sql.And(Sql.Comparison($1, Gte, $3), Sql.Comparison($1, Lte, $5)) }
| expression IN LPAREN selectStmt RPAREN { 
      scan_for_existence "IN" $4 Eq $1 
   }
| expression cmpOp SOME LPAREN selectStmt RPAREN { 
      scan_for_existence "SOME" $5 $2 $1 
   }
| expression cmpOp ALL LPAREN selectStmt RPAREN { 
      Sql.Not(scan_for_existence "ALL" $5 (inverse_of_cmp $2) $1)
   }
