%{
open Types

exception SQLParseError of string
exception FeatureUnsupported of string

let bail msg = raise (SQLParseError(msg))

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
let get_schema name = 
   if List.mem_assoc name !table_defs then
      let (_,sch,_,_) = List.assoc name !table_defs in
         sch
   else
      bail ("Reference to Undefined Table '"^name^"'")

let select_schema name q = 
   re_source_vars name (Sql.select_schema (List.map snd !table_defs) q)

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
%token DATE
%token EQ NE LT LE GT GE
%token SUM MINUS
%token PRODUCT DIVIDE
%token AND OR NOT BETWEEN
%token COMMA LPAREN RPAREN PERIOD
%token AS
%token JOIN INNER OUTER LEFT RIGHT ON NATURAL EXISTS IN SOME ALL
%token CREATE TABLE FROM USING DELIMITER SELECT WHERE GROUP BY HAVING ORDER
%token SOCKET FILE FIXEDWIDTH VARSIZE OFFSET ADJUSTBY SETVALUE LINE DELIMITED
%token POSTGRES RELATION PIPE
%token ASC DESC
%token SOURCE ARGS INSTANCE TUPLE ADAPTOR BINDINGS STREAM
%token EOSTMT
%token EOF

%left AND OR NOT
%left EQ NE LT LE GT GE
%left SUM MINUS
%left PRODUCT DIVIDE
%nonassoc UMINUS

// start   
%start dbtoasterSqlList
%type < Sql.file_t > dbtoasterSqlList
    
%%

dbtoasterSqlList:
| dbtoasterSqlStmt EOF           { Sql.mk_file $1 }
| dbtoasterSqlStmt EOSTMT EOF    { Sql.mk_file $1 }
| dbtoasterSqlStmt EOSTMT dbtoasterSqlList
                                 { Sql.add_to_file $1 $3 }

dbtoasterSqlStmt:
| createTableStmt        { Sql.Create_Table($1) }
| selectStmt             { Sql.Select(bind_select_vars $1) }

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
| DATE                      { TInt    }

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
               get_schema (String.uppercase $1)) }
| ID ID     { ((String.uppercase $2, (Sql.Table(String.uppercase $1))), 
               get_schema (String.uppercase $1)) }
| ID AS ID  { ((String.uppercase $3, (Sql.Table(String.uppercase $1))), 
               get_schema (String.uppercase $1)) }
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
         ($2, from, Sql.mk_and join_conds $4, $5)
    }

//
// Expressions

variable:
| ID           { (None, String.uppercase $1, TAny) }
| ID PERIOD ID { ((Some(String.uppercase $1)), String.uppercase $3, TAny) }

op:
| SUM     { Sql.Sum }
| PRODUCT { Sql.Prod }
| MINUS   { Sql.Sub}
| DIVIDE  { Sql.Div }

expression:
| LPAREN expression RPAREN { $2 }
| INT      { Sql.Const(CInt($1)) }
| FLOAT    { Sql.Const(CFloat($1)) }
| STRING   { Sql.Const(CString($1)) }
| variable { Sql.Var($1) }
| expression op expression      { Sql.SQLArith($1, $2, $3) }
| MINUS expression %prec UMINUS { Sql.Negation($2) }
| LPAREN selectStmt RPAREN      { Sql.NestedQ($2) }
| ID LPAREN expression RPAREN   {
             if (String.uppercase $1) = "SUM" then
               Sql.Aggregate(Sql.SumAgg, $3)
             else if (String.uppercase $1) = "COUNT" then
               Sql.Aggregate(Sql.SumAgg, Sql.Const(CInt(1)))
             else
               bail ("Unknown Aggregate Function '"^$1^"'")
           }
| DATE LPAREN STRING RPAREN     {
            if (Str.string_match
                (Str.regexp "\\([0-9]+\\)-\\([0-9]+\\)-\\([0-9]+\\)") $3 0)
						then (
                let y = (int_of_string (Str.matched_group 1 $3)) in
                let m = (int_of_string (Str.matched_group 2 $3)) in
                let d = (int_of_string (Str.matched_group 3 $3)) in
                    if (m > 12) then bail 
                        ("Invalid month ("^(string_of_int m)^") in date: "^$3);                                         
                    if (d > 31) then bail
                        ("Invalid day ("^(string_of_int d)^") in date: "^$3);
                    Sql.Const(CInt((y * 10000) + (m * 100) + (d * 1))) 
            ) else
                bail ("Improperly formatted date: "^$3)	             
          }
						
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
| expression BETWEEN expression AND expression { Sql.And(Sql.Comparison($1, Gte, $3), Sql.Comparison($1, Lte, $5)) }

