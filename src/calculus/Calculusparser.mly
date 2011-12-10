%{
open Types
open Arithmetic
open Calculus
open Calculus.BasicCalculus

let incorporate_stmt ?(old = (Schema.empty_db, [])) (rel, query) =
   let (old_rels,old_queries) = old in
   begin match rel with 
      | Some(name, schema, reltype, source, adaptor) ->
         Schema.add_rel old_rels ~source:source ~adaptor:adaptor
                        (name, schema, reltype, TInt)
      | None    -> ()
   end;
   (old_rels, query @ old_queries)
;;


%}

%token <Types.type_t> TYPE
%token <string> ID STRING
%token <int> INT
%token <float> FLOAT
%token <bool> BOOL
%token EQ NEQ LT LTE GT GTE
%token VARCHAR
%token EOF EOSTMT
%token CREATE TABLE STREAM
%token LPAREN RPAREN LBRACKET RBRACKET
%token COMMA COLON PLUS TIMES MINUS DIVIDE POUND
%token AS FROM
%token DECLARE QUERY
%token FILE SOCKET FIXEDWIDTH LINE DELIMITED
%token AGGSUM
%token LIFT SETVALUE 

// start
%start statementList calculusExpr
%type < Schema.t * (string * Calculus.BasicCalculus.expr_t) list > statementList
%type < Calculus.BasicCalculus.expr_t > calculusExpr

%%

statementList:
| statement EOF                    { incorporate_stmt $1 }
| statement EOSTMT EOF             { incorporate_stmt $1 }
| statement EOSTMT statementList   { incorporate_stmt ~old:$3 $1 }

statement:
| relStatement                { (Some($1), []) }
| queryStatement              { (None,     [$1]) }

relStatement:
| CREATE tableOrStream ID LPAREN emptyFieldList RPAREN
   { (String.uppercase $3, $5, $2, Schema.NoSource, ("",[])) }
| CREATE tableOrStream ID LPAREN emptyFieldList RPAREN FROM sourceStmt
   { (String.uppercase $3, $5, $2, fst $8, snd $8) }

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
|   FILE STRING bytestreamParams 
  { (Schema.FileSource($2, fst $3), snd $3) }
|   SOCKET STRING INT bytestreamParams
  { (Schema.SocketSource(Unix.inet_addr_of_string $2, $3, fst $4), snd $4) }
|   SOCKET INT bytestreamParams
  { (Schema.SocketSource(Unix.inet_addr_any, $2, fst $3), snd $3) }

bytestreamParams: 
|   framingStmt adaptorStmt { ($1, $2) }

framingStmt:
|   FIXEDWIDTH INT                  { Schema.FixedSize($2) }
|   LINE DELIMITED                  { Schema.Delimited("\n") }
|   STRING DELIMITED                { Schema.Delimited($1) }

adaptorStmt:
|   ID LPAREN RPAREN               { (String.lowercase $1, []) }
|   ID LPAREN adaptorParams RPAREN { (String.lowercase $1, $3) }
|   ID                             { (String.lowercase $1, []) }

adaptorParams:
|   ID SETVALUE STRING                     { [(String.lowercase $1,$3)] }
|   ID SETVALUE STRING COMMA adaptorParams { (String.lowercase $1,$3)::$5 }

dbtType:
| TYPE                      { $1 }
| VARCHAR LPAREN INT RPAREN { TString($3) }

queryStatement:
| DECLARE QUERY STRING AS calculusExpr { ($3, $5) }

emptyValueExprList:
|                                { [] }
| valueExprList                  { $1 }

valueExprList: 
| valueExpr                      { [$1] }
| valueExpr COMMA valueExprList  { $1::$3}

valueExpr:
| LPAREN valueExpr RPAREN     { $2 }
| valueExpr TIMES valueExpr   { ValueRing.mk_prod [$1; $3] }
| valueExpr PLUS  valueExpr   { ValueRing.mk_sum  [$1; $3] }
| valueExpr MINUS valueExpr   { ValueRing.mk_sum  [$1; ValueRing.mk_neg $3] }
| MINUS valueExpr             { match $2 with
                                | ValueRing.Val(AConst(c)) ->
                                    ValueRing.mk_val 
                                       (AConst(Arithmetic.prod c (CInt(-1)))) 
                                | _ -> ValueRing.mk_neg $2 }
| constant                    { ValueRing.mk_val (AConst $1) }
| variable                    { ValueRing.mk_val (AVar $1) }
| functionDefn                { ValueRing.mk_val $1 }

calculusExpr:
| LPAREN calculusExpr RPAREN      { $2 }
| calculusExpr TIMES calculusExpr { CalcRing.mk_prod [$1; $3] }
| calculusExpr PLUS  calculusExpr { CalcRing.mk_sum [$1; $3] }
| calculusExpr MINUS calculusExpr { CalcRing.mk_sum [$1; CalcRing.mk_neg $3] }
| MINUS calculusExpr              { CalcRing.mk_neg $2 }
| POUND valueExpr POUND           { CalcRing.mk_val (Value $2) }
| AGGSUM LPAREN LBRACKET emptyVariableList RBRACKET COMMA calculusExpr RPAREN
                                  { CalcRing.mk_val (AggSum($4, $7)) }
| relationDefn                    { let (reln, relv, relt) = $1 in
                                    CalcRing.mk_val (Rel(reln, relv, relt)) }
| externalDefn                    { let (en, iv, ov, et, em) = $1 in
                                    CalcRing.mk_val (External(en,iv,ov,et,em)) }
| LPAREN valueExpr comparison valueExpr RPAREN
                                  { CalcRing.mk_val (Cmp($3,$2,$4)) }
| LPAREN variable LIFT calculusExpr RPAREN
                                  { CalcRing.mk_val (Lift($2, $4)) }

comparison:
| EQ  { Types.Eq  } | NEQ { Types.Neq } | LT  { Types.Lt  } 
| LTE { Types.Lte } | GT  { Types.Gt  } | GTE { Types.Gte }

relationDefn:
| ID LPAREN emptyVariableList RPAREN                { ($1, $3, TInt) }
| ID LPAREN emptyVariableList COMMA dbtType RPAREN  { ($1, $3, $5) }

externalDefn:
| ID LBRACKET emptyVariableList RBRACKET LBRACKET emptyVariableList RBRACKET
                                 { ($1, $3, $6, TFloat, NullMeta.default) }
| ID LPAREN dbtType RPAREN LBRACKET emptyVariableList RBRACKET LBRACKET 
  emptyVariableList RBRACKET     { ($1, $6, $9, $3, NullMeta.default) }

constant:
| BOOL      { CBool($1) }
| INT       { CInt($1) }
| FLOAT     { CFloat($1) }
| STRING    { CString($1) }

emptyVariableList:
//|                             { [] }
| variableList                { $1 }

variableList: 
| variable                    { [$1] }
| variable COMMA variableList { $1::$3 }

variable: 
| ID COLON dbtType { ($1, $3) }
| ID               { ($1, TFloat) }

functionDefn:
| LBRACKET ID COLON dbtType RBRACKET LPAREN emptyValueExprList RPAREN
         { AFn($2, $7, $4) }
| LBRACKET DIVIDE COLON dbtType RBRACKET LPAREN emptyValueExprList RPAREN
         { AFn("/", $7, $4) }

