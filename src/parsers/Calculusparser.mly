%{
open Types
open Arithmetic
open Calculus
open Plan

let incorporate_stmt ?(old = (Schema.empty_db (), [])) (rel, query) =
   let (old_rels,old_queries) = old in
   begin match rel with 
      | Some(name, schema, reltype, source, adaptor) ->
         Schema.add_rel old_rels ~source:source ~adaptor:adaptor
                        (name, schema, reltype)
      | None    -> ()
   end;
   (old_rels, query @ old_queries)
;;

type map_metadata = 
   MapIsQuery | MapIsPartial | MapInitializedAtStart

%}

%token <Types.type_t> TYPE
%token <string> ID STRING
%token <int> INT
%token <float> FLOAT
%token <bool> BOOL
%token EQ NEQ LT LTE GT GTE
%token VARCHAR CHAR DATE
%token EOF EOSTMT
%token CREATE TABLE STREAM
%token LPAREN RPAREN LBRACKET RBRACKET LBRACE RBRACE
%token COMMA COLON PLUS TIMES MINUS DIVIDE POUND
%token AS FROM ON DO SYSTEM READY
%token DECLARE QUERY MAP PARTIAL INITIALIZED
%token FILE SOCKET FIXEDWIDTH LINE DELIMITED
%token AGGSUM
%token LIFT SETVALUE INCREMENT

// start
%start statementList calculusExpr mapProgram mapTriggerStmt
%type < Schema.t * (string * Calculus.expr_t) list > statementList
%type < M3.prog_t > mapProgram
%type < Plan.stmt_t > mapTriggerStmt
%type < Calculus.expr_t > calculusExpr
%type < Calculus.expr_t > ivcCalculusExpr

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
| CHAR LPAREN INT RPAREN    { TString }
| VARCHAR LPAREN INT RPAREN { TString }
| VARCHAR                   { TString }
| CHAR                      { TString }
| DATE                      { TDate    }

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
| valueLeaf                   { $1 }

valueLeaf:
| constant                    { ValueRing.mk_val (AConst $1) }
| variable                    { ValueRing.mk_val (AVar $1) }
| functionDefn                { ValueRing.mk_val $1 }

calculusExpr:
| ivcCalculusExpr             { (* Calculus.strip_calc_metadata *) $1 }

ivcCalculusExpr:
| LPAREN ivcCalculusExpr RPAREN   { $2 }
| ivcCalculusExpr TIMES ivcCalculusExpr 
                                  { CalcRing.mk_prod [$1; $3] }
| ivcCalculusExpr PLUS  ivcCalculusExpr 
                                  { CalcRing.mk_sum [$1; $3] }
| ivcCalculusExpr MINUS ivcCalculusExpr 
                                  { CalcRing.mk_sum [$1; CalcRing.mk_neg $3] }
| MINUS ivcCalculusExpr           { CalcRing.mk_neg $2 }
| LBRACE valueExpr RBRACE         { CalcRing.mk_val (Value $2) }
| valueLeaf                       { CalcRing.mk_val (Value $1) }
| AGGSUM LPAREN LBRACKET emptyVariableList RBRACKET COMMA ivcCalculusExpr RPAREN
                                  { CalcRing.mk_val (AggSum($4, $7)) }
| relationDefn                    { let (reln, relv) = $1 in
                                    CalcRing.mk_val (Rel(reln, relv)) }
| externalDefn                    { let (en, iv, ov, et, em) = $1 in
                                    CalcRing.mk_val (External(en,iv,ov,et,em)) }
| LBRACE valueExpr comparison valueExpr RBRACE
                                  { CalcRing.mk_val (Cmp($3,$2,$4)) }
| LPAREN variable LIFT ivcCalculusExpr RPAREN
  { let t_var, _ = $2 in 
   let t_type = Calculus.type_of_expr $4 in
   let target = (t_var, t_type) in
    CalcRing.mk_val (Lift(target, $4)) }

comparison:
| EQ  { Types.Eq  } | NEQ { Types.Neq } | LT  { Types.Lt  } 
| LTE { Types.Lte } | GT  { Types.Gt  } | GTE { Types.Gte }

relationDefn:
| ID LPAREN RPAREN                                  { ($1, []) }
| ID LPAREN variableList RPAREN                     { ($1, $3) }

externalDefn:
| externalDefnWithoutMeta { $1 }
| externalDefnWithoutMeta optionalColon LPAREN ivcCalculusExpr RPAREN {
      let (name, ivars, ovars, extType, _) = $1 in
         (name, ivars, ovars, extType, (Some($4)))
   }

optionalColon:
| COLON { () }
|       { () }

externalDefnWithoutMeta:
| ID LBRACKET emptyVariableList RBRACKET LBRACKET emptyVariableList RBRACKET
                                 { ($1, $3, $6, TFloat, None) }
| ID LPAREN dbtType RPAREN LBRACKET emptyVariableList RBRACKET LBRACKET 
  emptyVariableList RBRACKET     { ($1, $6, $9, $3, None) }

constant:
| BOOL      { CBool($1) }
| INT       { CInt($1) }
| FLOAT     { CFloat($1) }
| STRING    { CString($1) }
| DATE LPAREN STRING RPAREN     { Types.parse_date $3 }

emptyVariableList:
|                             { [] }
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

//////////////////////////////////////////////////////////////////////////
////////////////// M3 Specific Productions ///////////////////////////////
//////////////////////////////////////////////////////////////////////////

mapProgram:
| mapProgramStatementList EOF {
      let (rels, views, triggers, queries) = $1 in
         let db = Schema.empty_db () in (
            List.iter (fun (name, schema, reltype, source, adaptor) -> 
               Schema.add_rel db ~source:source ~adaptor:adaptor 
                                 (name, schema, reltype)
            ) rels;
            let prog = M3.init db in
               List.iter (M3.add_view prog) views;
               List.iter (fun (n,d) -> M3.add_query prog n d) queries;
               List.iter (fun (e,sl) -> 
                  List.iter (fun s -> M3.add_stmt prog e s) sl
               ) triggers;
               prog
         )
   }

mapProgramStatementList:
| mapProgramStatement mapProgramStatementList
      {  let (orels, oviews, otriggers, oqueries) = $2 in
         let (nrels, nviews, ntriggers, nqueries) = $1 in
            (  nrels @ orels,          nviews @ oviews, 
               ntriggers @ otriggers,  nqueries @ oqueries) 
      }
| mapProgramStatement 
      {  $1 }

mapProgramStatement:
| relStatement EOSTMT { ([$1], [], [], []) }
| mapView      EOSTMT { ([], [fst $1], [], snd $1) }
| mapTrigger          { ([], [], [$1], []) }
| mapQuery     EOSTMT { ([], [], [], [$1]) }

mapView:
| DECLARE mapViewMetadata MAP externalDefnWithoutMeta SETVALUE ivcCalculusExpr { 
      let (name, ivars, ovars, dtype, _) = $4 in
      (  {
            Plan.ds_name = Plan.mk_ds_name name (ivars,ovars) dtype;
            Plan.ds_definition = $6
         },
         if List.mem MapIsQuery $2
         then [name, CalcRing.mk_val (External($4))] else []
      )
   }

mapViewMetadata:
|                             { [] }
| QUERY mapViewMetadata       { MapIsQuery::$2 }
| PARTIAL mapViewMetadata     { MapIsPartial::$2 }
| INITIALIZED mapViewMetadata { MapInitializedAtStart::$2 }

mapQuery:
| DECLARE QUERY ID SETVALUE ivcCalculusExpr { ($3, $5) }

mapTrigger:
| ON mapEvent LBRACE mapTriggerStmtList RBRACE { ($2, $4) }

mapEvent:
| PLUS  schemaRelationDefn { Schema.InsertEvent($2) }
| MINUS schemaRelationDefn { Schema.DeleteEvent($2) }
| SYSTEM READY             { Schema.SystemInitializedEvent }

schemaRelationDefn:
| relationDefn {
   let (reln, relv) = $1 in (reln, relv, Schema.StreamRel)
}

mapTriggerStmtList:
| mapTriggerStmt EOSTMT mapTriggerStmtList { $1 :: $3 }
|                                          { [] }

mapTriggerStmt:
| externalDefn mapTriggerType ivcCalculusExpr { 
      {  target_map  = CalcRing.mk_val (External($1));
         update_type = $2;
         update_expr = $3
      }
   }

mapTriggerType: 
| SETVALUE  { ReplaceStmt }
| INCREMENT { UpdateStmt  }
