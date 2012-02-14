%{
open Types
open Arithmetic
open Calculus
open Statement

let incorporate_stmt ?(old = (Schema.empty_db (), [])) (rel, query) =
   let (old_rels,old_queries) = old in
   begin match rel with 
      | Some(name, schema, reltype, source, adaptor) ->
         Schema.add_rel old_rels ~source:source ~adaptor:adaptor
                        (name, schema, reltype, TInt)
      | None    -> ()
   end;
   (old_rels, query @ old_queries)
;;

let strip_calc_metadata:(Calculus.expr_t -> Calculus.expr_t) =
   rewrite_leaves (fun _ lf -> match lf with
      | External(en, eiv, eov, et, em) ->
         CalcRing.mk_val (External(en, eiv, eov, et, None))
      | _ -> CalcRing.mk_val lf
   )

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
%token AS FROM ON
%token DECLARE QUERY MAP
%token FILE SOCKET FIXEDWIDTH LINE DELIMITED
%token AGGSUM
%token LIFT SETVALUE INCREMENT

// start
%start statementList calculusExpr mapProgram
%type < Schema.t * (string * Calculus.expr_t) list > statementList
%type < M3.prog_t > mapProgram
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
| valueLeaf                   { $1 }

valueLeaf:
| constant                    { ValueRing.mk_val (AConst $1) }
| variable                    { ValueRing.mk_val (AVar $1) }
| functionDefn                { ValueRing.mk_val $1 }

calculusExpr:
| ivcCalculusExpr             { strip_calc_metadata $1 }

ivcCalculusExpr:
| LPAREN ivcCalculusExpr RPAREN   { $2 }
| ivcCalculusExpr TIMES ivcCalculusExpr 
                                  { CalcRing.mk_prod [$1; $3] }
| ivcCalculusExpr PLUS  ivcCalculusExpr 
                                  { CalcRing.mk_sum [$1; $3] }
| ivcCalculusExpr MINUS ivcCalculusExpr 
                                  { CalcRing.mk_sum [$1; CalcRing.mk_neg $3] }
| MINUS ivcCalculusExpr           { CalcRing.mk_neg $2 }
| POUND valueExpr POUND           { CalcRing.mk_val (Value $2) }
| valueLeaf                       { CalcRing.mk_val (Value $1) }
| AGGSUM LPAREN LBRACKET emptyVariableList RBRACKET COMMA ivcCalculusExpr RPAREN
                                  { CalcRing.mk_val (AggSum($4, $7)) }
| relationDefn                    { let (reln, relv, relt) = $1 in
                                    CalcRing.mk_val (Rel(reln, relv, relt)) }
| externalDefn                    { let (en, iv, ov, et, em) = $1 in
                                    CalcRing.mk_val (External(en,iv,ov,et,em)) }
| LPAREN valueExpr comparison valueExpr RPAREN
                                  { CalcRing.mk_val (Cmp($3,$2,$4)) }
| LPAREN variable LIFT ivcCalculusExpr RPAREN
                                  { CalcRing.mk_val (Lift($2, $4)) }

comparison:
| EQ  { Types.Eq  } | NEQ { Types.Neq } | LT  { Types.Lt  } 
| LTE { Types.Lte } | GT  { Types.Gt  } | GTE { Types.Gte }

relationDefn:
| ID LPAREN RPAREN                                  { ($1, [], TInt) }
| ID LPAREN dbtType RPAREN                          { ($1, [], $3) }
| ID LPAREN variableList RPAREN                     { ($1, $3, TInt) }
| ID LPAREN variableList COMMA dbtType RPAREN       { ($1, $3, $5) }

externalDefn:
| externalDefnWithoutMeta { $1 }
| externalDefnWithoutMeta LPAREN ivcCalculusExpr RPAREN {
      let (name, ivars, ovars, extType, _) = $1 in
         (name, ivars, ovars, extType, (Some($3)))
   }

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
                                 (name, schema, reltype, TInt)
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
| mapProgramStatement EOSTMT mapProgramStatementList
      {  let (orels, oviews, otriggers, oqueries) = $3 in
         let (nrels, nviews, ntriggers, nqueries) = $1 in
            (  nrels @ orels,          nviews @ oviews, 
               ntriggers @ otriggers,  nqueries @ oqueries) 
      }
| mapProgramStatement EOSTMT
      {  $1 }

mapProgramStatement:
| relStatement { ([$1], [], [], []) }
| mapView      { ([], [fst $1], [], snd $1) }
| mapTrigger   { ([], [], [$1], []) }
| mapQuery     { ([], [], [], [$1]) }

mapView:
| DECLARE mapViewIsQuery MAP externalDefnWithoutMeta SETVALUE ivcCalculusExpr { 
      let (name, ivars, ovars, dtype, init) = $4 in
      (  {  M3.name = name;
            M3.schema = (ivars, ovars);
            M3.definition = $6;
         },
         if $2 then [name, CalcRing.mk_val (External($4))] else []
      )
   }

mapViewIsQuery:
|       { false }
| QUERY { true }

mapQuery:
| DECLARE QUERY ID SETVALUE ivcCalculusExpr { ($3, $5) }

mapTrigger:
| ON mapEvent mapTriggerStmtList { ($2, $3) }

mapEvent:
| mapEventType relationDefn {
      let (reln, relv, relt) = $2 in ($1, (reln, relv, Schema.StreamRel, relt))
   }

mapEventType:
| PLUS   { Schema.InsertEvent }
| MINUS  { Schema.DeleteEvent }

mapTriggerStmtList:
| mapTriggerStmt EOSTMT mapTriggerStmtList { $1 :: $3 }
| mapTriggerStmt EOSTMT                    { [$1] }

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