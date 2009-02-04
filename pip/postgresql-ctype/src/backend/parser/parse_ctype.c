#include <stdio.h>
#include <stdlib.h>
#include "postgres.h"
#include "miscadmin.h"
#include "access/hash.h"
#include "access/nbtree.h"
#include "catalog/pg_type.h"
#include "catalog/pg_proc.h"
#include "commands/dbcommands.h"
#include "libpq/libpq.h"
#include "libpq/pqformat.h"
#include "libpq/pqsignal.h"
#include "nodes/makefuncs.h"
#include "parser/parse_coerce.h"
#include "parser/parse_clause.h"
#include "parser/parse_expr.h"
#include "parser/parse_node.h"
#include "parser/parse_relation.h"
#include "parser/parse_target.h"
#include "parser/parse_func.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/datum.h"

static Node *transformCTypedAndExpr(ParseState *pstate, A_Expr *andExpr, List **conditions);
static Node *extractCTypeOrCoerceBooleanExpr(ParseState *pstate, Node *expr, List **conditions, const char *constructName);
static bool functionIsAggregate(ParseState *pstate, FuncCall *fc, bool *cType_agg);
static RangeTblEntry *rteForColumnRefStar(ParseState *pstate, ColumnRef *cref);
static bool ptrInList(void *ptr, List *l);
static List *expandRelCTypeAttrs(ParseState *pstate, RangeTblEntry *rte, int rtindex, int sublevels_up);

#define PING() elog(NOTICE, "%s:%d  ->  %s()", __FILE__, __LINE__, __FUNCTION__)

// Constraint types add two features
// 1) A constraint type may be listed in a where clause; In this case, 
// it will be moved to the output portion of the query in question.
// 2) All constraint type columns of a select/join's input tables are 
// transparently fed through, regardless of whether it's asked for.

//We define special "CTYPE" tuple types by using the typtup field of the pg_type catalog.
//This type is effectively treated as TYPTYPE_BASE, except for our purposes.

//Pulling this masquerade off involves a set of changes:
//  include/catalog/pg_type.h: 
//      ^--- Added TYPTYPE_CTYPE enumerator
//  utils/cache/lsyscache.c: getTypeIOParam
//      ^--- Unchanged; Though a potential problem site on future changes
//  utils/cache/lsyscache.c: getTypeIOParam
//      ^--- The main masquerade point; nearly everything uses this call.  This has been 
//           changed to return TYPTYPE_BASE in place of TYPTYPE_CTYPE
//  utils/cache/typecache.c: 
//      ^--- Unchanged; Though a potential problem site on future changes
//  commands/typecmds.c: 
//      ^--- Unchanged; Though a potential problem site on future changes, and will need 
//           changes if we want domains over ctypes.
//  ../bin/pg_dump/pg_dump.c: getTypes();
//      ^--- Added a check for TYPTYPE_CTYPE; This value is replaced by TYPTYPE_BASE
//  ../test/regress/sql/type_sanity.sql:
//      ^--- These need to be updated, or regression tests will fail if not run on a clean DB

//At the same time, we need to have some way of specifying constraint types.  This involves
//messing with the parser in a handful of places.
//  parser/gram.y: %token <keyword>
//      ^--- Added CTYPE keyword to general keyword list
//  parser/gram.y: unreserved_keyword:
//      ^--- Added CTYPE keyword to unreserved keyword list
//  parser/keyword.c: static const ScanKeyword ScanKeywords[] 
//      ^--- Added CTYPE keyword to keyword list as unreserved
//  tcop/typecmds.c: DefineType()
//      ^--- Check for "constrainttype" clause and modify the created type accordingly

//To actually implement the CTYPE functionality, we need to modify WHERE clauses.  These 
//appear in the following places
//parser/analyze.c : transformSelectStmt() 
//      ^--- replaced calls to transformWhereClause with transformCTypedBoolExpr
//           This applies to both WHERE and the HAVING; Since the output of both 
//           is a column, both are treated the same way.
//parser/analyze.c : transformDeleteStmt()
//      ^--- Need a way to add negation here...  No changes yet.
//parser/analyze.c : transformUpdateStmt()
//      ^--- This also become strange... because it needs to get rewritten into a
//           SELECT INTO via repair key.  It is also incapable of rewriting a schema, so
//           the way the code is written now, this can not be implemented;  Merging all
//           CTypes into an array would theoretically fix this.
//parser/parse_clause.c : transformJoinOnClause()
//      ^--- These are going to be nested inside of a SELECT anyway, so leave this to the 
//           components that actually matter for dropping values.
//parser/parse_utilcmd.c : transformIndexStmt() 
//      ^--- Does NOT need to be changed (partial indeces shouldn't be conditined on vars)

//One other note, the file parser/parse_target.c: transformTargetList() gives us
//some idea of the way target entries are handled; In effect, we're going to be
//adding a component that scans through the where clause, identifies components that
//are weird, and moves them over to the target list.

//The final component of this whole debacle is making sure that CType columns get passed
//through all selection predicates cleanly.  Joins shouldn't be an issue here since they
//naturally pass everything through.  Since we don't uniquely identify constraint columns
//we need to hack the Fully Natural Join code to ignore CType columns.
//parser/parse_clause.c:transformFromClauseItem()
//      ^--- (IsA(JoinExpr) && j->isNatural) case hacked to ignore "__constraint_col__"

//We also need to adjust all projections; SELECT x,y,z,... needs to automatically include
//all CType columns.  We do this in transformTargetListKeepingCTypes(); which is injected
//into postgres in the following locations:
//parser/analyze.c : transformSelectStmt()
//      ^--- transformTargetList() replaced by transformTargetListKeepingCTypes()

//Note that this behavior must change if the SELECT statement ends up doing a GROUP BY or 
//computing an aggregate.  Specifically, normal aggregate operations do not work properly
//on tables with CType columns, as the outputs end up being dependent on the probability 
//of the CType columns being satisfied.  Consequently we introduce the notion of a CType
//aggregate function.  We don't want to disallow the use of non-CType aggregate functions
//on CTables, but we do want to alert the user that the aggregate will not be producing 
//values accurately.  In the future we might consider enabling support for converting 
//existing aggregate functions into CType aggregate functions via random sampling (run 
//the aggregate in parallel in 1000 instances), but for now we just spit out a warning.
//We store the fact that a function is a CType aggregate in the 'volatile' field in 
//pg_proc; It doesn't look like this field is relevant for aggregates, so we'll just 
//create a special value that masquerades as the default aggregate value IMMUTABLE.  This 
//involves changes in the following places:
//include/catalog/pg_proc.h
//      ^--- Defined PROVOLATILE_CTYPEAGG
//utils/cache/lsyscache.c: func_volatile()
//      ^--- substitute in PROVOLATILE_IMMUTABLE where PROVOLATILE_CTYPEAGG is seen
//optimizer/util/clauses.c: evaluate_function()
//      ^--- treat PROVOLATILE_CTYPEAGG equivalently to PROVOLATILE_IMMUTABLE
//optimizer/util/clauses.c: inline_function()
//      ^--- treat PROVOLATILE_CTYPEAGG equivalently to PROVOLATILE_IMMUTABLE
//../bin/pg_dump/pg_dump.c: inline_function()
//      ^--- treat PROVOLATILE_CTYPEAGG equivalently to PROVOLATILE_IMMUTABLE



bool varIsCType(Oid typid)
{
  HeapTuple  tp;
  

  tp = SearchSysCache(TYPEOID,
            ObjectIdGetDatum(typid),
            0, 0, 0);
  if (HeapTupleIsValid(tp))
  {
    Form_pg_type typtup = (Form_pg_type) GETSTRUCT(tp);
    char    result;

    result = typtup->typtype;
    ReleaseSysCache(tp);
    return result == TYPTYPE_CTYPE;
  }
  else
    return false;
}

//based on transformExpr
//This is a variant of transformExpr in parser/parse_expr.c for use when transforming WHERE clauses
//  The provided whereClause is transformed by the normal post-parse cleanup rewriter.  However, if the 
//  value evaluates to type defined as a CType, it is removed from whereClause and added to the 
//  conditions list.
//  Raw AND expressions are ok to break up, since the condition clauses are supposed to be joined by ANDs
//  Pass everything else to the normal rewriter and see if it comes back with something that evaluates
//  to a CType
Node *transformCTypedBoolExpr(ParseState *pstate, Node *whereClause, List **conditions, const char *constructName)
{
  Node *result = NULL;
  
  if (whereClause == NULL)
    return NULL;
  
  switch (nodeTag(whereClause)) {
    case T_A_Expr:
      {
        A_Expr     *a = (A_Expr *) whereClause;
        
        switch (a->kind) {
          case AEXPR_AND:
            result = transformCTypedAndExpr(pstate, a, conditions);
            break;
            
          default:
            result = transformExpr(pstate, whereClause);
            break;
        }
      }
      break;
    
    default:
      //if we don't know how to handle it... just pass it off to the regular transform
      result = transformExpr(pstate, whereClause);
  }
  
  result = extractCTypeOrCoerceBooleanExpr(pstate, result, conditions, constructName);
    
  return result;
}

Node *transformCTypedAndExpr(ParseState *pstate, A_Expr *andExpr, List **conditions)
{
  //AND commutes, so we can recurse cleanly.
  Node     *lexpr = transformCTypedBoolExpr(pstate, andExpr->lexpr, conditions, "AND-LEFT (Cond)");
  Node     *rexpr = transformCTypedBoolExpr(pstate, andExpr->rexpr, conditions, "AND-RIGHT (Cond)");

  //transformCTypedExpr nullifies all C-Typed expressions.  
  //If it did so for either half, we can just return the other half
  //We still need to coerce the value to a boolean (which we can
  //do safely, since it's definitely not a C-Type).  
  if(lexpr == NULL){
    if(rexpr == NULL){
      return NULL;
    }
    return coerce_to_boolean(pstate, rexpr, "AND (Cond)");
  }
  if(rexpr == NULL){
    return coerce_to_boolean(pstate, lexpr, "AND (Cond)");
  }

  //if we got a boolean value from both halves, we need to form the expression as in transformAExprAnd()
  lexpr = coerce_to_boolean(pstate, lexpr, "AND (Cond)");
  rexpr = coerce_to_boolean(pstate, rexpr, "AND (Cond)");
  return (Node *) makeBoolExpr(AND_EXPR,
                 list_make2(lexpr, rexpr));
}

Node *extractCTypeOrCoerceBooleanExpr(ParseState *pstate, Node *expr, List **conditions, const char *constructName)
{
  Node *targetEntry;
  
  if((expr == NULL) || (exprType(expr) == BOOLOID)) { return expr; }
  
  if(varIsCType(exprType(expr))){
    targetEntry = 
      (Node *)makeTargetEntry((Expr *) expr,
          (AttrNumber) pstate->p_next_resno++,
          "__constraint_col__",
          false);
    
    lappend((*conditions), targetEntry);
    return NULL;
  }
  
  return coerce_to_boolean(pstate, expr, constructName);
}

static bool functionIsAggregate(ParseState *pstate, FuncCall *fc, bool *cType_agg)
{
	int            nargs = 0;
	Oid	           actual_arg_types[FUNC_MAX_ARGS];
  FuncDetailCode fdresult;
  List          *targs = list_copy(fc->args);
	ListCell      *l;
	ListCell      *nextl;
	Oid			       rettype;
	Oid			       funcid;
	bool		       retset;
	Oid		        *declared_arg_types;
	
	if(fc->agg_star || fc->agg_distinct) return true;
	
	for (l = list_head(targs); l != NULL; l = nextl)
	{
		Node	   *arg = transformExpr(pstate, (Node *)lfirst(l)); //eeew... but there really isn't any other simple way
		Oid			argtype = exprType(arg);

		nextl = lnext(l);

		if (argtype == VOIDOID && IsA(arg, Param))
		{
			continue;
		}

		actual_arg_types[nargs++] = argtype;
	}
	
	fdresult = func_get_detail(
    fc->funcname, 
    targs, 
    nargs, 
    actual_arg_types,
    &funcid, 
    &rettype, 
    &retset,
    &declared_arg_types
  );
  
  if(cType_agg != NULL){
    if(fdresult == FUNCDETAIL_AGGREGATE){
      HeapTuple	ftup;
      Form_pg_proc pform;
  
      ftup = SearchSysCache(PROCOID,
                  ObjectIdGetDatum(funcid),
                  0, 0, 0);
      if (!HeapTupleIsValid(ftup))	/* should not happen */
        elog(ERROR, "cache lookup failed for function %u",
           funcid);
      pform = (Form_pg_proc) GETSTRUCT(ftup);
      *cType_agg = (pform->provolatile == PROVOLATILE_CTYPEAGG);
      ReleaseSysCache(ftup);
    } else {
      cType_agg = false;
    }
  }
							   
	return fdresult == FUNCDETAIL_AGGREGATE;
}

//Hacked frontend to backend/parser/parse_target.c: TransformTargetList().
//Ensures that all CTyped variables in the namespace are implicitly added to
//the output list (if they haven't been already included by a * operator).
//Note that this does NOT prevent duplication of CType columns that have been
//explicitly named in the select list.
//
//As a general rule, the only way that a CType column will be added by the 
//user is if the column is explicitly named, or if it's coming in through 
//a direct * operator.  In the latter case, we can filter them out relatively
//cleanly.  In the former case, we should assume that the user is adding a
//a column in the non-hacked manner; Even if they aren't, this doesn't 
//violate correctness, just efficiency.  Indirection *s are array subscripts 
//and record identifiers, which for our purposes can't include CTypes.
List *transformTargetListKeepingCTypes(ParseState *pstate, List *targetlist)
{
  List     *transformedTargetlist = NULL;
  ListCell *target, *input;
  List     *excludedColumns = NULL;
  bool      globalStar = false, aggregate = false, cType_agg, have_non_cType_agg = false;

  foreach(target, targetlist)
  {
    ResTarget  *res = (ResTarget *) lfirst(target);
    
    //We need to find out if the resource is a table.* if this is the case, 
    //we need to avoid duplicating columns that will be added to the target 
    //list by transformTargetList().
    if(IsA(res->val, ColumnRef)){
      ColumnRef *cref = (ColumnRef *) res->val;
      if (strcmp(strVal(llast(cref->fields)), "*") == 0){
        if(list_length(cref->fields) == 1){ 
          globalStar = true; 
        }
        else { 
          RangeTblEntry *rte = rteForColumnRefStar(pstate, cref);
          if(rte) { excludedColumns = lappend(excludedColumns, rte); }
        }
      }
    } 
    //Alternatively, it might be the case that we're being asked to compute
    //an aggregate.  If this is the case, we shouldn't be appending targets
    //to the target list all willy nilly.  
    else if(IsA(res->val, FuncCall)){
      if(functionIsAggregate(pstate, (FuncCall *) res->val, &cType_agg)) { 
        aggregate = true;
        have_non_cType_agg |= !cType_agg;
      }
    }
  }
  
  //Ok, we've got the list, now transform the old one.
  transformedTargetlist = transformTargetList(pstate, targetlist);
  
  //If the user asked for *, we can just skip this step; everything's already 
  //been added.
  //Alternatively, if the user asked for an aggregate, what things really depend
  //on is whether they asked for one of our aggregates that operates on CTYPEs,
  //or if they just asked for a generic aggregate; I'm not sure what to do about
  //the latter case, so for now, we're just going to drop the CTYPEs at this point
  if(!globalStar && (!aggregate || have_non_cType_agg)) { 
    if(pstate->p_varnamespace){ 
      //And tack on all known CTypes
      foreach(input, pstate->p_varnamespace) 
      {
        RangeTblEntry *rte = (RangeTblEntry *) lfirst(input);
  
        //Exclude columns that have been already added.
        if(!ptrInList(rte, excludedColumns)){ 
          int      rtindex = RTERangeTablePosn(pstate, rte, NULL);
          List   *cvarCols = expandRelCTypeAttrs(pstate, rte, rtindex, 0);
  
          if(cvarCols){
            //Warn the user if they're trying to compute a classical aggregate of a cType
            //dataset
            if(aggregate && have_non_cType_agg){
              elog(WARNING, "Query includes non-conditional aggregate(s) evaluated on a c-table.  C-columns will be ignored by these function(s) and projected out.  Please ensure that this is the behavior you expect.");
              goto transformTargetListKeepingCTypes_finish;
            }
            
            //Require read access, but only on columns that actually have
            //conditions sucked out.
            rte->requiredPerms |= ACL_SELECT;
            transformedTargetlist = list_concat(transformedTargetlist, cvarCols);
          }
        }
      }
    }
  }
  
  
transformTargetListKeepingCTypes_finish:

  return transformedTargetlist;
}

//Based on backend/parser/parse_target.c:ExpandColumnRefStar().  This function is
//virtually identical, save that it stops before actually expanding the column refs.
//This allows us to generate a list of RTEs that we should ignore when propagating
//CType columns;  
static RangeTblEntry *rteForColumnRefStar(ParseState *pstate, ColumnRef *cref)
{
  List     *fields = cref->fields;
  int        numnames = list_length(fields);
  char     *schemaname;
  char     *relname;
  RangeTblEntry *rte;

  /*
   * Target item is relation.*, expand that table; The case of '*' should be 
   * handled before we're called.
   * 
   * (e.g., SELECT emp.*, dname FROM emp, dept)
   */

  switch (numnames)
  {
    case 2:
      schemaname = NULL;
      relname = strVal(linitial(fields));
      break;
    case 3:
      schemaname = strVal(linitial(fields));
      relname = strVal(lsecond(fields));
      break;
    case 4:
      {
        char     *name1 = strVal(linitial(fields));

        /*
         * We check the catalog name and then ignore it.
         */
        if (strcmp(name1, get_database_name(MyDatabaseId)) != 0)
          return NULL; //don't throw an error; let the normal PG mechanisms handle it later.
        schemaname = strVal(lsecond(fields));
        relname = strVal(lthird(fields));
        break;
      }
    default:
      return NULL; //don't throw an error; let the normal PG mechanisms handle it later.
  }

  rte = refnameRangeTblEntry(pstate, schemaname, relname, NULL);
  
  //no need to check for NULL, if it's an implicitly generated link, we don't care.
  return rte;
}

static bool ptrInList(void *ptr, List *l)
{
  ListCell *le;
  foreach(le, l)
  {
    if(lfirst(le) == ptr) return true;
  }
  return false;
}

//Based on backend/parser/parse_relation.c: expandRelAttrs().  The only difference is that
//this function only expands CType columns.
static List *expandRelCTypeAttrs(ParseState *pstate, RangeTblEntry *rte, int rtindex, int sublevels_up)
{
  List      *names,
            *vars;
  ListCell  *name,
            *var;
  List      *te_list = NIL;

  expandRTE(rte, rtindex, sublevels_up, false,
        &names, &vars);

  forboth(name, names, var, vars)
  {
    char     *label = strVal(lfirst(name));
    Node     *varnode = (Node *) lfirst(var);

    if(varIsCType(exprType(varnode))){
      TargetEntry *te = makeTargetEntry((Expr *) varnode,
                 (AttrNumber) pstate->p_next_resno++,
                 label,
                 false);
      te_list = lappend(te_list, te);
    }
  }

  Assert(name == NULL && var == NULL);    /* lists not the same length? */

  return te_list;
}
