package org.dbtoaster.io;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.dbtoaster.model.DatasetManager;
import org.dbtoaster.model.DatasetManager.Dataset;
import org.eclipse.datatools.modelbase.sql.datatypes.DataType;
import org.eclipse.datatools.modelbase.sql.datatypes.PredefinedDataType;
import org.eclipse.datatools.modelbase.sql.datatypes.PrimitiveType;
import org.eclipse.datatools.modelbase.sql.query.GroupingExpression;
import org.eclipse.datatools.modelbase.sql.query.GroupingSpecification;
import org.eclipse.datatools.modelbase.sql.query.OrderBySpecification;
import org.eclipse.datatools.modelbase.sql.query.Predicate;
import org.eclipse.datatools.modelbase.sql.query.PredicateBasic;
import org.eclipse.datatools.modelbase.sql.query.PredicateBetween;
import org.eclipse.datatools.modelbase.sql.query.PredicateComparisonOperator;
import org.eclipse.datatools.modelbase.sql.query.PredicateExists;
import org.eclipse.datatools.modelbase.sql.query.PredicateIn;
import org.eclipse.datatools.modelbase.sql.query.PredicateInValueList;
import org.eclipse.datatools.modelbase.sql.query.PredicateInValueRowSelect;
import org.eclipse.datatools.modelbase.sql.query.PredicateInValueSelect;
import org.eclipse.datatools.modelbase.sql.query.PredicateIsNull;
import org.eclipse.datatools.modelbase.sql.query.PredicateLike;
import org.eclipse.datatools.modelbase.sql.query.PredicateQuantified;
import org.eclipse.datatools.modelbase.sql.query.PredicateQuantifiedRowSelect;
import org.eclipse.datatools.modelbase.sql.query.PredicateQuantifiedType;
import org.eclipse.datatools.modelbase.sql.query.PredicateQuantifiedValueSelect;
import org.eclipse.datatools.modelbase.sql.query.QueryExpressionBody;
import org.eclipse.datatools.modelbase.sql.query.QueryResultSpecification;
import org.eclipse.datatools.modelbase.sql.query.QuerySearchCondition;
import org.eclipse.datatools.modelbase.sql.query.QuerySelect;
import org.eclipse.datatools.modelbase.sql.query.QuerySelectStatement;
import org.eclipse.datatools.modelbase.sql.query.QueryValueExpression;
import org.eclipse.datatools.modelbase.sql.query.ResultColumn;
import org.eclipse.datatools.modelbase.sql.query.ResultTableAllColumns;
import org.eclipse.datatools.modelbase.sql.query.SearchConditionCombined;
import org.eclipse.datatools.modelbase.sql.query.SearchConditionCombinedOperator;
import org.eclipse.datatools.modelbase.sql.query.SearchConditionNested;
import org.eclipse.datatools.modelbase.sql.query.TableCorrelation;
import org.eclipse.datatools.modelbase.sql.query.TableExpression;
import org.eclipse.datatools.modelbase.sql.query.TableFunction;
import org.eclipse.datatools.modelbase.sql.query.TableInDatabase;
import org.eclipse.datatools.modelbase.sql.query.TableJoined;
import org.eclipse.datatools.modelbase.sql.query.TableJoinedOperator;
import org.eclipse.datatools.modelbase.sql.query.TableNested;
import org.eclipse.datatools.modelbase.sql.query.TableReference;
import org.eclipse.datatools.modelbase.sql.query.ValueExpressionCase;
import org.eclipse.datatools.modelbase.sql.query.ValueExpressionCaseSearch;
import org.eclipse.datatools.modelbase.sql.query.ValueExpressionCaseSimple;
import org.eclipse.datatools.modelbase.sql.query.ValueExpressionCast;
import org.eclipse.datatools.modelbase.sql.query.ValueExpressionColumn;
import org.eclipse.datatools.modelbase.sql.query.ValueExpressionCombined;
import org.eclipse.datatools.modelbase.sql.query.ValueExpressionCombinedOperator;
import org.eclipse.datatools.modelbase.sql.query.ValueExpressionDefaultValue;
import org.eclipse.datatools.modelbase.sql.query.ValueExpressionFunction;
import org.eclipse.datatools.modelbase.sql.query.ValueExpressionNested;
import org.eclipse.datatools.modelbase.sql.query.ValueExpressionNullValue;
import org.eclipse.datatools.modelbase.sql.query.ValueExpressionRow;
import org.eclipse.datatools.modelbase.sql.query.ValueExpressionScalarSelect;
import org.eclipse.datatools.modelbase.sql.query.ValueExpressionSimple;
import org.eclipse.datatools.modelbase.sql.query.ValueExpressionVariable;
import org.eclipse.datatools.modelbase.sql.query.WithTableReference;
import org.eclipse.datatools.modelbase.sql.query.helper.StatementHelper;
import org.eclipse.datatools.modelbase.sql.routines.Function;
import org.eclipse.datatools.modelbase.sql.schema.SQLObject;
import org.eclipse.datatools.modelbase.sql.tables.Column;
import org.eclipse.datatools.modelbase.sql.tables.Table;
import org.eclipse.datatools.modelbase.sql.tables.TemporaryTable;
import org.eclipse.emf.common.util.EList;

public class DBToasterTMLWriter
{
    static final String declarations = "<declarations>\n"
        + "<attributeDecl name=\"attribute_identifier\" type=\"String\"/>\n"
        + "<attributeDecl name=\"variable_identifier\"  type=\"String\"/>\n"
        + "<attributeDecl name=\"function_identifier\"  type=\"String\"/>\n"
        + "<attributeDecl name=\"type_identifier\"      type=\"String\"/>\n"
        + "<attributeDecl name=\"field_identifier\"     type=\"String\"/>\n"
        + "<attributeDecl name=\"relation_identifier\"  type=\"String\"/>\n"
        + "<attributeDecl name=\"state_identifier\"     type=\"String\"/>\n"
        + "<attributeDecl name=\"delta\"                type=\"String\"/>\n"
        + "<attributeDecl name=\"aggregate\"            type=\"String\"/>\n"
        + "<attributeDecl name=\"oplus\"                type=\"String\"/>\n"
        + "<attributeDecl name=\"poplus\"               type=\"String\"/>\n"
        + "<attributeDecl name=\"eterm-type\"           type=\"String\"/>\n"
        + "<attributeDecl name=\"eterm-val\"            type=\"String\"/>\n"
        + "<attributeDecl name=\"meterm-type\"          type=\"String\"/>\n"
        + "<attributeDecl name=\"meterm-val\"           type=\"String\"/>\n"
        + "<attributeDecl name=\"expression\"           type=\"String\"/>\n"
        + "<attributeDecl name=\"bterm\"                type=\"String\"/>\n"
        + "<attributeDecl name=\"boolexpression\"       type=\"String\"/>\n"
        + "<attributeDecl name=\"mapexpression\"        type=\"String\"/>\n"
        + "<attributeDecl name=\"plan\"                 type=\"String\"/>\n"
        + "</declarations>\n";

    public class CreateTMLException extends Exception
    {
        private static final long serialVersionUID = -7691008887549313523L;

        CreateTMLException()
        {
        }

        CreateTMLException(String message)
        {
            super(message);
        }
    }

    public class lastRelationArgs
    {
    	String datasets;
    	String relName;
    	
    	public lastRelationArgs(String d, String r) {
    		datasets = d;
    		relName = r;
    	}
    	
    	public String getDatasets() {
    		return datasets;
    	}
    	
    	public String getRelName() {
    		return relName;
    	}
    }
    
    public class DBToasterUnhandledException extends Exception
    {
        private static final long serialVersionUID = -7673484728787571132L;

        public DBToasterUnhandledException()
        {
        }

        public DBToasterUnhandledException(String msg)
        {
            super(msg);
        }
    };

    // Datasets defining base relations for use in queries.
    DatasetManager datasetMgr;
    LinkedHashMap<String, Integer> relationCounters;
    LinkedHashMap<String, String> columnSuffixesForAliases;
    Vector<lastRelationArgs> lastRelationsUsed;
    LinkedHashMap<String, Integer> groupByRelationSuffixes;

    HashSet<String> aggregateFunctions;

    HashSet<String> exprNodeTypes;
    HashSet<String> btermNodeTypes;
    HashSet<String> boolExprNodeTypes;
    HashSet<String> mapExprNodeTypes;
    HashSet<String> planNodeTypes;

    public DBToasterTMLWriter(DatasetManager mgr)
    {
        datasetMgr = mgr;

        relationCounters = new LinkedHashMap<String, Integer>();
        columnSuffixesForAliases = new LinkedHashMap<String, String>();
        lastRelationsUsed = new Vector<lastRelationArgs>();
        init();
    }

    private void init()
    {
        // TODO: read keywords from a specification file generated by the
        // DBToaster compiler.
        aggregateFunctions = new HashSet<String>();

        String[] aggFns = { "sum", "min", "max" };
        for (String s : aggFns)
            aggregateFunctions.add(s);

        exprNodeTypes = new HashSet<String>();
        btermNodeTypes = new HashSet<String>();
        boolExprNodeTypes = new HashSet<String>();
        mapExprNodeTypes = new HashSet<String>();
        planNodeTypes = new HashSet<String>();

        // Define keywords
        // Note this is a partial list for user input alone.
        String[] exprNodes = { "eterm", "uminus", "sum", "minus", "product",
            "divide" };

        String[] btermNodes = { "eq", "ne", "lt", "le", "gt", "ge", "meq",
            "mneq", "mlt", "mle", "mgt", "mge" };

        String[] boolExprNodes = { "bterm", "not", "and", "or" };

        String[] mapExprNodes = { "meterm", "sum", "minus", "product", "min",
            "max", "mapaggregate" };

        String[] planNodes = { "relation", "select", "project", "union",
            "cross" };

        for (String s : exprNodes)
            exprNodeTypes.add(s);
        for (String s : btermNodes)
            btermNodeTypes.add(s);
        for (String s : boolExprNodes)
            boolExprNodeTypes.add(s);
        for (String s : mapExprNodes)
            mapExprNodeTypes.add(s);
        for (String s : planNodes)
            planNodeTypes.add(s);
    }

    public void reset()
    {
        relationCounters.clear();
        columnSuffixesForAliases.clear();
        lastRelationsUsed.clear();
    }

    private String getDBToasterType(PrimitiveType pType)
        throws CreateTMLException
    {
        String r = "";
        int vType = pType.getValue();

        if (vType == PrimitiveType.INTEGER || vType == PrimitiveType.SMALLINT)
        {
            r = "int";
        }
        else if (vType == PrimitiveType.FLOAT
            || vType == PrimitiveType.DOUBLE_PRECISION
            || vType == PrimitiveType.DECIMAL)
        {
            r = "float";
        }
        else if (vType == PrimitiveType.BIGINT)
        {
            r = "long";
        }
        else if (vType == PrimitiveType.CHARACTER
            || vType == PrimitiveType.CHARACTER_VARYING)
        {
            r = "string";
        }
        else
        {
            throw new CreateTMLException("Unsupported type: "
                + PrimitiveType.get(vType).getName());
        }

        return r;
    }

    public Vector<lastRelationArgs> getRelationsUsedFromParsing()
    {
        return lastRelationsUsed;
    }

    ///////////////////////////////
    //
    // Basic AST helpers.
    private boolean isColumn(QueryValueExpression qve)
    {
        return (qve instanceof ValueExpressionColumn);
    }

    ///////////////////////////////
    //
    // getSelectQueries overloads
    // -- returns flattened list of any selection queries in
    // any SQL statement (i.e. traverses to leaves to include subselects)

    // TODO: traverse group-bys and having clauses.
    private LinkedList<QuerySelect> getSelectQueries(QuerySelect select)
        throws DBToasterUnhandledException
    {
        LinkedList<QuerySelect> r = new LinkedList<QuerySelect>();
        r.add(select);

        // Look in from clauses
        EList<?> fromList = select.getFromClause();
        if (fromList != null && !fromList.isEmpty())
        {
            Iterator<?> fromIt = fromList.iterator();
            for (; fromIt.hasNext();)
            {
                TableReference tr = (TableReference) fromIt.next();
                r.addAll(getSelectQueries(tr));
            }
        }

        // Look in where clauses
        if (select.isSetHavingClause())
        {
            QuerySearchCondition wc = select.getWhereClause();
            if (wc != null)
                r.addAll(getSelectQueries(wc));
        }

        // TODO: group-bys, having clauses.

        return r;
    }

    private LinkedList<QuerySelect> getSelectQueries(TableReference tr)
        throws DBToasterUnhandledException
    {
        LinkedList<QuerySelect> r = new LinkedList<QuerySelect>();

        if (tr instanceof TableJoined)
        {
            TableJoined join = (TableJoined) tr;

            // TableJoinedOperator joinOp = join.getJoinOperator();
            QuerySearchCondition joinCond = join.getJoinCondition();
            TableReference leftRef = join.getTableRefLeft();
            TableReference rightRef = join.getTableRefRight();

            r.addAll(getSelectQueries(leftRef));
            r.addAll(getSelectQueries(rightRef));

            // TODO: can subselects really appear in the join condition?
            r.addAll(getSelectQueries(joinCond));
        }

        // TableExpression subclasses
        else if (tr instanceof QueryExpressionBody)
        {
            QueryExpressionBody qbe = (QueryExpressionBody) tr;
            r.addAll(getSelectQueries((QuerySelect) qbe));
        }

        // No-op
        else if (tr instanceof TableInDatabase)
        {
        }

        // TableNested subclasses
        else if (tr instanceof TableNested)
        {
            TableNested nested = (TableNested) tr;
            r.addAll(getSelectQueries(nested.getNestedTableRef()));
        }

        // TODO
        else if (tr instanceof TableFunction)
        {

        }
        else if (tr instanceof WithTableReference)
        {

        }
        else if (tr instanceof TableExpression)
        {

        }

        else
        {
            String msg = "Unhandled TableReference type: "
                + tr.getClass().getName();

            throw new DBToasterUnhandledException(msg);
        }

        return r;
    }

    // TODO: traverse order-bys in SelectStatements

    private LinkedList<QuerySelect> getSelectQueries(QueryValueExpression qve)
        throws DBToasterUnhandledException
    {
        LinkedList<QuerySelect> r = new LinkedList<QuerySelect>();

        if (qve instanceof ValueExpressionCombined)
        {
            ValueExpressionCombined combined = (ValueExpressionCombined) qve;

            // Handle left, right exprs
            r.addAll(getSelectQueries(combined.getLeftValueExpr()));
            r.addAll(getSelectQueries(combined.getRightValueExpr()));
        }

        // Subqueries
        else if (qve instanceof ValueExpressionScalarSelect)
        {
            ValueExpressionScalarSelect scalarSelect = (ValueExpressionScalarSelect) qve;

            // TODO: order-bys

            QuerySelect select = (QuerySelect) scalarSelect.getQueryExpr()
                .getQuery();

            // Continue traversing to get all selects in subqueries.
            // Assume getSelectQueries(QuerySelect) adds the argument.
            r.addAll(getSelectQueries(select));
        }

        // No-ops
        else if (qve instanceof ValueExpressionColumn)
        {
        }

        else if (qve instanceof ValueExpressionSimple)
        {
        }

        else if (qve instanceof ValueExpressionFunction)
        {
        }

        // TODO ...
        else if (qve instanceof ValueExpressionNullValue)
        {
            throw new DBToasterUnhandledException(
                "Unsupported: null values in getSelectQueries.");
        }

        else if (qve instanceof ValueExpressionRow)
        {
            throw new DBToasterUnhandledException(
                "Unsupported: rows in getSelectQueries.");
        }

        // Case clauses
        else if (qve instanceof ValueExpressionCaseSearch)
        {
            throw new DBToasterUnhandledException(
                "Unsupported: case statements.");
        }
        else if (qve instanceof ValueExpressionCaseSimple)
        {
            throw new DBToasterUnhandledException(
                "Unsupported: case statements.");
        }
        else if (qve instanceof ValueExpressionCase)
        {
            throw new DBToasterUnhandledException(
                "Unsupported: case statements.");
        }

        // Cast clauses
        else if (qve instanceof ValueExpressionCast)
        {
            throw new DBToasterUnhandledException(
                "Unsupported: casts in getSelectQueries.");
        }

        else if (qve instanceof ValueExpressionVariable)
        {
            throw new DBToasterUnhandledException(
                "Unsupported: ValueExpressionVariable.");
        }

        // Unclear what these are
        else if (qve instanceof ValueExpressionDefaultValue)
        {
            throw new DBToasterUnhandledException(
                "Unsupported: ValueExpressionDefaultValue.");
        }

        else if (qve instanceof ValueExpressionNested)
        {
            throw new DBToasterUnhandledException(
                "Unsupported: ValueExpressionNested.");
        }
        else
        {
            String msg = "Unhandled QueryValueExpression type: "
                + qve.getClass().getName();

            throw new DBToasterUnhandledException(msg);
        }

        return r;
    }

    private LinkedList<QuerySelect> getSelectQueries(QuerySearchCondition qsc)
        throws DBToasterUnhandledException
    {
        System.out.println("getSelectQueries QSC: " + qsc.toString());

        LinkedList<QuerySelect> r = new LinkedList<QuerySelect>();

        if (qsc instanceof SearchConditionCombined)
        {
            r = getSelectQueries((SearchConditionCombined) qsc);
        }
        else if (qsc instanceof SearchConditionNested)
        {
            r = getSelectQueries((SearchConditionNested) qsc);
        }
        else if (qsc instanceof Predicate)
        {

            try
            {

                Method getSelectQueriesMethod = this.getClass()
                    .getDeclaredMethod("getSelectQueries",
                        getDispatchClass(qsc));
                r = (LinkedList<QuerySelect>)
                    getSelectQueriesMethod.invoke(this, qsc);

            } catch (NoSuchMethodException e)
            {
                e.printStackTrace();
            } catch (IllegalAccessException e)
            {
                e.printStackTrace();
            } catch (InvocationTargetException e)
            {
                System.out.println("Failed on: " + qsc);
                e.printStackTrace();
            }
        }
        else
        {
            String msg = "Unhandled QuerySearchCondition type: "
                + qsc.getClass().getName();

            throw new DBToasterUnhandledException(msg);
        }

        return r;
    }

    private LinkedList<QuerySelect> getSelectQueries(SearchConditionCombined scc)
        throws DBToasterUnhandledException
    {
        LinkedList<QuerySelect> r = new LinkedList<QuerySelect>();
        r.addAll(getSelectQueries(scc.getLeftCondition()));
        r.addAll(getSelectQueries(scc.getRightCondition()));
        return r;
    }

    private LinkedList<QuerySelect> getSelectQueries(SearchConditionNested scn)
        throws DBToasterUnhandledException
    {
        return getSelectQueries(scn.getNestedCondition());
    }

    private LinkedList<QuerySelect> getSelectQueries(PredicateBasic p)
        throws DBToasterUnhandledException
    {
        LinkedList<QuerySelect> r = new LinkedList<QuerySelect>();

        if (p.getLeftValueExpr() != null)
            r.addAll(getSelectQueries(p.getLeftValueExpr()));

        if (p.getRightValueExpr() != null)
            r.addAll(getSelectQueries(p.getRightValueExpr()));

        return r;
    }

    // TODO

    private LinkedList<String> getSelectQueries(PredicateBetween p)
    {
        return null;
    }

    private LinkedList<String> getSelectQueries(PredicateExists p)
    {
        return null;
    }

    private LinkedList<String> getSelectQueries(PredicateIn p)
    {
        return null;
    }

    private LinkedList<String> getSelectQueries(PredicateInValueList p)
    {
        return null;
    }

    private LinkedList<String> getSelectQueries(PredicateInValueRowSelect p)
    {
        return null;
    }

    private LinkedList<String> getSelectQueries(PredicateInValueSelect p)
    {
        return null;
    }

    private LinkedList<String> getSelectQueries(PredicateIsNull p)
    {
        return null;
    }

    private LinkedList<String> getSelectQueries(PredicateLike p)
    {
        return null;
    }

    private LinkedList<String> getSelectQueries(PredicateQuantified p)
    {
        return null;
    }

    private LinkedList<String> getSelectQueries(PredicateQuantifiedRowSelect p)
    {
        return null;
    }

    private LinkedList<String> getSelectQueries(PredicateQuantifiedType p)
    {
        return null;
    }

    private LinkedList<String> getSelectQueries(PredicateQuantifiedValueSelect p)
    {
        return null;
    }

    // ////////////////////////////
    //
    // getAggregates overloads

    // Assume selects is a flattened list, that is an element of the list
    // may be a sub-select of another element. Thus we only need to
    // check select-lists and do not need to traverse into subqueries.

    private LinkedList<Function> getAggregates(LinkedList<QuerySelect> selects)
        throws DBToasterUnhandledException
    {
        LinkedList<Function> r = new LinkedList<Function>();

        for (QuerySelect qs : selects)
        {
            Iterator<?> selColIt = qs.getSelectClause().iterator();
            for (; selColIt.hasNext();)
            {
                QueryResultSpecification qsr = (QueryResultSpecification) selColIt
                    .next();

                // Cannot support TABLE.*, this has no aggregates.
                if (qsr instanceof ResultTableAllColumns)
                {
                    throw new DBToasterUnhandledException(
                        "SELECT Table.* queries unsupported");
                }

                ResultColumn col = (ResultColumn) qsr;
                QueryValueExpression colValExpr = (QueryValueExpression) col
                    .getValueExpr();

                r.addAll(getAggregateFunctions(colValExpr));
            }
        }

        return r;
    }

    LinkedList<Function> getAggregateFunctions(QueryValueExpression qve)
        throws DBToasterUnhandledException
    {
        LinkedList<Function> r = new LinkedList<Function>();

        if (qve instanceof ValueExpressionFunction)
        {
            ValueExpressionFunction f = (ValueExpressionFunction) qve;

            // Check if function is one DBToaster can handle.
            Function fn = f.getFunction();
            String fnName = f == null ? null : f.getName().toLowerCase();
            if (!(f.isColumnFunction() && aggregateFunctions.contains(fnName)))
            {
                String msg = fnName == null ? f.toString() : fnName;
                throw new DBToasterUnhandledException("Unsupported function: "
                    + msg);
            }
            else
            {
                r.add(fn);
            }
        }

        else if (qve instanceof ValueExpressionCombined)
        {
            ValueExpressionCombined combined = (ValueExpressionCombined) qve;

            // Handle left, right exprs
            r.addAll(getAggregateFunctions(combined.getLeftValueExpr()));
            r.addAll(getAggregateFunctions(combined.getRightValueExpr()));
        }

        // No-ops
        else if (qve instanceof ValueExpressionScalarSelect)
        {
        }

        else if (qve instanceof ValueExpressionColumn)
        {
        }

        else if (qve instanceof ValueExpressionSimple)
        {
        }

        // TODO ...
        else if (qve instanceof ValueExpressionNullValue)
        {
            throw new DBToasterUnhandledException(
                "Unsupported: null values in getSelectQueries.");
        }

        else if (qve instanceof ValueExpressionRow)
        {
            throw new DBToasterUnhandledException(
                "Unsupported: rows in getSelectQueries.");
        }

        // Case clauses
        else if (qve instanceof ValueExpressionCaseSearch)
        {
            throw new DBToasterUnhandledException(
                "Unsupported: case statements.");
        }
        else if (qve instanceof ValueExpressionCaseSimple)
        {
            throw new DBToasterUnhandledException(
                "Unsupported: case statements.");
        }
        else if (qve instanceof ValueExpressionCase)
        {
            throw new DBToasterUnhandledException(
                "Unsupported: case statements.");
        }

        // Cast clauses
        else if (qve instanceof ValueExpressionCast)
        {
            throw new DBToasterUnhandledException(
                "Unsupported: casts in getSelectQueries.");
        }

        else if (qve instanceof ValueExpressionVariable)
        {
            throw new DBToasterUnhandledException(
                "Unsupported: ValueExpressionVariable.");
        }

        // Unclear what these are
        else if (qve instanceof ValueExpressionDefaultValue)
        {
            throw new DBToasterUnhandledException(
                "Unsupported: ValueExpressionDefaultValue.");
        }

        else if (qve instanceof ValueExpressionNested)
        {
            throw new DBToasterUnhandledException(
                "Unsupported: ValueExpressionNested.");
        }
        else
        {
            String msg = "Unhandled QueryValueExpression type: "
                + qve.getClass().getName();

            throw new DBToasterUnhandledException(msg);
        }

        return r;
    }

    // ///////////////////////////////////////
    //
    // DBToaster TreeML generation

    // Basic DBToaster TreeML constructors

    private LinkedList<String> indent(LinkedList<String> sl)
    {
        LinkedList<String> r = new LinkedList<String>();
        for (String s : sl)
            r.add("    " + s);
        return r;
    }

    private LinkedList<String> makeBranch(LinkedList<String> subTree)
    {
        LinkedList<String> r = indent(subTree);
        r.addFirst("<branch>");
        r.addLast("</branch>");
        return r;
    }

    private LinkedList<String> makeLeaf(LinkedList<String> subTree)
    {
        LinkedList<String> r = indent(subTree);
        r.addFirst("<leaf>");
        r.addLast("</leaf>");
        return r;
    }

    private String createIdentifier(String name, String value)
    {
        return "<attribute name=\"" + name + "\" value=\"" + value + "\"/>";
    }

    private String createAttributeIdentifier(String relation, String field)
    {
        String attrIdVal = "qualified," + relation + "," + field;
        return createIdentifier("attribute_identifier", attrIdVal);
    }

    private String createAttributeIdentifier(String field)
    {
        String attrIdVal = "unqualified," + field;
        return createIdentifier("attribute_identifier", attrIdVal);
    }

    private String createVariableIdentifier(String var)
    {
        return createIdentifier("variable_identifier", var);
    }

    private String createFieldIdentifier(String field)
    {
        return createIdentifier("field_identifier", field);
    }

    private String createTypeIdentifier(String type)
    {
        return createIdentifier("type_identifier", type);
    }

    private String createRelationIdentifier(String relation)
    {
        return createIdentifier("relation_identifier", relation);
    }

    private LinkedList<String> createFieldList(
        LinkedHashMap<String, String> idsAndTypes)
    {
        LinkedList<String> r = new LinkedList<String>();
        for (Map.Entry<String, String> e : idsAndTypes.entrySet())
        {
            r.add(createFieldIdentifier(e.getKey()));
            r.add(createTypeIdentifier(e.getValue()));
        }
        return r;
    }

    private String createAggregateFunction(String function)
    {
        return "<attribute name=\"aggregate\" value=\"" + function + "\"/>";
    }

    private String createOplus(String function)
    {
        return "<attribute name=\"oplus\" value=\"" + function + "\"/>";
    }

    private String createPoplus(String function)
    {
        return "<attribute name=\"poplus\" value=\"" + function + "\"/>";
    }

    private LinkedList<String> createETerm(String type, String val, String rel)
    {
        LinkedList<String> r = new LinkedList<String>();
        r.add("<attribute name=\"eterm-type\" value=\"" + type + "\"/>");
        if (type == "attr") {
        	if (rel.equals(""))
        		r.add(createAttributeIdentifier(val));
        	else
        		r.add(createAttributeIdentifier(rel, val));
        }
        else if (type == "var")
            r.add(createVariableIdentifier(val));
        else
            r.add("<attribute name=\"eterm-val\" value=\"" + val + "\"/>");

        return makeLeaf(r);
    }

    private LinkedList<String> createMETerm(String type, String val, String rel)
    {
        LinkedList<String> r = new LinkedList<String>();
        r.add("<attribute name=\"meterm-type\" value=\"" + type + "\"/>");
        if (type == "attr") {
        	if (rel.equals(""))
        		r.add(createAttributeIdentifier(val));
        	else
        		r.add(createAttributeIdentifier(rel, val));
            
        }
        else if (type == "var")
            r.add(createVariableIdentifier(val));
        else
            r.add("<attribute name=\"meterm-val\" value=\"" + val + "\"/>");

        return makeLeaf(r);
    }

    private String createExpressionNode(String nodeType)
        throws CreateTMLException
    {
        if (!exprNodeTypes.contains(nodeType))
            throw new CreateTMLException("Invalid expression node: " + nodeType);

        return "<attribute name=\"expression\" value=\"" + nodeType + "\"/>";
    }

    private String createBTermNode(String nodeType) throws CreateTMLException
    {
        if (!btermNodeTypes.contains(nodeType))
            throw new CreateTMLException("Invalid bterm node: " + nodeType);

        return "<attribute name=\"bterm\" value=\"" + nodeType + "\"/>";
    }

    private String createBoolExprNode(String nodeType)
        throws CreateTMLException
    {
        if (!boolExprNodeTypes.contains(nodeType))
            throw new CreateTMLException("Invalid boolexpression node: "
                + nodeType);

        return "<attribute name=\"boolexpression\" value=\"" + nodeType
            + "\"/>";
    }

    private String createMapExpressionNode(String nodeType)
        throws CreateTMLException
    {
        if (!mapExprNodeTypes.contains(nodeType))
            throw new CreateTMLException("Invalid mapexpression node: "
                + nodeType);

        return "<attribute name=\"mapexpression\" value=\"" + nodeType + "\"/>";
    }

    private String createPlanNode(String nodeType) throws CreateTMLException
    {
        if (!planNodeTypes.contains(nodeType))
            throw new CreateTMLException("Invalid plan node: " + nodeType);

        return "<attribute name=\"plan\" value=\"" + nodeType + "\"/>";
    }

    public String createTreeML(String mapExpressions)
    {
        String header = "<tree>\n";
        String footer = "</tree>";

        return header + declarations + mapExpressions + footer;
    }

    public String createSelectStatementTreeML(QuerySelectStatement selectStmt)
        throws CreateTMLException
    {
        // Reset parsing
        reset();

        System.out.println("SelectStatement: " + selectStmt);

        QuerySelect select = (QuerySelect) selectStmt.getQueryExpr().getQuery();

        if (!selectStmt.getOrderByClause().isEmpty())
            throw new CreateTMLException("ORDER BY unsupported.");

        LinkedList<String> selectTML = createSelectTreeML(select);
        String r = ""; for (String s : selectTML) r += (s + "\n");
        return r;
    }

    public LinkedList<String> createSelectTreeML(QuerySelect select)
        throws CreateTMLException
    {
        System.out.println("Select: " + select);

        // Issue warnings
        // -- for now INTO clauses simply yield warnings.
        if (!select.getIntoClause().isEmpty())
            throw new CreateTMLException("SELECT INTO unsupported.");

        // TODO: handle having clauses, i.e. predicates on aggregates
        // -- this could be done with nested aggregates
        if (select.getHavingClause() != null && select.isSetHavingClause())
            throw new CreateTMLException("HAVING unsupported.");

        // Generate TML.
        // -- generate for relational stuff first, e.g. from clause
        // and where clause, before generating for select-list
        LinkedList<String> fromTML = createFromClauseTreeML(select
            .getFromClause());

        System.out.println("Generating whereTML: " + select.isSetWhereClause()
            + " (null: " + (select.getWhereClause() == null) + ")");

        LinkedList<String> whereTML = ((select.getWhereClause() != null
            && select.isSetWhereClause()) ?
                createWhereClauseTreeML(select.getWhereClause(), fromTML) : fromTML);

        System.out.println("Generating groupByTML.");
        LinkedList<String> groupByFields = null;
        LinkedList<String> groupByTML = null;
        if (!select.getGroupByClause().isEmpty()) {
            groupByFields = getGroupByFields(select.getGroupByClause());
            groupByTML = createGroupByTreeML(select.getGroupByClause(), whereTML);
        }
        else
            groupByTML = whereTML;

        System.out.println("Generating selectListTML.");

        LinkedList<String> aggregateTML = createSelectListTreeML(
            select.getSelectClause(), groupByTML, groupByFields);

        LinkedList<String> r = aggregateTML;
        return r;
    }

    // Select list
    // DBToaster currently only supports aggregate queries.
    // Make sure we have an aggregation function.

    private LinkedList<String> createSelectListTreeML(EList<?> selectList,
            LinkedList<String> relationalTML, LinkedList<String> groupByFields)
        throws CreateTMLException
    {
        System.out.println("Select list size: " + selectList.size());

        LinkedList<String> r = new LinkedList<String>();

        for (Iterator<?> selIt = selectList.iterator(); selIt.hasNext();)
        {
            QueryResultSpecification nextResult =
                (QueryResultSpecification) selIt.next();

            // Cannot support TABLE.*, this has no aggregates.
            if (nextResult instanceof ResultTableAllColumns)
            {
                throw new CreateTMLException(
                    "SELECT Table.* queries unsupported");
            }

            System.out.println("QSR: " + nextResult);

            ResultColumn col = (ResultColumn) nextResult;
            QueryValueExpression colValExpr =
                (QueryValueExpression) col.getValueExpr();

            // Check if column is a group-by column, and skip.
            if ( isColumn(colValExpr) )
            {
                LinkedList<String> relAndColName = getGroupByColumnName(colValExpr);
                
                if ( relAndColName == null || relAndColName.size() != 2 ) {
                    String msg = "Invalid group-by column: " + colValExpr;
                    throw new CreateTMLException(msg);
                }
                
                String colName = relAndColName.get(1);
                if ( groupByFields.contains(colName) )
                    continue;
                else
                {
                    String msg = "Non-group by column found: " + colName;
                    throw new CreateTMLException(msg);
                }
            }

            // Start off as a map expression
            r.addAll(createQueryValueExpressionTreeML(
                colValExpr, relationalTML, true));
        }

        return r;
    }

    private LinkedList<String> getGroupByColumnName(QueryValueExpression qve)
        throws CreateTMLException
    {
        String relName = null;
        String columnName = null;

        System.out.println("Found group-by QVE: " + qve);

        if (qve instanceof ValueExpressionColumn)
        {
            ValueExpressionColumn column = (ValueExpressionColumn) qve;
            columnName = column.getName().toLowerCase();

            // Check column exists in base relation
            if (column.getTableInDatabase() != null)
                relName = column.getTableInDatabase().getName().toLowerCase();
            else if (column.getTableExpr() != null)
                relName = column.getTableExpr().getName().toLowerCase();
            else
            {
                String msg = "Unable to find binding relation for column "
                    + column;
                throw new CreateTMLException(msg);
            }

            String dsName = datasetMgr.getRelationDataset(relName);
            LinkedHashMap<String, String> relFields = null;
            if (dsName != null
                && (relFields = datasetMgr.getRelationFields(relName)) != null)
            {
                if (!(relFields.containsKey(column.getName()) || relFields
                    .containsKey(columnName)))
                {
                    String msg = "Unable to find attribute " + column.getName()
                        + " in relation " + relName;
                    throw new CreateTMLException(msg);
                }
            }
            else
            {
                throw new CreateTMLException("Unable to find binding relation "
                    + relName);
            }
            
            // Get aliased column name.
            if (!(column.getTableExpr() == null ||
                    column.getTableExpr().getTableCorrelation() == null))
            {
                TableCorrelation corr = column.getTableExpr()
                    .getTableCorrelation();
                String relAlias = corr.getName().toLowerCase();
                if (columnSuffixesForAliases.containsKey(relAlias))
                    columnName += columnSuffixesForAliases.get(relAlias);
            }
        }
        else {
            String msg = "Unsupported group-by expression: " + qve;
            throw new CreateTMLException(msg);
        }
        
        LinkedList<String> r = null;
        if ( !(relName == null || columnName == null) ) {
            r = new LinkedList<String>();
            r.add(relName);
            r.add(columnName);
        }
        return r;
    }

    private LinkedList<String> createQueryValueExpressionTreeML(
            QueryValueExpression qve, LinkedList<String> relationalTML,
            boolean mapExpression)
        throws CreateTMLException
    {
        LinkedList<String> r = new LinkedList<String>();

        System.out.println("Found QVE: " + qve);

        if (qve instanceof ValueExpressionColumn)
        {
            ValueExpressionColumn column = (ValueExpressionColumn) qve;
            String columnName = column.getName().toLowerCase();

            System.out.println("Column: " + column);
            System.out.println("Column table: " + column.getTableInDatabase());
            System.out.println("Column table expr: " + column.getTableExpr());
            System.out.println("Column table expr cols: "
                + column.getTableExpr().getColumnList().size());
            System.out.println("Column table expr val cols: "
                + column.getTableExpr().getValueExprColumns().size());
            System.out.println("Column corr: "
                + column.getTableExpr().getTableCorrelation());

            // Check column exists in base relation
            String relName = "";
            if (column.getTableInDatabase() != null)
                relName = column.getTableInDatabase().getName().toLowerCase();
            else if (column.getTableExpr() != null)
                relName = column.getTableExpr().getName().toLowerCase();
            else
            {
                String msg = "Unable to find binding relation for column "
                    + column;
                throw new CreateTMLException(msg);
            }

            String dsName = datasetMgr.getRelationDataset(relName);
            LinkedHashMap<String, String> relFields = null;
            if (dsName != null
                && (relFields = datasetMgr.getRelationFields(relName)) != null)
            {
                if (!(relFields.containsKey(column.getName()) || relFields
                    .containsKey(columnName)))
                {
                    String msg = "Unable to find attribute " + column.getName()
                        + " in relation " + relName;
                    throw new CreateTMLException(msg);
                }
            }
            else
            {
                throw new CreateTMLException("Unable to find binding relation "
                    + relName);
            }

            // Get unique column name.
            if (!(column.getTableExpr() == null ||
                    column.getTableExpr().getTableCorrelation() == null))
            {
                TableCorrelation corr = column.getTableExpr()
                    .getTableCorrelation();
                String relAlias = corr.getName().toLowerCase();
                if (columnSuffixesForAliases.containsKey(relAlias))
                    columnName += columnSuffixesForAliases.get(relAlias);
            }
            
            // `ETerm or `METerm `Attribute
            if (mapExpression)
            {
                r.add(createMapExpressionNode("meterm"));
                r.addAll(createMETerm("attr", columnName, relName));
            }
            else
            {
                r.add(createExpressionNode("eterm"));
                r.addAll(createETerm("attr", columnName, relName));
            }

            r = makeBranch(r);
        }

        else if (qve instanceof ValueExpressionSimple)
        {
            ValueExpressionSimple simple = (ValueExpressionSimple) qve;

            System.out.println("Constant: " + simple);
            DataType valType = simple.getDataType();

            if (valType instanceof PredefinedDataType)
            {
                PredefinedDataType sqlType = (PredefinedDataType) valType;

                // `ETerm or `METerm simple.getValue()
                String dbtType = getDBToasterType(sqlType.getPrimitiveType());

                if (mapExpression)
                {
                    r.add(createMapExpressionNode("meterm"));
                    r.addAll(createMETerm(dbtType, simple.getValue(), ""));
                }
                else
                {
                    r.add(createExpressionNode("eterm"));
                    r.addAll(createETerm(dbtType, simple.getValue(), ""));
                }

                r = makeBranch(r);
            }
            else
            {
                throw new CreateTMLException("Unsupported constant type: "
                    + valType.getClass().getName());
            }
        }

        else if (qve instanceof ValueExpressionFunction)
        {
            ValueExpressionFunction f = (ValueExpressionFunction) qve;

            System.out.println("Function: " + f);

            // Check if function is one DBToaster can handle.
            String fnName = f.getName().toLowerCase();
            if (!(mapExpression && f.isColumnFunction() && aggregateFunctions
                .contains(fnName)))
            {
                Function fn = f.getFunction();
                String msg = fn == null ? f.toString() : fn.getName()
                    .toLowerCase();
                throw new CreateTMLException("Unsupported function: " + msg);
            }

            // aggregate function
            r = createMapAggregateTreeML(f, relationalTML);
        }
        else if (qve instanceof ValueExpressionCombined)
        {
            ValueExpressionCombined combined = (ValueExpressionCombined) qve;

            System.out.println("VE combined: " + combined);

            // Handle left, right exprs
            LinkedList<String> left = createQueryValueExpressionTreeML(
                combined.getLeftValueExpr(), relationalTML, mapExpression);

            LinkedList<String> right = createQueryValueExpressionTreeML(
                combined.getRightValueExpr(), relationalTML, mapExpression);

            // Expression or MapExpression op(l,r)
            r = createCombinedOperatorTreeML(combined.getCombinedOperator(),
                left, right, mapExpression);
        }

        // Subqueries
        else if (qve instanceof ValueExpressionScalarSelect)
        {
            ValueExpressionScalarSelect scalarSelect = (ValueExpressionScalarSelect) qve;

            System.out.println("Scalar select: " + scalarSelect);
            r = createSelectTreeML(
                (QuerySelect) scalarSelect.getQueryExpr().getQuery());
        }

        // TODO ...
        else if (qve instanceof ValueExpressionNullValue)
        {
            throw new CreateTMLException(
                "Unsupported: null values in select list.");
        }

        else if (qve instanceof ValueExpressionRow)
        {
            throw new CreateTMLException("Unsupported: rows in select list.");
        }

        // Case clauses
        else if (qve instanceof ValueExpressionCaseSearch)
        {
            throw new CreateTMLException("Unsupported: case statements.");
        }
        else if (qve instanceof ValueExpressionCaseSimple)
        {
            throw new CreateTMLException("Unsupported: case statements.");
        }
        else if (qve instanceof ValueExpressionCase)
        {
            throw new CreateTMLException("Unsupported: case statements.");
        }

        // Cast clauses
        else if (qve instanceof ValueExpressionCast)
        {
            throw new CreateTMLException("Unsupported: casts in select list.");
        }

        else if (qve instanceof ValueExpressionVariable)
        {
            throw new CreateTMLException(
                "Unsupported: ValueExpressionVariable.");
        }

        // Unclear what these are
        else if (qve instanceof ValueExpressionDefaultValue)
        {
            throw new CreateTMLException(
                "Unsupported: ValueExpressionDefaultValue.");
        }

        else if (qve instanceof ValueExpressionNested)
        {
            throw new CreateTMLException("Unsupported: ValueExpressionNested.");
        }
        else
        {
            String msg = "Unhandled QueryValueExpression type: "
                + qve.getClass().getName();

            throw new CreateTMLException(msg);
        }

        return r;
    }

    private LinkedList<String> createCombinedOperatorTreeML(
        ValueExpressionCombinedOperator op, LinkedList<String> left,
        LinkedList<String> right, boolean mapExpression)
        throws CreateTMLException
    {
        System.out.println("Op: " + op);

        LinkedList<String> r = new LinkedList<String>();

        // Expression or MapExpression op(left,right)
        int opVal = op.getValue();
        if (mapExpression)
        {
            String dbtOpName = null;
            if (opVal == ValueExpressionCombinedOperator.ADD)
                dbtOpName = "sum";
            else if (opVal == ValueExpressionCombinedOperator.SUBTRACT)
                dbtOpName = "minus";
            else if (opVal == ValueExpressionCombinedOperator.MULTIPLY)
                dbtOpName = "product";
            else
            {
                String msg = "Unsupported map expr op: " + op.getName();
                throw new CreateTMLException(msg);
            }

            r.add(createMapExpressionNode(dbtOpName));
        }
        else
        {
            String dbtOpName = null;
            if (opVal == ValueExpressionCombinedOperator.ADD)
                dbtOpName = "sum";
            else if (opVal == ValueExpressionCombinedOperator.SUBTRACT)
                dbtOpName = "minus";
            else if (opVal == ValueExpressionCombinedOperator.MULTIPLY)
                dbtOpName = "product";
            else if (opVal == ValueExpressionCombinedOperator.DIVIDE)
                dbtOpName = "divide";
            else
            {
                String msg = "Unsupported map expr op: " + op.getName();
                throw new CreateTMLException(msg);
            }

            r.add(createExpressionNode(dbtOpName));
        }

        r.addAll(left);
        r.addAll(right);

        return makeBranch(r);
    }

    // Assume this is only called when handling a map expression

    private LinkedList<String> createMapAggregateTreeML(
        ValueExpressionFunction func, LinkedList<String> relationalTML)
        throws CreateTMLException
    {
        // `MapAggregate function, params, ?query?
        // -- query should include parsing from clause, and where clause first,
        // and
        // handing resulting TreeML to this method.

        // Create map expression for aggregate parameter
        if (func.getParameterList().size() != 1)
        {
            String msg = "Unsupported: aggregates with "
                + func.getParameterList().size() + " parameters.";
            throw new CreateTMLException(msg);
        }

        QueryValueExpression f = (QueryValueExpression) func.getParameterList()
            .get(0);

        LinkedList<String> r = new LinkedList<String>();
        r.add(createMapExpressionNode("mapaggregate"));
        r.add(createAggregateFunction(func.getName().toLowerCase()));
        r.addAll(createQueryValueExpressionTreeML(f, null, true));
        r.addAll(relationalTML);

        return makeBranch(r);
    }

    // From clause: joins, base relations

    private LinkedList<String> createFromClauseTreeML(EList<?> fromClause)
        throws CreateTMLException
    {
        LinkedList<String> acc = null;
        for (Iterator<?> fromIt = fromClause.iterator(); fromIt.hasNext();)
        {
            TableReference tableRef = (TableReference) fromIt.next();

            if (acc == null)
                acc = createTableReferenceTML(tableRef);
            else
            {
                acc = createCrossProductTreeML(acc,
                    createTableReferenceTML(tableRef));
            }
        }

        LinkedList<String> r = new LinkedList<String>();
        r.addAll(acc);
        return r;
    }

    private LinkedList<String> createTableReferenceTML(TableReference tableRef)
        throws CreateTMLException
    {
        System.out.println("TableRef: " + tableRef);

        LinkedList<String> r = null;
        if (tableRef instanceof TableJoined)
        {
            TableJoined join = (TableJoined) tableRef;

            TableJoinedOperator joinOp = join.getJoinOperator();
            QuerySearchCondition joinCond = join.getJoinCondition();
            TableReference leftRef = join.getTableRefLeft();
            TableReference rightRef = join.getTableRefRight();

            System.out.println("Join op: " + joinOp);
            System.out.println("Join cond: " + joinCond);
            System.out.println("Left: " + leftRef);
            System.out.println("Right: " + rightRef);

            LinkedList<String> leftTML = createTableReferenceTML(leftRef);
            LinkedList<String> rightTML = createTableReferenceTML(rightRef);

            // `Select(pred, `Cross(left, right))
            LinkedList<String> crossTML = createCrossProductTreeML(leftTML,
                rightTML);
            r = createWhereClauseTreeML(joinCond, crossTML);
        }

        // TableExpression subclasses
        else if (tableRef instanceof QueryExpressionBody)
        {
            QueryExpressionBody qbe = (QueryExpressionBody) tableRef;
            System.out.println("Subquery: " + qbe);
            r = createSelectTreeML((QuerySelect) qbe);
        }
        else if (tableRef instanceof TableInDatabase)
        {
            TableInDatabase baseTable = (TableInDatabase) tableRef;
            System.out.println("Base relation: " + baseTable);
            System.out.println("Base table: " + baseTable.getDatabaseTable());

            // `Relation
            r = makeLeaf(createBaseRelationTreeML(baseTable));
        }

        // TableNested subclasses
        else if (tableRef instanceof TableNested)
        {
            TableNested nested = (TableNested) tableRef;
            System.out.println("Nested: " + nested);
            r = createTableReferenceTML(nested.getNestedTableRef());
        }

        // TODO
        else if (tableRef instanceof TableFunction)
        {

        }
        else if (tableRef instanceof WithTableReference)
        {

        }
        else if (tableRef instanceof TableExpression)
        {

        }

        else
        {
            String msg = "Unhandled TableReference type: "
                + tableRef.getClass().getName();

            throw new CreateTMLException(msg);
        }

        return r;
    }

    private LinkedList<String> createCrossProductTreeML(
        LinkedList<String> leftTML, LinkedList<String> rightTML)
        throws CreateTMLException
    {
        LinkedList<String> r = new LinkedList<String>();
        r.addFirst(createPlanNode("cross"));
        r.addAll(leftTML);
        r.addAll(rightTML);
        return makeBranch(r);
    }

    private LinkedList<String> createBaseRelationTreeML(TableInDatabase table)
        throws CreateTMLException
    {
        LinkedList<String> r = new LinkedList<String>();

        r.add(createPlanNode("relation"));

        LinkedHashMap<String, String> idsAndTypes = null;

        String tableName = table.getName().toLowerCase();
        Table dbTable = table.getDatabaseTable();
        String dbTableName = dbTable.getName().toLowerCase();

        // Temporary tables have no fields
        System.out.println("Name arr: " + tableName.split("\\.").length);

        String[] dsRelName = tableName.split("\\.");
        String datasetName = "";
        String relName = "";

        if (dsRelName.length == 0)
            throw new CreateTMLException("Invalid table name: "
                + table.getName());

        else if (dsRelName.length != 2)
            relName = dsRelName[0].toLowerCase();

        else
        {
            datasetName = dsRelName[0].toLowerCase();
            relName = dsRelName[1].toLowerCase();
        }

        System.out.println("hasDSRel: "
            + datasetMgr.hasDatasetRelation(datasetName, relName));

        if (datasetMgr.hasDatasetRelation(datasetName, relName))
        {
            System.out.println("DB table: " + dbTable.toString());
            System.out.println("DB table cols: " + dbTable.getColumns().size());

            System.out.println("Table corr: " + table.getTableCorrelation());
            System.out.println("Table cols: " + table.getColumnList().size());
            System.out.println("Table der cols: "
                + table.getDerivedColumnList().size());
            System.out.println("Table val cols: "
                + table.getValueExprColumns().size());

            for (Iterator<?> it = table.getValueExprColumns().iterator(); it
                .hasNext();)
            {
                ValueExpressionColumn c = (ValueExpressionColumn) it.next();
                System.out.println("Val col: " + c);
            }

            // Handle aliased tables.
            String fieldSuffix = "";
            if (table.getTableCorrelation() != null)
            {
                TableCorrelation corr = table.getTableCorrelation();
                String corrName = corr.getName().toLowerCase();
                if (relationCounters.containsKey(tableName))
                {
                    Integer current = relationCounters.get(tableName);
                    relationCounters.put(tableName, current + 1);
                    fieldSuffix = Integer.toString(current + 1);
                    columnSuffixesForAliases.put(corrName, fieldSuffix);
                }
                else
                {
                    // Track first visit.
                    relationCounters.put(tableName, 0);
                }
            }
            else
            {
                if (relationCounters.containsKey(tableName))
                {
                    String msg = "Ambiguous use of relation " + tableName;
                    throw new CreateTMLException(msg);
                }

                relationCounters.put(tableName, 0);
            }

            if (datasetName == null || datasetName.isEmpty())
            {
                System.out.println("Finding first instance of " + relName);
                datasetName = datasetMgr.getRelationDataset(relName);
                idsAndTypes = datasetMgr.getRelationFields(relName);
            }
            else
            {
                Dataset ds = datasetMgr.getDataset(datasetName);
                idsAndTypes = ds.getRelationFields(relName);
            }

            // Add any suffixes for aliases
            if (!fieldSuffix.isEmpty())
            {
                LinkedHashMap<String, String> aliasedIdsAndTypes = new LinkedHashMap<String, String>();

                for (Map.Entry<String, String> e : idsAndTypes.entrySet())
                    aliasedIdsAndTypes.put(e.getKey() + fieldSuffix, e
                        .getValue());

                idsAndTypes = aliasedIdsAndTypes;
            }

            lastRelationsUsed.add(new lastRelationArgs(datasetName, relName));
        }
        else
        {
            String msg = "Failed to find relation " + relName
                + " in any dataset.";
            throw new CreateTMLException(msg);
        }

        System.out.println("idsAndTypes: " + idsAndTypes);

        r.add(createRelationIdentifier(relName));
        r.addAll(createFieldList(idsAndTypes));
        return r;
    }

    private LinkedList<String> getGroupByFields(EList<?> groupByClause)
        throws CreateTMLException
    {
        LinkedList<String> r = new LinkedList<String>();

        for (Iterator<?> groupIt = groupByClause.iterator(); groupIt.hasNext();)
        {
            GroupingExpression groupExpr = (GroupingExpression) groupIt.next();
            
            LinkedList<String> groupColRelAndName =
                getGroupByColumnName(groupExpr.getValueExpr());
            
            if ( groupColRelAndName == null || groupColRelAndName.size() != 2 )
            {
                String msg = "Invalid group-by column: " + groupExpr.getValueExpr();
                throw new CreateTMLException(msg);
            }

            r.add(groupColRelAndName.get(1));
        }
        
        return r;
    }
    
    private LinkedList<String> createGroupByTreeML(
            EList<?> groupByClause, LinkedList<String> relationalTML)
        throws CreateTMLException
    {
        LinkedList<String> groupByPred = null;

        for (Iterator<?> groupIt = groupByClause.iterator(); groupIt.hasNext();)
        {
            GroupingExpression groupExpr = (GroupingExpression) groupIt.next();
            System.out.println("Group expr val: " + groupExpr.getValueExpr());
            
            LinkedList<String> groupColRelAndName =
                getGroupByColumnName(groupExpr.getValueExpr());
            
            if ( groupColRelAndName == null || groupColRelAndName.size() != 2 )
            {
                String msg = "Invalid group-by column: " + groupExpr.getValueExpr();
                throw new CreateTMLException(msg);
            }

            // Get unique column name.
            String relName = groupColRelAndName.get(0);
            String columnName = groupColRelAndName.get(1);

            Integer groupBySuffix = 0;
            if ( relationCounters.containsKey(relName) )
                groupBySuffix = relationCounters.get(relName);

            relationCounters.put(relName, groupBySuffix + 1);
            
            String groupByColumnName = columnName + groupBySuffix;

            LinkedList<String> colTerm = new LinkedList<String>();
            colTerm.add(createExpressionNode("eterm"));
            colTerm.addAll(createETerm("attr", columnName,""));
            colTerm = makeBranch(colTerm);

            LinkedList<String> gbcolTerm = new LinkedList<String>();
            gbcolTerm.add(createExpressionNode("eterm"));
            gbcolTerm.addAll(createETerm("attr", groupByColumnName,""));
            gbcolTerm = makeBranch(gbcolTerm);

            LinkedList<String> bterm = new LinkedList<String>();
            bterm.add(createBTermNode("eq"));
            bterm.addAll(colTerm);
            bterm.addAll(gbcolTerm);
            bterm = makeBranch(bterm);
            bterm.addFirst(createBoolExprNode("bterm"));

            if ( groupByPred == null ) {
                groupByPred = new LinkedList<String>();
                groupByPred.addAll(bterm);
            }
            else
            {
                LinkedList<String> conjunct = new LinkedList<String>();
                conjunct.add(createBoolExprNode("and"));
                conjunct.addAll(makeBranch(groupByPred));
                conjunct.addAll(makeBranch(bterm));
                groupByPred = conjunct;
            }
        }
        
        LinkedList<String> r = new LinkedList<String>();
        r.add(createPlanNode("select"));
        r.addAll(makeBranch(groupByPred));
        r.addAll(relationalTML);
        r = makeBranch(r);
        return r;
    }

    // TODO: having

    private String createHavingClauseTreeML(QuerySearchCondition havingClause)
        throws CreateTMLException
    {
        throw new CreateTMLException("HAVING unsupported.");
    }

    // TODO: order by

    private String createOrderClauseTreeML(EList orderByClause)
    {
        for (Iterator it = orderByClause.iterator(); it.hasNext();)
        {
            OrderBySpecification orderBySpec = (OrderBySpecification) it.next();
            if (StatementHelper.isOrderBySpecificationValid(orderBySpec))
            {
            }
        }
        return null;
    }

    // Where clauses, and predicates

    private LinkedList<String> createWhereClauseTreeML(
        QuerySearchCondition whereClause, LinkedList<String> fromTML)
        throws CreateTMLException
    {
        System.out.println("Where: " + whereClause);
        LinkedList<String> r = new LinkedList<String>();

        LinkedList<String> whereTML = createQuerySearchConditionTreeML(whereClause);

        // `Select(pred, fromTML)
        r.add(createPlanNode("select"));
        r.addAll(whereTML);
        r.addAll(fromTML);
        return makeBranch(r);
    }

    private LinkedList<String> createQuerySearchConditionTreeML(
        QuerySearchCondition sc) throws CreateTMLException
    {
        System.out.println("QSC: " + sc.toString());
        LinkedList<String> r = null;

        if (sc instanceof SearchConditionCombined)
        {
            r = createSearchConditionCombinedTreeML((SearchConditionCombined) sc);
        }
        else if (sc instanceof SearchConditionNested)
        {
            r = createSearchConditionNestedTreeML((SearchConditionNested) sc);
        }
        else if (sc instanceof Predicate)
        {

            try
            {

                Method predicateMethod = this.getClass().getDeclaredMethod(
                    "predicateTML", getDispatchClass(sc));
                r = (LinkedList<String>) predicateMethod.invoke(this, sc);

            } catch (NoSuchMethodException e)
            {
                e.printStackTrace();
                throw new CreateTMLException(e.getMessage());
            } catch (IllegalAccessException e)
            {
                e.printStackTrace();
                throw new CreateTMLException(e.getMessage());
            } catch (InvocationTargetException e)
            {
                System.out.println("Failed on: " + sc);
                e.printStackTrace();
                throw new CreateTMLException(e.getCause().getMessage());
            }
        }
        else
        {
            String msg = "Unhandled QuerySearchCondition type: "
                + sc.getClass().getName();

            throw new CreateTMLException(msg);
        }
        return r;
    }

    private String predicateComparisonOpTreeML(PredicateComparisonOperator op,
        boolean mapExpression)
    {
        System.out.println("Pred op: " + op);

        String r = (mapExpression ? "m" : "");
        switch (op.getValue()) {
        case (PredicateComparisonOperator.EQUAL):
            r += "eq";
            break;

        // TODO: standardize "mneq" => "mne"
        case (PredicateComparisonOperator.NOT_EQUAL):
            r = (mapExpression ? "mneq" : "ne");
            break;

        case (PredicateComparisonOperator.GREATER_THAN):
            r += "gt";
            break;

        case (PredicateComparisonOperator.GREATER_THAN_OR_EQUAL):
            r += "ge";
            break;

        case (PredicateComparisonOperator.LESS_THAN):
            r += "lt";
            break;

        case (PredicateComparisonOperator.LESS_THAN_OR_EQUAL):
            r += "le";
            break;

        default:
            break;
        }
        return r;
    }

    private LinkedList<String> predicateTML(PredicateBasic p)
        throws CreateTMLException
    {
        System.out.println("Basic pred: " + p);

        LinkedList<String> r = new LinkedList<String>();
        LinkedList<String> leftTML = null;
        LinkedList<String> rightTML = null;

        boolean mapExpression = false;

        if (p.getLeftValueExpr() == null || p.getRightValueExpr() == null)
        {
            String msg = "Invalid predicate: (left: "
                + (p.getLeftValueExpr() == null) + ") (right: "
                + (p.getRightValueExpr() == null) + ") " + p;

            throw new CreateTMLException(msg);
        }

        LinkedList<QuerySelect> subselects = new LinkedList<QuerySelect>();
        try
        {
            subselects.addAll(getSelectQueries(p.getLeftValueExpr()));
            subselects.addAll(getSelectQueries(p.getRightValueExpr()));

            LinkedList<Function> nestedAggs = getAggregates(subselects);
            mapExpression = !nestedAggs.isEmpty();

            System.out.println("Found " + nestedAggs.size()
                + " nested aggregates.");

        } catch (DBToasterUnhandledException dbte)
        {
            dbte.printStackTrace();
            throw new CreateTMLException(dbte.getMessage());
        }

        leftTML = createQueryValueExpressionTreeML(
            p.getLeftValueExpr(), null, mapExpression);

        rightTML = createQueryValueExpressionTreeML(
            p.getRightValueExpr(), null, mapExpression);

        if (mapExpression)
        {
            // Rewrite in homogeneous form.
            LinkedList<String> tmp = new LinkedList<String>();
            tmp.add(createMapExpressionNode("minus"));
            tmp.addAll(leftTML);
            tmp.addAll(rightTML);
            tmp = makeBranch(tmp);
            tmp.addFirst(createBTermNode(predicateComparisonOpTreeML(
                p.getComparisonOperator(), mapExpression)));

            r.add(createBoolExprNode("bterm"));
            r.addAll(makeBranch(tmp));
        }
        else
        {
            LinkedList<String> tmp = new LinkedList<String>();

            tmp.add(createBTermNode(predicateComparisonOpTreeML(p
                .getComparisonOperator(), mapExpression)));

            tmp.addAll(leftTML);
            tmp.addAll(rightTML);

            r.add(createBoolExprNode("bterm"));
            r.addAll(makeBranch(tmp));
        }

        return makeBranch(r);
    }

    private LinkedList<String> createSearchConditionCombinedTreeML(
        SearchConditionCombined pred) throws CreateTMLException
    {
        System.out.println("SC: " + pred);

        LinkedList<String> r = new LinkedList<String>();
        r.add(createSearchConditionCombinedOperatorTreeML(pred
            .getCombinedOperator()));

        LinkedList<String> leftTML = null;
        LinkedList<String> rightTML = null;

        if (pred.getLeftCondition() != null)
        {
            leftTML = createQuerySearchConditionTreeML(pred.getLeftCondition());
        }

        if (pred.getRightCondition() != null)
        {
            rightTML = createQuerySearchConditionTreeML(pred
                .getRightCondition());
        }

        if (leftTML == null || rightTML == null)
        {
            String msg = "Invalid bool expr: (left: " + (leftTML == null)
                + ") (right: " + (rightTML == null) + ") " + pred;

            throw new CreateTMLException(msg);
        }

        r.addAll(leftTML);
        r.addAll(rightTML);

        return makeBranch(r);
    }

    private String createSearchConditionCombinedOperatorTreeML(
        SearchConditionCombinedOperator op) throws 	CreateTMLException
    {
        System.out.println("SC op: " + op);

        String r = "";
        switch (op.getValue()) {
        case (SearchConditionCombinedOperator.AND):
        	r = createBoolExprNode("and");
            break;
        case (SearchConditionCombinedOperator.OR):
            r = createBoolExprNode("or");
            break;
        }

        return r;
    }

    private LinkedList<String> createSearchConditionNestedTreeML(
        SearchConditionNested pred) throws CreateTMLException
    {
        System.out.println("SC nested: " + pred);
        return createQuerySearchConditionTreeML(pred.getNestedCondition());
    }

    // TODO:

    private LinkedList<String> predicateTML(PredicateBetween p)
        throws CreateTMLException
    {
        throw new CreateTMLException("Unsupported: PredicateBetween");
    }

    private LinkedList<String> predicateTML(PredicateExists p)
        throws CreateTMLException
    {
        throw new CreateTMLException("Unsupported: PredicateExists.");
    }

    private LinkedList<String> predicateTML(PredicateIn p)
        throws CreateTMLException
    {
        throw new CreateTMLException("Unsupported: PredicateIn");
    }

    private LinkedList<String> predicateTML(PredicateInValueList p)
        throws CreateTMLException
    {
        throw new CreateTMLException("Unsupported: PredicateInValue");
    }

    private LinkedList<String> predicateTML(PredicateInValueRowSelect p)
        throws CreateTMLException
    {
        throw new CreateTMLException("Unsupported: PredicateInValueRowSelect");
    }

    private LinkedList<String> predicateTML(PredicateInValueSelect p)
        throws CreateTMLException
    {
        throw new CreateTMLException("Unsupported: PredicateInValueSelect");
    }

    private LinkedList<String> predicateTML(PredicateIsNull p)
        throws CreateTMLException
    {
        throw new CreateTMLException("Unsupported: PredicateIsNull");
    }

    private LinkedList<String> predicateTML(PredicateLike p)
        throws CreateTMLException
    {
        throw new CreateTMLException("Unsupported: PredicateLike");
    }

    private LinkedList<String> predicateTML(PredicateQuantified p)
        throws CreateTMLException
    {
        throw new CreateTMLException("Unsupported: PredicateQuantified");
    }

    private LinkedList<String> predicateTML(PredicateQuantifiedRowSelect p)
        throws CreateTMLException
    {
        throw new CreateTMLException(
            "Unsupported: PredicateQuantifiedRowSelect");
    }

    private LinkedList<String> predicateTML(PredicateQuantifiedType p)
        throws CreateTMLException
    {
        throw new CreateTMLException("Unsupported: PredicateQuantifiedType");
    }

    private LinkedList<String> predicateTML(PredicateQuantifiedValueSelect p)
        throws CreateTMLException
    {
        throw new CreateTMLException(
            "Unsupported: PredicateQuantifiedValueSelect");
    }

    // Predicate overloads dispatch helpers

    String getInterfaceName(Class<?> sqlObjectClass)
    {
        if (sqlObjectClass == null)
        {
            return null;
        }

        StringBuffer className = null;
        String interfaceName = null;

        className = new StringBuffer(sqlObjectClass.getName());

        // get the interface type of the given SQLQueryObject
        if (sqlObjectClass.getPackage().getName().endsWith("impl"))
        {
            int implStart = className.lastIndexOf(".impl.") + 1;
            int implEnd = implStart + 5;
            className.delete(implStart, implEnd);
        }
        // we are only working with interfaces
        if (sqlObjectClass.getName().endsWith("Impl"))
        {
            className.delete(className.length() - 4, className.length());
        }

        interfaceName = className.toString();
        return interfaceName;
    }

    private Class<?> getDispatchClass(SQLObject sqlObject)
    {
        if (sqlObject == null)
        {
            return null;
        }

        String interfaceName = null;

        Class<?> sqlObjectClass = sqlObject.getClass();
        Class<?> sqlObjectInterfaceClass = sqlObjectClass;

        if (sqlObjectClass.getName().endsWith("Impl"))
        {
            // if we have an impl we need to find its interface as all
            // appendSQL methods have the interface as argument
            // Class.forName doesn't help us in the eclipse runtime as
            // the class loader of the SQLQuery model has no access to its
            // extending plugins (no runtime dependency)
            interfaceName = getInterfaceName(sqlObject.getClass());
            Class<?>[] sqlObjectInterfaces = sqlObjectClass.getInterfaces();

            for (int i = 0; i < sqlObjectInterfaces.length; i++)
            {
                Class<?> interfaceClass = sqlObjectClass.getInterfaces()[i];
                if (interfaceClass.getName().equals(interfaceName))
                {
                    sqlObjectInterfaceClass = interfaceClass;
                    break;
                }
            }
        }

        return sqlObjectInterfaceClass;
    }

}
