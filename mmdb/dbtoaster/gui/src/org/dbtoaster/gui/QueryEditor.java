package org.dbtoaster.gui;

import java.awt.BorderLayout;
import java.awt.Container;
import java.awt.Frame;
import java.awt.Graphics;
import java.awt.Panel;
import java.io.ByteArrayInputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Iterator;
import java.util.List;

import javax.swing.JRootPane;

import org.eclipse.datatools.modelbase.sql.query.GroupingSpecification;
import org.eclipse.datatools.modelbase.sql.query.OrderBySpecification;
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
import org.eclipse.datatools.modelbase.sql.query.QuerySearchCondition;
import org.eclipse.datatools.modelbase.sql.query.QuerySelect;
import org.eclipse.datatools.modelbase.sql.query.QuerySelectStatement;
import org.eclipse.datatools.modelbase.sql.query.QueryStatement;
import org.eclipse.datatools.modelbase.sql.query.SearchConditionCombined;
import org.eclipse.datatools.modelbase.sql.query.SearchConditionCombinedOperator;
import org.eclipse.datatools.modelbase.sql.query.TableReference;
import org.eclipse.datatools.modelbase.sql.query.helper.StatementHelper;
import org.eclipse.datatools.modelbase.sql.query.util.SQLQuerySourceWriter;
import org.eclipse.datatools.modelbase.sql.schema.SQLObject;
import org.eclipse.datatools.sqltools.parsers.sql.SQLParseErrorInfo;
import org.eclipse.datatools.sqltools.parsers.sql.SQLParserException;
import org.eclipse.datatools.sqltools.parsers.sql.SQLParserInternalException;
import org.eclipse.datatools.sqltools.parsers.sql.query.SQLQueryParseResult;
import org.eclipse.datatools.sqltools.parsers.sql.query.SQLQueryParserManager;
import org.eclipse.datatools.sqltools.parsers.sql.query.SQLQueryParserManagerProvider;
import org.eclipse.emf.common.util.EList;
import org.eclipse.swt.SWT;
import org.eclipse.swt.awt.SWT_AWT;
import org.eclipse.swt.events.ControlEvent;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.SelectionListener;
import org.eclipse.swt.graphics.Font;
import org.eclipse.swt.graphics.FontData;
import org.eclipse.swt.layout.FillLayout;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Text;
import org.eclipse.ui.part.ViewPart;

import prefuse.Constants;
import prefuse.controls.ControlAdapter;
import prefuse.data.Tree;
import prefuse.data.io.DataIOException;
import prefuse.data.io.TreeMLReader;
import prefuse.util.ColorLib;

public class QueryEditor extends ViewPart
{
	public static final String ID = "dbtoaster_gui.queryeditor";
	
	private final String DEFAULT_QUERY =
		"select avg(b2.price * b2.volume)" +
		"\n\tfrom bids b2" +
		"\n\twhere k*(select sum(volume) from bids) > " +
		"\n\t\t(select sum(volume) from bids b1" +
		"\n\t\t\twhere b1.price > b2.price);";

	private final String DEFAULT_TML =
		"<tree>"+
		"<declarations>"+
		"<attributeDecl name=\"id\" type=\"String\"/>" +
		"<attributeDecl name=\"op\" type=\"String\"/>" +
		"<attributeDecl name=\"param\" type=\"String\"/>" +
		"<attributeDecl name=\"incr\" type=\"boolean\"/>" +
		"</declarations>" +
		"<branch>" +
			"<attribute name=\"id\" value=\"a\"/>" +
			"<attribute name=\"op\" value=\"sum\"/>" +
			"<attribute name=\"param\" value=\"p2 * v2\"/>" +
			"<attribute name=\"incr\" value=\"false\"/>" +
			"<branch>" +
				"<attribute name=\"id\" value=\"b\"/>" +
				"<attribute name=\"op\" value=\"select\"/>" +
				"<attribute name=\"param\" value=\"l &gt; r\"/>" +
				"<attribute name=\"incr\" value=\"false\"/>" +
				"<leaf>" +
					"<attribute name=\"id\" value=\"c\"/>" +
					"<attribute name=\"op\" value=\"table R\"/>" +
					"<attribute name=\"param\" value=\"p,v\"/>" +
					"<attribute name=\"incr\" value=\"false\"/>" +
				"</leaf>" +
				"<branch>" +
					"<attribute name=\"id\" value=\"d\"/>" +
					"<attribute name=\"op\" value=\"&lt;\"/>" +
					"<attribute name=\"param\" value=\"none\"/>" +
					"<attribute name=\"incr\" value=\"false\"/>" +
					"<branch>" +
						"<attribute name=\"id\" value=\"e\"/>" +
						"<attribute name=\"op\" value=\"sum\"/>" +
						"<attribute name=\"param\" value=\"v\"/>" +
						"<attribute name=\"incr\" value=\"false\"/>" +
						"<leaf>" +
							"<attribute name=\"id\" value=\"f\"/>" +
							"<attribute name=\"op\" value=\"table R\"/>" +
							"<attribute name=\"param\" value=\"p,v\"/>" +
							"<attribute name=\"incr\" value=\"TRUE\"/>" +
						"</leaf>" +
					"</branch>" +
					"<branch>" +
						"<attribute name=\"id\" value=\"g\"/>" +
						"<attribute name=\"op\" value=\"sum\"/>" +
						"<attribute name=\"param\" value=\"v\"/>" +
						"<attribute name=\"incr\" value=\"false\"/>" +
						"<branch>" +
							"<attribute name=\"id\" value=\"h\"/>" +
							"<attribute name=\"op\" value=\"select\"/>" +
							"<attribute name=\"param\" value=\"l.price &gt; p2\"/>" +
							"<attribute name=\"incr\" value=\"false\"/>" +
							"<leaf>" +
								"<attribute name=\"id\" value=\"i\"/>" +
								"<attribute name=\"op\" value=\"table R\"/>" +
								"<attribute name=\"param\" value=\"p,v\"/>" +
								"<attribute name=\"incr\" value=\"false\"/>" +
							"</leaf>" +
						"</branch>" +
					"</branch>" +
				"</branch>" +
			"</branch>" +
		"</branch>" +
		"</tree>";
	
	class QueryTextVis extends Composite {

		private Text queryText;
		private TreeVisPanel queryPanel;

	    private static final String incrFrontierNodes = "tree.incr";
	    private static final String derivationNodes = "tree.derivation";

		public QueryTextVis(Composite parent, int style) {
			super(parent, style);
			setLayout(new GridLayout());
			
			Composite visComposite =
				new Composite(this, SWT.EMBEDDED | SWT.NO_BACKGROUND);
			
			try {
				System.setProperty("sun.awt.noerasebackground", "true");
			} catch (NoSuchMethodError e) {}

			Frame visFrame = SWT_AWT.new_Frame(visComposite);
			Panel visPanel = new Panel(new BorderLayout()) {
				private static final long serialVersionUID = 2839020058150476495L;
				
				public void update(Graphics g) { paint(g); }
			};
			
			visFrame.add(visPanel);
			JRootPane visRoot = new JRootPane();
			visPanel.add(visRoot);

			try {
				Tree t = (Tree) new TreeMLReader().readGraph(
					new ByteArrayInputStream(DEFAULT_TML.getBytes()));
				
				queryPanel =
					new TreeVisPanel(t, "op", "SQL Query",
						Constants.ORIENT_TOP_BOTTOM);

				queryPanel.selectNodes(
					incrFrontierNodes, "id", "incr = true",
					ColorLib.rgb(255,128,90));
				
			} catch (DataIOException e) {
				e.printStackTrace();
				queryPanel = new TreeVisPanel(null);
			}
			
			Container contentPane = visRoot.getContentPane();
			contentPane.setLayout(new BorderLayout());
			contentPane.add(queryPanel);
			
			visFrame.pack();
			visFrame.setVisible(true);

			visComposite.setLayoutData(
				new GridData(GridData.FILL, GridData.FILL, true, true));
		
			queryText = new Text(this, style);
			queryText.setLayoutData(
				new GridData(GridData.FILL, GridData.FILL, true, true));
				
			queryText.setText(DEFAULT_QUERY);
			FontData fd = new FontData(
				queryText.getFont().getFontData()[0].toString());
			fd.setHeight(14);
			queryText.setFont(new Font(queryText.getFont().getDevice(), fd));
		}
		
		public void setText(String text) {
			queryText.setText(text);
		}
		
		public String toastQuery() {
			
			String returnStatus = "DBToaster failed to compile!";

            try {
                SQLQueryParserManager parserManager =
                	SQLQueryParserManagerProvider.getInstance().
                		getParserManager(null, null);

                SQLQueryParseResult parseResult =
                	parserManager.parseQuery(queryText.getText());
                
                QueryStatement userQuery = parseResult.getQueryStatement();
                
                String parsedSQL = userQuery.getSQL();
                System.out.println("Toasting:" + parsedSQL);

                switch(StatementHelper.getStatementType(userQuery)) {
	                case StatementHelper.STATEMENT_TYPE_FULLSELECT:
	                	returnStatus = "Found FULLSELECT query.";
	                	break;
	
	                case StatementHelper.STATEMENT_TYPE_SELECT:
	                	System.out.println("TreeML: " +
                			createSelectTreeML((QuerySelectStatement) userQuery));
	                	returnStatus = "Found SELECT query.";
	                	break;
	                
	                default:
	                	// Set return status.
	                	returnStatus = "Invalid query for compilation.";
                }
   
            } catch (SQLParserException spe) {
                // handle the syntax error
                System.out.println(spe.getMessage());
                
                List syntacticErrors = spe.getErrorInfoList();
                Iterator itr = syntacticErrors.iterator();
                while (itr.hasNext()) {
                    SQLParseErrorInfo errorInfo = (SQLParseErrorInfo) itr.next();
                    // Example usage of the SQLParseErrorInfo object
                    // the error message
                    String errorMessage = errorInfo.getParserErrorMessage();
                    // the line numbers of error
                    int errorLine = errorInfo.getLineNumberStart();
                    int errorColumn = errorInfo.getColumnNumberStart();
                }
            } catch (SQLParserInternalException spie) {
                // handle the exception
                System.out.println(spie.getMessage());
            }
            
            return returnStatus;
		}
	
		public String createSelectTreeML(QuerySelectStatement selectStmt) {
			String header = "<tree>";
			String footer = "</tree>";
			String decls = "<declarations>" +
				"<attributeDecl name=\"op\" type=\"String\"/>" +
				"<attributeDecl name=\"param\" type=\"String\"/>" +
				"</declarations>";

			// TODO: handle INTO clause -- possible failing?
			QuerySelect select = (QuerySelect) selectStmt.getQueryExpr().getQuery();
			String body = createSelectListTreeML(select.getSelectClause()) +
				createFromClauseTreeML(select.getFromClause()) +
				createWhereClauseTreeML(select.getWhereClause()) +
				createGroupByTreeML(select.getGroupByClause()) +
				createHavingClauseTreeML(select.getHavingClause()) +
				createOrderClauseTreeML(selectStmt.getOrderByClause());

			String r = header + decls + body + footer;
			return r;
		}

		private String createOrderClauseTreeML(EList orderByClause) {
			for (Iterator it = orderByClause.iterator(); it.hasNext();)
            {
                OrderBySpecification orderBySpec = (OrderBySpecification) it.next();
                if (StatementHelper.isOrderBySpecificationValid(orderBySpec)) {
                }
            }
			return null;
		}

		private String createHavingClauseTreeML(QuerySearchCondition havingClause) {
			// TODO Auto-generated method stub
			return null;
		}

		private String createGroupByTreeML(EList groupByClause) {
			for (Iterator groupIt = groupByClause.iterator(); groupIt.hasNext();)
            {
                GroupingSpecification groupSpec = (GroupingSpecification) groupIt.next();
            }
			return null;
		}

	    String getInterfaceName(Class sqlObjectClass)
	    {
	        if (sqlObjectClass == null) { return null; }

	        StringBuffer className = null;
	        String interfaceName = null;
	        
	        className =
	        	new StringBuffer(sqlObjectClass.getName());
	        
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
	    
		private Class getDispatchClass(SQLObject sqlObject) {
	        if (sqlObject == null) { return null; }

	        String interfaceName = null;

	        Class sqlObjectClass = sqlObject.getClass();
	        Class sqlObjectInterfaceClass = sqlObjectClass;

	        if (sqlObjectClass.getName().endsWith("Impl"))
	        {
	            // if we have an impl we need to find its interface as all
	            // appendSQL methods have the interface as argument
	            // Class.forName doesn't help us in the eclipse runtime as
	            // the class loader of the SQLQuery model has no access to its
	            // extending plugins (no runtime dependency)
	            interfaceName = getInterfaceName(sqlObject.getClass());
	            Class[] sqlObjectInterfaces = sqlObjectClass.getInterfaces();
	            
	            for (int i = 0; i < sqlObjectInterfaces.length; i++)
	            {
	                Class interfaceClass = sqlObjectClass.getInterfaces()[i];
	                if (interfaceClass.getName().equals(interfaceName))
	                {
	                    sqlObjectInterfaceClass = interfaceClass;
	                    break;
	                }
	            }
	        }
	        
	        return sqlObjectInterfaceClass;
		}

		private String createWhereClauseTreeML(QuerySearchCondition whereClause) {
			if ( whereClause instanceof SearchConditionCombined ) {
				searchConditionCombinedTML(
					(SearchConditionCombined) whereClause);
			}
			else {

				try {
			        
					Method predicateMethod =
						this.getClass().getDeclaredMethod(
							"predicateTML", getDispatchClass(whereClause));
					predicateMethod.invoke(this, whereClause);
					
				} catch (NoSuchMethodException e) {
					e.printStackTrace();
				} catch (IllegalAccessException e) {
					e.printStackTrace();
				} catch (InvocationTargetException e) {
					e.printStackTrace();
				}
			}
			
			return null;
		}

		// Predicates
		private String predicateTML(PredicateBasic p) {
			return null;
		}
		
		private String predicateTML(PredicateBetween p) {
			return null;
		}
		
		private String predicateTML(PredicateExists p) {
			return null;
		}
		
		private String predicateTML(PredicateIn p) {
			return null;
		}
		
		private String predicateTML(PredicateInValueList p) {
			return null;
		}
		
		private String predicateTML(PredicateInValueRowSelect p) {
			return null;
		}
		
		private String predicateTML(PredicateInValueSelect p) {
			return null;
		}
		
		private String predicateTML(PredicateIsNull p) {
			return null;
		}
		
		private String predicateTML(PredicateLike p) {
			return null;
		}
		
		private String predicateTML(PredicateQuantified p) {
			return null;
		}
		
		private String predicateTML(PredicateQuantifiedRowSelect p) {
			return null;
		}
		
		private String predicateTML(PredicateQuantifiedType p) {
			return null;
		}
		
		private String predicateTML(PredicateQuantifiedValueSelect p) {
			return null;
		}
		

		// From clause
		
		private String createFromClauseTreeML(EList fromClause) {
			for (Iterator fromIt = fromClause.iterator(); fromIt.hasNext();)
            {
                TableReference tableRef = (TableReference) fromIt.next();

            }
			return null;
		}
		
		private String createSelectListTreeML(EList selectList) {
			return null;
		}
		
		// Expressions
		private String predicateComparisonOpTreeML(PredicateComparisonOperator op) {
			if ( op == PredicateComparisonOperator.EQUAL_LITERAL ) {
				
			}
			else if ( op == PredicateComparisonOperator.NOT_EQUAL_LITERAL ) {
				
			}
			else if ( op == PredicateComparisonOperator.GREATER_THAN_LITERAL ) {
				
			}
			else if ( op == PredicateComparisonOperator.GREATER_THAN_OR_EQUAL_LITERAL ) {
				
			}
			else if ( op == PredicateComparisonOperator.LESS_THAN_LITERAL) {
				
			}
			else if ( op == PredicateComparisonOperator.LESS_THAN_OR_EQUAL_LITERAL ) {
				
			}
			return null;
		}
		
		private String searchConditionCombinedTML(SearchConditionCombined pred) {
			return null;
		}
		
		private String searchConditionCombinedOperatorTreeML(SearchConditionCombinedOperator op) {
			if ( op == SearchConditionCombinedOperator.AND_LITERAL ) {
				
			}
			else if ( op == SearchConditionCombinedOperator.OR_LITERAL ) {
				
			}
			return null;
		}
		
	};
	
	private QueryTextVis query;
	private Text status;
	private Button toast;
	
	public QueryEditor() {}

	public void createPartControl(Composite parent) {
		Composite top = new Composite(parent, SWT.NONE);
		GridLayout layout = new GridLayout();
		layout.marginWidth = 0;
		layout.marginHeight = 0;
		layout.numColumns = 2;
		top.setLayout(layout);

		query = new QueryTextVis(top, SWT.BORDER | SWT.MULTI | SWT.WRAP);
		GridData queryLayoutData =
			new GridData(GridData.FILL, GridData.FILL, true, true);
		queryLayoutData.horizontalSpan = 2;
		query.setLayoutData(queryLayoutData);
		
		status = new Text(top, SWT.BORDER | SWT.WRAP);
		GridData gridData = new GridData(SWT.FILL, SWT.FILL, true, false);
		status.setBackground(status.getDisplay().getSystemColor(
				SWT.COLOR_TITLE_INACTIVE_BACKGROUND));
		status.setForeground(status.getDisplay().getSystemColor(
				SWT.COLOR_TITLE_INACTIVE_FOREGROUND));
		gridData.heightHint = status.getLineHeight() * 2;
		status.setLayoutData(gridData);
		status.setText("Welcome to DBToaster v0.1!");
		
		toast = new Button(top, SWT.PUSH);
		toast.setBackground(toast.getDisplay().getSystemColor(SWT.COLOR_DARK_RED));
		toast.setForeground(toast.getDisplay().getSystemColor(SWT.COLOR_RED));
		toast.setText("TOAST!");
		
		GridData toastGridData = new GridData(SWT.END, SWT.FILL, false, false);
		toastGridData.minimumWidth = 50;
		toastGridData.heightHint = status.getLineHeight() * 2;
		toast.setLayoutData(toastGridData);
		
		toast.addSelectionListener(new SelectionListener() {
			public void widgetSelected(SelectionEvent e) {
				status.setText(query.toastQuery());
			}

			public void widgetDefaultSelected(SelectionEvent e) {}
		});
	}

	public void setFocus() {
		query.setFocus();
	}

}
