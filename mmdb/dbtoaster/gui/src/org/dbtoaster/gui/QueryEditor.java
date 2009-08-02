package org.dbtoaster.gui;

import java.awt.BorderLayout;
import java.awt.Container;
import java.awt.Frame;
import java.awt.Graphics;
import java.awt.Panel;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.util.Map;

import javax.swing.JRootPane;

import org.dbtoaster.model.DBToasterWorkspace;
import org.dbtoaster.model.DatasetManager;
import org.dbtoaster.model.Query;
import org.dbtoaster.model.Compiler.CompileMode;
import org.dbtoaster.model.DatasetManager.Dataset;
import org.eclipse.swt.SWT;
import org.eclipse.swt.awt.SWT_AWT;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.SelectionListener;
import org.eclipse.swt.graphics.Font;
import org.eclipse.swt.graphics.FontData;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Text;
import org.eclipse.swt.widgets.TreeItem;
import org.eclipse.ui.part.ViewPart;

import prefuse.Constants;
import prefuse.data.Tree;
import prefuse.data.io.DataIOException;
import prefuse.data.io.TreeMLReader;
import prefuse.util.ColorLib;

public class QueryEditor extends ViewPart
{
    public static final String ID = "dbtoaster_gui.queryeditor";

    private final String DEFAULT_QUERY = "select sum(bids.p * bids.v)"
            + "\n\tfrom bids"
            + "\n\twhere 0.25*(select sum(b2.v) from bids b2) > "
            + "\n\t\t(select sum(b1.v) from bids b1"
            + "\n\t\t\twhere b1.p > bids.p);";

    private final String DEFAULT_TML = "<tree>" + "<declarations>"
            + "<attributeDecl name=\"id\" type=\"String\"/>"
            + "<attributeDecl name=\"op\" type=\"String\"/>"
            + "<attributeDecl name=\"param\" type=\"String\"/>"
            + "<attributeDecl name=\"incr\" type=\"boolean\"/>"
            + "</declarations>" + "<branch>"
            + "<attribute name=\"id\" value=\"a\"/>"
            + "<attribute name=\"op\" value=\"sum\"/>"
            + "<attribute name=\"param\" value=\"p2 * v2\"/>"
            + "<attribute name=\"incr\" value=\"false\"/>" + "<branch>"
            + "<attribute name=\"id\" value=\"b\"/>"
            + "<attribute name=\"op\" value=\"select\"/>"
            + "<attribute name=\"param\" value=\"l &gt; r\"/>"
            + "<attribute name=\"incr\" value=\"false\"/>" + "<leaf>"
            + "<attribute name=\"id\" value=\"c\"/>"
            + "<attribute name=\"op\" value=\"table R\"/>"
            + "<attribute name=\"param\" value=\"p,v\"/>"
            + "<attribute name=\"incr\" value=\"false\"/>" + "</leaf>"
            + "<branch>" + "<attribute name=\"id\" value=\"d\"/>"
            + "<attribute name=\"op\" value=\"&lt;\"/>"
            + "<attribute name=\"param\" value=\"none\"/>"
            + "<attribute name=\"incr\" value=\"false\"/>" + "<branch>"
            + "<attribute name=\"id\" value=\"e\"/>"
            + "<attribute name=\"op\" value=\"sum\"/>"
            + "<attribute name=\"param\" value=\"v\"/>"
            + "<attribute name=\"incr\" value=\"false\"/>" + "<leaf>"
            + "<attribute name=\"id\" value=\"f\"/>"
            + "<attribute name=\"op\" value=\"table R\"/>"
            + "<attribute name=\"param\" value=\"p,v\"/>"
            + "<attribute name=\"incr\" value=\"TRUE\"/>" + "</leaf>"
            + "</branch>" + "<branch>" + "<attribute name=\"id\" value=\"g\"/>"
            + "<attribute name=\"op\" value=\"sum\"/>"
            + "<attribute name=\"param\" value=\"v\"/>"
            + "<attribute name=\"incr\" value=\"false\"/>" + "<branch>"
            + "<attribute name=\"id\" value=\"h\"/>"
            + "<attribute name=\"op\" value=\"select\"/>"
            + "<attribute name=\"param\" value=\"l.price &gt; p2\"/>"
            + "<attribute name=\"incr\" value=\"false\"/>" + "<leaf>"
            + "<attribute name=\"id\" value=\"i\"/>"
            + "<attribute name=\"op\" value=\"table R\"/>"
            + "<attribute name=\"param\" value=\"p,v\"/>"
            + "<attribute name=\"incr\" value=\"false\"/>" + "</leaf>"
            + "</branch>" + "</branch>" + "</branch>" + "</branch>"
            + "</branch>" + "</tree>";

    class DatasetPanel
    {
        DatasetManager datasets;
        Label datasetLbl;
        org.eclipse.swt.widgets.Tree datasetsTree;

        public DatasetPanel(Composite parent)
        {
            Composite dpComp = new Composite(parent, SWT.NONE);
            GridLayout layout = new GridLayout();
            layout.marginHeight = 0;
            layout.marginWidth = 0;
            layout.numColumns = 1;
            dpComp.setLayout(layout);

            GridData dpLayoutData = new GridData(GridData.FILL, GridData.FILL,
                    false, true);
            dpLayoutData.widthHint = 250;
            dpComp.setLayoutData(dpLayoutData);

            datasetLbl = new Label(dpComp, SWT.NONE);
            datasetLbl.setText("Dataset browser:");

            datasetsTree = new org.eclipse.swt.widgets.Tree(dpComp, SWT.SINGLE
                    | SWT.BORDER);

            GridData treeLayoutData = new GridData(GridData.FILL,
                    GridData.FILL, true, true);
            datasetsTree.setLayoutData(treeLayoutData);

            datasets = DBToasterWorkspace.getWorkspace().getDatasetManager();
            init();
        }

        private void init()
        {
            for (String dn : datasets.getDatasetNames())
            {
                Dataset ds = datasets.getDataset(dn);
                TreeItem datasetItem = new TreeItem(datasetsTree, SWT.NONE);
                datasetItem.setText(dn);

                for (String rn : ds.getRelationNames())
                {
                    TreeItem relationItem = new TreeItem(datasetItem, SWT.NONE);
                    relationItem.setText(rn);
                    for (Map.Entry<String, String> fieldEntry : ds
                            .getRelationFields(rn).entrySet())
                    {
                        TreeItem fieldItem = new TreeItem(relationItem,
                                SWT.NONE);
                        fieldItem.setText(fieldEntry.getKey() + " : "
                                + fieldEntry.getValue());
                    }
                }
            }
        }
    };

    class QueryHistoryPanel
    {
        private org.eclipse.swt.widgets.Tree qhTree;
        private DBToasterWorkspace dbtWorkspace;

        public QueryHistoryPanel(Composite parent)
        {
            dbtWorkspace = DBToasterWorkspace.getWorkspace();
            Label qhLabel = new Label(parent, SWT.NONE);
            qhLabel.setText("Input history:");
            GridData qhLabelLD = new GridData(SWT.FILL, SWT.FILL, false, false);
            qhLabelLD.widthHint = 250;
            qhLabel.setLayoutData(qhLabelLD);

            qhTree = new org.eclipse.swt.widgets.Tree(parent, SWT.SINGLE
                    | SWT.BORDER);
            GridData qhTreeLD = new GridData(SWT.FILL, SWT.FILL, false, true);
            qhTreeLD.widthHint = 250;
            qhTree.setLayoutData(qhTreeLD);

            redraw();
        }

        private void addQuery(String queryName, Query q)
        {
            TreeItem qItem = new TreeItem(qhTree, SWT.NONE);
            qItem.setText(queryName);

            if (q.isParsed())
            {
                TreeItem parseItem = new TreeItem(qItem, SWT.NONE);
                parseItem.setText("Parsed: " + q.getTML().getName());
            }

            if (q.isCompiled())
            {
                TreeItem codeItem = new TreeItem(qItem, SWT.NONE);
                codeItem.setText("Code: " + q.getCode().getName());

                TreeItem traceItem = new TreeItem(qItem, SWT.NONE);
                traceItem.setText("Trace: "
                        + q.getTrace().getEventPaths().size() + " event paths");
            }

            // TODO: add TreeItem metadata
            if (q.hasBinary())
            {
                TreeItem binItem = new TreeItem(qItem, SWT.NONE);
                binItem.setText("Engine: ");
            }

            if (q.hasDebugger())
            {
                TreeItem debugItem = new TreeItem(qItem, SWT.NONE);
                debugItem.setText("Debugger: ");
            }

            if (q.hasStatistics())
            {
                TreeItem statsItem = new TreeItem(qItem, SWT.NONE);
                statsItem.setText("Statistics: ");
            }
        }

        private void addQuery(String queryName)
        {
            Query q = dbtWorkspace.getQuery(queryName);
            if (q != null) addQuery(queryName, q);
        }

        public void redraw()
        {
            qhTree.clearAll(true);
            for (Map.Entry<String, Query> e : dbtWorkspace.getWorkingQueries()
                    .entrySet())
            {
                addQuery(e.getKey(), e.getValue());
            }
        }

        public void addSelectionListener(SelectionListener sl)
        {
            qhTree.addSelectionListener(sl);
        }
    };

    class QueryTextAndVis extends Composite
    {
        DBToasterWorkspace dbtWorkspace;

        private QueryHistoryPanel history;
        private Query currentQuery;

        private Text queryText;
        private TreeVisPanel queryPanel;

        private static final String incrFrontierNodes = "tree.incr";

        // private static final String derivationNodes = "tree.derivation";

        public QueryTextAndVis(Composite parent, int style,
                QueryHistoryPanel qhPanel)
        {
            super(parent, style);

            dbtWorkspace = DBToasterWorkspace.getWorkspace();
            history = qhPanel;
            currentQuery = null;

            // Add selection listener to the history panel to display the
            // sql code for the query in history.
            history.addSelectionListener(new SelectionListener()
            {
                public void widgetSelected(SelectionEvent e)
                {
                    // Load the query selected if valid.
                    TreeItem[] qSels = history.qhTree.getSelection();
                    if (qSels.length > 0)
                    {
                        String queryName = qSels[0].getText();
                        Query q = dbtWorkspace.getQuery(queryName);
                        if (q != null)
                        {
                            // Save the current query if not already saved.
                            if (currentQuery == null) saveQuery();

                            currentQuery = q;
                            queryText.setText(q.getQuery());
                        }
                    }
                }

                public void widgetDefaultSelected(SelectionEvent e)
                {
                }
            });

            // GUI setup
            GridLayout qtvLayout = new GridLayout();
            qtvLayout.marginWidth = 0;
            qtvLayout.marginHeight = 0;
            setLayout(qtvLayout);

            Composite visComposite = new Composite(this, SWT.EMBEDDED
                    | SWT.NO_BACKGROUND);

            try
            {
                System.setProperty("sun.awt.noerasebackground", "true");
            } catch (NoSuchMethodError e)
            {
            }

            Frame visFrame = SWT_AWT.new_Frame(visComposite);
            Panel visPanel = new Panel(new BorderLayout())
            {
                private static final long serialVersionUID = 2839020058150476495L;

                public void update(Graphics g)
                {
                    paint(g);
                }
            };

            visFrame.add(visPanel);
            JRootPane visRoot = new JRootPane();
            visPanel.add(visRoot);

            try
            {
                Tree t = (Tree) new TreeMLReader()
                        .readGraph(new ByteArrayInputStream(DEFAULT_TML
                                .getBytes()));

                queryPanel = new TreeVisPanel(t, "op", "SQL Query",
                        Constants.ORIENT_TOP_BOTTOM);

                queryPanel.selectNodes(incrFrontierNodes, "id", "incr = true",
                        ColorLib.rgb(255, 128, 90));

            } catch (DataIOException e)
            {
                e.printStackTrace();
                queryPanel = new TreeVisPanel(null);
            }

            Container contentPane = visRoot.getContentPane();
            contentPane.setLayout(new BorderLayout());
            contentPane.add(queryPanel);

            visFrame.pack();
            visFrame.setVisible(true);

            visComposite.setLayoutData(new GridData(GridData.FILL,
                    GridData.FILL, true, true));

            queryText = new Text(this, style);
            queryText.setLayoutData(new GridData(GridData.FILL, GridData.FILL,
                    true, true));

            queryText.setText(DEFAULT_QUERY);
            FontData fd = new FontData(queryText.getFont().getFontData()[0]
                    .toString());
            fd.setHeight(14);
            queryText.setFont(new Font(queryText.getFont().getDevice(), fd));
        }

        public void setText(String text)
        {
            queryText.setText(text);
        }

        public String saveQuery()
        {
            String queryName = dbtWorkspace.createQuery(queryText.getText());
            history.addQuery(queryName);
            return queryName;
        }

        public String toastQuery()
        {
            System.out.println(getClass().getName() + ".toastQuery(): dbtWs "
                    + (dbtWorkspace == null ? "null" : "valid"));

            String queryName = dbtWorkspace.createQuery(queryText.getText());
            currentQuery = dbtWorkspace.getQuery(queryName);

            String codeFile = queryName + "."
                    + DBToasterWorkspace.CODE_FILE_EXT;
            String compilationStatus = dbtWorkspace.compileQuery(queryName,
                    codeFile, CompileMode.ENGINE);

            System.out.println("Toasted query: " + queryName);

            // Add query to history.
            // TODO: replace with listener to underlying data in history
            history.addQuery(queryName);

            return compilationStatus;
        }
    };

    private DatasetPanel datasetPnl;
    private QueryHistoryPanel historyPnl;
    private QueryTextAndVis queryTv;
    private Text statusText;
    private Button toastBtn;

    public void createPartControl(Composite parent)
    {
        // TODO: move SQL editing to an eclipse editor
        /*
         * IWorkspace ws = ResourcesPlugin.getWorkspace(); IProjectDescription
         * projDesc = ws.newProjectDescription("DBToaster Queries");
         * projDesc.setLocation(new Path("/Users/yanif/tmp/dbtquery"));
         * projDesc.setComment("DBToaster queries"); IProject p =
         * ws.getRoot().getProject("DBToaster Queries");
         * 
         * try { if ( !p.exists() ) p.create(projDesc, null); p.open(null);
         * 
         * IFile file = p.getFile(new Path("vwap.sql")); if ( !file.exists() )
         * file.create(new ByteArrayInputStream(new byte[0]), IResource.NONE,
         * null);
         * 
         * IDE.openEditor(getViewSite().getPage(), file, true);
         * 
         * } catch (PartInitException e) { e.printStackTrace(); } catch
         * (CoreException e1) { e1.printStackTrace(); }
         */

        Composite top = new Composite(parent, SWT.NONE);
        GridLayout layout = new GridLayout(2, false);
        layout.marginWidth = 0;
        layout.marginHeight = 0;
        top.setLayout(layout);

        Composite dswqComp = new Composite(top, SWT.NONE);
        GridLayout dswqLayout = new GridLayout();
        dswqLayout.marginWidth = 0;
        dswqLayout.marginHeight = 0;
        dswqComp.setLayout(dswqLayout);

        GridData dswqLD = new GridData(SWT.FILL, SWT.FILL, false, true);
        dswqLD.widthHint = 300;
        dswqComp.setLayoutData(dswqLD);

        datasetPnl = new DatasetPanel(dswqComp);
        historyPnl = new QueryHistoryPanel(dswqComp);

        queryTv = new QueryTextAndVis(top, SWT.BORDER | SWT.MULTI | SWT.WRAP,
                historyPnl);
        GridData queryLayoutData = new GridData(GridData.FILL, GridData.FILL,
                true, true);
        queryLayoutData.horizontalSpan = 1;
        queryTv.setLayoutData(queryLayoutData);

        Composite btBarComp = new Composite(top, SWT.NONE);
        GridLayout btBarLayout = new GridLayout(2, false);
        btBarLayout.marginWidth = 0;
        btBarLayout.marginHeight = 0;
        btBarComp.setLayout(btBarLayout);
        GridData btBarLD = new GridData(SWT.FILL, SWT.FILL, true, false);
        btBarLD.horizontalSpan = 2;
        btBarComp.setLayoutData(btBarLD);

        statusText = new Text(btBarComp, SWT.BORDER | SWT.WRAP);
        GridData gridData = new GridData(SWT.FILL, SWT.FILL, true, false);
        statusText.setBackground(statusText.getDisplay().getSystemColor(
                SWT.COLOR_TITLE_INACTIVE_BACKGROUND));
        statusText.setForeground(statusText.getDisplay().getSystemColor(
                SWT.COLOR_TITLE_INACTIVE_FOREGROUND));
        gridData.heightHint = statusText.getLineHeight() * 2;
        statusText.setLayoutData(gridData);
        statusText.setText("Welcome to DBToaster v0.1.");

        toastBtn = new Button(btBarComp, SWT.PUSH);
        toastBtn.setBackground(toastBtn.getDisplay().getSystemColor(
                SWT.COLOR_DARK_RED));
        toastBtn.setForeground(toastBtn.getDisplay().getSystemColor(
                SWT.COLOR_RED));
        toastBtn.setText("TOAST!");

        GridData toastGridData = new GridData(SWT.END, SWT.FILL, false, false);
        toastGridData.minimumWidth = 50;
        toastGridData.heightHint = statusText.getLineHeight() * 2;
        toastBtn.setLayoutData(toastGridData);

        toastBtn.addSelectionListener(new SelectionListener()
        {
            public void widgetSelected(SelectionEvent e)
            {
                System.out.println("Toast button listener");
                statusText.setText(queryTv.toastQuery());
            }

            public void widgetDefaultSelected(SelectionEvent e)
            {
            }
        });
    }

    public void setFocus()
    {
    }

}
