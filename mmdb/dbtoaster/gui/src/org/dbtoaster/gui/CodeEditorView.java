package org.dbtoaster.gui;

import java.awt.BorderLayout;
import java.awt.Container;
import java.awt.Frame;
import java.awt.Panel;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;

import javax.swing.JRootPane;

import org.dbtoaster.model.CompilationTrace;
import org.dbtoaster.model.DBToasterWorkspace;
import org.dbtoaster.model.DependencyGraph;
import org.dbtoaster.model.Query;
import org.eclipse.core.resources.IFile;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.TreeItem;
import org.eclipse.ui.ide.IDE;
import org.eclipse.ui.IEditorPart;
import org.eclipse.ui.IPerspectiveDescriptor;
import org.eclipse.ui.IPerspectiveListener;
import org.eclipse.ui.IWorkbenchPage;
import org.eclipse.ui.PartInitException;
import org.eclipse.ui.part.ViewPart;
import org.eclipse.swt.awt.SWT_AWT;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.SelectionListener;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;

import org.eclipse.swt.SWT;

import prefuse.Constants;

import prefuse.data.Graph;
import prefuse.data.Tree;

public class CodeEditorView extends ViewPart
{
    public static final String ID = "dbtoaster_gui.codeeditorview";

    DBToasterWorkspace dbtWorkspace;
    Query currentQuery;
    IEditorPart currentQueryEditor;

    private TreeVisPanel exprPanel;
    private GraphVisPanel mapPanel;

    TracePanel tracePanel;

    // Trace panel redrawing (should really change data model to use an
    // observable map)
    String perspectiveId;
    int ttQueriesHash;

    class TracePanel
    {
        static final String EVENTPATH = "path";
        static final String STAGETYPE = "type";
        static final String STAGENAME = "name";

        org.eclipse.swt.widgets.Tree traceTree;
        DBToasterWorkspace dbtWorkspace;

        public TracePanel(Composite parent, DBToasterWorkspace dbtw)
        {
            dbtWorkspace = dbtw;

            traceTree = new org.eclipse.swt.widgets.Tree(
                    parent, SWT.SINGLE | SWT.BORDER);
            GridData traceTreeLD = new GridData(SWT.FILL, SWT.FILL, true, true);
            traceTree.setLayoutData(traceTreeLD);
            traceTree.addSelectionListener(new SelectionListener()
            {
                public void widgetSelected(SelectionEvent e)
                {
                    TreeItem[] selected = traceTree.getSelection();
                    if (selected.length > 0)
                    {
                        TreeItem s = selected[0];
                        String sn = (String) s.getData(STAGENAME);

                        if (sn != null)
                        {
                            TreeItem p = s.getParentItem();
                            TreeItem gp = p.getParentItem();
                            TreeItem ggp = gp.getParentItem();

                            String tn = (String) p.getData(STAGETYPE);
                            LinkedList<String> path =
                                (LinkedList<String>) gp.getData(EVENTPATH);
                            String qn = (String) ggp.getText();
                            
                            setQuery(dbtWorkspace.getQuery(qn));
                            
                            CompilationTrace traces = currentQuery.getTrace();

                            Tree stageTree =
                                traces.getCompilationStage(path, tn, sn);

                            if ( stageTree != null )
                                exprPanel.setData(stageTree, "mapexpression");
                            
                            DependencyGraph dependencies = currentQuery.getDependencyGraph();
                            Graph depGraph = dependencies.getDependencyGraph();
                            
                            if ( depGraph != null ) {
                                System.out.println("Visualizing dependency graph.");
                                mapPanel.setData(depGraph, "name");
                            }
                        }
                    }
                }

                public void widgetDefaultSelected(SelectionEvent e) {}
            });

            redraw();
        }

        private void redraw()
        {
            traceTree.removeAll();

            LinkedList<Query> compiledQueries = dbtWorkspace.getCompiledQueries();
            ttQueriesHash = compiledQueries.hashCode();

            System.out.println("DBToaster code viewer adding "
                    + compiledQueries.size() + " compiled queries.");

            for (Query q : compiledQueries)
            {
                CompilationTrace traces = q.getTrace();
                if (traces == null)
                {
                    System.err.println("Found null trace in compiled query "
                            + q.getQueryName());
                    break;
                }

                TreeItem qItem = new TreeItem(traceTree, SWT.NONE);
                qItem.setText(q.getQueryName());

                Set<LinkedList<String>> eventPaths = traces.getEventPaths();
                for (LinkedList<String> ep : eventPaths)
                {
                    TreeItem epItem = new TreeItem(qItem, SWT.NONE);
                    String epName = "";
                    for (String e : ep)
                        epName += (epName.isEmpty() ? "" : ",") + e;
                    epItem.setText(epName);
                    epItem.setData("path", ep);

                    LinkedHashMap<String, LinkedList<String>> epStages =
                        traces.getEventPathStages(ep);

                    for (Map.Entry<String, LinkedList<String>> stageEntry :
                            epStages.entrySet())
                    {
                        String tn = stageEntry.getKey();
                        TreeItem typeItem = new TreeItem(epItem, SWT.NONE);
                        typeItem.setText(tn);
                        typeItem.setData("type", tn);

                        for (String sn : stageEntry.getValue())
                        {
                            TreeItem stageItem = new TreeItem(typeItem,
                                    SWT.NONE);
                            stageItem.setData("name", sn);
                            stageItem.setText(sn);
                        }
                    }
                }
            }
        }
    }

    void setQueryEditor(IFile codeFile)
    {
        // Close the existing editor
        if ( currentQueryEditor != null ) {
            getViewSite().getPage().closeEditor(currentQueryEditor, false);
        }

        // Open new editor.
        try {
            currentQueryEditor =
                IDE.openEditor(getViewSite().getPage(), codeFile);
        } catch (PartInitException e) {
            System.err.println("Failed to open query code " +
                    codeFile.getLocation().toOSString());
        }
    }
    
    void setQuery(Query q)
    {
        currentQuery = q;
        setQueryEditor(q.getCode());
    }

    public void createPartControl(Composite parent)
    {
        dbtWorkspace = DBToasterWorkspace.getWorkspace();
        currentQuery = null;
        currentQueryEditor = null;

        LinkedList<Query> compiledQueries = dbtWorkspace.getCompiledQueries();
        
        if ( !compiledQueries.isEmpty() )
            setQuery(compiledQueries.getFirst());

        Composite top = new Composite(parent, SWT.NONE);
        GridLayout topLayout = new GridLayout();
        topLayout.marginWidth = 0;
        topLayout.marginHeight = 0;
        topLayout.numColumns = 2;
        top.setLayout(topLayout);

        Composite traceComp = new Composite(top, SWT.NONE);
        traceComp.setLayout(new GridLayout());
        GridData traceLD = new GridData(SWT.FILL, SWT.FILL, false, false);
        traceLD.widthHint = 300;
        traceLD.heightHint = 250;
        traceComp.setLayoutData(traceLD);

        Composite compileComp = new Composite(top, SWT.EMBEDDED);
        GridData compileLD = new GridData(SWT.FILL, SWT.FILL, true, true);
        compileLD.verticalSpan = 2;
        compileComp.setLayoutData(compileLD);

        Composite hdComp = new Composite(top, SWT.EMBEDDED);
        GridData hdLD = new GridData(SWT.FILL, SWT.FILL, false, false);
        hdLD.widthHint = 300;
        hdComp.setLayoutData(hdLD);

        // Initialize traces tree widget.
        tracePanel = new TracePanel(traceComp, dbtWorkspace);

        // Initialize default trees.
        exprPanel = new TreeVisPanel(new Tree(), "mapexpression",
                "Map expression", Constants.ORIENT_TOP_BOTTOM);
        mapPanel = new GraphVisPanel(new Graph(), "name", "Data dependencies");

        // Map panel
        Frame hdFrame = SWT_AWT.new_Frame(hdComp);
        hdFrame.setLayout(new BorderLayout());
        Panel hdPanel = new Panel();
        hdFrame.add(hdPanel, BorderLayout.CENTER);

        JRootPane hdRootPane = new JRootPane();
        hdPanel.setLayout(new BorderLayout());
        hdPanel.add(hdRootPane, BorderLayout.CENTER);

        Container hdContentPane = hdRootPane.getContentPane();
        hdContentPane.setLayout(new BorderLayout());
        hdContentPane.add(mapPanel, BorderLayout.CENTER);

        hdFrame.pack();
        hdFrame.setVisible(true);

        // Compiled map expressions
        Frame compileFrame = SWT_AWT.new_Frame(compileComp);
        compileFrame.setLayout(new BorderLayout());
        Panel compilePanel = new Panel();
        compileFrame.add(compilePanel, BorderLayout.CENTER);

        JRootPane compileRootPane = new JRootPane();
        compilePanel.setLayout(new BorderLayout());
        compilePanel.add(compileRootPane, BorderLayout.CENTER);

        Container compileContentPane = compileRootPane.getContentPane();
        compileContentPane.setLayout(new BorderLayout());
        compileContentPane.add(exprPanel, BorderLayout.CENTER);

        compileFrame.pack();
        compileFrame.setVisible(true);

        perspectiveId = getViewSite().getPage().getPerspective().getId();

        getViewSite().getWorkbenchWindow().addPerspectiveListener(
            new IPerspectiveListener()
            {
                public void perspectiveChanged(IWorkbenchPage page,
                        IPerspectiveDescriptor perspective, String changeId)
                {}

                public void perspectiveActivated(IWorkbenchPage page,
                        IPerspectiveDescriptor perspective)
                {
                    int queriesHash = dbtWorkspace.getCompiledQueries().hashCode();

                    System.out.println("TT hash comp: " + queriesHash + ", " + ttQueriesHash);

                    if (perspective.getId().equals(perspectiveId)
                            && ttQueriesHash != queriesHash)
                    {
                        System.out.println("Redrawing tracePanel");
                        tracePanel.redraw();
                    }
                }
            });
    }

    public void setFocus() {}
}