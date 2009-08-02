package org.dbtoaster.gui;

import java.awt.BorderLayout;
import java.awt.Container;
import java.awt.Frame;
import java.awt.Panel;
import java.io.ByteArrayInputStream;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;

import javax.swing.JRootPane;
import javax.swing.JSplitPane;

import org.dbtoaster.model.CompilationTrace;
import org.dbtoaster.model.DBToasterWorkspace;
import org.dbtoaster.model.Query;
import org.eclipse.core.runtime.CoreException;
import org.eclipse.core.runtime.Path;
import org.eclipse.core.resources.IFile;
import org.eclipse.core.resources.IProject;
import org.eclipse.core.resources.IProjectDescription;
import org.eclipse.core.resources.IResource;
import org.eclipse.core.resources.IWorkspace;
import org.eclipse.core.resources.ResourcesPlugin;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.TreeItem;
import org.eclipse.ui.ide.IDE;
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

import prefuse.data.Node;
import prefuse.data.Tree;
import prefuse.data.io.TreeMLReader;

public class CodeEditorView extends ViewPart
{

    public static final String ID = "dbtoaster_gui.codeeditorview";

    private TreeVisPanel exprPanel;
    private TreeVisPanel mapPanel;
    private TreeVisPanel handlerPanel;

    TracePanel tracePanel;

    // Trace panel redrawing (should really change data model to use an
    // observable map)
    String perspectiveId;
    int ttHash;

    class TracePanel
    {
        static final String EVENTPATH = "path";
        static final String STAGETYPE = "type";
        static final String STAGENAME = "name";

        org.eclipse.swt.widgets.Tree traceTree;
        DBToasterWorkspace dbtWorkspace;

        public TracePanel(Composite parent)
        {
            dbtWorkspace = DBToasterWorkspace.getWorkspace();

            traceTree = new org.eclipse.swt.widgets.Tree(parent, SWT.SINGLE
                    | SWT.BORDER);
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
                            LinkedList<String> path = (LinkedList<String>) gp
                                    .getData(EVENTPATH);
                            String qn = (String) ggp.getText();

                            CompilationTrace traces = dbtWorkspace
                                    .getCompilationTrace(qn);

                            Tree stageTree = traces.getCompilationStage(path,
                                    tn, sn);

                            exprPanel.setData(stageTree, "mapexpression");
                        }
                    }
                }

                public void widgetDefaultSelected(SelectionEvent e)
                {
                }
            });

            redraw();
        }

        private void redraw()
        {
            traceTree.clearAll(true);

            System.out.println("DBToaster code viewer adding "
                    + dbtWorkspace.getCompiledQueries().size()
                    + " compiled queries.");

            for (Query q : dbtWorkspace.getCompiledQueries())
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

                    LinkedHashMap<String, LinkedList<String>> epStages = traces
                            .getEventPathStages(ep);

                    for (Map.Entry<String, LinkedList<String>> stageEntry : epStages
                            .entrySet())
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

    public void createPartControl(Composite parent)
    {

        IWorkspace ws = ResourcesPlugin.getWorkspace();
        IProjectDescription projDesc = ws
                .newProjectDescription("DBToaster Code");
        projDesc.setLocation(new Path("/Users/yanif/tmp/dbtcode"));
        projDesc.setComment("DBToaster compiled code");
        IProject p = ws.getRoot().getProject("DBToaster Code");

        try
        {
            if (!p.exists()) p.create(projDesc, null);
            p.open(null);

            IFile file = p.getFile(new Path("vwap.cc"));
            if (!file.exists())
                file.create(new ByteArrayInputStream(new byte[0]),
                        IResource.NONE, null);

            IDE.openEditor(getViewSite().getPage(), file, true);

        } catch (PartInitException e)
        {
            e.printStackTrace();
        } catch (CoreException e1)
        {
            e1.printStackTrace();
        }

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
        traceLD.heightHint = 400;
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
        tracePanel = new TracePanel(traceComp);

        // Initialize default trees.
        Tree exprTree = null;
        try
        {
            exprTree = (Tree) new TreeMLReader()
                    .readGraph("/Users/yanif/tmp/dbtcode/vwap.tml");
        } catch (Exception e)
        {
            e.printStackTrace();
            System.exit(1);
        }

        Tree mapTree = new Tree();
        mapTree.getNodeTable().addColumn("name", String.class);
        Node root = mapTree.addRoot();
        root.set("name", "q[pmin]");

        Node mp2 = mapTree.addChild(root);
        mp2.set("name", "m[p2]");

        Node sv0 = mapTree.addChild(mp2);
        sv0.set("name", "sv0");

        Node sv1 = mapTree.addChild(mp2);
        sv1.set("name", "sv1[p]");

        Tree handlerTree = new Tree();
        handlerTree.getNodeTable().addColumn("name", String.class);
        Node droot = handlerTree.addRoot();
        droot.set("name", "result");

        Node ib = handlerTree.addChild(droot);
        ib.set("name", "insert(bids)");

        Node db = handlerTree.addChild(droot);
        db.set("name", "delete(bids)");

        exprPanel = new TreeVisPanel(exprTree, "op", "Map expression",
                Constants.ORIENT_TOP_BOTTOM);
        mapPanel = new TreeVisPanel(mapTree, "name", "Data structures",
                Constants.ORIENT_LEFT_RIGHT);
        handlerPanel = new TreeVisPanel(handlerTree, "name",
                "Handler functions", Constants.ORIENT_LEFT_RIGHT);

        // Map and handler panel
        Frame hdFrame = SWT_AWT.new_Frame(hdComp);
        hdFrame.setLayout(new BorderLayout());
        Panel hdPanel = new Panel();
        hdFrame.add(hdPanel, BorderLayout.CENTER);

        JRootPane hdRootPane = new JRootPane();
        hdPanel.setLayout(new BorderLayout());
        hdPanel.add(hdRootPane, BorderLayout.CENTER);

        JSplitPane split1 = new JSplitPane(JSplitPane.VERTICAL_SPLIT);
        split1.setLeftComponent(mapPanel);
        split1.setRightComponent(handlerPanel);
        split1.setOneTouchExpandable(false);
        split1.setContinuousLayout(false);
        split1.setDividerSize(2);
        split1.setResizeWeight(0.5);

        Container hdContentPane = hdRootPane.getContentPane();
        hdContentPane.setLayout(new BorderLayout());
        hdContentPane.add(split1, BorderLayout.CENTER);

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
                    {
                    }

                    public void perspectiveActivated(IWorkbenchPage page,
                            IPerspectiveDescriptor perspective)
                    {
                        // TODO: check if this works after compiling new
                        // queries.
                        if (perspective.getId().equals(perspectiveId)
                                && tracePanel.traceTree.hashCode() != ttHash)
                        {
                            System.out.println("Redrawing tracePanel");
                            tracePanel.redraw();
                            ttHash = tracePanel.traceTree.hashCode();
                        }
                    }
                });
    }

    public void setFocus()
    {
    }
}