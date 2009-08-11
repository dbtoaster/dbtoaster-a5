package org.dbtoaster.gui;

import java.awt.BorderLayout;
import java.awt.Container;
import java.awt.Frame;
import java.awt.Panel;
import java.util.LinkedList;

import javax.swing.JRootPane;

import org.dbtoaster.model.DBToasterWorkspace;
import org.dbtoaster.model.Query;
import org.eclipse.core.resources.IFile;
import org.eclipse.swt.SWT;
import org.eclipse.swt.awt.SWT_AWT;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.SelectionListener;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.TreeItem;
import org.eclipse.ui.IPerspectiveDescriptor;
import org.eclipse.ui.IPerspectiveListener;
import org.eclipse.ui.IWorkbenchPage;
import org.eclipse.ui.part.ViewPart;

import prefuse.Constants;
import prefuse.data.Tree;
import prefuse.data.io.DataIOException;
import prefuse.data.io.TreeMLReader;

public class CodeView extends ViewPart
{
    public static final String ID = "dbtoaster_gui.codeview";
    
    TreeVisPanel codeTreePanel;
    PseudocodePanel pseudoPanel;

    DBToasterWorkspace dbtWorkspace;
    Query currentQuery;
    
    String perspectiveId;
    int psQueriesHash;

    class PseudocodePanel
    {
        DBToasterWorkspace dbtWorkspace;
        org.eclipse.swt.widgets.Tree psTree;

        public PseudocodePanel(Composite parent, DBToasterWorkspace dbtw)
        {
            dbtWorkspace = dbtw;
            
            Composite psComp = new Composite(parent, SWT.NONE);
            psComp.setLayout(new GridLayout());
            GridData pscLD = new GridData(SWT.FILL, SWT.FILL, false, true);
            pscLD.widthHint = 125;
            psComp.setLayoutData(pscLD);

            Label psLabel = new Label(psComp, SWT.NONE);
            psLabel.setText("Profiled queries:");
            GridData pslbLD = new GridData(SWT.FILL, SWT.FILL, true, false);
            pslbLD.widthHint = 125;
            psLabel.setLayoutData(pslbLD);

            psTree = new org.eclipse.swt.widgets.Tree(
                    psComp, SWT.SINGLE | SWT.BORDER);
            GridData psLD = new GridData(SWT.FILL, SWT.FILL, false, true);
            psLD.widthHint = 125;
            psTree.setLayoutData(psLD);
            psTree.addSelectionListener(
                new SelectionListener()
                {
                    public void widgetSelected(SelectionEvent e)
                    {
                        TreeItem[] selected = psTree.getSelection();
                        if (selected.length > 0)
                        {
                            TreeItem s = selected[0];
                            String queryName = s.getText();

                            currentQuery = dbtWorkspace.getQuery(queryName);
                            IFile psFile = currentQuery.getPseudocode();
                            try
                            {
                                Tree newCode = (Tree) new TreeMLReader().
                                    readGraph(psFile.getLocation().toFile());
                                
                                codeTreePanel.setData(newCode, "statement");

                            } catch (DataIOException e1) {
                                String msg = "Failed to read TreeML for " +
                                    psFile.getLocation().toOSString();
                                System.err.println(msg);
                                e1.printStackTrace();
                            }
                        }
                    }
                    
                    public void widgetDefaultSelected(SelectionEvent e) {}
                });
            redraw();
        }
        
        private void redraw()
        {
            psTree.removeAll();
            
            LinkedList<Query> psQueries = dbtWorkspace.getQueriesWithPseudocode();
            psQueriesHash = psQueries.hashCode();

            for (Query q : psQueries)
            {
                TreeItem psItem = new TreeItem(psTree, SWT.NONE);
                psItem.setText(q.getQueryName());
            }
        }
    }

    public void createPartControl(Composite parent)
    {
        dbtWorkspace = DBToasterWorkspace.getWorkspace();

        Composite top = new Composite(parent, SWT.NONE);
        GridLayout topLayout = new GridLayout(2, false);
        topLayout.marginWidth = 0;
        topLayout.marginHeight = 0;
        top.setLayout(topLayout);
        
        pseudoPanel = new PseudocodePanel(top, dbtWorkspace);

        Composite cpComp = new Composite(top, SWT.EMBEDDED);
        GridLayout cpLayout = new GridLayout();
        cpLayout.marginWidth = 0;
        cpLayout.marginHeight = 0;
        cpComp.setLayout(cpLayout);
        GridData cpLD = new GridData(SWT.FILL, SWT.FILL, true, true);
        cpComp.setLayoutData(cpLD);

        Frame codeFrame = SWT_AWT.new_Frame(cpComp);
        codeFrame.setLayout(new BorderLayout());
        Panel codePanel = new Panel();
        codeFrame.add(codePanel, BorderLayout.CENTER);

        JRootPane codeRootPane = new JRootPane();
        codePanel.setLayout(new BorderLayout());
        codePanel.add(codeRootPane, BorderLayout.CENTER);

        codeTreePanel = new TreeVisPanel(
            new Tree(), "statement", "QP pseudocode",
            Constants.ORIENT_TOP_BOTTOM, TreeVisPanel.DEFAULT_LEVELS, 2.5);

        Container codeContentPane = codeRootPane.getContentPane();
        codeContentPane.setLayout(new BorderLayout());
        codeContentPane.add(codeTreePanel, BorderLayout.CENTER);

        codeFrame.pack();
        codeFrame.setVisible(true);
        
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
                    int queriesHash = dbtWorkspace.getQueriesWithPseudocode().hashCode();

                    if (perspective.getId().equals(perspectiveId)
                            && psQueriesHash != queriesHash)
                    {
                        pseudoPanel.redraw();
                    }
                }
            });

    }

    public void setFocus() {}
}
