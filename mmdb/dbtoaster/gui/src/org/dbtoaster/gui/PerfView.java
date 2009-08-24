package org.dbtoaster.gui;

import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;

import org.dbtoaster.model.DBToasterWorkspace;
import org.dbtoaster.model.Query;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabFolder2Adapter;
import org.eclipse.swt.custom.CTabFolderEvent;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Text;
import org.eclipse.swt.widgets.Tree;
import org.eclipse.swt.widgets.TreeItem;
import org.eclipse.swt.widgets.ExpandBar;
import org.eclipse.swt.widgets.ExpandItem;
import org.eclipse.swt.widgets.Widget;
import org.eclipse.ui.IPerspectiveDescriptor;
import org.eclipse.ui.IPerspectiveListener;
import org.eclipse.ui.IWorkbenchPage;
import org.eclipse.ui.part.ViewPart;

public class PerfView extends ViewPart
{
    public static final String ID = "dbtoaster_gui.perfview";

    DBToasterWorkspace dbtWorkspace;

    String perspectiveId;
    int exQueriesHash;

    private ExecuteQueryPanel eqPanel;
    private LinkedHashMap<String,DBPerfPanel> databasePanels;
    private Text perfStatus;

    class ExecuteQueryPanel
    {
        final Tree eqTree;
        DBToasterWorkspace dbtWorkspace;
        
        Query currentRunningQuery;

        public ExecuteQueryPanel(Composite parent, DBToasterWorkspace dbtw)
        {
            dbtWorkspace = dbtw;

            Composite eqComp = new Composite(parent, SWT.BORDER);
            eqComp.setLayout(new GridLayout());
            GridData eqcLD = new GridData(SWT.FILL, SWT.FILL, false, true);
            eqcLD.widthHint = 150;
            eqComp.setLayoutData(eqcLD);
            
            Label eqLabel = new Label(eqComp, SWT.NONE);
            eqLabel.setText("Queries:");
            GridData eqlbLD = new GridData(SWT.FILL, SWT.FILL, true, false);
            eqLabel.setLayoutData(eqlbLD);

            eqTree = new Tree(eqComp, SWT.SINGLE | SWT.NONE);
            GridData eqtLD = new GridData(SWT.FILL, SWT.FILL, true, true);
            eqtLD.widthHint = 150;
            eqTree.setLayoutData(eqtLD);
            
            redraw();
        }
        
        final private int getIndexOfDB(String name, String[] dbNames) 
        {
        	int i = 0;
        	for (String s:dbNames) {
        		if(s.equals(name))
        			break;
        		i++;
        	}
        	return i;
        }
        
        final private Query getQuery(String name)
        {
            LinkedList<Query> execQueries = dbtWorkspace.getExecutableQueries();
            for(Query q: execQueries) {
            	if(q.getQueryName() .equals(name) ){
            		return q;
            	}
            }
            return null;
        }
        
        void addDatabases(final LinkedHashMap<String, DBPerfPanel> dbPanels,
            final int numdatabases, final String[] dbNames)
        {
        	final Integer[] databases = new Integer[numdatabases];
        	for (int i = 0; i < numdatabases; ++i) {
        		databases[i] = 0;
        	}
            // Add check boxes per database.
            for (Map.Entry<String, DBPerfPanel> e : dbPanels.entrySet())
            {
                final Button chooseDB = new Button(eqTree.getParent(), SWT.CHECK);
                final Control perfPanel = e.getValue();
                chooseDB.setText(e.getKey());
                chooseDB.setSelection(false);
                GridData chooseData = new GridData(SWT.FILL, SWT.FILL, false, false);
                chooseDB.setLayoutData(chooseData);
                
                chooseDB.addSelectionListener(new SelectionAdapter() {
                	public void widgetSelected(SelectionEvent e) {
                		if(chooseDB.getSelection() == true) {
                			databases[getIndexOfDB(chooseDB.getText(), dbNames)] = 1;
                			perfPanel.setEnabled(true);
                		}
                		else {
                			databases[getIndexOfDB(chooseDB.getText(), dbNames)] = 0;
                			perfPanel.setEnabled(false);
                		}
                	}
                });
            }
            
            // Add run button.
            final Button runButton = new Button(eqTree.getParent(), SWT.PUSH);
            runButton.setText("Run query");
            GridData runLD = new GridData(SWT.FILL, SWT.FILL, false, false);
            runLD.minimumWidth = 50;
            runButton.setLayoutData(runLD);

            // Add stop button.
            final Button stopButton = new Button(eqTree.getParent(), SWT.PUSH);
            stopButton.setText("Stop query");
            GridData stopLD = new GridData(SWT.FILL, SWT.FILL, false, false);
            stopLD.minimumWidth = 50;
            stopButton.setLayoutData(runLD);
            stopButton.setEnabled(false);
            

            SelectionAdapter runListener = new SelectionAdapter() {
            	public void widgetSelected(SelectionEvent e)
            	{
            		if (eqTree.getSelectionCount() == 1)
            		{
            			for (TreeItem i: eqTree.getSelection()) {
                			currentRunningQuery = getQuery(i.getText());
                		}
            			
            	//		for (int i= 0 ; i < numdatabases; i ++) {
                //			if(databases[i] == 1) {
                //				System.out.println(dbNames[i] + " is selected");
                //			}
            	//		}
            			
            			if ( currentRunningQuery != null ) {
                            perfStatus.setText("Running " +
                                currentRunningQuery.getQueryName());

                            currentRunningQuery.getExecutor().
            			        runComparison(currentRunningQuery, dbPanels, databases, dbNames);

                            runButton.setEnabled(false);
                            stopButton.setEnabled(true);
            			}
            		}
            	}
            };
            runButton.addSelectionListener(runListener);
            
            SelectionAdapter stopListener = new SelectionAdapter()
            {
                public void widgetSelected(SelectionEvent e)
                {
                    if ( currentRunningQuery != null ) {
                        currentRunningQuery.stopQuery();
                        perfStatus.setText("Stopped " +
                            currentRunningQuery.getQueryName());
                        stopButton.setEnabled(false);
                        runButton.setEnabled(true);
                    }
                }
            };
            stopButton.addSelectionListener(stopListener);

        }
        
        private void redraw()
        {
            eqTree.removeAll();

            LinkedList<Query> execQueries = dbtWorkspace.getExecutableQueries();
            exQueriesHash = execQueries.hashCode();
            System.out.println("Found " + execQueries.size() + " engine binaries.");

            for (Query q : dbtWorkspace.getExecutableQueries())
            {
                TreeItem qItem = new TreeItem(eqTree, SWT.NONE);
                qItem.setText(q.getQueryName());
            }
        }
    }
    
    public void createPartControl(Composite parent)
    {
        dbtWorkspace = DBToasterWorkspace.getWorkspace();

        final Composite top = new Composite(parent, SWT.NONE);
        GridLayout layout = new GridLayout(2,false);
        layout.marginWidth = 0;
        layout.marginHeight = 0;
        top.setLayout(layout);

        // Query executor panel.
        eqPanel = new ExecuteQueryPanel(top, dbtWorkspace);

        // Graph panel
        String[] dbNames = { "DBToaster", "Postgres", "HSQLDB" };//, "DBMS1", "SPE1" };
        int numDatabases = dbNames.length;
        databasePanels = new LinkedHashMap<String, DBPerfPanel>();

        final Composite dbcomp = new Composite(top, SWT.EMBEDDED);
        dbcomp.setLayoutData(new GridData(SWT.FILL, SWT.FILL, true, true));
        GridLayout dbcLayout = new GridLayout(2, false);
        dbcomp.setLayout(dbcLayout);

        Label yCpuLabel = new Label(dbcomp, SWT.VERTICAL);
        yCpuLabel.setText("ms/Tuple");
        GridData yCpuData = new GridData(SWT.FILL, SWT.CENTER, false, true);
        yCpuData.horizontalSpan = 1;
        yCpuData.verticalSpan = numDatabases;
        yCpuLabel.setLayoutData(yCpuData);
        
         for (int i = 0; i < numDatabases; ++i)
        {
        	final CTabFolder folder = new CTabFolder(dbcomp, SWT.BORDER);
        	GridData folderData = new GridData(SWT.FILL, SWT.TOP, true, true);
        	folderData.heightHint = 200;
        	folder.setLayoutData(folderData);
        	folder.setSimple(true);
        	folder.setUnselectedImageVisible(false);
        	folder.setUnselectedCloseVisible(false);
        	folder.setMinimizeVisible(false);

        	final CTabItem item = new CTabItem(folder, SWT.CLOSE);
        	item.setShowClose(false);
        	item.setText(dbNames[i] + " Performance");

        	DBPerfPanel perfPanel = new DBPerfPanel(folder, SWT.NO_TRIM);
        	databasePanels.put(dbNames[i], perfPanel);
            GridData frameData = new GridData(SWT.FILL, SWT.FILL, true, true);
            frameData.widthHint = 700;
            frameData.heightHint = 150;
            frameData.horizontalSpan = 1;
            perfPanel.setLayoutData(frameData);
        	item.setControl(perfPanel);
        	
        	folder.setSelection(0);
        	folder.showSelection();
        	dbcomp.pack();
        	 
        }

        Label timeLabel1 = new Label(dbcomp, SWT.BORDER);
        timeLabel1.setText("Time");
        GridData t1Data = new GridData(SWT.CENTER, SWT.FILL, true, false);
        t1Data.horizontalSpan=2;
        timeLabel1.setLayoutData(t1Data);

        perfStatus = new Text(top, SWT.BORDER | SWT.WRAP);
        GridData psData = new GridData(SWT.FILL, SWT.FILL, true, false);
        psData.horizontalSpan = 2;
        psData.heightHint = perfStatus.getLineHeight();
        perfStatus.setLayoutData(psData);

        Display d = perfStatus.getDisplay();
        perfStatus.setBackground(d
                .getSystemColor(SWT.COLOR_TITLE_INACTIVE_BACKGROUND));
        perfStatus.setForeground(d
                .getSystemColor(SWT.COLOR_TITLE_INACTIVE_FOREGROUND));

        perfStatus.setText("No databases running.");
        
        eqPanel.addDatabases(databasePanels, numDatabases, dbNames);
        
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
                    int queriesHash = dbtWorkspace.getExecutableQueries().hashCode();

                    System.out.println("EX hash comp: " + queriesHash + ", " + exQueriesHash);

                    if (perspective.getId().equals(perspectiveId)
                            && exQueriesHash != queriesHash)
                    {
                        System.out.println("Redrawing eqPanel");
                        eqPanel.redraw();
                    }
                }
            });

    }
    
    public void setFocus()
    {

    }

}
